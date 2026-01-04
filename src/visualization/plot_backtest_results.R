# plot_backtest_results.R
# Visualization Module: Backtest Results
#
# Input:  data/processed/century_backtest_results.csv
# Output: reports/figures/century_*.png
#
# Usage:
#   source("src/visualization/plot_backtest_results.R")
#   plots <- plot_backtest_results(results_df)

# ------------------------------------------------------------------------------
# SETUP
# ------------------------------------------------------------------------------

suppressPackageStartupMessages({
    library(ggplot2)
    library(dplyr)
    library(readr)
    library(scales)
    library(patchwork)
    library(tidyr)
})

log_info <- function(...) message(sprintf("[%s] INFO  %s", Sys.time(), sprintf(...)))

# ------------------------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------------------------

FIGURES_DIR <- "reports/figures"

COLORS <- list(
    l1_norm = "#2E86AB",
    l2_norm = "#7B2CBF",
    tau_warning = "#E63946",
    tau_normal = "#2A9D8F",
    drawdown = "#F77F00",
    crash_line = "#D62828",
    correction_line = "#F4A261",
    loops = "#264653"
)

CRASH_DATES <- list(
    "1987" = as.Date("1987-10-19"),
    "2000" = as.Date("2000-03-10"),
    "2008" = as.Date("2008-09-15"),
    "2020" = as.Date("2020-03-16"),
    "2022" = as.Date("2022-01-03")
)

# ------------------------------------------------------------------------------
# PLOT FUNCTIONS
# ------------------------------------------------------------------------------

#' L^1 and L^2 Norm Time Series
#' @export
plot_norms_timeseries <- function(results_df, show_crashes = TRUE) {
    
    # Prepare data for both norms
    plot_data <- results_df %>%
        select(Date, L1_Norm, L2_Norm) %>%
        pivot_longer(cols = c(L1_Norm, L2_Norm), names_to = "Norm", values_to = "Value") %>%
        mutate(Norm = ifelse(Norm == "L1_Norm", "L¹ Norm", "L² Norm"))
    
    p <- ggplot(plot_data, aes(x = Date, y = Value, color = Norm)) +
        geom_line(linewidth = 0.3, alpha = 0.6) +
        geom_smooth(method = "loess", span = 0.05, linewidth = 0.8, se = FALSE) +
        scale_color_manual(values = c("L¹ Norm" = COLORS$l1_norm, "L² Norm" = COLORS$l2_norm)) +
        scale_y_continuous(labels = scales::scientific) +
        facet_wrap(~ Norm, scales = "free_y", ncol = 1) +
        labs(title = "Persistence Landscape Norms (H₁ Dimension)", x = NULL, y = "Norm Value") +
        theme_minimal(base_size = 11) +
        theme(
            plot.title = element_text(face = "bold", size = 12),
            panel.grid.minor = element_blank(),
            legend.position = "none",
            strip.text = element_text(face = "bold")
        )
    
    if (show_crashes) {
        for (d in CRASH_DATES) {
            if (d >= min(results_df$Date) && d <= max(results_df$Date)) {
                p <- p + geom_vline(xintercept = d, linetype = "dashed", 
                                    color = COLORS$crash_line, alpha = 0.4)
            }
        }
    }
    
    return(p)
}

#' Number of H1 loops per window (diagnostic plot)
#' @export
plot_n_loops <- function(results_df, show_crashes = TRUE) {
    
    p <- ggplot(results_df, aes(x = Date, y = N_Loops)) +
        geom_line(color = COLORS$loops, linewidth = 0.3, alpha = 0.7) +
        geom_smooth(method = "loess", span = 0.05, 
                    color = COLORS$loops, linewidth = 0.8, se = FALSE) +
        labs(title = "H₁ Loops Detected per Window", x = NULL, y = "Number of Loops") +
        theme_minimal(base_size = 11) +
        theme(
            plot.title = element_text(face = "bold", size = 12),
            panel.grid.minor = element_blank()
        )
    
    if (show_crashes) {
        for (d in CRASH_DATES) {
            if (d >= min(results_df$Date) && d <= max(results_df$Date)) {
                p <- p + geom_vline(xintercept = d, linetype = "dashed", 
                                    color = COLORS$crash_line, alpha = 0.4)
            }
        }
    }
    
    return(p)
}

#' Mann-Kendall Tau Time Series (L1 and L2)
#' @export
plot_tau_timeseries <- function(results_df, tau_threshold = 0.30) {
    
    # Handle both old and new column names
    if ("L1_Tau" %in% names(results_df)) {
        plot_data <- results_df %>%
            select(Date, L1_Tau, L2_Tau) %>%
            filter(!is.na(L1_Tau) | !is.na(L2_Tau)) %>%
            pivot_longer(cols = c(L1_Tau, L2_Tau), names_to = "Source", values_to = "Tau") %>%
            mutate(
                Source = ifelse(Source == "L1_Tau", "L¹ τ", "L² τ"),
                Above = Tau > tau_threshold
            )
    } else if ("Tau" %in% names(results_df)) {
        # Backwards compatibility
        plot_data <- results_df %>%
            filter(!is.na(Tau)) %>%
            mutate(Source = "τ", Above = Tau > tau_threshold)
    } else {
        stop("No Tau column found in results_df")
    }
    
    p <- ggplot(plot_data, aes(x = Date, y = Tau)) +
        geom_hline(yintercept = tau_threshold, linetype = "dashed", 
                   color = COLORS$tau_warning, linewidth = 0.6) +
        geom_hline(yintercept = 0, color = "gray50", linewidth = 0.3) +
        geom_line(aes(color = Above), linewidth = 0.4, alpha = 0.8) +
        scale_color_manual(
            values = c("FALSE" = COLORS$tau_normal, "TRUE" = COLORS$tau_warning),
            labels = c("Normal", "Warning"),
            name = NULL
        ) +
        labs(title = "Mann-Kendall τ (250-day lookback)", x = NULL, y = "τ") +
        theme_minimal(base_size = 11) +
        theme(
            plot.title = element_text(face = "bold", size = 12),
            panel.grid.minor = element_blank(),
            legend.position = "bottom",
            strip.text = element_text(face = "bold")
        )
    
    # Facet if we have both L1 and L2
    if ("L1_Tau" %in% names(results_df)) {
        p <- p + facet_wrap(~ Source, ncol = 1)
    }
    
    return(p)
}

#' Forward Drawdown Time Series
#' @export
plot_forward_drawdown <- function(results_df, 
                                   crash_threshold = 0.20,
                                   correction_threshold = 0.10) {
    
    plot_data <- results_df %>% filter(!is.na(Forward_DD))
    
    p <- ggplot(plot_data, aes(x = Date, y = Forward_DD * 100)) +
        geom_hline(yintercept = crash_threshold * 100, linetype = "dashed", 
                   color = COLORS$crash_line, linewidth = 0.6) +
        geom_hline(yintercept = correction_threshold * 100, linetype = "dotted", 
                   color = COLORS$correction_line, linewidth = 0.5) +
        geom_line(color = COLORS$drawdown, linewidth = 0.3, alpha = 0.7) +
        geom_smooth(method = "loess", span = 0.05,
                    color = COLORS$drawdown, linewidth = 0.8, se = FALSE) +
        labs(title = "Forward 250-Day Max Drawdown", x = NULL, y = "Drawdown (%)") +
        theme_minimal(base_size = 11) +
        theme(
            plot.title = element_text(face = "bold", size = 12),
            panel.grid.minor = element_blank()
        )
    
    return(p)
}

#' Classification Summary Bar Chart
#' @export
plot_classification_summary <- function(results_df) {
    
    class_counts <- results_df %>%
        filter(!is.na(Classification)) %>%
        count(Classification) %>%
        mutate(
            Classification = factor(Classification, levels = c(
                "TRUE_POSITIVE", "CORRECTION", "FALSE_POSITIVE", 
                "FALSE_NEGATIVE", "TRUE_NEGATIVE"
            )),
            Pct = n / sum(n) * 100
        )
    
    p <- ggplot(class_counts, aes(x = Classification, y = n, fill = Classification)) +
        geom_col(alpha = 0.9) +
        geom_text(aes(label = sprintf("%d\n%.0f%%", n, Pct)), 
                  vjust = -0.2, size = 3) +
        scale_fill_manual(values = c(
            "TRUE_POSITIVE" = "#2A9D8F",
            "CORRECTION" = "#E9C46A",
            "FALSE_POSITIVE" = "#E76F51",
            "FALSE_NEGATIVE" = "#9B2335",
            "TRUE_NEGATIVE" = "#264653"
        )) +
        scale_y_continuous(expand = expansion(mult = c(0, 0.15))) +
        labs(title = "Classification Summary", x = NULL, y = "Days") +
        theme_minimal(base_size = 11) +
        theme(
            plot.title = element_text(face = "bold", size = 12),
            legend.position = "none",
            axis.text.x = element_text(angle = 45, hjust = 1, size = 9)
        )
    
    return(p)
}

#' Annual Warning Heatmap
#' @export
plot_annual_heatmap <- function(results_df) {
    
    plot_data <- results_df %>%
        filter(!is.na(Signal_State)) %>%
        mutate(
            Year = as.integer(format(Date, "%Y")),
            Month = as.integer(format(Date, "%m"))
        ) %>%
        group_by(Year, Month) %>%
        summarize(Warning_Pct = mean(Signal_State == "Warning") * 100, .groups = "drop")
    
    p <- ggplot(plot_data, aes(x = Month, y = Year, fill = Warning_Pct)) +
        geom_tile(color = "white", linewidth = 0.1) +
        scale_fill_gradient2(
            low = "#2A9D8F", mid = "#F4A261", high = "#E63946",
            midpoint = 50, name = "Warning %", limits = c(0, 100)
        ) +
        scale_x_continuous(breaks = 1:12, labels = month.abb) +
        labs(title = "Warning Signal by Month/Year", x = NULL, y = NULL) +
        theme_minimal(base_size = 11) +
        theme(
            plot.title = element_text(face = "bold", size = 12),
            panel.grid = element_blank()
        )
    
    return(p)
}

#' Combined Dashboard
#' @export
plot_backtest_dashboard <- function(results_df) {
    
    p1 <- plot_norms_timeseries(results_df)
    p2 <- plot_n_loops(results_df)
    p3 <- plot_tau_timeseries(results_df)
    p4 <- plot_forward_drawdown(results_df)
    p5 <- plot_classification_summary(results_df)
    
    # Left column: time series
    left_col <- p1 / p2 / p4
    
    # Right column: tau and classification
    right_col <- p3 / p5
    
    dashboard <- left_col | right_col +
        plot_layout(widths = c(2, 1))
    
    dashboard <- dashboard +
        plot_annotation(
            title = "TDA Crash Prediction Backtest",
            theme = theme(plot.title = element_text(size = 14, face = "bold"))
        )
    
    return(dashboard)
}

# ------------------------------------------------------------------------------
# MAIN FUNCTION
# ------------------------------------------------------------------------------

#' Generate all backtest visualizations
#' @export
plot_backtest_results <- function(results_df,
                                   save_plots = TRUE,
                                   output_dir = FIGURES_DIR) {
    
    log_info("=== Generating Visualizations ===")
    
    if (save_plots && !dir.exists(output_dir)) {
        dir.create(output_dir, recursive = TRUE)
    }
    
    plots <- list()
    
    log_info("Creating plots...")
    plots$norms <- plot_norms_timeseries(results_df)
    plots$n_loops <- plot_n_loops(results_df)
    plots$tau <- plot_tau_timeseries(results_df)
    plots$drawdown <- plot_forward_drawdown(results_df)
    plots$classification <- plot_classification_summary(results_df)
    plots$heatmap <- plot_annual_heatmap(results_df)
    plots$dashboard <- plot_backtest_dashboard(results_df)
    
    if (save_plots) {
        log_info("Saving to %s/", output_dir)
        
        ggsave(file.path(output_dir, "century_norms.png"), 
               plots$norms, width = 14, height = 6, dpi = 150)
        
        ggsave(file.path(output_dir, "century_n_loops.png"), 
               plots$n_loops, width = 14, height = 4, dpi = 150)
        
        ggsave(file.path(output_dir, "century_tau.png"), 
               plots$tau, width = 14, height = 6, dpi = 150)
        
        ggsave(file.path(output_dir, "century_drawdown.png"), 
               plots$drawdown, width = 14, height = 4, dpi = 150)
        
        ggsave(file.path(output_dir, "century_classification.png"), 
               plots$classification, width = 7, height = 5, dpi = 150)
        
        ggsave(file.path(output_dir, "century_heatmap.png"), 
               plots$heatmap, width = 10, height = 8, dpi = 150)
        
        ggsave(file.path(output_dir, "century_dashboard.png"), 
               plots$dashboard, width = 18, height = 14, dpi = 150)
        
        log_info("Done.")
    }
    
    return(plots)
}

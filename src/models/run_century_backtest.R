# run_century_backtest.R
# Orchestration Script: Grand Historical Backtest (1957–2025)
#
# This script orchestrates the full TDA-based crash prediction backtest by
# sourcing modular components from across the src/ directory:
#
#   1. src/data/get_century_data.R        - Data acquisition from Yahoo Finance
#   2. src/features/compute_tda_signals.R - TDA feature engineering
#   3. src/features/classify_signals.R    - Signal classification
#   4. src/visualization/plot_backtest_results.R - Visualization
#
# Methodology: Gidea & Katz (2017) - Topological Data Analysis of Financial Time Series
#
# Output:
#   - data/interim/century_aligned_prices.csv
#   - data/interim/century_log_returns.csv
#   - data/processed/century_tda_signals.csv
#   - data/processed/century_backtest_results.csv
#   - reports/figures/century_*.png
#
# Usage: source("src/models/run_century_backtest.R")

# ------------------------------------------------------------------------------
# SETUP
# ------------------------------------------------------------------------------

# Ensure we're running from project root
if (!file.exists("README.md")) {
    stop("Please run this script from the project root directory")
}

# Source modular components
source("src/data/get_century_data.R")
source("src/features/compute_tda_signals.R")
source("src/features/classify_signals.R")
source("src/visualization/plot_backtest_results.R")

# Logging
log_info  <- function(...) message(sprintf("[%s] INFO  %s", Sys.time(), sprintf(...)))

# ------------------------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------------------------

# Pipeline control flags
CONFIG <- list(
    # Data acquisition
    use_cached_data = FALSE,      # If TRUE, skip download and load from interim
    
    # TDA parameters
    topology_window = 50,         # Sliding window for point cloud
    max_scale = 0.20,             # Rips filtration radius (3-5x point cloud diameter)
    
    # Signal parameters
    rolling_sd_window = 50,       # Rolling SD window
    mk_lookback = 250,            # Mann-Kendall lookback
    tau_threshold = 0.30,         # Warning threshold
    
    # Classification parameters
    forward_window = 250,         # Look-forward for drawdown
    crash_threshold = 0.20,       # >= 20% = crash
    correction_threshold = 0.10,  # >= 10% = correction
    
    # Output control
    save_plots = TRUE,
    save_files = TRUE
)

# ------------------------------------------------------------------------------
# MAIN PIPELINE
# ------------------------------------------------------------------------------

run_century_backtest <- function(config = CONFIG) {
    
    log_info(paste(rep("=", 70), collapse = ""))
    log_info("GRAND HISTORICAL BACKTEST: TDA Crash Prediction (1957-2025)")
    log_info("Methodology: Gidea & Katz (2017)")
    log_info(paste(rep("=", 70), collapse = ""))
    
    pipeline_start <- Sys.time()
    
    # =========================================================================
    # STAGE 1: DATA ACQUISITION
    # =========================================================================
    log_info("\n" %>% paste0(rep("=", 50) %>% paste(collapse = "")))
    log_info("STAGE 1: DATA ACQUISITION")
    log_info(rep("=", 50) %>% paste(collapse = ""))
    
    if (config$use_cached_data) {
        log_info("Loading cached data from interim...")
        data <- load_century_data()
        
        if (is.null(data)) {
            log_info("Cache not found. Fetching fresh data...")
            data <- get_century_data(save_files = config$save_files)
        }
    } else {
        data <- get_century_data(save_files = config$save_files)
    }
    
    aligned_prices <- data$aligned_prices
    log_returns <- data$log_returns
    
    log_info("Data ready: %d trading days (%s to %s)",
             nrow(log_returns), 
             min(log_returns$date), 
             max(log_returns$date))
    
    # =========================================================================
    # STAGE 2: TDA FEATURE ENGINEERING
    # =========================================================================
    log_info("\n" %>% paste0(rep("=", 50) %>% paste(collapse = "")))
    log_info("STAGE 2: TDA FEATURE ENGINEERING")
    log_info(rep("=", 50) %>% paste(collapse = ""))
    
    signals_df <- compute_tda_signals(
        returns_df = log_returns,
        return_cols = c("DJI", "DJT", "SPX"),
        window_size = config$topology_window,
        max_scale = config$max_scale,
        sd_window = config$rolling_sd_window,
        mk_lookback = config$mk_lookback,
        tau_threshold = config$tau_threshold,
        save_file = config$save_files
    )
    
    log_info("Signals computed: %d observations", nrow(signals_df))
    
    # =========================================================================
    # STAGE 3: SIGNAL CLASSIFICATION
    # =========================================================================
    log_info("\n" %>% paste0(rep("=", 50) %>% paste(collapse = "")))
    log_info("STAGE 3: SIGNAL CLASSIFICATION")
    log_info(rep("=", 50) %>% paste(collapse = ""))
    
    results_df <- classify_signals(
        signals_df = signals_df,
        prices_df = aligned_prices,
        reference_col = "SPX",
        forward_days = config$forward_window,
        crash_threshold = config$crash_threshold,
        correction_threshold = config$correction_threshold,
        save_file = config$save_files
    )
    
    # Get metrics from attribute
    metrics <- attr(results_df, "metrics")
    
    # =========================================================================
    # STAGE 4: CRASH PERIOD ANALYSIS
    # =========================================================================
    log_info("\n" %>% paste0(rep("=", 50) %>% paste(collapse = "")))
    log_info("STAGE 4: CRASH PERIOD ANALYSIS")
    log_info(rep("=", 50) %>% paste(collapse = ""))
    
    crash_analysis <- analyze_crash_periods(results_df)
    
    # =========================================================================
    # STAGE 5: VISUALIZATION
    # =========================================================================
    log_info("\n" %>% paste0(rep("=", 50) %>% paste(collapse = "")))
    log_info("STAGE 5: VISUALIZATION")
    log_info(rep("=", 50) %>% paste(collapse = ""))
    
    if (config$save_plots) {
        plots <- plot_backtest_results(
            results_df = results_df,
            save_plots = TRUE
        )
    } else {
        log_info("Skipping plot generation (save_plots = FALSE)")
        plots <- NULL
    }
    
    # =========================================================================
    # SUMMARY
    # =========================================================================
    pipeline_elapsed <- as.numeric(difftime(Sys.time(), pipeline_start, units = "mins"))
    
    log_info("\n" %>% paste0(rep("=", 70) %>% paste(collapse = "")))
    log_info("BACKTEST COMPLETE")
    log_info(rep("=", 70) %>% paste(collapse = ""))
    
    log_info("\nPipeline Summary:")
    log_info("  Total runtime: %.1f minutes", pipeline_elapsed)
    log_info("  Date range: %s to %s", min(results_df$Date), max(results_df$Date))
    log_info("  Total observations: %d", nrow(results_df))
    
    log_info("\nSignal Performance:")
    log_info("  True Positives:  %d", metrics$true_positives)
    log_info("  Corrections:     %d", metrics$corrections)
    log_info("  False Positives: %d", metrics$false_positives)
    log_info("  False Negatives: %d", metrics$false_negatives)
    log_info("  True Negatives:  %d", metrics$true_negatives)
    log_info("")
    log_info("  Precision (strict):       %.1f%%", metrics$precision_strict * 100)
    log_info("  Precision (incl. corr.):  %.1f%%", metrics$precision_incl_corrections * 100)
    log_info("  Recall:                   %.1f%%", metrics$recall * 100)
    
    log_info("\nOutput Files:")
    log_info("  data/interim/century_aligned_prices.csv")
    log_info("  data/interim/century_log_returns.csv")
    log_info("  data/processed/century_tda_signals.csv")
    log_info("  data/processed/century_backtest_results.csv")
    if (config$save_plots) {
        log_info("  reports/figures/century_*.png")
    }
    
    log_info(rep("=", 70) %>% paste(collapse = ""))
    
    # Return results for programmatic access
    invisible(list(
        config = config,
        aligned_prices = aligned_prices,
        log_returns = log_returns,
        signals = signals_df,
        results = results_df,
        metrics = metrics,
        crash_analysis = crash_analysis,
        plots = plots
    ))
}

# ------------------------------------------------------------------------------
# EXECUTION
# ------------------------------------------------------------------------------

backtest_results <- run_century_backtest(CONFIG)

message("\n", paste(rep("=", 70), collapse = ""))
message("✓ Century backtest completed successfully")
message("")
message("  Final results: data/processed/century_backtest_results.csv")
message("  Visualizations: reports/figures/century_*.png")
message("")
message("  Pipeline Structure:")
message("    src/data/get_century_data.R        → Data acquisition")
message("    src/features/compute_tda_signals.R → TDA feature engineering")
message("    src/features/classify_signals.R    → Signal classification")
message("    src/visualization/plot_backtest_results.R → Visualization")
message("")
message("  Methodology: Gidea & Katz (2017)")
message(paste(rep("=", 70), collapse = ""))

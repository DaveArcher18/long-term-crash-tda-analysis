# classify_signals.R
# Classification Module: Forward Drawdown Computation and Signal Classification
#
# Evaluates TDA signal quality by computing forward-looking maximum drawdowns
# and classifying each day's signal as True Positive, False Positive, etc.
#
# Classification Logic (Ground Truth):
#   - TRUE POSITIVE:   Signal ON  + Forward Drawdown >= 20%
#   - CORRECTION:      Signal ON  + Forward Drawdown 10-20%
#   - FALSE POSITIVE:  Signal ON  + Forward Drawdown < 10%
#   - FALSE NEGATIVE:  Signal OFF + Forward Drawdown >= 20%
#   - TRUE NEGATIVE:   Signal OFF + Forward Drawdown < 20%
#
# Input:  TDA signals + aligned prices
# Output: Classified results with forward drawdowns
#
# Usage:
#   source("src/features/classify_signals.R")
#   results <- classify_signals(signals_df, prices_df)

# ------------------------------------------------------------------------------
# SETUP
# ------------------------------------------------------------------------------

suppressPackageStartupMessages({
    library(dplyr)
    library(readr)
})

# Logging utilities
log_info  <- function(...) message(sprintf("[%s] INFO  %s", Sys.time(), sprintf(...)))
log_warn  <- function(...) warning(sprintf("[%s] WARN  %s", Sys.time(), sprintf(...)), call. = FALSE)

# ------------------------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------------------------

# Classification parameters
FORWARD_WINDOW <- 250           # Look-forward period for drawdown (trading days)
CRASH_THRESHOLD <- 0.20         # >= 20% drawdown = Crash
CORRECTION_THRESHOLD <- 0.10    # >= 10% drawdown = Correction

# Reference index for drawdown calculation
REFERENCE_INDEX <- "SPX"

# Output paths
PROCESSED_DIR <- "data/processed"
RESULTS_FILE <- file.path(PROCESSED_DIR, "century_backtest_results.csv")

# ------------------------------------------------------------------------------
# CLASSIFICATION FUNCTIONS
# ------------------------------------------------------------------------------

#' Compute forward maximum drawdown from price series
#'
#' For each date t, computes the maximum drawdown in the next `forward_days` days.
#' Drawdown is measured as (peak - trough) / peak, where peak is the current price.
#'
#' @param prices Numeric vector of prices
#' @param forward_days Look-forward window (default: 250)
#' @return Numeric vector of forward drawdowns (proportion, e.g., 0.15 = 15%)
#' @export
compute_forward_drawdowns <- function(prices, forward_days = FORWARD_WINDOW) {
    n <- length(prices)
    drawdowns <- rep(NA_real_, n)
    
    for (i in 1:(n - 1)) {
        end_idx <- min(i + forward_days, n)
        
        if (end_idx > i) {
            forward_prices <- prices[(i + 1):end_idx]
            peak <- prices[i]  # Current price is the reference
            max_trough <- min(forward_prices)
            
            drawdowns[i] <- (peak - max_trough) / peak
        }
    }
    
    return(drawdowns)
}

#' Classify signal outcomes based on forward drawdown
#'
#' Maps each day's signal state and forward drawdown to a classification:
#'   - TRUE_POSITIVE:  Signal ON + Crash
#'   - CORRECTION:     Signal ON + Correction (10-20% DD)
#'   - FALSE_POSITIVE: Signal ON + No significant drawdown
#'   - FALSE_NEGATIVE: Signal OFF + Crash
#'   - TRUE_NEGATIVE:  Signal OFF + No crash
#'
#' @param signal_state Character vector ("Warning" or "Normal")
#' @param forward_dd Numeric vector of forward drawdowns
#' @param crash_threshold Threshold for crash (default: 0.20)
#' @param correction_threshold Threshold for correction (default: 0.10)
#' @return Character vector of classifications
#' @export
classify_outcomes <- function(signal_state, 
                               forward_dd, 
                               crash_threshold = CRASH_THRESHOLD,
                               correction_threshold = CORRECTION_THRESHOLD) {
    
    n <- length(signal_state)
    classification <- character(n)
    
    for (i in seq_len(n)) {
        if (is.na(signal_state[i]) || is.na(forward_dd[i])) {
            classification[i] <- NA_character_
        } else if (signal_state[i] == "Warning") {
            # Signal is ON
            if (forward_dd[i] >= crash_threshold) {
                classification[i] <- "TRUE_POSITIVE"
            } else if (forward_dd[i] >= correction_threshold) {
                classification[i] <- "CORRECTION"
            } else {
                classification[i] <- "FALSE_POSITIVE"
            }
        } else {
            # Signal is OFF (Normal)
            if (forward_dd[i] >= crash_threshold) {
                classification[i] <- "FALSE_NEGATIVE"
            } else {
                classification[i] <- "TRUE_NEGATIVE"
            }
        }
    }
    
    return(classification)
}

#' Compute classification metrics (Precision, Recall, etc.)
#'
#' @param classification Character vector of classifications
#' @return Named list of performance metrics
#' @export
compute_classification_metrics <- function(classification) {
    tp <- sum(classification == "TRUE_POSITIVE", na.rm = TRUE)
    fp <- sum(classification == "FALSE_POSITIVE", na.rm = TRUE)
    fn <- sum(classification == "FALSE_NEGATIVE", na.rm = TRUE)
    tn <- sum(classification == "TRUE_NEGATIVE", na.rm = TRUE)
    corr <- sum(classification == "CORRECTION", na.rm = TRUE)
    
    # Precision: Of all warnings, how many were actual crashes?
    precision_strict <- if ((tp + fp + corr) > 0) tp / (tp + fp + corr) else NA
    precision_incl_corr <- if ((tp + fp + corr) > 0) (tp + corr) / (tp + fp + corr) else NA
    
    # Recall: Of all crashes, how many did we catch?
    recall <- if ((tp + fn) > 0) tp / (tp + fn) else NA
    
    # F1 Score
    f1 <- if (!is.na(precision_strict) && !is.na(recall) && (precision_strict + recall) > 0) {
        2 * precision_strict * recall / (precision_strict + recall)
    } else {
        NA
    }
    
    return(list(
        true_positives = tp,
        false_positives = fp,
        false_negatives = fn,
        true_negatives = tn,
        corrections = corr,
        precision_strict = precision_strict,
        precision_incl_corrections = precision_incl_corr,
        recall = recall,
        f1_score = f1
    ))
}

# ------------------------------------------------------------------------------
# MAIN CLASSIFICATION FUNCTION
# ------------------------------------------------------------------------------

#' Classify TDA signals with forward drawdown analysis
#'
#' Main classification function that computes forward drawdowns and classifies
#' each day's signal as TP/FP/FN/TN/CORRECTION.
#'
#' @param signals_df Data frame with Date, Signal_State columns
#' @param prices_df Data frame with date and reference price columns
#' @param reference_col Column name for reference index (default: "SPX")
#' @param forward_days Look-forward period (default: 250)
#' @param crash_threshold Crash threshold (default: 0.20)
#' @param correction_threshold Correction threshold (default: 0.10)
#' @param save_file Whether to save output CSV (default: TRUE)
#' @return Data frame with Forward_DD and Classification columns added
#' @export
classify_signals <- function(signals_df,
                              prices_df,
                              reference_col = REFERENCE_INDEX,
                              forward_days = FORWARD_WINDOW,
                              crash_threshold = CRASH_THRESHOLD,
                              correction_threshold = CORRECTION_THRESHOLD,
                              save_file = TRUE) {
    
    log_info("=== Signal Classification ===")
    log_info("Input signals: %d observations", nrow(signals_df))
    log_info("Reference index: %s", reference_col)
    log_info("Forward window: %d trading days", forward_days)
    log_info("Crash threshold: %.0f%% drawdown", crash_threshold * 100)
    log_info("Correction threshold: %.0f%% drawdown", correction_threshold * 100)
    
    # -------------------------------------------------------------------------
    # Step 1: Align prices with signal dates
    # -------------------------------------------------------------------------
    log_info("\nStep 1: Aligning prices with signal dates")
    
    # Ensure consistent date column naming
    if ("date" %in% names(prices_df)) {
        prices_df <- prices_df %>% rename(Date = date)
    }
    
    price_aligned <- prices_df %>%
        filter(Date %in% signals_df$Date) %>%
        arrange(Date)
    
    log_info("  Aligned %d price observations", nrow(price_aligned))
    
    # -------------------------------------------------------------------------
    # Step 2: Compute forward drawdowns
    # -------------------------------------------------------------------------
    log_info("\nStep 2: Computing forward maximum drawdowns")
    
    forward_dd <- compute_forward_drawdowns(
        price_aligned[[reference_col]], 
        forward_days
    )
    
    # Add to signals dataframe
    results_df <- signals_df %>%
        filter(Date %in% price_aligned$Date) %>%
        arrange(Date) %>%
        mutate(Forward_DD = forward_dd)
    
    log_info("  Forward DD summary: mean=%.1f%%, max=%.1f%%",
             mean(results_df$Forward_DD, na.rm = TRUE) * 100,
             max(results_df$Forward_DD, na.rm = TRUE) * 100)
    
    # -------------------------------------------------------------------------
    # Step 3: Classify outcomes
    # -------------------------------------------------------------------------
    log_info("\nStep 3: Classifying signal outcomes")
    
    results_df$Classification <- classify_outcomes(
        results_df$Signal_State,
        results_df$Forward_DD,
        crash_threshold,
        correction_threshold
    )
    
    # -------------------------------------------------------------------------
    # Step 4: Compute and report metrics
    # -------------------------------------------------------------------------
    log_info("\nStep 4: Computing classification metrics")
    
    metrics <- compute_classification_metrics(results_df$Classification)
    
    log_info("\n  Classification Breakdown:")
    log_info("    TRUE_POSITIVE:  %6d", metrics$true_positives)
    log_info("    CORRECTION:     %6d", metrics$corrections)
    log_info("    FALSE_POSITIVE: %6d", metrics$false_positives)
    log_info("    FALSE_NEGATIVE: %6d", metrics$false_negatives)
    log_info("    TRUE_NEGATIVE:  %6d", metrics$true_negatives)
    log_info("")
    log_info("  Performance Metrics:")
    log_info("    Precision (strict):          %.2f%%", metrics$precision_strict * 100)
    log_info("    Precision (incl. corr.):     %.2f%%", metrics$precision_incl_corrections * 100)
    log_info("    Recall (crash detection):    %.2f%%", metrics$recall * 100)
    if (!is.na(metrics$f1_score)) {
        log_info("    F1 Score:                    %.2f%%", metrics$f1_score * 100)
    }
    
    # -------------------------------------------------------------------------
    # Step 5: Save results
    # -------------------------------------------------------------------------
    if (save_file) {
        log_info("\nStep 5: Saving classified results")
        
        if (!dir.exists(PROCESSED_DIR)) {
            dir.create(PROCESSED_DIR, recursive = TRUE)
        }
        
        write_csv(results_df, RESULTS_FILE)
        log_info("  Saved to: %s", RESULTS_FILE)
    }
    
    log_info("\n=== Signal Classification Complete ===")
    
    # Attach metrics as attribute for programmatic access
    attr(results_df, "metrics") <- metrics
    
    return(results_df)
}

#' Load cached backtest results from processed file
#'
#' @return Data frame with results, or NULL if not found
#' @export
load_backtest_results <- function() {
    if (!file.exists(RESULTS_FILE)) {
        log_warn("Cached results not found. Run classify_signals() first.")
        return(NULL)
    }
    
    log_info("Loading cached backtest results from processed...")
    results_df <- read_csv(RESULTS_FILE, show_col_types = FALSE)
    log_info("  Loaded %d observations", nrow(results_df))
    
    return(results_df)
}

#' Analyze specific crash periods
#'
#' @param results_df Data frame with Classification and Signal_State columns
#' @param crash_periods Named list of date ranges (each a 2-element character vector)
#' @return Data frame with period analysis summary
#' @export
analyze_crash_periods <- function(results_df, crash_periods = NULL) {
    
    # Default notable crash periods
    if (is.null(crash_periods)) {
        crash_periods <- list(
            "1962 Flash Crash" = c("1962-05-01", "1962-06-30"),
            "1973-74 Bear Market" = c("1973-01-01", "1974-12-31"),
            "1987 Black Monday" = c("1987-08-01", "1987-10-31"),
            "2000 Dot-com Bust" = c("2000-03-01", "2000-04-30"),
            "2008 Financial Crisis" = c("2008-09-01", "2008-11-30"),
            "2020 COVID Crash" = c("2020-02-01", "2020-03-31"),
            "2022 Bear Market" = c("2022-01-01", "2022-10-31")
        )
    }
    
    log_info("\n=== Notable Crash Periods Analysis ===")
    
    analysis <- lapply(names(crash_periods), function(period_name) {
        dates <- crash_periods[[period_name]]
        period_data <- results_df %>%
            filter(Date >= as.Date(dates[1]), Date <= as.Date(dates[2]))
        
        if (nrow(period_data) > 0) {
            n_warnings <- sum(period_data$Signal_State == "Warning", na.rm = TRUE)
            max_dd <- max(period_data$Forward_DD, na.rm = TRUE)
            
            log_info("  %s:", period_name)
            log_info("    Warning signals: %d / %d days (%.1f%%)",
                     n_warnings, nrow(period_data),
                     100 * n_warnings / nrow(period_data))
            log_info("    Max forward DD: %.1f%%", max_dd * 100)
            
            data.frame(
                Period = period_name,
                Start = dates[1],
                End = dates[2],
                Days = nrow(period_data),
                Warning_Days = n_warnings,
                Warning_Pct = 100 * n_warnings / nrow(period_data),
                Max_Forward_DD = max_dd * 100,
                stringsAsFactors = FALSE
            )
        } else {
            log_info("  %s: No data available", period_name)
            NULL
        }
    })
    
    bind_rows(analysis)
}


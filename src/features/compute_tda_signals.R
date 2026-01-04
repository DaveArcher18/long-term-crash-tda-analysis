# compute_tda_signals.R
# Feature Engineering Module: TDA-based Market Instability Signals
#
# Computes topological features from 3D log-return point clouds following
# Gidea & Katz (2017) methodology:
#   1. Sliding window point clouds → Vietoris-Rips persistence diagrams
#   2. H1 (loops) → Persistence Landscape L^1 and L^2 norms
#   3. Rolling SD of norms → "instability volatility"
#   4. Mann-Kendall Tau on rolling SD → trend detection signal
#
# Input:  data/interim/century_log_returns.csv
# Output: data/processed/century_tda_signals.csv
#
# Usage:
#   source("src/features/compute_tda_signals.R")
#   signals <- compute_tda_signals(returns_df)

# ------------------------------------------------------------------------------
# SETUP
# ------------------------------------------------------------------------------

suppressPackageStartupMessages({
    library(TDA)
    library(Kendall)
    library(parallel)
    library(dplyr)
    library(readr)
    library(zoo)
})

# Logging utilities
log_info  <- function(...) message(sprintf("[%s] INFO  %s", Sys.time(), sprintf(...)))
log_warn  <- function(...) warning(sprintf("[%s] WARN  %s", Sys.time(), sprintf(...)), call. = FALSE)

# ------------------------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------------------------

# TDA parameters (following Gidea & Katz 2017)
TDA_TOPOLOGY_WINDOW <- 50       # w: sliding window for point cloud (days)
TDA_MAX_DIMENSION <- 1          # H1 loops only

# Max scale should be ~3-5x the typical point cloud diameter
# For equity log-returns with SD ~0.01, max daily return ~0.10
# Point cloud diameter in 3D: sqrt(3) * 0.10 ≈ 0.17
# Use slightly larger to capture all features
TDA_MAX_SCALE <- 0.20           # Filtration radius

TDA_LANDSCAPE_KK <- 1:5         # Number of landscape functions
TDA_LANDSCAPE_RESOLUTION <- 500 # Grid resolution for landscape evaluation

# Signal parameters
SIGNAL_ROLLING_SD_WINDOW <- 50  # Window for rolling SD of norms
SIGNAL_MK_LOOKBACK <- 250       # Lookback for Mann-Kendall Tau
SIGNAL_TAU_THRESHOLD <- 0.30    # Warning threshold for Tau

# Parallelization
TDA_N_CORES <- max(1, parallel::detectCores() - 1)

# Output paths
PROCESSED_DIR <- "data/processed"
SIGNALS_FILE <- file.path(PROCESSED_DIR, "century_tda_signals.csv")

# ------------------------------------------------------------------------------
# TDA COMPUTATION FUNCTIONS
# ------------------------------------------------------------------------------

#' Compute persistence metrics for a single point cloud
#'
#' Returns L^1 norm, L^2 norm, total persistence, and number of H1 features.
#'
#' @param cloud Matrix representing point cloud (w x d)
#' @param max_scale Maximum filtration radius
#' @param max_dim Maximum homology dimension (default: 1)
#' @param landscape_kk Landscape functions to compute (default: 1:5)
#' @param resolution Grid resolution for landscape (default: 500)
#' @return Named vector with l1_norm, l2_norm, total_persistence, n_loops
compute_persistence_metrics <- function(cloud, 
                                         max_scale = TDA_MAX_SCALE,
                                         max_dim = TDA_MAX_DIMENSION,
                                         landscape_kk = TDA_LANDSCAPE_KK,
                                         resolution = TDA_LANDSCAPE_RESOLUTION) {
    
    # Compute Vietoris-Rips persistence diagram
    diag_out <- ripsDiag(
        X = cloud,
        maxdimension = max_dim,
        maxscale = max_scale,
        library = "GUDHI",
        printProgress = FALSE
    )
    
    diagram <- diag_out$diagram
    
    # Filter for H1 features only (loops)
    diag_h1 <- diagram[diagram[, 1] == 1, , drop = FALSE]
    
    # Handle case with no H1 features
    if (nrow(diag_h1) == 0) {
        return(c(
            l1_norm = 0,
            l2_norm = 0,
            total_persistence = 0,
            max_persistence = 0,
            n_loops = 0
        ))
    }
    
    # Extract birth-death pairs
    births <- diag_h1[, 2]
    deaths <- diag_h1[, 3]
    persistence <- deaths - births
    
    # Total persistence (sum of lifetimes) - direct from diagram
    total_persistence <- sum(persistence)
    max_persistence <- max(persistence)
    n_loops <- nrow(diag_h1)
    
    # Create grid for landscape evaluation
    tseq <- seq(0, max(deaths) * 1.1, length.out = resolution)
    
    # Compute persistence landscape
    L <- landscape(
        Diag = diag_h1,
        dimension = 1,
        KK = landscape_kk,
        tseq = tseq
    )
    
    # Numerical integration step
    dx <- tseq[2] - tseq[1]
    
    # L^1 norm
    l1_norm <- sum(abs(L)) * dx
    
    # L^2 norm
    l2_norm <- sqrt(sum(L^2) * dx)
    
    return(c(
        l1_norm = l1_norm,
        l2_norm = l2_norm,
        total_persistence = total_persistence,
        max_persistence = max_persistence,
        n_loops = n_loops
    ))
}

#' Compute metrics for a batch of windows (parallel worker function)
#'
#' @param window_indices Vector of window start indices
#' @param returns_matrix Matrix of log-returns (n x d)
#' @param window_size Size of sliding window
#' @param max_scale Maximum filtration radius
#' @return Matrix of metrics (one row per window)
compute_metrics_batch <- function(window_indices, returns_matrix, window_size, max_scale) {
    n <- length(window_indices)
    results <- matrix(NA_real_, nrow = n, ncol = 5)
    colnames(results) <- c("l1_norm", "l2_norm", "total_persistence", "max_persistence", "n_loops")
    
    for (j in seq_along(window_indices)) {
        i <- window_indices[j]
        cloud <- returns_matrix[i:(i + window_size - 1), , drop = FALSE]
        metrics <- compute_persistence_metrics(cloud, max_scale = max_scale)
        results[j, ] <- metrics
    }
    
    return(results)
}

#' Compute all persistence metrics with optional parallel processing
#'
#' @param returns_matrix Matrix of log-returns (n x d)
#' @param window_size Sliding window size (default: 50)
#' @param max_scale Maximum filtration radius (default: 0.20)
#' @param n_cores Number of parallel workers (default: auto-detect)
#' @return Data frame with l1_norm, l2_norm, total_persistence, max_persistence, n_loops
#' @export
compute_all_metrics <- function(returns_matrix, 
                                 window_size = TDA_TOPOLOGY_WINDOW,
                                 max_scale = TDA_MAX_SCALE,
                                 n_cores = TDA_N_CORES) {
    
    n_windows <- nrow(returns_matrix) - window_size + 1
    
    log_info("Computing persistence metrics for %d windows using %d cores...", n_windows, n_cores)
    log_info("  max_scale = %.3f (should be ~3-5x point cloud diameter)", max_scale)
    
    start_time <- Sys.time()
    
    if (n_cores > 1) {
        # Parallel execution
        cluster <- makeCluster(n_cores)
        on.exit(stopCluster(cluster), add = TRUE)
        
        # Export required functions and packages to workers
        clusterEvalQ(cluster, {
            suppressPackageStartupMessages(library(TDA))
        })
        
        clusterExport(cluster, c("compute_persistence_metrics", "TDA_MAX_DIMENSION", 
                                  "TDA_LANDSCAPE_KK", "TDA_LANDSCAPE_RESOLUTION"),
                      envir = environment())
        
        # Split indices into chunks
        all_indices <- seq_len(n_windows)
        chunk_size <- ceiling(n_windows / n_cores)
        chunks <- split(all_indices, ceiling(seq_along(all_indices) / chunk_size))
        
        # Execute in parallel
        results_list <- parLapply(cluster, chunks, compute_metrics_batch,
                                   returns_matrix = returns_matrix,
                                   window_size = window_size,
                                   max_scale = max_scale)
        
        results <- do.call(rbind, results_list)
        
    } else {
        # Sequential execution with progress reporting
        results <- matrix(NA_real_, nrow = n_windows, ncol = 5)
        colnames(results) <- c("l1_norm", "l2_norm", "total_persistence", "max_persistence", "n_loops")
        
        progress_interval <- max(1, n_windows %/% 20)
        
        for (i in seq_len(n_windows)) {
            cloud <- returns_matrix[i:(i + window_size - 1), , drop = FALSE]
            results[i, ] <- compute_persistence_metrics(cloud, max_scale = max_scale)
            
            if (i %% progress_interval == 0 || i == n_windows) {
                pct <- round(100 * i / n_windows)
                elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
                eta <- elapsed / i * (n_windows - i)
                log_info("  Progress: %d%% (%d/%d) - ETA: %.0f sec", 
                         pct, i, n_windows, eta)
            }
        }
    }
    
    elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "mins"))
    log_info("  TDA computation complete in %.1f minutes", elapsed)
    
    return(as.data.frame(results))
}

# ------------------------------------------------------------------------------
# SIGNAL COMPUTATION FUNCTIONS
# ------------------------------------------------------------------------------

#' Compute rolling standard deviation
#'
#' @param x Numeric vector
#' @param window Rolling window size
#' @return Numeric vector of rolling SD values (NA for first window-1 values)
#' @export
compute_rolling_sd <- function(x, window = SIGNAL_ROLLING_SD_WINDOW) {
    zoo::rollapply(x, width = window, FUN = sd, fill = NA, align = "right")
}

#' Compute Mann-Kendall Tau with rolling lookback window
#'
#' @param x Numeric vector (time series)
#' @param lookback Number of observations for MK calculation
#' @return Numeric vector of Tau values (NA for first lookback-1 values)
#' @export
compute_rolling_mk_tau <- function(x, lookback = SIGNAL_MK_LOOKBACK) {
    n <- length(x)
    tau_values <- rep(NA_real_, n)
    
    for (i in lookback:n) {
        segment <- x[(i - lookback + 1):i]
        segment_clean <- segment[!is.na(segment)]
        
        if (length(segment_clean) >= 10) {
            mk_result <- tryCatch({
                MannKendall(segment_clean)
            }, error = function(e) NULL)
            
            if (!is.null(mk_result)) {
                tau_values[i] <- as.numeric(mk_result$tau)
            }
        }
    }
    
    return(tau_values)
}

#' Determine signal state based on Tau threshold
#'
#' @param tau Numeric vector of Mann-Kendall Tau values
#' @param threshold Warning threshold (default: 0.30)
#' @return Character vector: "Warning" if tau > threshold, else "Normal"
#' @export
compute_signal_state <- function(tau, threshold = SIGNAL_TAU_THRESHOLD) {
    ifelse(is.na(tau), NA_character_,
           ifelse(tau > threshold, "Warning", "Normal"))
}

# ------------------------------------------------------------------------------
# MAIN FEATURE ENGINEERING FUNCTION
# ------------------------------------------------------------------------------

#' Compute TDA-based signals from log-returns
#'
#' Main feature engineering function that computes all TDA-based instability
#' signals from a matrix of log-returns.
#'
#' @param returns_df Data frame with date column and return columns
#' @param return_cols Column names for returns (default: c("DJI", "DJT", "SPX"))
#' @param window_size TDA sliding window size (default: 50)
#' @param max_scale Maximum filtration radius (default: 0.20)
#' @param sd_window Rolling SD window size (default: 50)
#' @param mk_lookback Mann-Kendall lookback period (default: 250)
#' @param tau_threshold Warning signal threshold (default: 0.30)
#' @param n_cores Parallel cores (default: auto-detect)
#' @param save_file Whether to save output CSV (default: TRUE)
#' @return Data frame with all metrics and signals
#' @export
compute_tda_signals <- function(returns_df,
                                 return_cols = c("DJI", "DJT", "SPX"),
                                 window_size = TDA_TOPOLOGY_WINDOW,
                                 max_scale = TDA_MAX_SCALE,
                                 sd_window = SIGNAL_ROLLING_SD_WINDOW,
                                 mk_lookback = SIGNAL_MK_LOOKBACK,
                                 tau_threshold = SIGNAL_TAU_THRESHOLD,
                                 n_cores = TDA_N_CORES,
                                 save_file = TRUE) {
    
    log_info("=== TDA Signal Computation ===")
    log_info("Input observations: %d", nrow(returns_df))
    log_info("Return columns: %s", paste(return_cols, collapse = ", "))
    
    returns_matrix <- as.matrix(returns_df[, return_cols])
    
    # Report data characteristics for max_scale calibration
    return_sd <- apply(returns_matrix, 2, sd)
    return_range <- apply(returns_matrix, 2, function(x) diff(range(x)))
    log_info("  Return SDs: %s", paste(sprintf("%.4f", return_sd), collapse = ", "))
    log_info("  Return ranges: %s", paste(sprintf("%.3f", return_range), collapse = ", "))
    
    # -------------------------------------------------------------------------
    # Step 1: Compute persistence metrics
    # -------------------------------------------------------------------------
    log_info("\nStep 1: Computing persistence metrics (L^1, L^2 norms)")
    log_info("  Topology window (w): %d days", window_size)
    log_info("  Max filtration scale: %.3f", max_scale)
    log_info("  Parallel cores: %d", n_cores)
    
    metrics_df <- compute_all_metrics(
        returns_matrix = returns_matrix,
        window_size = window_size,
        max_scale = max_scale,
        n_cores = n_cores
    )
    
    # Create results dataframe (dates align with end of each window)
    signals_df <- data.frame(
        Date = returns_df$date[window_size:nrow(returns_df)],
        L1_Norm = metrics_df$l1_norm,
        L2_Norm = metrics_df$l2_norm,
        Total_Persistence = metrics_df$total_persistence,
        Max_Persistence = metrics_df$max_persistence,
        N_Loops = metrics_df$n_loops
    )
    
    # Report diagnostics
    log_info("  Metrics computed: %d values", nrow(signals_df))
    log_info("  H1 loops per window: mean=%.1f, max=%d, zero_count=%d",
             mean(signals_df$N_Loops), max(signals_df$N_Loops),
             sum(signals_df$N_Loops == 0))
    log_info("  L^1 norm: mean=%.2e, sd=%.2e, max=%.2e",
             mean(signals_df$L1_Norm), sd(signals_df$L1_Norm), max(signals_df$L1_Norm))
    log_info("  L^2 norm: mean=%.2e, sd=%.2e, max=%.2e",
             mean(signals_df$L2_Norm), sd(signals_df$L2_Norm), max(signals_df$L2_Norm))
    log_info("  Total persistence: mean=%.2e, max=%.2e",
             mean(signals_df$Total_Persistence), max(signals_df$Total_Persistence))
    
    # -------------------------------------------------------------------------
    # Step 2: Compute rolling SD for both L1 and L2
    # -------------------------------------------------------------------------
    log_info("\nStep 2: Computing rolling standard deviation")
    log_info("  Rolling SD window: %d days", sd_window)
    
    signals_df$L1_Rolling_SD <- compute_rolling_sd(signals_df$L1_Norm, sd_window)
    signals_df$L2_Rolling_SD <- compute_rolling_sd(signals_df$L2_Norm, sd_window)
    
    # -------------------------------------------------------------------------
    # Step 3: Compute Mann-Kendall Tau for both L1 and L2
    # -------------------------------------------------------------------------
    log_info("\nStep 3: Computing Mann-Kendall Tau")
    log_info("  MK lookback: %d days", mk_lookback)
    log_info("  (This may take a few minutes...)")
    
    signals_df$L1_Tau <- compute_rolling_mk_tau(signals_df$L1_Rolling_SD, mk_lookback)
    signals_df$L2_Tau <- compute_rolling_mk_tau(signals_df$L2_Rolling_SD, mk_lookback)
    
    # -------------------------------------------------------------------------
    # Step 4: Determine signal states (using L1 as primary, L2 as secondary)
    # -------------------------------------------------------------------------
    log_info("\nStep 4: Determining signal states")
    log_info("  Tau threshold: %.2f", tau_threshold)
    
    signals_df$L1_Signal <- compute_signal_state(signals_df$L1_Tau, tau_threshold)
    signals_df$L2_Signal <- compute_signal_state(signals_df$L2_Tau, tau_threshold)
    
    # Combined signal: Warning if EITHER L1 or L2 triggers
    signals_df$Signal_State <- ifelse(
        is.na(signals_df$L1_Signal) | is.na(signals_df$L2_Signal),
        NA_character_,
        ifelse(signals_df$L1_Signal == "Warning" | signals_df$L2_Signal == "Warning",
               "Warning", "Normal")
    )
    
    n_l1_warnings <- sum(signals_df$L1_Signal == "Warning", na.rm = TRUE)
    n_l2_warnings <- sum(signals_df$L2_Signal == "Warning", na.rm = TRUE)
    n_combined <- sum(signals_df$Signal_State == "Warning", na.rm = TRUE)
    n_valid <- sum(!is.na(signals_df$Signal_State))
    
    log_info("  L^1 warnings: %d / %d (%.1f%%)", n_l1_warnings, n_valid, 100 * n_l1_warnings / n_valid)
    log_info("  L^2 warnings: %d / %d (%.1f%%)", n_l2_warnings, n_valid, 100 * n_l2_warnings / n_valid)
    log_info("  Combined warnings: %d / %d (%.1f%%)", n_combined, n_valid, 100 * n_combined / n_valid)
    
    # -------------------------------------------------------------------------
    # Step 5: Save results
    # -------------------------------------------------------------------------
    if (save_file) {
        log_info("\nStep 5: Saving signals to processed directory")
        
        if (!dir.exists(PROCESSED_DIR)) {
            dir.create(PROCESSED_DIR, recursive = TRUE)
        }
        
        write_csv(signals_df, SIGNALS_FILE)
        log_info("  Saved to: %s", SIGNALS_FILE)
    }
    
    log_info("\n=== TDA Signal Computation Complete ===")
    
    return(signals_df)
}

#' Load cached TDA signals from processed file
#'
#' @return Data frame with signals, or NULL if not found
#' @export
load_tda_signals <- function() {
    if (!file.exists(SIGNALS_FILE)) {
        log_warn("Cached signals not found. Run compute_tda_signals() first.")
        return(NULL)
    }
    
    log_info("Loading cached TDA signals from processed...")
    signals_df <- read_csv(SIGNALS_FILE, show_col_types = FALSE)
    log_info("  Loaded %d observations", nrow(signals_df))
    
    return(signals_df)
}

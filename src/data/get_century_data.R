# get_century_data.R
# Data Acquisition Module: Fetch historical index data for Grand Historical Backtest
#
# Downloads daily close prices for major US indices from Stooq (free historical data),
# aligns to common trading dates, and computes log-returns for TDA analysis.
#
# Data Source: Stooq.com (provides free historical data back to 1950s)
#
# Tickers:
#   - ^DJI  (Dow Jones Industrial Average) - available from 1914
#   - ^DJT  (Dow Jones Transportation Average) - available from 1914
#   - ^SPX  (S&P 500) - available from 1950
#
# Date Range: 1957-01-01 to present
#
# Output:
#   - data/raw/{ticker}_raw.csv (individual raw downloads)
#   - data/interim/century_aligned_prices.csv (aligned daily prices)
#   - data/interim/century_log_returns.csv (3D log-returns for TDA)
#
# Usage:
#   source("src/data/get_century_data.R")
#   data <- get_century_data()

# ------------------------------------------------------------------------------
# SETUP
# ------------------------------------------------------------------------------

suppressPackageStartupMessages({
    library(dplyr)
    library(readr)
    library(xts)
    library(zoo)
})

# Logging utilities
log_info  <- function(...) message(sprintf("[%s] INFO  %s", Sys.time(), sprintf(...)))
log_warn  <- function(...) warning(sprintf("[%s] WARN  %s", Sys.time(), sprintf(...)), call. = FALSE)
log_error <- function(...) stop(sprintf("[%s] ERROR %s", Sys.time(), sprintf(...)), call. = FALSE)

# ------------------------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------------------------

# Stooq ticker symbols
STOOQ_TICKERS <- c("^DJI", "^DJT", "^SPX")
TICKER_NAMES <- c("DJI", "DJT", "SPX")
START_DATE <- as.Date("1957-01-01")

# Output paths
RAW_DIR <- "data/raw"
INTERIM_DIR <- "data/interim"
PRICES_FILE <- file.path(INTERIM_DIR, "century_aligned_prices.csv")
RETURNS_FILE <- file.path(INTERIM_DIR, "century_log_returns.csv")

# ------------------------------------------------------------------------------
# STOOQ DOWNLOAD FUNCTIONS
# ------------------------------------------------------------------------------

#' Download historical data from Stooq
#'
#' Stooq provides free CSV downloads via direct URL.
#' URL format: https://stooq.com/q/d/l/?s={symbol}&d1={start}&d2={end}
#'
#' @param symbol Stooq ticker symbol (e.g., "^DJI")
#' @param start_date Start date
#' @param end_date End date
#' @return data.frame with date and close columns, or NULL on failure
download_stooq <- function(symbol, start_date, end_date = Sys.Date()) {
    log_info("  Downloading %s from Stooq...", symbol)
    
    # Format dates for Stooq URL (YYYYMMDD)
    d1 <- format(start_date, "%Y%m%d")
    d2 <- format(end_date, "%Y%m%d")
    
    # Construct URL
    url <- sprintf("https://stooq.com/q/d/l/?s=%s&d1=%s&d2=%s", 
                   URLencode(symbol, reserved = TRUE), d1, d2)
    
    tryCatch({
        # Download to temporary file
        temp_file <- tempfile(fileext = ".csv")
        download.file(url, temp_file, quiet = TRUE, method = "curl")
        
        # Read CSV
        df <- read_csv(temp_file, show_col_types = FALSE)
        unlink(temp_file)
        
        # Check if we got valid data
        if (is.null(df) || nrow(df) == 0) {
            log_warn("    No data returned from Stooq for %s", symbol)
            return(NULL)
        }
        
        # Stooq returns: Date, Open, High, Low, Close, Volume
        # Standardize column names
        names(df) <- tolower(names(df))
        
        if (!"date" %in% names(df) || !"close" %in% names(df)) {
            log_warn("    Unexpected column format from Stooq for %s", symbol)
            return(NULL)
        }
        
        result <- data.frame(
            date = as.Date(df$date),
            close = as.numeric(df$close)
        )
        
        # Clean data
        result <- result[!is.na(result$close) & result$close > 0, ]
        result <- result[order(result$date), ]
        
        log_info("    Retrieved %d observations (%s to %s)",
                 nrow(result), min(result$date), max(result$date))
        
        return(result)
        
    }, error = function(e) {
        log_warn("    Stooq download failed for %s: %s", symbol, e$message)
        return(NULL)
    })
}

#' Download from Yahoo Finance (fallback)
#'
#' @param symbol Yahoo ticker symbol
#' @param start_date Start date
#' @param end_date End date
#' @return data.frame with date and close columns
download_yahoo <- function(symbol, start_date, end_date = Sys.Date()) {
    log_info("    Trying Yahoo Finance fallback for %s...", symbol)
    
    # Convert Stooq symbols to Yahoo format
    yahoo_symbol <- symbol
    if (symbol == "^SPX") yahoo_symbol <- "^GSPC"
    
    tryCatch({
        suppressPackageStartupMessages(library(quantmod))
        
        data_env <- new.env()
        
        suppressWarnings({
            getSymbols(
                yahoo_symbol,
                src = "yahoo",
                from = start_date,
                to = end_date,
                env = data_env,
                auto.assign = TRUE
            )
        })
        
        symbol_key <- ls(data_env)[1]
        if (length(symbol_key) == 0) return(NULL)
        
        xts_data <- get(symbol_key, envir = data_env)
        
        if (is.null(xts_data) || nrow(xts_data) == 0) {
            return(NULL)
        }
        
        # Use Adjusted close from Yahoo
        adj_col <- grep("\\.Adjusted$", colnames(xts_data), value = TRUE)
        if (length(adj_col) == 0) {
            adj_col <- grep("\\.Close$", colnames(xts_data), value = TRUE)[1]
        }
        
        result <- data.frame(
            date = index(xts_data),
            close = as.numeric(xts_data[, adj_col[1]])
        )
        
        result <- result[!is.na(result$close), ]
        result <- result[order(result$date), ]
        
        log_info("    Yahoo retrieved %d observations (%s to %s)",
                 nrow(result), min(result$date), max(result$date))
        
        return(result)
        
    }, error = function(e) {
        log_warn("    Yahoo download also failed for %s: %s", symbol, e$message)
        return(NULL)
    })
}

#' Download prices with fallback
#'
#' Tries Stooq first, falls back to Yahoo if needed.
#'
#' @param symbol Ticker symbol
#' @param start_date Start date
#' @param end_date End date
#' @return data.frame with date and close columns
download_prices <- function(symbol, start_date, end_date = Sys.Date()) {
    # Try Stooq first (has longer history)
    result <- download_stooq(symbol, start_date, end_date)
    
    # Fall back to Yahoo if Stooq fails
    if (is.null(result) || nrow(result) < 100) {
        result_yahoo <- download_yahoo(symbol, start_date, end_date)
        
        if (!is.null(result_yahoo)) {
            # Use whichever has more data
            if (is.null(result) || nrow(result_yahoo) > nrow(result)) {
                result <- result_yahoo
            }
        }
    }
    
    return(result)
}

#' Align multiple price series to common trading dates
#'
#' Uses inner join to keep only dates present in ALL series.
#'
#' @param df_list Named list of data frames with date and close columns
#' @return data.frame with aligned dates and prices for all assets
align_price_series <- function(df_list) {
    xts_list <- lapply(names(df_list), function(name) {
        df <- df_list[[name]]
        xts_obj <- xts(df$close, order.by = df$date)
        colnames(xts_obj) <- name
        return(xts_obj)
    })
    
    # Inner join - only dates present in ALL series
    merged <- do.call(merge, c(xts_list, join = "inner"))
    
    # Forward-fill any remaining gaps
    merged <- na.locf(merged, na.rm = FALSE)
    merged <- na.omit(merged)
    
    result <- data.frame(
        date = index(merged),
        as.data.frame(merged)
    )
    
    return(result)
}

#' Compute log-returns from price series
#'
#' @param prices Numeric vector of prices
#' @return Numeric vector of log-returns
compute_log_returns <- function(prices) {
    diff(log(prices))
}

# ------------------------------------------------------------------------------
# MAIN FUNCTION
# ------------------------------------------------------------------------------

#' Main data acquisition function for century backtest
#'
#' Downloads index data from Stooq (with Yahoo fallback), aligns series,
#' computes log-returns, and saves results.
#'
#' @param start_date Start date (default: 1957-01-01)
#' @param end_date End date (default: today)
#' @param save_files Whether to save CSV files (default: TRUE)
#' @return List with aligned_prices and log_returns data frames
#' @export
get_century_data <- function(start_date = START_DATE, 
                              end_date = Sys.Date(),
                              save_files = TRUE) {
    
    log_info("=== Century Data Acquisition ===")
    log_info("Date range: %s to %s", start_date, end_date)
    log_info("Tickers: %s", paste(STOOQ_TICKERS, collapse = ", "))
    log_info("Primary source: Stooq.com (fallback: Yahoo Finance)")
    
    # -------------------------------------------------------------------------
    # Step 1: Download all index data
    # -------------------------------------------------------------------------
    log_info("\nStep 1: Downloading index data")
    
    raw_data <- list()
    
    # Create raw directory if needed
    if (save_files && !dir.exists(RAW_DIR)) {
        dir.create(RAW_DIR, recursive = TRUE)
    }
    
    for (i in seq_along(STOOQ_TICKERS)) {
        ticker <- STOOQ_TICKERS[i]
        name <- TICKER_NAMES[i]
        
        prices <- download_prices(ticker, start_date, end_date)
        
        if (is.null(prices) || nrow(prices) == 0) {
            log_error("Failed to download %s - aborting", ticker)
        }
        
        raw_data[[name]] <- prices
        
        # Save individual raw data files
        if (save_files) {
            raw_file <- file.path(RAW_DIR, paste0(name, "_raw.csv"))
            write_csv(prices, raw_file)
            log_info("    Saved to: %s", raw_file)
        }
    }
    
    # -------------------------------------------------------------------------
    # Step 2: Align series to common dates
    # -------------------------------------------------------------------------
    log_info("\nStep 2: Aligning price series to common dates")
    
    aligned_prices <- align_price_series(raw_data)
    
    log_info("  Aligned data: %d trading days", nrow(aligned_prices))
    log_info("  Date range: %s to %s", 
             min(aligned_prices$date), max(aligned_prices$date))
    
    # Report coverage by decade
    decades <- seq(1950, 2020, by = 10)
    for (decade in decades) {
        decade_start <- as.Date(paste0(decade, "-01-01"))
        decade_end <- as.Date(paste0(decade + 9, "-12-31"))
        decade_data <- aligned_prices[aligned_prices$date >= decade_start & 
                                       aligned_prices$date <= decade_end, ]
        if (nrow(decade_data) > 0) {
            log_info("    %ds: %d trading days", decade, nrow(decade_data))
        }
    }
    
    # -------------------------------------------------------------------------
    # Step 3: Compute log-returns
    # -------------------------------------------------------------------------
    log_info("\nStep 3: Computing log-returns")
    
    returns_df <- data.frame(
        date = aligned_prices$date[-1]
    )
    
    for (name in TICKER_NAMES) {
        returns_df[[name]] <- compute_log_returns(aligned_prices[[name]])
    }
    
    log_info("  Log-returns computed: %d observations", nrow(returns_df))
    
    # Data quality summary
    for (name in TICKER_NAMES) {
        log_info("    %s: mean=%.6f, sd=%.4f", 
                 name, mean(returns_df[[name]]), sd(returns_df[[name]]))
    }
    
    # -------------------------------------------------------------------------
    # Step 4: Save to interim
    # -------------------------------------------------------------------------
    if (save_files) {
        log_info("\nStep 4: Saving to interim directory")
        
        if (!dir.exists(INTERIM_DIR)) {
            dir.create(INTERIM_DIR, recursive = TRUE)
        }
        
        write_csv(aligned_prices, PRICES_FILE)
        log_info("  Prices saved to: %s", PRICES_FILE)
        
        write_csv(returns_df, RETURNS_FILE)
        log_info("  Returns saved to: %s", RETURNS_FILE)
    }
    
    log_info("\n=== Data Acquisition Complete ===")
    
    return(list(
        aligned_prices = aligned_prices,
        log_returns = returns_df
    ))
}

#' Load cached century data from interim files
#'
#' @return List with aligned_prices and log_returns, or NULL if not found
#' @export
load_century_data <- function() {
    if (!file.exists(PRICES_FILE) || !file.exists(RETURNS_FILE)) {
        log_warn("Cached data not found. Run get_century_data() first.")
        return(NULL)
    }
    
    log_info("Loading cached century data from interim...")
    
    aligned_prices <- read_csv(PRICES_FILE, show_col_types = FALSE)
    log_returns <- read_csv(RETURNS_FILE, show_col_types = FALSE)
    
    log_info("  Loaded %d price observations, %d return observations",
             nrow(aligned_prices), nrow(log_returns))
    
    return(list(
        aligned_prices = aligned_prices,
        log_returns = log_returns
    ))
}

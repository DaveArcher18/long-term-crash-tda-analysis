# install_packages.R
# Reproducible environment setup for Great Depression TDA Analysis
# Run: source("install_packages.R")

# ------------------------------------------------------------------------------
# CORE DEPENDENCIES
# ------------------------------------------------------------------------------

required_packages <- c(
    # Data Ingestion
    "quantmod",      # Financial data from Yahoo Finance
    
    # Data Manipulation
    "dplyr",         # Data wrangling
    "readr",         # Fast CSV I/O
    "tidyr",         # Data tidying
    "lubridate",     # Date handling
    "xts",           # Time series objects
    "zoo",           # Time series infrastructure
    
    # Topological Data Analysis
    "TDA",           # Persistence diagrams, Rips complexes
    "TDAstats",      # Additional TDA utilities
    
    # Visualization
    "ggplot2",       # Grammar of graphics
    "scales",        # Plot scales
    "patchwork",     # Combine plots
    
    # Logging & Diagnostics
    "logger",        # Structured logging
    
    # Development
    "testthat"       # Unit testing
)

# ------------------------------------------------------------------------------
# INSTALLATION
# ------------------------------------------------------------------------------

install_if_missing <- function(pkg) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
        message(sprintf("Installing %s...", pkg))
        install.packages(pkg, repos = "https://cloud.r-project.org/")
    } else {
        message(sprintf("✓ %s already installed", pkg))
    }
}

message("=== Installing Required Packages ===\n")

for (pkg in required_packages) {
    install_if_missing(pkg)
}

# ------------------------------------------------------------------------------
# VERIFICATION
# ------------------------------------------------------------------------------

message("\n=== Verifying Installation ===\n")

verify_package <- function(pkg) {
    if (requireNamespace(pkg, quietly = TRUE)) {
        version <- as.character(packageVersion(pkg))
        message(sprintf("✓ %s (%s)", pkg, version))
        return(TRUE)
    } else {
        message(sprintf("✗ %s FAILED", pkg))
        return(FALSE)
    }
}

results <- sapply(required_packages, verify_package)

if (all(results)) {
    message("\n=== All packages installed successfully ===")
} else {
    failed <- names(results)[!results]
    warning(sprintf("\nFailed to install: %s", paste(failed, collapse = ", ")))
}

# ------------------------------------------------------------------------------
# SESSION INFO
# ------------------------------------------------------------------------------

message("\n=== Session Info ===\n")
message(sprintf("R Version: %s", R.version.string))
message(sprintf("Platform: %s", R.version$platform))
message(sprintf("Date: %s", Sys.Date()))


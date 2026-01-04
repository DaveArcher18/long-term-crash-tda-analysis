# Topological Data Analysis of Financial Time Series

Applying the Gidea & Katz (2017) methodology to detect early warning signals of market crashes.

## Overview

This project evaluates whether Topological Data Analysis (TDA) can provide statistically reliable early warning signals for major US equity market crashes. We run a systematic backtest across 69 years of market history (1957–2025) using the methodology from:

> Gidea, M., & Katz, Y. (2017). *Topological Data Analysis of Financial Time Series: Landscapes of Crashes.* arXiv:1703.04385.

## Hypothesis

Persistence landscape norms computed from sliding windows of log-returns exhibit increasing volatility prior to major market crashes. The Mann-Kendall τ statistic on this rolling volatility provides an actionable early warning signal.

## Parameters

| Parameter | Value |
|-----------|-------|
| Indices | DJI, DJT, S&P 500 |
| Data Source | Stooq.com |
| Date Range | 1957-01-02 to 2026-01-02 (17,368 days) |
| Topology Window | 50 trading days |
| Signal Threshold | Mann-Kendall τ > 0.30 |
| MK Lookback | 250 trading days |
| Crash Definition | ≥20% forward max drawdown |

## Pipeline

```
src/data/get_century_data.R          → data/interim/century_*.csv
        ↓
src/features/compute_tda_signals.R   → data/processed/century_tda_signals.csv
        ↓
src/features/classify_signals.R      → data/processed/century_backtest_results.csv
        ↓
src/visualization/plot_backtest_results.R → reports/figures/
```

## Usage

```r
# Run the full backtest (from project root)
source("src/models/run_century_backtest.R")
```

Or run individual stages:

```r
source("src/data/get_century_data.R")
data <- get_century_data()

source("src/features/compute_tda_signals.R")
signals <- compute_tda_signals(data$log_returns)

source("src/features/classify_signals.R")
results <- classify_signals(signals, data$aligned_prices)

source("src/visualization/plot_backtest_results.R")
plots <- plot_backtest_results(results)
```

## Output Files

| File | Description |
|------|-------------|
| `data/interim/century_aligned_prices.csv` | Aligned daily prices |
| `data/interim/century_log_returns.csv` | Log-returns for TDA |
| `data/processed/century_tda_signals.csv` | L¹ norms, Rolling SD, MK τ |
| `data/processed/century_backtest_results.csv` | Classifications |
| `reports/figures/century_*.png` | Visualizations |

## Classification

| Label | Condition |
|-------|-----------|
| TRUE_POSITIVE | Signal ON + Forward DD ≥ 20% |
| CORRECTION | Signal ON + Forward DD 10–20% |
| FALSE_POSITIVE | Signal ON + Forward DD < 10% |
| FALSE_NEGATIVE | Signal OFF + Forward DD ≥ 20% |
| TRUE_NEGATIVE | Signal OFF + Forward DD < 20% |

## Results Summary

| Metric | Value |
|--------|-------|
| Precision (strict) | 20.0% |
| Precision (incl. corrections) | 44.6% |
| Recall | 38.1% |
| F1 Score | 26.2% |

**See [Full Report](reports/TDA_Crash_Prediction_Assessment.md) for detailed analysis.**

## Historical Crashes Evaluated

| Period | Event | Warning Rate | Max DD |
|--------|-------|--------------|--------|
| 1962 | Kennedy Slide | 0% ✗ | 21% |
| 1973–74 | Oil Crisis | 40% ✓ | 44% |
| 1987 | Black Monday | 100% ✓ | 34% |
| 2000–02 | Dot-com Bust | 7% ⚠ | 27% |
| 2008–09 | Financial Crisis | 49% ✓ | 47% |
| 2020 | COVID Crash | 0% ✗ | 34% |
| 2022 | Inflation Bear | 50% ✓ | 25% |

## Installation

```r
source("install_packages.R")
```

Required: `quantmod`, `TDA`, `Kendall`, `parallel`, `dplyr`, `readr`, `zoo`, `ggplot2`, `patchwork`, `scales`

## Project Structure

```
├── src/
│   ├── data/get_century_data.R
│   ├── features/compute_tda_signals.R
│   ├── features/classify_signals.R
│   ├── models/run_century_backtest.R
│   └── visualization/plot_backtest_results.R
├── data/
│   ├── raw/                    # Individual index downloads
│   ├── interim/                # Aligned prices and returns
│   └── processed/              # TDA signals and classifications
├── reports/
│   ├── figures/                # Visualizations
│   └── TDA_Crash_Prediction_Assessment.pdf
└── docs/
```

## References

- Gidea & Katz (2017). Topological Data Analysis of Financial Time Series: Landscapes of Crashes.
- Bubenik (2015). Statistical Topological Data Analysis using Persistence Landscapes.

## License

MIT

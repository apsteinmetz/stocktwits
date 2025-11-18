library(tidyverse)
library(duckplyr)
# development version of yahoofinancer fixes a bug pulling adjusted close as a list-column
# pak::pak("rsquaredacademy/yahoofinancer")
library(yahoofinancer)

ticker_hist <- function(ticker, start_date, end_date, interval_pd = "1d") {
  ticker_obj <- Ticker$new(ticker)
  px_hist <- ticker_obj$get_history(
    interval = interval_pd,
    start = start_date,
    end = end_date + 7 # buffer to include week after last mention
  ) |>
    mutate(date = as.Date(date)) |>
    as_tibble()
  return(px_hist)
}

cat("Downloading price data for popular tickers...\n")
popular_tickers <- read_parquet_duckdb("data/popular_tickers.parquet")

all_prices <- list()
methods_restore() # to avoid a warning from lubridate
start_row <- 1
for (i in start_row:nrow(popular_tickers)) {
  ticker <- popular_tickers$ticker[i]
  start_date <- popular_tickers$start_date[i]
  end_date <- popular_tickers$end_date[i]
  print(paste(
    "Downloading prices for",
    ticker,
    "from",
    start_date,
    "to",
    end_date
  ))
  prices <- ticker_hist(ticker, start_date, end_date)
  # Check if API request failed
  if (is.null(prices) || nrow(prices) == 0) {
    print(paste("Error: Failed to download prices for", ticker))
    next # Skip to next ticker
  }
  all_prices[[ticker]] <- prices
}


# combine all prices into a single data frame
# with ticker as a column
prices_df <- bind_rows(all_prices, .id = "ticker") |>
  mutate(date = as.Date(date))
# save to parquet
# there won't 500 tickers due to failed ticker searches
duckplyr::compute_parquet(prices_df, "price_history_top500.parquet")

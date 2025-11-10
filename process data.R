# preprocess data
library(tidyverse)
library(duckplyr)
library(yahoofinancer)
library(lubridate)

# READ AND PROCESS SENTIMENT DATA ==============================
# read parquet file
sentiments <- duckplyr::read_parquet_duckdb(
  "data/sentiments_filtered.parquet"
)
summarise(sentiments, n = n())

# START PROCESSING ==============================

user_stats <- sentiments |>
  summarise(
    .by = user_id,
    date_range_days = as.integer(max(date) - min(date)),
    years_active = as.integer(max(date) - min(date)) / 365.25,
    count = n(),
    unique_tickers = n_distinct(ticker)
  )

print(user_stats)

# PRICING DATA DOWNLOAD AND PROCESSING ==============================
# function to get historical prices for a ticker from yahoofinancer

# download prices for popular tickers
download_prices <- FALSE
if (download_prices) {
  source("get_prices.r")
} else {
  prices_df <- duckplyr::read_parquet_duckdb(
    "data/price_history_top500.parquet"
  )
}

prices_df |> summarise(prices = n(), unique_tickers = n_distinct(ticker))

cat("Calculating 1-week percentage price changes...\n")
price_changes <- prices_df |>
  arrange(ticker, date) |>
  mutate(
    .by = ticker,
    adj_close_lead7 = lead(adj_close, 5),
    pct_change_7d = (adj_close_lead7 - adj_close) / adj_close
  ) |>
  select(ticker, date, pct_change_7d) |>
  # add another column for pct change minus the SPY change
  left_join(
    prices_df |>
      filter(ticker == "SPY") |>
      arrange(date) |>
      mutate(
        .by = ticker,
        adj_close_lead5 = lead(adj_close, 5),
        spy_pct_change_7d = (adj_close_lead5 - adj_close) / adj_close
      ) |>
      select(date, spy_pct_change_7d),
    by = "date"
  ) |>
  mutate(pct_change_minus_spy = pct_change_7d - spy_pct_change_7d) |>
  select(ticker, date, pct_change_7d, pct_change_minus_spy) #

# ticker is SPY then make pct_change_minus_spy equal to pct_change_7d
price_changes <- price_changes |>
  filter(ticker == "SPY") |>
  mutate(pct_change_minus_spy = pct_change_7d) |>
  union_all((price_changes)) |>
  # remove rows where ticker is SPY and pct_change_minus_spy is 0
  filter(!(ticker == "SPY" & pct_change_minus_spy != pct_change_7d))

# TRACK RECORD CALCULATION ==============================
# join popular_sentiments with price_changes
cat("Calculating user track records...\n")
track_record <- sentiments |>
  left_join(price_changes, by = c("ticker", "date")) |>
  #  na.omit() |>
  # add true/false column for whether pct_change_7d  has the same sign as sentiment
  mutate(
    win_absolute = bullish == (pct_change_7d > 0),
    win_vs_SPY = bullish == (pct_change_minus_spy > 0)
  ) |>
  #  select(user_id, ticker, date, bullish, win_absolute)
  select(user_id, ticker, date, bullish, win_absolute, win_vs_SPY) |>
  filter(!is.na(win_absolute) & !is.na(win_vs_SPY))

track_record_bull <- track_record |>
  filter(bullish)
track_record_bear <- track_record |>
  filter(!bullish)

# compute win rate by user_id
get_win_rate <- function(track_record, abs_or_rel = c("relative", "absolute")) {
  if (abs_or_rel == "relative") {
    user_win_rate <- summarise(
      track_record,
      .by = user_id,
      total = n(),
      wins = sum(win_vs_SPY),
      win_rate = sum(win_vs_SPY) / n()
    )
  } else {
    {
      user_win_rate <- summarise(
        track_record,
        .by = user_id,
        total = n(),
        wins = sum(win_absolute),
        win_rate = sum(win_absolute) / n()
      )
    } |>
      arrange(desc(win_rate)) |>
      filter(!is.na(win_rate))
  }
  return(user_win_rate)
}
user_win_rate <- get_win_rate(track_record, "relative")
bear_win_rate <- get_win_rate(track_record_bear, "relative")
bull_win_rate <- get_win_rate(track_record_bull, "relative")
methods_restore()
print(bull_win_rate)

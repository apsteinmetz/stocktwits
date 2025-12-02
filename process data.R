# preprocess data
library(tidyverse)
library(duckplyr)
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

# prices_df |> filter(adj_close > 10000) |> summarise(ticker)

cat("Calculating 1-week percentage price changes...\n")
price_changes <- prices_df |>
  arrange(ticker, date) |>
  mutate(
    .by = ticker,
    adj_close_lead_w = lead(adj_close, 5),
    adj_change_1w = (adj_close_lead_w - adj_close) / adj_close
  ) |>
  select(ticker, date, adj_change_1w) |>
  # add another column for pct change minus the SPY change
  left_join(
    prices_df |>
      filter(ticker == "SPY") |>
      arrange(date) |>
      mutate(
        .by = ticker,
        adj_close_lead_w = lead(adj_close, 5),
        spy_adj_change_1w = (adj_close_lead_w - adj_close) / adj_close
      ) |>
      select(date, spy_adj_change_1w),
    by = "date"
  ) |>
  mutate(pct_change_minus_spy = adj_change_1w - spy_adj_change_1w) |>
  select(ticker, date, adj_change_1w, pct_change_minus_spy) #

# ticker is SPY then make pct_change_minus_spy equal to adj_change_1w
price_changes <- price_changes |>
  filter(ticker == "SPY") |>
  mutate(pct_change_minus_spy = adj_change_1w) |>
  union_all((price_changes)) |>
  # remove rows where ticker is SPY and pct_change_minus_spy is 0
  filter(!(ticker == "SPY" & pct_change_minus_spy != adj_change_1w))

# TRACK RECORD CALCULATION ==============================
# join popular_sentiments with price_changes
cat("Calculating user track records...\n")
track_record <- sentiments |>
  left_join(price_changes, by = c("ticker", "date")) |>
  #  na.omit() |>
  # add true/false column for whether adj_change_1w  has the same sign as sentiment
  mutate(
    win_absolute = bullish == (adj_change_1w > 0),
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

# compute volatility (std dev) of returns for all tickers
price_volatility <- prices_df |>
  arrange(ticker, date) |>
  mutate(
    .by = ticker,
    daily_return = (adj_close / lag(adj_close)) - 1
  ) |>
  summarise(
    .by = ticker,
    volatility = sd(daily_return, na.rm = TRUE) * sqrt(252) # annualized volatility
  ) |>
  arrange(desc(volatility))

# plot histogram of volatilities with SPY labeled with a vertical line
gg <- price_volatility |>
  ggplot(aes(x = volatility)) +
  geom_histogram(binwidth = 0.01, fill = "lightblue", color = "black") +
  geom_vline(
    data = price_volatility |> filter(ticker == "SPY"),
    aes(xintercept = volatility),
    color = "red",
    linetype = "dashed",
    size = 1
  ) +
  # log x scale with percentage labels
  scale_x_log10(labels = scales::percent_format(accuracy = 1)) +
  labs(
    title = "Distribution of Annualized Volatility of Popular Stocktwits Tickers",
    x = "Annualized Volatility (log scale)",
    y = "Count of Tickers"
  ) +
  # label the SPY line
  annotate(
    "text",
    x = price_volatility |> filter(ticker == "SPY") |> pull(volatility) * 1.1,
    y = 3,
    label = glue::glue(
      "SPY ",
      price_volatility |>
        filter(ticker == "SPY") |>
        pull(volatility) |>
        scales::percent(accuracy = 0.1)
    ),
    # label = "SPY",
    color = "red",
    angle = 90,
    hjust = -0.5
  ) +
  theme_minimal()
print(gg)

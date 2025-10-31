# preprocess data
library(tidyverse)
library(duckplyr)
library(yahoofinancer)

# READ AND PROCESS SENTIMENT DATA ==============================
# read parquet file
load_data <- function() {
  cat("Loading sentiment data...\n")
  sentiments <- duckplyr::read_parquet_duckdb(
    "data/symbol_sentiments_30.parquet"
  )
  # sentiments <- duckplyr::read_csv_duckdb("data/symbol_sentiments_30.csv")
  summarise(sentiments, n = n())

  # use only necessary columns and convert types to save memory
  sentiments <- sentiments |>
    select(-message_id) |>
    mutate(user_id = as.integer(user_id)) |>
    # convert sentiment to boolean within duckplyr to save some space
    # not bullish  means bearish
    mutate(bullish = sentiment == 1) |>
    select(-sentiment)
  summarise(sentiments, n = n())

  # expand symbol_list into multiple rows
  sentiments <- sentiments |>
    mutate(symbol_list = gsub("\\[|\\]|'| ", "", symbol_list)) |>
    collect() |>
    separate_longer_delim(symbol_list, delim = ",") |>
    as_duckdb_tibble() |>
    # retain only rows with valid symbols (A-Z, 1-5 characters)
    filter(grepl("^[A-Z]{1,5}$", symbol_list)) |>
    rename(date = created_at, ticker = symbol_list)

  summarise(sentiments, n = n())
  return(sentiments)
}
sentiments <- load_data()
summarise(sentiments, n = n())
cat("Limiting data to active users and popular tickers...\n")
# limit rows to users with at least 200 entries over at least 180 days in the most popular tickers
MIN_POSTS <- 200
MIN_DAYS <- 180 # were they active over at least 6 months
NUM_TICKERS <- 500
sentiments <- sentiments |>
  # when people post multiple messages about the same ticker on the same day, filter out.
  distinct(user_id, ticker, date, bullish, .keep_all = TRUE) |>
  # only include user_ids with at least 200 entries
  summarise(
    .by = user_id,
    date_range = max(date) - min(date),
    count = n()
  ) |>
  filter(count >= MIN_POSTS, date_range >= MIN_DAYS) |>
  select(user_id) |>
  inner_join(sentiments, by = "user_id")

summarise(sentiments, n = n())

# include only the most popular tickers
sentiments <- sentiments |>
  summarise(.by = ticker, count = n()) |>
  arrange(desc(count)) |>
  head(NUM_TICKERS) |>
  select(-count) |>
  inner_join(sentiments, by = "ticker")

summarise(sentiments, n = n())

# PRICING DATA DOWNLOAD AND PROCESSING ==============================
# function to get historical prices for a ticker from yahoofinancer
ticker_hist <- function(ticker, start_date, end_date, interval_pd = "1d") {
  ticker_obj <- Ticker$new(ticker)
  ticker_obj$get_history(
    interval = interval_pd,
    start = start_date,
    end = end_date + 7 # buffer to include
  )
}


# download prices for popular tickers
download_prices <- FALSE
if (download_prices) {
  cat("Downloading price data for popular tickers...\n")
  # get date range for popular tickers
  date_ranges <- sentiments |>
    summarise(
      .by = ticker,
      count = n(),
      start_date = min(date),
      end_date = max(date)
    ) |>
    #collect() |>
    #left_join(popular_tickers, by = "ticker") |>
    arrange(desc(count))
  all_prices <- list()
  start_row <- 1
  for (i in start_row:nrow(date_ranges)) {
    ticker <- date_ranges$ticker[i]
    start_date <- date_ranges$start_date[i]
    end_date <- date_ranges$end_date[i]
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
    prices <- prices |> unnest(adj_close)
    all_prices[[ticker]] <- prices
  }
  # combine all prices into a single data frame
  # with ticker as a column
  prices_df2 <- bind_rows(all_prices, .id = "ticker") |>
    as_tibble() |>
    mutate(date = as.Date(date))
  # save to parquet
  # there won't 500 tickers due to failed ticker searches
  duckplyr::compute_parquet(prices_df, "price_history_top500.parquet")
} else {
  prices_df <- duckplyr::read_parquet_duckdb("price_history_top500.parquet")
}


prices_df |>
  summarise(n = n())

# find number of unique tickers
prices_df |>
  summarise(n = n_distinct(ticker))

cat("Calculating 5-day percentage price changes...\n")
prices_change <- prices_df |>
  arrange(ticker, date) |>
  mutate(
    .by = ticker,
    adj_close_lead5 = lead(adj_close, 5),
    pct_change_5d = (adj_close_lead5 - adj_close) / adj_close
  ) |>
  select(ticker, date, pct_change_5d) |>
  # add another column for pct change minus the SPY change
  left_join(
    prices_df |>
      filter(ticker == "SPY") |>
      arrange(date) |>
      mutate(
        .by = ticker,
        adj_close_lead5 = lead(adj_close, 5),
        spy_pct_change_5d = (adj_close_lead5 - adj_close) / adj_close
      ) |>
      select(date, spy_pct_change_5d),
    by = "date"
  ) |>
  mutate(pct_change_minus_spy = pct_change_5d - spy_pct_change_5d) |>
  select(ticker, date, pct_change_5d, pct_change_minus_spy) #

# ticker is SPY then make pct_change_minus_spy equal to pct_change_5d
prices_change <- prices_change |>
  filter(ticker == "SPY") |>
  mutate(pct_change_minus_spy = pct_change_5d) |>
  union_all((prices_change)) |>
  # remove rows where ticker is SPY and pct_change_minus_spy is 0
  filter(!(ticker == "SPY" & pct_change_minus_spy != pct_change_5d))


# mutate(
#   pct_change_minus_spy = ifelse(
#     ticker == "SPY",
#     pct_change_5d,
#     pct_change_minus_spy
#   )

# TRACK RECORD CALCULATION ==============================
# join popular_sentiments with prices_change
cat("Calculating user track records...\n")
track_record <- sentiments |>
  left_join(prices_change, by = c("ticker", "date")) |>
  filter(!is.na(pct_change_5d)) |>
  # add true/false column for whether pct_change_5d  has the same sign as sentiment
  mutate(
    win_absolute = bullish == (pct_change_5d > 0) #,
    # win_vs_SPY = bullish == (pct_change_minus_spy > 0)
  ) |>
  select(user_id, ticker, date, bullish, win_absolute)
# select(user_id, ticker, date, bullish, win_absolute, win_vs_SPY)

# compute win rate by user_id
user_win_rate <- track_record |>
  summarise(
    .by = user_id,
    total = n(),
    wins_absolute = sum(win_absolute),
    # wins_vs_SPY = sum(win_vs_SPY),
    # win_rate_vs_spy = sum(win_vs_SPY) / n(),
    win_rate = sum(win_absolute) / n()
  ) |>
  arrange(desc(win_rate))

print(user_win_rate)

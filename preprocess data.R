# preprocess data
library(tidyverse)
library(duckplyr)
library(lubridate)

# READ AND PROCESS SENTIMENT DATA ==============================
# read parquet file
cat("Loading sentiment data...\n")
sentiments <- duckplyr::read_parquet_duckdb(
  "data/symbol_sentiments_*.parquet"
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

# expand python-like symbol_list into multiple rows
sentiments <- sentiments |>
  mutate(symbol_list = gsub("\\[|\\]|'| ", "", symbol_list)) |>
  collect() |>
  separate_longer_delim(symbol_list, delim = ",") |>
  as_duckdb_tibble() |>
  # retain only rows with valid symbols (A-Z, 1-5 characters)
  filter(grepl("^[A-Z]{1,5}$", symbol_list)) |>
  rename(date = created_at, ticker = symbol_list)

summarise(sentiments, n = n())

# when people post multiple messages about the same ticker on the same day, filter out.
sentiments <- sentiments |>
  distinct(user_id, ticker, date, bullish, .keep_all = TRUE) |>
  # save cleaned data
  compute_parquet("data/sentiments_cleaned.parquet")

summarise(sentiments, n = n())

# SAVE TICKER POST COUNTS ==============================
cat("Computing ticker post counts...\n")
ticker_count <- sentiments |>
  summarise(.by = ticker, post_count = n()) |>
  arrange(desc(post_count)) |>
  mutate(rank = row_number())
compute_parquet(ticker_count, "data/ticker_post_counts.parquet")


# LIMIT DATA SIZE ==============================
cat("Limiting data to active users and popular tickers...\n")
# limit rows to users with at least 200 entries over at least 180 days in the most popular tickers

MIN_POSTS <- 100
MIN_DAYS <- 180 # were they active over at least 6 months
NUM_TICKERS <- 500

# determine the most popular tickers
popular_tickers <- sentiments |>
  summarise(
    .by = ticker,
    count = n(),
    start_date = min(date),
    end_date = max(date)
  ) |>
  arrange(desc(count)) |>
  head(NUM_TICKERS)

# save popular tickers to download price data later
compute_parquet(popular_tickers, "data/popular_tickers.parquet")

# include only the most popular tickers
sentiments <- popular_tickers |>
  select(ticker) |>
  inner_join(sentiments, by = "ticker")

summarise(sentiments, n = n())

# only include user_ids with at least 200 entries
sentiments <- sentiments |>
  summarise(
    .by = user_id,
    date_range = max(date) - min(date),
    count = n()
  ) |>
  inner_join((sentiments), by = "user_id")
summarise(sentiments, n = n())

sentiments <- sentiments |>
  filter(count >= MIN_POSTS, date_range >= MIN_DAYS) |>
  select(-date_range, -count)

summarise(sentiments, n = n())

compute_parquet(sentiments, "data/sentiments_filtered.parquet")

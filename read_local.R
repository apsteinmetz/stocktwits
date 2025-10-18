# read stocktwits with duckdb and dplyr
library(tidyverse)
library(duckplyr)

sentiment_data <- read_csv_duckdb('data/symbol_sentiments_*.csv')
# read parquet file with duckplyr
sentiment_data <- read_parquet_duckdb('data/sentiment_data.parquet')


cat("✓ Connected to all CSV files via DuckDB!\n\n")

# Now we can use dplyr syntax - it runs on DuckDB, not in R!
cat("Total rows across all files:\n")
num_rows <- sentiment_data |>
  summarise(n = n()) |>
  collect() |>
  print()

cat("\nDate range:\n")
sentiment_data |>
  summarise(
    earliest = min(created_at),
    latest = max(created_at)
  ) |>
  collect() |>
  print()

cat("\nSample data:\n")
sentiment_data |>
  select(message_id, user_id, created_at, sentiment, symbol_list) |>
  head(5) |>
  collect()
# Parse the symbol_list column to extract individual symbols
# Remove brackets and quotes, then split by comma
# Reconnect if needed

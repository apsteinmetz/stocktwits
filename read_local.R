# read stocktwits with duckdb and dplyr
library(tidyverse)
library(duckplyr)

all_data <- read_csv_duckdb('data/symbol_sentiments_*.csv')

cat("✓ Connected to all CSV files via DuckDB!\n\n")

# Now we can use dplyr syntax - it runs on DuckDB, not in R!
cat("Total rows across all files:\n")
num_rows <- all_data |>
  summarise(n = n()) |>
  collect() |>
  print()

cat("\nDate range:\n")
all_data |>
  summarise(
    earliest = min(created_at),
    latest = max(created_at)
  ) |>
  collect() |>
  print()

cat("\nSample data:\n")
all_data |>
  select(message_id, user_id, created_at, sentiment, symbol_list) |>
  head(5) |>
  collect()
# Parse the symbol_list column to extract individual symbols
# Remove brackets and quotes, then split by comma
# Reconnect if needed
if (!exists("all_data")) {
  con <- DBI::dbConnect(duckdb::duckdb())
  all_data <- tbl(
    con,
    sql("SELECT * FROM read_csv_auto('data/symbol_sentiments_*.csv')")
  )
}

parsed_data <- tbl(con, "parsed_symbols")

# Test it - should be super fast since it's still lazy
parsed_data |>
  head(10) |>
  collect()

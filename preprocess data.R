# preprocess data
library(tidyverse)
library(duckplyr)

# read parquet file
sentiments <- duckplyr::read_parquet_duckdb("data/symbol_sentiments_30.parquet")
sentiments <- sentiments |>
  select(-message_id)

short_list <- sentiments |>
  head(1000)


sentiments <- sentiments |>
  mutate(symbol_list = gsub("\\[|\\]|'| ", "", symbol_list)) |>
  collect() |>
  separate_longer_delim(symbol_list, delim = ",") |>
  as_duckplyr_df() |>
  # retain only rows with valid symbols (A-Z, 1-5 characters)
  filter(grepl("^[A-Z]{1,5}$", symbol_list))

popular_tickers <- sentiments |>
  summarise(.by = symbol_list, count = n()) |>
  arrange(desc(count)) |>
  filter(count >= 1000) |>
  collect()

tickers <- sentiments |>
  distinct(symbol_list) |>
  arrange(symbol_list)

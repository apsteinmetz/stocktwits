# preprocess data
library(tidyverse)
library(duckplyr)

# read parquet file
sentiments <- duckplyr::read_parquet_duckdb("data/symbol_sentiments_30.parquet")
sentiments <- sentiments |>
  select(-message_id)

short_list <- sentiments |>
  head(1000) |>
  as_tibble()

library(tidyverse)
library(duckplyr)

con <- duckplyr:::get_default_duckdb_connection()
DBI::dbExecute(con, "INSTALL httpfs;")
DBI::dbExecute(con, "LOAD httpfs;")
DBI::dbExecute(con, "SET s3_region='us-west-2';")
# DBI::dbExecute(con, "SET s3_url_style='path';")

BASE_URL <- "s3://stocktwits-nyu"
CSV_URL <- paste0(BASE_URL, "/dataset/v1/data/csv")

target_path <- paste0(CSV_URL, "/symbol_sentiments/symbol_sentiments_*.csv")
# target_path <- paste0(CSV_URL, "/sentiments/sentiments_*.csv")
sentiment_data <- duckplyr::read_csv_duckdb(path = target_path)

sentiment_data

# Query directly from S3 to extract unique symbols
# Use the S3 path that sentiment_data was loaded from

sentiment_path <- target_path

# Create a table with unique symbols directly from S3
query <- sprintf("
  CREATE OR REPLACE TEMP TABLE unique_symbols AS
  SELECT DISTINCT unnest(
    string_split(
      regexp_replace(symbol_list, '[\\[\\]'']', '', 'g'),
      ', '
    )
  ) as symbol
  FROM read_csv_auto('%s', 
    filename=false,
    union_by_name=true
  )
  WHERE symbol_list IS NOT NULL
  ORDER BY symbol
", sentiment_path)

DBI::dbExecute(con, query)

# Check the result
result <- DBI::dbGetQuery(con, "SELECT COUNT(*) as total_symbols FROM unique_symbols")
cat(sprintf("✓ Total unique symbols: %d\n\n", result$total_symbols))

tickers <- DBI::dbGetQuery(con, "SELECT * FROM unique_symbols")
tickers <- tickers |> 
  #filter to include only 4-letter symbols
  filter(grepl("^[A-Z]{4}$", symbol))

DBI::dbDisconnect(con, shutdown = TRUE)

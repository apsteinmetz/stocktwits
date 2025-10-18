library(tidyverse)
library(duckplyr)

con <- DBI::dbConnect(duckdb::duckdb())
DBI::dbExecute(con, "INSTALL httpfs;")
DBI::dbExecute(con, "LOAD httpfs;")
DBI::dbExecute(con, "SET s3_region='us-west-2';")
DBI::dbExecute(con, "SET s3_url_style='path';")

BASE_URL <- "s3://stocktwits-nyu"
CSV_URL <- paste0(BASE_URL, "/dataset/v1/data/csv")

target_path <- paste0(CSV_URL, "/symbol_sentiments/symbol_sentiments_*.csv")
sentiment_data <- read_csv_duckdb(path = target_path)

sentiment_data

# DBI::dbDisconnect(con, shutdown = TRUE)

# https://github.com/Jaxingjili/StockTwits-from-2008-to-2022/blob/main/dataset_access.md
library(tidyverse)
library(duckplyr)
library(DBI)


# Create DuckDB connection (returns a connection object, not driver)
create_con <- function() {
  con <- DBI::dbConnect(duckdb::duckdb())

  # Install and load the httpfs extension for S3 access
  DBI::dbExecute(con, "INSTALL httpfs;")
  DBI::dbExecute(con, "LOAD httpfs;")

  # Configure S3 settings for the public bucket
  DBI::dbExecute(con, "SET s3_region='us-west-2';") # Correct region for stocktwits-nyu bucket
  DBI::dbExecute(con, "SET s3_url_style='path';")
  return(con)
}

con <- create_con()
# Define S3 paths
BASE_URL <- "s3://stocktwits-nyu"
CSV_URL <- paste0(BASE_URL, "/dataset/v1/data/csv")

# Read sample data from feature_wo_messages
sentiment_path <- paste0(CSV_URL, "/symbol_sentiments/symbol_sentiments_*.csv")
# all_data <- DBI::dbGetQuery(con, sprintf("SELECT * FROM '%s' LIMIT 10", sentiment_path))
all_data <- read_csv_duckdb('data/symbol_sentiments_*.csv')

print("✓ Successfully connected to StockTwits S3 bucket!")
print("\n=== Sample Data from Feature Without Messages ===")
print(all_data)

# Show structure
print("\n=== Data Structure ===")
explain(all_data)

DBI::dbDisconnect(con, shutdown = TRUE)

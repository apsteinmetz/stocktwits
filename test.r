# Install and load necessary packages if you haven't already
# install.packages(c("duckplyr", "dplyr", "duckdb"))

library(tidyverse)
library(duckplyr)

# --- 1. Configuration: Set S3 path and AWS Region ---

create_con <- function() {
  con <- DBI::dbConnect(duckdb::duckdb())
  DBI::dbExecute(con, "INSTALL httpfs;")
  DBI::dbExecute(con, "LOAD httpfs;")
  DBI::dbExecute(con, "SET s3_region='us-west-2';")
  DBI::dbExecute(con, "SET s3_url_style='path';")
  return(con)
}

BASE_URL <- "s3://stocktwits-nyu"
CSV_URL <- paste0(BASE_URL, "/dataset/v1/data/csv")

con <- create_con()

# IMPORTANT: Replace these placeholders with your actual S3 details
# S3_BUCKET_PATH <- "s3://your-bucket-name/path/to/your_file.csv"
S3_BUCKET_PATH <- paste0(CSV_URL, "/symbol_sentiments/symbol_sentiments_*.csv") # Example S3 path with wildcard
AWS_REGION <- "us-west-2" # Your S3 bucket's region

# --- 2. Configure DuckDB for S3 Access (via httpfs extension) ---

# DuckDB's httpfs extension is required to read from S3.
# We execute SQL commands on duckplyr's internal DuckDB connection.

# Get the default DuckDB connection used by duckplyr
con <- duckplyr:::get_default_duckdb_connection()

# 1. Install and Load the httpfs extension
DBI::dbExecute(con, "INSTALL httpfs;")
DBI::dbExecute(con, "LOAD httpfs;")

# 2. Set the AWS Region
# Note: For public buckets, setting the region might be enough.
DBI::dbExecute(con, paste0("SET s3_region = '", AWS_REGION, "';"))

# Optional: For private S3 buckets, you would need to set credentials.
# DuckDB will automatically look for credentials in standard AWS locations
# (e.g., environment variables, ~/.aws/credentials) if you don't set them here.
# If you must set them explicitly, uncomment and replace:
# DBI::dbExecute(con, "SET s3_access_key_id = 'YOUR_ACCESS_KEY_ID';")
# DBI::dbExecute(con, "SET s3_secret_access_key = 'YOUR_SECRET_ACCESS_KEY';")

# --- 3. Read the CSV file as a lazy duckplyr data frame ---

# Use read_csv_duckdb() to create a lazy data frame from the S3 URI.
# The data is not loaded into R's memory yet; only the schema is read.
# Use the 'options' argument to pass any DuckDB specific CSV reader options (e.g., delimiter, header, etc.)
sentiment_data <- read_csv_duckdb(
  path = S3_BUCKET_PATH,
  options = list(header = TRUE, delim = ",", auto_detect = TRUE)
)
sentiment_data

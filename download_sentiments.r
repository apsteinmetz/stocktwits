# Load required libraries
library(tidyverse)
library(duckdb)
library(duckplyr)

# Create DuckDB connection with S3 support
con <- dbConnect(duckdb())

# Install and load S3 extension for DuckDB
dbExecute(con, "INSTALL httpfs;")
dbExecute(con, "LOAD httpfs;")
dbExecute(con, "SET s3_region='us-west-2';")

cat("✓ S3 region set to us-west-2\n")
cat("✓ DuckDB connection established with S3 support\n")

BASE_URL <- "https://stocktwits-nyu.s3.amazonaws.com/dataset/v1/data/csv/"
DATASET <- "symbol_sentiments"
STEM_URL <- paste0(BASE_URL, DATASET, "/", DATASET, "_")

get_sentiment_file <- function(file_num) {
  file_num_str <- sprintf("%02d", file_num)
  csv_url <- paste0(STEM_URL, file_num_str, ".csv")
  parquet_filename <- paste0("data/", DATASET, "_", file_num_str, ".parquet")
  # Read CSV directly from S3 using DuckDB's HTTP support
  query <- paste0("SELECT * FROM read_csv_auto('", csv_url, "')")
  # Execute query and get as duckplyr dataframe
  df <- dbGetQuery(con, query) |>
    as_duckdb_tibble() |>
    compute_parquet(parquet_filename)
  print(paste(
    parquet_filename,
    format(file.info(parquet_filename)$size),
    "bytes"
  ))
}

# there are 34 csv files in the S3 bucket for symbol_sentiments
walk(0:33, get_sentiment_file)


# ALTERNATIVELY
# Function to download and process symbol_sentiments files using duckplyr
# with more robust error handling
download_and_save_symbol_files <- function(file_numbers) {
  results <- list()

  for (file_num in file_numbers) {
    file_num_str <- sprintf("%02d", file_num)
    csv_url <- paste0(STEM_URL, file_num_str, ".csv")
    parquet_filename <- paste0("data/", DATASET, "_", file_num_str, ".parquet")

    cat("Processing file", file_num_str, "...\n")

    tryCatch(
      {
        # Read CSV directly from S3 using DuckDB's HTTP support
        query <- paste0("SELECT * FROM read_csv_auto('", csv_url, "')")

        # Execute query and get as duckplyr dataframe
        df <- dbGetQuery(con, query) |>
          as_duckdb_tibble()

        # Save as parquet using duckplyr::compute_parquet()
        df |>
          compute_parquet(parquet_filename)

        # Get file info
        file_info <- file.info(parquet_filename)
        rows <- nrow(df)
        size_mb <- round(file_info$size / 1024^2, 1)

        cat(
          "  ✓ Saved:",
          parquet_filename,
          "| Rows:",
          rows,
          "| Size:",
          size_mb,
          "MB\n"
        )

        results[[file_num_str]] <- list(
          success = TRUE,
          rows = rows,
          size_mb = size_mb,
          filename = parquet_filename
        )
      },
      error = function(e) {
        cat("  ✗ Failed to process file", file_num_str, ":", e$message, "\n")
        results[[file_num_str]] <- list(success = FALSE, error = e$message)
      }
    )
  }

  return(results)
}
cat("Downloading symbol_sentiments files...\n")
file_batch <- 0:40
results <- download_and_save_symbol_files(file_batch)
dbDisconnect(con, shutdown = TRUE)

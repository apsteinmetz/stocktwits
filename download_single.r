# First, let's create a function to systematically download multiple files
# Based on the naming pattern, there are likely multiple numbered files

# Function to download and convert a single symbol_sentiments file
download_symbol_sentiment_file <- function(
  file_number,
  base_url = "https://stocktwits-nyu.s3.amazonaws.com/dataset/v1/data/csv/symbol_sentiments/"
) {
  # Format file number with leading zeros (assuming 2-digit format)
  file_num_str <- sprintf("%02d", file_number)
  file_url <- paste0(base_url, "symbol_sentiments_", file_num_str, ".csv")
  output_filename <- paste0("symbol_sentiments_", file_num_str, ".parquet")

  cat("Attempting to download:", file_url, "\n")

  # Try to download
  response <- GET(file_url)

  if (status_code(response) == 200) {
    cat("✓ Successfully downloaded file", file_num_str, "\n")

    # Read CSV content
    csv_content <- content(response, "text", encoding = "UTF-8")
    data <- read_csv(csv_content, show_col_types = FALSE)

    # Save as parquet
    write_parquet(data, output_filename)

    file_info <- file.info(output_filename)
    cat(
      "  Saved as:",
      output_filename,
      " | Rows:",
      nrow(data),
      " | Size:",
      round(file_info$size / 1024^2, 1),
      "MB\n"
    )

    return(list(
      success = TRUE,
      rows = nrow(data),
      size_mb = round(file_info$size / 1024^2, 1)
    ))
  } else {
    cat(
      "✗ File",
      file_num_str,
      "not found (status:",
      status_code(response),
      ")\n"
    )
    return(list(success = FALSE, rows = 0, size_mb = 0))
  }
}

# Test with a few file numbers to see what's available
cat("Testing file availability:\n")
test_results <- list()
for (i in 1:5) {
  test_results[[i]] <- download_symbol_sentiment_file(i)
  Sys.sleep(0.5) # Small delay to be respectful to the server
}

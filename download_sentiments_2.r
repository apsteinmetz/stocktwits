# Load required libraries
library(tidyverse)
library(duckplyr)

db_exec("INSTALL httpfs;")
db_exec("LOAD httpfs;")
db_exec("SET s3_region='us-west-2';")

base_url <- "https://stocktwits-nyu.s3.amazonaws.com/dataset/v1/data/csv/"
dataset <- "symbol_sentiments"
file_nums <- sprintf("%02d", 0:33)
S3_file_list <- paste0(BASE_URL, DATASET, "/", DATASET, "_", file_nums, ".csv")
local_file_list <- paste0("data/", DATASET, "_", file_nums, ".parquet")

for (n in 1:length(S3_file_list)) {
  df <- read_csv_duckdb(S3_file_list[n]) |>
    compute_parquet(local_file_list[n])
}

# Read all parquet files and off you go - instantly!
sentiments <- read_parquet_duckdb(local_file_list)
sentiments |> summarise(total_rows = n())

#start timer
start_time <- Sys.time()
df <- read_csv_duckdb(S3_file_list[n]) |>
  compute_parquet(local_file_list[n])
#end timer
end_time <- Sys.time()
#display time taken
time_taken <- end_time - start_time
print(paste("Time taken to read and save file", file_nums[n], ":", time_taken))

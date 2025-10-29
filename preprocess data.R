# preprocess data
library(tidyverse)
library(duckplyr)
library(yahoofinancer)

# read parquet file
#sentiments <- duckplyr::read_parquet_duckdb("data/symbol_sentiments_30.parquet")
sentiments <- duckplyr::read_csv_duckdb("data/symbol_sentiments_30.csv")
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
  filter(grepl("^[A-Z]{1,5}$", symbol_list)) |> 
  rename(date = created_at,ticker = symbol_list)

sentiments |> 
  summarise(n = n())
sentiments |> 
  # get date range
  summarise(min_date = min(date), max_date = max(date))
popular_tickers <- sentiments |>
  summarise(.by = ticker, count = n()) |>
  arrange(desc(count)) |>
  head(100) |>
  collect()

tickers <- sentiments |>
  distinct(symbol_list) |>
  arrange(symbol_list)

tickers |> 
  summarise(n = n())

# function to download prices from yfinancer
get_prices <- function(ticker, start_date, end_date) {
  prices <- yfinance::yf_get(
    symbol = ticker,
    start_date = start_date,
    end_date = end_date,
    interval = "1d",
    auto_adjust = TRUE
  )
  prices <- prices |>
    select(date, open, high, low, close, volume) |>
    mutate(ticker = ticker)
  return(prices)
}


duckplyr::methods_restore()
ticker_hist <- function(ticker,start_date,end_date,interval_pd = "1wk"){
  ticker_obj <- Ticker$new(ticker)
  ticker_obj$get_history(
    interval = interval_pd,
    start = start_date,
    end = end_date + 7 # buffer to include
  )
}

short_sentiments <- sentiments |>
  filter(ticker %in% popular_tickers$ticker) |>
  collect() 

# get date range for popular tickers
date_ranges <- short_sentiments |>
  summarise(.by = ticker,
            start_date = min(date),
            end_date = max(date)) |>
  collect() |>
  left_join(popular_tickers, by = "ticker") |> 
  arrange(desc(count))

# download prices for popular tickers
all_prices <- list()
for (i in 1:nrow(date_ranges)) {
  ticker <- date_ranges$ticker[i]
  start_date <- date_ranges$start_date[i]
  end_date <- date_ranges$end_date[i]
  print(paste("Downloading prices for", ticker, "from", start_date, "to", end_date))
  prices <- ticker_hist(ticker,start_date,end_date)
  all_prices[[ticker]] <- prices
}

# combine all prices into a single data frame
# with ticker as a column
prices_df <- bind_rows(all_prices, .id = "ticker") |> 
  as_tibble() |> 
  unnest(adj_close) |> 
  mutate(date = as.Date(date))

prices_df |>
  summarise(n = n())
# save to parquet
duckplyr::compute_parquet(prices_df, "price_history_1.parquet")




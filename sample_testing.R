library(tidyverse)
library(duckplyr)
library(dbplyr)

date_range <- sentiments |>
  summarise(min_date = min(date), max_date = max(date)) |>
  collect() |>
  as_tibble()

# create a  tibble of 3-month date range windows for the sentiment data
create_date_windows <- function(start_date, end_date, window_days = 90) {
  date_windows <- tibble(
    window_start = seq.Date(
      from = start_date,
      to = end_date - window_days,
      by = "1 month"
    )
  ) |>
    mutate(window_end = window_start + window_days) |>
    collect() |>
    rowid_to_column("window")
  return(date_windows)
}

date_windows <- create_date_windows(date_range$min_date, date_range$max_date)

# compute win rate by user_id
get_win_rate <- function(
  track_record,
  abs_or_rel = c("relative", "absolute")
) {
  if (abs_or_rel == "relative") {
    user_win_rate <- summarise(
      track_record,
      .by = user_id,
      total = n(),
      wins = sum(win_vs_SPY),
      win_rate = sum(win_vs_SPY) / n()
    )
  } else {
    {
      user_win_rate <- summarise(
        track_record,
        .by = user_id,
        total = n(),
        wins = sum(win_absolute),
        win_rate = sum(win_absolute) / n()
      )
    } |>
      arrange(desc(win_rate)) |>
      filter(!is.na(win_rate))
  }
  return(user_win_rate)
}

# make trade history for the selected date window
all_windows <- list()
# for (window_index in nrow(date_windows)) {
for (window_index in 1:nrow(date_windows)) {
  start_date <- date_windows$window_start[window_index]
  end_date <- date_windows$window_end[window_index]
  print(paste(
    "Processing window",
    window_index,
    "of",
    nrow(date_windows),
    "from",
    start_date,
    "to",
    end_date
  ))
  test_end_date <- end_date + days(30)
  short_sentiments_sample <- sentiments |>
    filter(date >= start_date, date <= end_date)

  short_record <- track_record |>
    filter(date >= start_date, date <= end_date)

  # track_record_bull <- short_record |>
  # filter(bullish)
  # track_record_bear <- short_record |>
  #  filter(!bullish)

  user_win_rate <- get_win_rate(track_record, "relative")
  # bear_win_rate <- get_win_rate(track_record_bear, "relative")
  # bull_win_rate <- get_win_rate(track_record_bull, "relative")
  # methods_restore()
  # print(bull_win_rate)

  significant_posters <- get_significanct_posters(user_win_rate)

  short_sentiments_test <- sentiments |>
    filter(date > end_date, date <= test_end_date) |>
    inner_join(
      select(significant_posters, user_id, alpha_direction),
      by = "user_id"
    ) |>
    mutate(
      buy_or_sell = as.integer((as.integer(bullish) * 2 - 1) * alpha_direction)
    ) |>
    # add ticker price change over next 5 days
    # filter(ticker == "SPY") |>
    left_join(
      select(price_changes, ticker, date, pct_change_5d),
      by = c("ticker", "date")
    ) |>
    mutate(gain_or_loss = pct_change_5d * buy_or_sell) # |>
  # as_tibble()
  all_windows[[window_index]] <- collect(short_sentiments_test)
}

for (n in 1:length(all_windows)) {
  print(n)
  all_windows[[n]] <- as_tibble(all_windows[[n]])
}

all_windows_df <- all_windows |>
  bind_rows(all_windows, .id = "window") |>
  filter(!is.na(gain_or_loss)) |>
  mutate(window = as.integer(window)) # |>
compute_parquet("data/skill_90_30.parquet")

all_windows_df <- read_parquet_duckdb("data/skill_90_30.parquet")

# test results with a smaller trade volume
# limit trades first 100 signals per month
all_windows_df_sample <- all_windows_df |>
  # limit windows to after stocktwits has more than 1000 valid posts per window
  filter(window > 30) |>
  as_tibble() |>
  slice_head(by = window, n = 100)

result_history <- all_windows_df_sample |>
  summarise(
    .by = window,
    start_date = min(date),
    median_gain = median(gain_or_loss),
    mean_gain = mean(gain_or_loss),
    trades = n()
  ) |>
  as_tibble() |>
  arrange(window)
methods_restore()

# plot median gain by window
result_history |>
  ggplot(aes(x = start_date, y = median_gain)) +
  geom_point(color = "red", size = 2) +
  geom_hline(yintercept = 0, linewidth = 1) +
  labs(
    title = "Median Gain in Month After 3-Month Sentiment Sample",
    x = "Window Start Date",
    y = "Median Gain/Loss per Trade"
  ) +
  theme_minimal()

# plot number of trades over time
result_history |>
  ggplot(aes(x = start_date, y = trades)) +
  geom_line(color = "blue", size = 2) +
  labs(
    title = "Number of Trades in Month After 3-Month Sentiment Sample",
    x = "Window Start Date",
    y = "Number of Trades"
  ) +
  scale_y_continuous(labels = scales::comma) +
  theme_minimal()


all_windows_df |>
  summarise(
    .by = c(bullish, alpha_direction),
    trades = n(),
    median_gain = median(gain_or_loss)
  )
all_windows_df |>
  summarise(
    count = n(),
    median_gain = median(gain_or_loss),
    mean_gain = mean(gain_or_loss)
  )


all_windows_df_sample |>
  summarise(
    .by = c(bullish, alpha_direction),
    trades = n(),
    median_gain = median(gain_or_loss)
  )

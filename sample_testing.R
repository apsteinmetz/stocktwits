library(dplyr)
library(lubridate)

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
    mutate(window_end = window_start + window_days)
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
  all_windows[[window_index]] <- short_sentiments_test
}

all_windows_df <- bind_rows(all_windows, .id = "window") |>
  filter(!is.na(gain_or_loss)) |>
  compute_parquet("data/skill_90_30.parquet")

all_windows_df |> summarise(.by = window, median_gain = median(gain_or_loss))

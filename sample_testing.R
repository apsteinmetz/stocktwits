library(dplyr)
library(lubridate)

# create a  tibble of 3-month date range windows for the sentiment data
create_date_windows <- function(start_date, end_date, window_months = 3) {
  date_windows <- tibble(
    window_start = seq.Date(
      from = start_date,
      to = end_date + months(window_months),
      by = "1 month"
    )
  ) |>
    mutate(window_end = window_start + months(window_months) + days(7))

  return(date_windows)
}
date_range <- sentiments |>
  summarise(min_date = min(date), max_date = max(date))

date_windows <- create_date_windows(date_range$min_date, date_range$max_date)

window_index <- 140
start_date <- date_windows$window_start[window_index]
end_date <- date_windows$window_end[window_index]
test_end_date <- end_date + month(1)
short_sentiments_sample <- sentiments |>
  filter(date >= start_date, date <= end_date)

date_windows[window_index, ]

short_record <- track_record |>
  filter(date >= start_date, date <= end_date)

track_record_bull <- short_record |>
  filter(bullish)
track_record_bear <- short_record |>
  filter(!bullish)

# compute win rate by user_id
get_win_rate <- function(track_record, abs_or_rel = c("relative", "absolute")) {
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
user_win_rate <- get_win_rate(track_record, "relative")
bear_win_rate <- get_win_rate(track_record_bear, "relative")
bull_win_rate <- get_win_rate(track_record_bull, "relative")
methods_restore()
print(bull_win_rate)

significant_posters <- get_significanct_posters(user_win_rate)

short_sentiments_test <- sentiments |>
  filter(date > end_date, date <= test_end_date) |>
  inner_join(
    select(significant_posters, user_id, alpha_direction),
    by = "user_id"
  ) |>
  mutate(buy_or_sell = (as.integer(bullish) * 2 - 1) * alpha_direction) |>
  # add ticker price change over next 5 days
  filter(ticker == "SPY") |>

  left_join(
    select(price_changes, ticker, date, pct_change_5d),
    by = c("ticker", "date")
  )

# prices missing. why?

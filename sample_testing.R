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
short_sentiments <- sentiments |>
  filter(date >= start_date, date <= end_date)
date_windows[window_index, ]
short_sentiments

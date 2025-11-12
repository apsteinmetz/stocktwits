library(tidyverse)
library(duckplyr)

# load sentiment data filtered for popular tickers and active users
sentiments <- read_parquet_duckdb("data/sentiments_filtered.parquet")
# load price change data for most popular tickers
prices_df <- read_parquet_duckdb("data/price_history_top500.parquet")

date_range <- sentiments |>
  summarise(min_date = min(date), max_date = max(date)) |>
  collect() |>
  as_tibble()

# create a  tibble of 3-month date range windows for the sentiment data
create_date_windows <- function(start_date, end_date, window_days = 90) {
  methods_restore()
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
  methods_restore()
  return(date_windows)
}
date_windows <- create_date_windows(date_range$min_date, date_range$max_date)
prices_df |> summarise(prices = n(), unique_tickers = n_distinct(ticker))

cat("Calculating 7-day percentage price changes...\n")
price_changes <- prices_df |>
  arrange(ticker, date) |>
  mutate(
    .by = ticker,
    adj_close_lead7 = lead(adj_close, 5),
    pct_change_7d = (adj_close_lead7 - adj_close) / adj_close
  ) |>
  # select(ticker, date, pct_change_7d) |>
  # add another column for pct change minus the SPY change
  left_join(
    prices_df |>
      filter(ticker == "SPY") |>
      arrange(date) |>
      mutate(
        .by = ticker,
        adj_close_lead7 = lead(adj_close, 5),
        spy_pct_change_7d = (adj_close_lead7 - adj_close) / adj_close
      ) |>
      select(date, spy_pct_change_7d),
    by = "date"
  ) |>
  mutate(pct_change_minus_spy = pct_change_7d - spy_pct_change_7d) |>
  select(
    ticker,
    date,
    adj_close,
    adj_close_lead7,
    pct_change_7d,
    pct_change_minus_spy
  )

# ticker is SPY then make pct_change_minus_spy equal to pct_change_7d
price_changes <- price_changes |>
  filter(ticker == "SPY") |>
  mutate(pct_change_minus_spy = pct_change_7d) |>
  union_all((price_changes)) |>
  # remove rows where ticker is SPY and pct_change_minus_spy is 0
  filter(!(ticker == "SPY" & pct_change_minus_spy != pct_change_7d)) |>
  collect()

price_changes |>
  compute_parquet("data/price_changes_7d.parquet")


# TRACK RECORD CALCULATION ==============================
# join popular_sentiments with price_changes
cat("Calculating user track records...\n")
track_record <- sentiments |>
  left_join(price_changes, by = c("ticker", "date")) |>
  #  na.omit() |>
  # add true/false column for whether pct_change_7d  has the same sign as sentiment
  mutate(
    win_absolute = bullish == (pct_change_7d > 0),
    win_vs_SPY = bullish == (pct_change_minus_spy > 0)
  ) |>
  #  select(user_id, ticker, date, bullish, win_absolute)
  # select(user_id, ticker, date, bullish, win_absolute, win_vs_SPY) |>
  filter(!is.na(win_absolute) & !is.na(win_vs_SPY))
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

get_significanct_posters <- function(win_rate) {
  results <- win_rate |>
    as_tibble() |>
    rowwise() |>
    mutate(
      # Perform two-sided binomial test
      binom_test = list(binom.test(
        wins,
        total,
        p = 0.5,
        alternative = "two.sided"
      )),
      p_value = binom_test$p.value,
      conf_low = binom_test$conf.int[1],
      conf_high = binom_test$conf.int[2]
    ) |>
    ungroup() |>
    select(-binom_test, -starts_with("conf")) |>
    arrange(p_value)

  # Filter for statistically significant results (p < 0.05)
  significant_posters <- results |>
    filter(p_value < 0.05) |>
    mutate(
      alpha_direction = as.integer(case_when(
        win_rate > 0.5 ~ 1,
        win_rate < 0.5 ~ -1,
        TRUE ~ 0
      ))
    )
  return(significant_posters)
}

# make trade history for the selected date window
CREATE_WINDOWED_RECORDS <- FALSE
if (CREATE_WINDOWED_RECORDS) {
  all_windows <- list()
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
        buy_or_sell = as.integer(
          (as.integer(bullish) * 2 - 1) * alpha_direction
        )
      ) |>
      # add ticker price change over next 5 days
      left_join(
        select(price_changes, ticker, date, pct_change_7d),
        by = c("ticker", "date")
      ) |>
      mutate(gain_or_loss = pct_change_7d * buy_or_sell) # |>
    # as_tibble()
    all_windows[[window_index]] <- collect(short_sentiments_test)
  }
  all_windows_df <- all_windows |>
    bind_rows(.id = "window") |>
    filter(!is.na(gain_or_loss)) |>
    mutate(window = as.integer(window)) |>
    compute_parquet("data/skill_90_30.parquet")
} else {
  cat("Loading precomputed windowed records...\n")
  all_windows_df <- read_parquet_duckdb("data/skill_90_30.parquet")
}

# test results with a smaller trade volume
all_windows_df_limited <- all_windows_df |>
  # limit windows to after stocktwits has more than 1000 valid posts per window
  filter(window > 30) |>
  # limit windows to buy only
  # filter(buy_or_sell == 1) |>
  as_tibble() |>
  arrange(date) |>
  # limit trades first 100 signals per month
  slice_head(by = window, n = 100) |>
  # add SPY price change for comparison
  left_join(
    prices_df |>
      filter(ticker == "SPY") |>
      arrange(date) |>
      mutate(
        .by = ticker,
        adj_close_lead7 = lead(adj_close, 5),
        spy_pct_change_7d = (adj_close_lead7 - adj_close) / adj_close
      ) |>
      select(date, spy_pct_change_7d),
    by = "date"
  ) |>
  mutate(gain_or_loss_minus_spy = gain_or_loss - spy_pct_change_7d)


result_history <- all_windows_df_limited |>
  summarise(
    .by = window,
    start_date = min(date),
    median_gain = median(gain_or_loss),
    mean_gain = mean(gain_or_loss),
    median_excess_gain = median(gain_or_loss_minus_spy),
    mean_excess_gain = mean(gain_or_loss_minus_spy),
    trades = n()
  ) |>
  as_tibble() |>
  arrange(window)
methods_restore()

# plot median gain by window
result_history |>
  ggplot(aes(x = start_date, y = mean_excess_gain)) +
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


trade_summary <- all_windows_df |>
  summarise(
    .by = c(bullish, alpha_direction),
    trades = n(),
    median_gain = median(gain_or_loss)
  )
# heatmap of gain by bullish and alpha_direction
# bullish on x axis, alpha_direction on y axis and trades in fill

trade_summary |>
  ggplot(aes(x = as.factor(bullish), y = as.factor(alpha_direction))) +
  geom_tile(aes(fill = median_gain), color = "white") +
  scale_fill_gradient2(
    low = "red",
    mid = "white",
    high = "darkgreen",
    midpoint = 0,
    name = "Median Trade Gain",
    labels = scales::label_percent(accuracy = 0.1)
  ) +
  # format labels with commas
  geom_text(
    aes(label = paste(format(trades, big.mark = ","), "Trades")),
    color = "black",
    size = 5
  ) +
  scale_x_discrete(
    labels = c("FALSE" = "Bearish", "TRUE" = "Bullish")
  ) +
  scale_y_discrete(
    labels = c("-1" = "Negative", "1" = "Positive", "0" = "Neutral")
  ) +
  labs(
    title = "Median Gain by Sentiment and Alpha Direction",
    x = "Sentiment",
    y = "Alpha"
  ) +
  theme_minimal()


all_windows_df |>
  summarise(
    count = n(),
    median_gain = median(gain_or_loss),
    mean_gain = mean(gain_or_loss)
  )


all_windows_df_limited |>
  summarise(
    .by = c(bullish, alpha_direction),
    trades = n(),
    median_gain = median(gain_or_loss),
    mean_gain = mean(gain_or_loss)
  )


all_recs_limited <- all_windows_df_limited |>
  select(ticker, date, buy_or_sell) |>
  compute_parquet("data/all_recs_limited.parquet")

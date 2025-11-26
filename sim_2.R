library(tidyverse)
library(duckplyr)

# load all_recs
recommendations <- read_parquet_duckdb("data/all_recs_limited.parquet") |>
  as_tibble()
# load price data
prices_df <- read_parquet_duckdb("data/price_history_top500.parquet") |>
  as_tibble() |>
  select(ticker, date, adj_close)

methods_restore()
# simulate strategy: start $10k, $1k per trade, 7-day holds
initial_capital <- 10000
capital <- initial_capital
trade_fraction <- .01 # percent of capital per trade
hold_days <- 7
risk_free_rate <- 0.02 # annual rate
invest_idle_in <- "cash" # options: "cash", "SPY"
daily_trade_limit <- 100

# Long only strategy
recommendations <- recommendations |>
  filter(buy_or_sell == 1)

# truncate for testing ==========================================================
recommendations <- recommendations |> filter(date > as.Date("2020-01-01"))
# randomize  ==========================================================
# randomize order of tickers for testing
# recommendations$ticker <- recommendations$ticker |> sample()

# add entry price to recommendations
trade_blotter <- recommendations |>
  left_join(
    prices_df |>
      rename(entry_price = adj_close),
    by = c("ticker", "date")
  ) |>
  mutate(
    exit_date = date + hold_days,
  ) |>
  left_join(
    prices_df |>
      rename(exit_price = adj_close),
    by = c("ticker" = "ticker", "exit_date" = "date")
  ) |>
  mutate(
    return_factor = (exit_price / entry_price)
  ) |>
  filter(!is.na(return_factor))


spy_prices <- prices_df |>
  filter(ticker == "SPY") |>
  select(date, spy_price = adj_close) |>
  mutate(spy_ret = (spy_price / lag(spy_price)) - 1)

# Create a complete date range
date_range <- seq(
  min(trade_blotter$date, na.rm = TRUE),
  max(trade_blotter$exit_date, na.rm = TRUE),
  by = "day"
)

# Prepare trade entries and exits
trades <- trade_blotter |>
  filter(!is.na(entry_price)) |>
  filter(!is.na(return_factor)) |>
  select(ticker, date, buy_or_sell, entry_price, exit_date, return_factor) |>
  mutate(trade_id = row_number()) |>
  mutate(trade_size = NA_real_) |>
  slice_head(by = date, n = daily_trade_limit)

# trade_exits <- trade_entries |>
#  select(trade_id, exit_date, return_factor)

# Function to build account over time =========================================
build_account <- function(end_index = length(date_range)) {
  capital_tracker <- tibble(
    date = date_range,
    starting_capital = NA_real_,
    exits_today = 0,
    freed_capital = 0, # proceeds from sales (principal * return_factor)
    trade_pnl = 0, # realized P&L from closed trades
    entries_today = 0,
    available_for_entries = 0,
    deployed_capital = 0, # capital currently deployed in trades
    idle_capital = 0, # capital not deployed
    ending_capital = NA_real_
  )

  capital_tracker$starting_capital[1] <- initial_capital
  capital_tracker$ending_capital[1] <- initial_capital
  capital_tracker$idle_capital[1] <- initial_capital
  capital_tracker$available_for_entries[1] <- initial_capital

  for (i in 1:end_index) {
    # for (i in 1:nrow(capital_tracker)) {
    current_date <- capital_tracker$date[i]

    capital_tracker$starting_capital[i] <- if (i == 1) {
      initial_capital
    } else {
      capital_tracker$ending_capital[i - 1]
    }

    # Exiting positions: principal, proceeds and realized P&L
    todays_exits <- trades |>
      filter(exit_date == current_date)
    capital_tracker$exits_today[i] <- nrow(todays_exits)
    capital_tracker$freed_capital[i] <- sum(
      todays_exits$trade_size * todays_exits$return_factor,
      na.rm = TRUE
    )
    capital_tracker$trade_pnl[i] <- sum(
      todays_exits$trade_size * (todays_exits$return_factor - 1),
      na.rm = TRUE
    )

    capital_tracker$available_for_entries[i] <-
      capital_tracker$idle_capital[max(i - 1, 1)] +
      capital_tracker$freed_capital[i]

    # Entering positions: allocate capital
    # this will set trade size and be used in the future trade exits
    todays_entries <- trades |>
      filter(date == current_date)
    capital_tracker$entries_today[i] <- nrow(todays_entries)

    new_trade_size <- capital_tracker$available_for_entries[i] /
      nrow(todays_entries)
    # set trade size for today's date in trades
    trades <- trades |>
      mutate(
        trade_size = if_else(
          date == current_date,
          new_trade_size,
          trade_size
        )
      )
    todays_entries <- trades |>
      filter(date == current_date)

    deployed_today <- sum(todays_entries$trade_size, na.rm = TRUE)
    previously_deployed <- capital_tracker$deployed_capital[max(i - 1, 1)]
    capital_tracker$deployed_capital[i] <-
      previously_deployed -
      capital_tracker$freed_capital[i] +
      capital_tracker$trade_pnl[i] +
      deployed_today

    # Idle capital after entries (non-negative)
    capital_tracker$idle_capital[i] <-
      max(
        capital_tracker$starting_capital[i] -
          capital_tracker$deployed_capital[i],
        0
      )

    idle_cap <- capital_tracker$idle_capital[i]
    if (invest_idle_in == "cash") {
      idle_ret <- idle_cap * (risk_free_rate / 252)
    } else if (invest_idle_in == "SPY") {
      idle_ret <- idle_cap * spy_prices$spy_ret[i]
    }

    # End of day capital: starting + realized P&L + idle return
    capital_tracker$ending_capital[i] <-
      capital_tracker$starting_capital[i] +
      capital_tracker$trade_pnl[i] +
      idle_ret
  }
  return(capital_tracker)
}

capital_tracker <- build_account()

# compute CAPM metrics based on ending capital
mean_return <- mean(capital_tracker$daily_return, na.rm = TRUE)
sd_return <- sd(capital_tracker$daily_return, na.rm = TRUE)
sharpe_ratio <- (mean_return - (risk_free_rate / 252)) / sd_return * sqrt(252)
cat("Final Capital: $", round(tail(capital_tracker$ending_capital, 1), 2), "\n")
cat("Annualized Return: ", round(mean_return * 252 * 100, 2), "%\n", sep = "")
cat(
  "Annualized Volatility: ",
  round(sd_return * sqrt(252) * 100, 2),
  "%\n",
  sep = ""
)
cat("Sharpe Ratio: ", round(sharpe_ratio, 2), "\n", sep = "")
# compute beta and alpha if SPY data is available

if (!is.null(spy_prices)) {
  spy_returns <- spy_prices |>
    arrange(date) |>
    mutate(
      daily_return = (spy_price - lag(spy_price)) / lag(spy_price)
    ) |>
    select(date, spy_daily_return = daily_return)

  merged_returns <- capital_tracker |>
    left_join(spy_returns, by = "date") |>
    filter(!is.na(daily_return), !is.na(spy_daily_return)) |>
    # add column for SPY capital value
    mutate(spy_capital = initial_capital * cumprod(1 + spy_daily_return))

  covariance <- cov(
    merged_returns$daily_return,
    merged_returns$spy_daily_return
  )
  beta <- covariance / var(merged_returns$spy_daily_return)
  alpha <- mean(merged_returns$daily_return) -
    beta * mean(merged_returns$spy_daily_return)
}
cat("Beta: ", round(beta, 4), "\n", sep = "")
cat("Alpha (daily): ", round(alpha * 100, 4), "%\n", sep = "")
cat("Alpha (annualized): ", round(alpha * 252 * 100, 2), "%\n", sep = "")

# Plot equity curve
merged_returns |>
  ggplot(aes(x = date, y = ending_capital)) +
  geom_line(color = "blue", linewidth = 1) +
  geom_line(aes(y = spy_capital)) +
  labs(
    title = "Equity Curve",
    x = "Date",
    y = "Ending Capital ($)"
  ) +
  theme_minimal()

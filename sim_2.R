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
trade_size <- .01 # percent of capital per trade
hold_days <- 7
risk_free_rate <- 0.02 # annual rate
invest_idle_in <- "cash" # options: "cash", "SPY"

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
  select(date, spy_price = adj_close)

# Create a complete date range
date_range <- seq(
  min(trade_blotter$date, na.rm = TRUE),
  max(trade_blotter$exit_date, na.rm = TRUE),
  by = "day"
)

# Prepare trade entries and exits
trade_entries <- trade_blotter |>
  filter(!is.na(entry_price)) |>
  select(ticker, date, buy_or_sell, entry_price, exit_date, return_factor) |>
  mutate(trade_id = row_number())

trade_exits <- trade_entries |>
  filter(!is.na(return_factor)) |>
  select(trade_id, exit_date, return_factor)

build_account <- function() {
  # Initialize daily capital tracker
  capital_tracker <- tibble(
    date = date_range,
    starting_capital = NA_real_,
    entries_today = 0,
    exits_today = 0,
    deployed_capital = 0,
    idle_capital = 0,
    idle_return = 0,
    daily_pnl = 0,
    ending_capital = NA_real_
  )
  # Set initial capital
  capital_tracker$starting_capital[1] <- initial_capital
  capital_tracker$ending_capital[1] <- initial_capital

  # Simulate day by day
  for (i in 1:nrow(capital_tracker)) {
    current_date <- capital_tracker$date[i]
    current_capital <- if (i == 1) {
      initial_capital
    } else {
      capital_tracker$ending_capital[i - 1]
    }
    capital_tracker$starting_capital[i] <- current_capital

    # Process entries today
    entries <- trade_entries |> filter(date == current_date)
    capital_tracker$entries_today[i] <- nrow(entries)

    # Process exits today
    exits <- trade_exits |> filter(exit_date == current_date)
    capital_tracker$exits_today[i] <- nrow(exits)

    # Calculate deployed capital (trades still open)
    open_trades <- trade_entries |>
      filter(date <= current_date, exit_date > current_date)

    deployed <- 0
    if (nrow(open_trades) > 0) {
      for (k in 1:nrow(open_trades)) {
        entry_capital <- capital_tracker |>
          filter(date == open_trades$date[k]) |>
          pull(starting_capital)
        deployed <- deployed + entry_capital * trade_size
      }
    }
    capital_tracker$deployed_capital[i] <- deployed

    # Calculate idle capital
    idle <- current_capital - deployed
    capital_tracker$idle_capital[i] <- idle

    # Calculate return on idle capital
    idle_return <- 0
    if (invest_idle_in == "cash") {
      # Daily risk-free rate
      idle_return <- idle * (risk_free_rate / 252)
    } else if (invest_idle_in == "SPY" && !is.null(spy_prices)) {
      # Get SPY return for the day
      if (i > 1) {
        prev_date <- capital_tracker$date[i - 1]
        spy_today <- spy_prices |>
          filter(date == current_date) |>
          pull(spy_price)
        spy_prev <- spy_prices |> filter(date == prev_date) |> pull(spy_price)

        if (length(spy_today) > 0 && length(spy_prev) > 0 && spy_prev > 0) {
          spy_return <- (spy_today / spy_prev) - 1
          idle_return <- idle * spy_return
        }
      }
    }
    capital_tracker$idle_return[i] <- idle_return

    # Calculate P&L from exits
    pnl <- 0
    if (nrow(exits) > 0) {
      for (j in 1:nrow(exits)) {
        trade_id <- exits$trade_id[j]
        entry_row <- trade_entries |> filter(trade_id == !!trade_id)
        entry_capital <- capital_tracker |>
          filter(date == entry_row$date) |>
          pull(starting_capital)
        trade_amount <- entry_capital * trade_size
        pnl <- pnl + trade_amount * (exits$return_factor[j] - 1)
      }
    }

    capital_tracker$daily_pnl[i] <- pnl
    capital_tracker$ending_capital[i] <- current_capital + pnl + idle_return
  }
  capital_tracker <- capital_tracker |>
    mutate(
      daily_return = (ending_capital - starting_capital) / starting_capital
    )
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

library(tidyverse)
library(lubridate)

# simulate strategy: start $10k, $1k per trade, 7-day holds
initial_capital <- 10000
trade_size <- 1000
hold_days <- 7

simulate_strategy <- function(
  all_windows_df_limited,
  prices_df,
  initial_capital = 10000,
  trade_size = 1000,
  hold_days = 7
) {
  trades <- all_windows_df_limited |>
    as_tibble() |>
    mutate(
      open_date = as_date(date),
      close_date = open_date + days(hold_days)
    ) |>
    arrange(open_date)

  # Build SPY price series aligned to the simulation date range (used for idle-capital valuation and conversions)
  start_date <- min(trades$open_date, na.rm = TRUE)
  end_date <- max(trades$close_date, na.rm = TRUE)
  all_dates <- seq.Date(from = start_date, to = end_date, by = "day")

  spy_prices <- prices_df |>
    filter(ticker == "SPY") |>
    select(date, adj_close) |>
    arrange(date) |>
    mutate(date = as_date(date)) |>
    distinct(date, .keep_all = TRUE)

  spy_on_sim_dates <- tibble(date = all_dates) |>
    left_join(spy_prices, by = "date") |>
    arrange(date) |>
    tidyr::fill(adj_close, .direction = "down")

  # if SPY price still missing at very start, use first available SPY price
  if (is.na(spy_on_sim_dates$adj_close[1])) {
    first_spy <- spy_prices$adj_close[1]
    spy_on_sim_dates <- spy_on_sim_dates |>
      mutate(adj_close = if_else(is.na(adj_close), first_spy, adj_close))
  }

  # fast lookup
  spy_price_map <- setNames(
    spy_on_sim_dates$adj_close,
    as.character(spy_on_sim_dates$date)
  )

  # bookkeeping: idle capital is held as fractional SPY shares
  start_spy_price <- spy_on_sim_dates$adj_close[1]
  idle_spy_shares <- initial_capital / start_spy_price
  invested <- 0
  active_positions <- tibble()
  trades_executed <- tibble()

  # daily loop: open trades by selling idle SPY shares; on close convert proceeds back to SPY shares
  daily_log_list <- vector("list", length(all_dates))
  for (i in seq_along(all_dates)) {
    today <- as_date(all_dates[i])
    current_spy_price <- as.numeric(spy_price_map[as.character(today)])

    # Close positions that mature today
    if (nrow(active_positions) > 0) {
      to_close_idx <- which(active_positions$close_date == today)
      if (length(to_close_idx) > 0) {
        closing <- active_positions[to_close_idx, , drop = FALSE]

        # proceeds in dollars from each trade
        proceeds_dollars <- closing$trade_size * (1 + closing$gain_or_loss)
        profit_dollars <- proceeds_dollars - closing$trade_size

        # convert proceeds to SPY shares at today's price and add to idle holdings
        shares_bought <- proceeds_dollars / current_spy_price
        idle_spy_shares <- idle_spy_shares + sum(shares_bought)

        invested <- invested - sum(closing$trade_size)

        closing <- closing |>
          mutate(
            close_date = today,
            proceeds = as.numeric(proceeds_dollars),
            profit = as.numeric(profit_dollars)
          )

        trades_executed <- bind_rows(trades_executed, closing)
        active_positions <- active_positions[-to_close_idx, , drop = FALSE]
      }
    }

    # Open new trades signalled today (in order).
    todays_signals <- trades |> filter(open_date == today)
    if (nrow(todays_signals) > 0) {
      for (r in seq_len(nrow(todays_signals))) {
        row <- todays_signals[r, , drop = FALSE]

        # value available in idle SPY today
        idle_value <- idle_spy_shares * current_spy_price

        if (idle_value >= trade_size) {
          # sell fractional SPY shares to fund the trade
          shares_to_sell <- trade_size / current_spy_price
          idle_spy_shares <- idle_spy_shares - shares_to_sell

          invested <- invested + trade_size

          pos <- row |>
            mutate(trade_size = trade_size)
          active_positions <- bind_rows(active_positions, pos)
        } else {
          # not enough idle-SPY value -> skip signal
        }
      }
    }

    # equity = value of idle SPY holdings + principal locked in active trades
    daily_log_list[[i]] <- tibble(
      date = today,
      idle_spy_shares = idle_spy_shares,
      idle_spy_value = idle_spy_shares * current_spy_price,
      invested = invested,
      equity = idle_spy_shares * current_spy_price + invested,
      open_positions = nrow(active_positions)
    )
  }

  daily_log <- bind_rows(daily_log_list)

  # finalize: if any remaining active_positions never closed within window, mark NA for proceeds/profit
  if (nrow(active_positions) > 0) {
    still_open <- active_positions |>
      mutate(proceeds = NA_real_, profit = NA_real_)
    trades_executed <- bind_rows(trades_executed, still_open)
  }

  # Build trade blotter (one row per executed trade)
  trade_blotter <- trades_executed |>
    as_tibble() |>
    mutate(
      open_date = as_date(open_date),
      close_date = as_date(close_date),
      trade_id = row_number()
    ) |>
    select(
      trade_id,
      ticker,
      open_date,
      close_date,
      trade_size,
      buy_or_sell,
      gain_or_loss,
      proceeds,
      profit
    ) |>
    arrange(open_date)

  # Build SPY buy-and-hold series for comparison (initial_capital invested at start)
  spy_shares_bh <- initial_capital / start_spy_price
  spy_series <- spy_on_sim_dates |>
    mutate(spy_equity = spy_shares_bh * adj_close)

  # mountain chart: strategy equity vs SPY buy-and-hold
  plot_df <- daily_log |>
    select(date, equity) |>
    left_join(spy_series |> select(date, spy_equity), by = "date")

  mountain_plot <- ggplot(plot_df, aes(x = date)) +
    geom_area(aes(y = equity), fill = "#2c7fb8", alpha = 0.25) +
    geom_line(aes(y = equity), color = "#0868ac", size = 1) +
    geom_line(
      aes(y = spy_equity),
      color = "#a50f15",
      size = 1,
      linetype = "dashed"
    ) +
    labs(
      title = "Strategy Equity (area) vs SPY Buy-and-Hold (dashed)",
      x = "Date",
      y = "Capital ($)"
    ) +
    theme_minimal()

  list(
    trades_executed = trades_executed,
    trade_blotter = trade_blotter,
    daily_series = daily_log,
    spy_series = spy_series,
    mountain_plot = mountain_plot
  )
}

# Example usage (assumes all_windows_df_limited and prices_df are present in environment):
sim_out <- simulate_strategy(
  all_windows_df_limited,
  prices_df,
  initial_capital,
  trade_size,
  hold_days
)

sim_out$trade_blotter
sim_out$mountain_plot
sim_out$daily_series

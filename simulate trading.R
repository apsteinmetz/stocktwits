# simulate trading
library(dplyr)
library(lubridate)

# simulate strategy: start $10k, $1k per trade, 7-day holds, max concurrent = 10k/1k = 100
simulate_trades <- function(
  all_windows_df_limited,
  prices_df,
  initial_capital = 10000,
  trade_size = 1000,
  hold_days = 7
) {
  # prepare SPY returns for hold_days
  spy_returns <- prices_df |>
    filter(ticker == "SPY") |>
    arrange(date) |>
    mutate(
      spy_ret_ndays = (lead(adj_close, hold_days) - adj_close) / adj_close
    ) |>
    select(date, spy_ret_ndays)

  trades <- all_windows_df_limited |>
    as_tibble() |>
    mutate(
      open_date = as_date(date),
      close_date = open_date + days(hold_days)
    ) |>
    # attach SPY return for the same open date (may be NA; drop those)
    left_join(spy_returns, by = c("open_date" = "date")) |>
    filter(!is.na(spy_ret_ndays)) |>
    mutate(
      total_pct = spy_ret_ndays + gain_or_loss
    ) |>
    arrange(open_date)

  max_concurrent <- floor(initial_capital / trade_size)

  # event-driven simulation over sorted unique dates (open and close dates)
  event_dates <- sort(unique(c(trades$open_date, trades$close_date)))

  cash <- initial_capital
  active_positions <- tibble(
    open_date = as_date(character()),
    close_date = as_date(character()),
    total_pct = numeric(),
    trade_id = integer()
  )
  executed <- list()
  next_trade_id <- 1L

  daily_log <- vector("list", length(event_dates))
  names(daily_log) <- as.character(event_dates)

  for (i in seq_along(event_dates)) {
    d <- event_dates[i]

    # close positions that mature today -> realize proceeds
    to_close_idx <- which(active_positions$close_date == d)
    if (length(to_close_idx) > 0) {
      closing <- active_positions[to_close_idx, , drop = FALSE]
      proceeds <- trade_size * (1 + closing$total_pct)
      cash <- cash + sum(proceeds)
      # record executed trade results
      for (j in seq_len(nrow(closing))) {
        executed[[length(executed) + 1L]] <- tibble(
          trade_id = closing$trade_id[j],
          open_date = closing$open_date[j],
          close_date = closing$close_date[j],
          total_pct = closing$total_pct[j],
          proceeds = trade_size * (1 + closing$total_pct[j]),
          profit = trade_size * closing$total_pct[j]
        )
      }
      if (length(to_close_idx) > 0) {
        active_positions <- active_positions[-to_close_idx, , drop = FALSE]
      }
    }

    # open trades that start today (respect capital / max concurrent)
    todays_trades <- trades |> filter(open_date == d)
    if (nrow(todays_trades) > 0) {
      for (r in seq_len(nrow(todays_trades))) {
        if ((nrow(active_positions) < max_concurrent) && (cash >= trade_size)) {
          row <- todays_trades[r, ]
          cash <- cash - trade_size
          active_positions <- bind_rows(
            active_positions,
            tibble(
              open_date = row$open_date,
              close_date = row$close_date,
              total_pct = row$total_pct,
              trade_id = next_trade_id
            )
          )
          next_trade_id <- next_trade_id + 1L
        } else {
          # skip the trade when capital/concurrency exhausted
        }
      }
    }

    # record daily snapshot: cash, open positions, invested, equity assumption (cash + invested)
    n_open <- nrow(active_positions)
    invested <- n_open * trade_size
    equity <- cash + invested
    daily_log[[i]] <- tibble(
      date = d,
      cash = cash,
      n_open = n_open,
      invested = invested,
      equity = equity
    )
  }

  trades_executed <- if (length(executed) > 0) bind_rows(executed) else tibble()
  daily_series <- bind_rows(daily_log)

  summary <- tibble(
    initial_capital = initial_capital,
    final_cash = cash,
    final_equity = cash, # all positions close on or before last event date; equity equals cash
    total_trades = nrow(trades_executed),
    total_profit = sum(trades_executed$profit, na.rm = TRUE),
    total_return = (cash - initial_capital) / initial_capital,
    win_rate = mean(trades_executed$profit > 0, na.rm = TRUE)
  )

  list(
    trades_executed = trades_executed,
    daily_series = daily_series,
    summary = summary
  )
}

# run simulation on the limited dataset
sim_results <- simulate_trades(
  all_windows_df_limited,
  prices_df,
  initial_capital = 10000,
  trade_size = 1000,
  hold_days = 7
)

# return sim_results invisibly (useful for interactive)
sim_results

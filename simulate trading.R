library(tidyverse)
library(duckplyr)

# load all_recs
all_recs_limited <- read_parquet_duckdb("data/all_recs_limited.parquet")
# load price data
prices_df <- read_parquet_duckdb("data/price_history_top500.parquet")


# simulate strategy: start $10k, $1k per trade, 7-day holds
initial_capital <- 10000
trade_size <- 100
hold_days <- 7
risk_free_rate <- 0.03
invest_idle_in <- "cash" # or "SPY"


simulate_strategy <- function(
  recommendations,
  prices_df,
  initial_capital = 10000,
  trade_size = 100,
  hold_days = 7,
  invest_idle_in = c("SPY", "cash"),
  risk_free_rate = 0.03
) {
  invest_idle_in <- match.arg(invest_idle_in)

  trades <- recommendations |>
    as_tibble() |>
    mutate(
      open_date = as.Date(date),
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
    mutate(date = as.Date(date)) |>
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

  # bookkeeping
  start_spy_price <- spy_on_sim_dates$adj_close[1]

  if (invest_idle_in == "SPY") {
    # idle capital held in fractional SPY shares
    idle_spy_shares <- initial_capital / start_spy_price
    idle_cash <- NULL
  } else {
    # idle capital held as cash that accrues at risk-free rate
    idle_cash <- initial_capital
    idle_spy_shares <- NULL
    # daily compounding factor
    daily_rf <- (1 + risk_free_rate)^(1 / 365.25) - 1
  }

  invested <- 0
  active_positions <- tibble()
  trades_executed <- tibble()

  # daily loop: close then open
  daily_log_list <- vector("list", length(all_dates))
  for (i in seq_along(all_dates)) {
    today <- as.Date(all_dates[i])
    current_spy_price <- as.numeric(spy_price_map[as.character(today)])

    # accrue interest on idle cash at start of day (cash mode)
    if (invest_idle_in == "cash") {
      idle_cash <- idle_cash * (1 + daily_rf)
    }

    # Close positions that mature today
    if (nrow(active_positions) > 0) {
      to_close_idx <- which(active_positions$close_date == today)
      if (length(to_close_idx) > 0) {
        closing <- active_positions[to_close_idx, , drop = FALSE]

        # proceeds in dollars from each trade
        proceeds_dollars <- closing$trade_size * (1 + closing$gain_or_loss)
        profit_dollars <- proceeds_dollars - closing$trade_size

        if (invest_idle_in == "SPY") {
          # convert proceeds to SPY shares at today's price and add to idle holdings
          shares_bought <- proceeds_dollars / current_spy_price
          idle_spy_shares <- idle_spy_shares + sum(shares_bought)
        } else {
          # add proceeds to idle cash
          idle_cash <- idle_cash + sum(as.numeric(proceeds_dollars))
        }

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

        if (invest_idle_in == "SPY") {
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
            # skip if not enough idle-SPY value
          }
        } else {
          # cash mode
          if (idle_cash >= trade_size) {
            idle_cash <- idle_cash - trade_size
            invested <- invested + trade_size

            pos <- row |>
              mutate(trade_size = trade_size)
            active_positions <- bind_rows(active_positions, pos)
          } else {
            # skip if not enough cash
          }
        }
      }
    }

    # equity = value of idle holdings + principal locked in active trades
    if (invest_idle_in == "SPY") {
      idle_value_today <- idle_spy_shares * current_spy_price
      equity_today <- idle_value_today + invested
      idle_spy_shares_today <- idle_spy_shares
      idle_cash_today <- NA_real_
    } else {
      idle_value_today <- idle_cash
      equity_today <- idle_cash + invested
      idle_spy_shares_today <- NA_real_
      idle_cash_today <- idle_cash
    }

    daily_log_list[[i]] <- tibble(
      date = today,
      idle_spy_shares = idle_spy_shares_today,
      idle_spy_value = ifelse(
        is.na(idle_spy_shares_today),
        NA_real_,
        idle_spy_shares_today * current_spy_price
      ),
      idle_cash = idle_cash_today,
      invested = invested,
      equity = equity_today,
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
      open_date = as.Date(open_date),
      close_date = as.Date(close_date),
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
  if (invest_idle_in == "SPY") {
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
        title = paste0(
          "Strategy Equity (area) vs SPY Buy-and-Hold (dashed) — idle invested in: ",
          invest_idle_in
        ),
        x = "Date",
        y = "Capital ($)"
      ) +
      theme_minimal()
  } else {
    plot_df <- daily_log |>
      select(date, equity)

    mountain_plot <- ggplot(plot_df, aes(x = date)) +
      geom_area(aes(y = equity), fill = "#2c7fb8", alpha = 0.25) +
      geom_line(aes(y = equity), color = "#0868ac", size = 1) +
      labs(
        title = paste0(
          "Strategy Equity (area) — idle invested in: ",
          invest_idle_in
        ),
        x = "Date",
        y = "Capital ($)"
      ) +
      theme_minimal()
  }

  list(
    trades_executed = trades_executed,
    trade_blotter = trade_blotter,
    daily_series = daily_log,
    spy_series = spy_series,
    mountain_plot = mountain_plot
  )
}
# Example usage (assumes recommendations and prices_df are present in environment):
sim_out <- simulate_strategy(
  all_recs_limited,
  prices_df,
  initial_capital,
  trade_size,
  hold_days,
  invest_idle_in = "cash"
)

sim_out$trade_blotter
sim_out$mountain_plot
sim_out$daily_series

# compute compound annual growth rate (CAGR) for strategy and SPY buy-and-hold
compute_cagr <- function(daily_series, spy_series) {
  start_date <- min(daily_series$date)
  end_date <- max(daily_series$date)
  num_years <- as.numeric(difftime(end_date, start_date, units = "days")) /
    365.25

  start_equity <- daily_series$equity[daily_series$date == start_date]
  end_equity <- daily_series$equity[daily_series$date == end_date]
  strategy_cagr <- (end_equity / start_equity)^(1 / num_years) - 1

  start_spy_equity <- spy_series$spy_equity[spy_series$date == start_date]
  end_spy_equity <- spy_series$spy_equity[spy_series$date == end_date]
  spy_cagr <- (end_spy_equity / start_spy_equity)^(1 / num_years) - 1

  tibble(
    strategy_cagr = strategy_cagr,
    spy_cagr = spy_cagr
  )
}
cagr_results <- compute_cagr(sim_out$daily_series, sim_out$spy_series)
print(cagr_results)

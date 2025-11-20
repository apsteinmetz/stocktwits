library(tidyverse)
library(duckplyr)

# load all_recs
all_recs_limited <- read_parquet_duckdb("data/all_recs_limited.parquet") |>
  as_tibble()
# load price data
prices_df <- read_parquet_duckdb("data/price_history_top500.parquet") |>
  as_tibble() |>
  select(ticker, date, adj_close)

methods_restore()
# simulate strategy: start $10k, $1k per trade, 7-day holds
initial_capital <- 10000
trade_size <- 100
hold_days <- 7
risk_free_rate <- 0.02
invest_idle_in <- "SPY" # or "SPY"

compute_capm_stats <- function(daily_series, spy_series) {
  merged <- daily_series |>
    select(date, equity) |>
    left_join(spy_series |> select(date, spy_equity), by = "date")
  merged <- merged |>
    arrange(date) |>
    mutate(
      equity_return = (equity / lag(equity)) - 1,
      spy_return = (spy_equity / lag(spy_equity)) - 1
    ) |>
    filter(!is.na(equity_return) & !is.na(spy_return))
  model <- lm(equity_return ~ spy_return, data = merged)
  summary_model <- summary(model)
  beta <- summary_model$coefficients["spy_return", "Estimate"]
  alpha <- summary_model$coefficients["(Intercept)", "Estimate"]
  # annulize alpha and beta
  alpha <- (1 + alpha)^252 - 1
  r_squared <- summary_model$r.squared
  tibble(
    alpha = alpha,
    beta = beta,
    r_squared = r_squared
  )
}

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

  # prepare prices: forward-fill adj_close per ticker so we can lookup entry/exit prices
  # prices_filled <- prices_df |>
  #   arrange(ticker, date) |>
  #   group_by(ticker) |>
  #   tidyr::fill(adj_close, .direction = "down") |>
  #   ungroup() |>
  #   select(ticker, date, adj_close)

  # build trades from recommendations: use recommendation date as open_date and lookup entry/exit prices
  trades <- recommendations |>
    as_tibble() |>
    rename(open_date = date) |>
    mutate(close_date = open_date + hold_days) |>
    # join entry price
    left_join(prices_df, by = c("ticker", "open_date" = "date")) |>
    rename(entry_price = adj_close) |>
    # join exit price
    left_join(prices_df, by = c("ticker", "close_date" = "date")) |>
    rename(exit_price = adj_close) |>
    # compute returns and signed gain_or_loss using buy_or_sell (1 for long, -1 for short)
    mutate(
      price_return = (exit_price - entry_price) / entry_price,
      gain_or_loss = price_return * buy_or_sell
    ) |>
    # drop signals lacking price data
    filter(!is.na(entry_price) & !is.na(exit_price)) |>
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

  if (is.na(spy_on_sim_dates$adj_close[1])) {
    first_spy <- spy_prices$adj_close[1]
    spy_on_sim_dates <- spy_on_sim_dates |>
      mutate(adj_close = if_else(is.na(adj_close), first_spy, adj_close))
  }

  spy_price_map <- setNames(
    spy_on_sim_dates$adj_close,
    as.character(spy_on_sim_dates$date)
  )

  # bookkeeping for idle capital
  start_spy_price <- spy_on_sim_dates$adj_close[1]
  if (invest_idle_in == "SPY") {
    idle_spy_shares <- initial_capital / start_spy_price
    idle_cash <- NULL
  } else {
    idle_cash <- initial_capital
    idle_spy_shares <- NULL
    daily_rf <- (1 + risk_free_rate)^(1 / 365.25) - 1
  }

  invested <- 0
  active_positions <- tibble()
  trades_executed <- tibble()

  # daily loop
  daily_log_list <- vector("list", length(all_dates))
  for (i in seq_along(all_dates)) {
    today <- as.Date(all_dates[i])
    current_spy_price <- as.numeric(spy_price_map[as.character(today)])

    if (invest_idle_in == "cash") {
      idle_cash <- idle_cash * (1 + daily_rf)
    }

    # Close positions that mature today
    if (nrow(active_positions) > 0) {
      to_close_idx <- which(active_positions$close_date == today)
      if (length(to_close_idx) > 0) {
        closing <- active_positions[to_close_idx, , drop = FALSE]

        proceeds_dollars <- closing$trade_size * (1 + closing$gain_or_loss)
        profit_dollars <- proceeds_dollars - closing$trade_size

        if (invest_idle_in == "SPY") {
          shares_bought <- proceeds_dollars / current_spy_price
          idle_spy_shares <- idle_spy_shares + sum(shares_bought)
        } else {
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

    # Open new trades signalled today
    todays_signals <- trades |> filter(open_date == today)
    if (nrow(todays_signals) > 0) {
      for (r in seq_len(nrow(todays_signals))) {
        row <- todays_signals[r, , drop = FALSE]

        if (invest_idle_in == "SPY") {
          idle_value <- idle_spy_shares * current_spy_price
          if (idle_value >= trade_size) {
            shares_to_sell <- trade_size / current_spy_price
            idle_spy_shares <- idle_spy_shares - shares_to_sell
            invested <- invested + trade_size
            pos <- row |> mutate(trade_size = trade_size)
            active_positions <- bind_rows(active_positions, pos)
          }
        } else {
          if (idle_cash >= trade_size) {
            idle_cash <- idle_cash - trade_size
            invested <- invested + trade_size
            pos <- row |> mutate(trade_size = trade_size)
            active_positions <- bind_rows(active_positions, pos)
          }
        }
      }
    }

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

  if (nrow(active_positions) > 0) {
    still_open <- active_positions |>
      mutate(proceeds = NA_real_, profit = NA_real_)
    trades_executed <- bind_rows(trades_executed, still_open)
  }

  trade_blotter <- trades_executed |>
    as_tibble() |>
    mutate(trade_id = row_number()) |>
    select(
      trade_id,
      ticker,
      open_date,
      close_date,
      entry_price,
      exit_price,
      trade_size,
      buy_or_sell,
      gain_or_loss,
      proceeds,
      profit
    ) |>
    arrange(open_date)

  # SPY buy-and-hold series (for plotting when idle invested in SPY)
  spy_shares_bh <- initial_capital / start_spy_price
  spy_series <- spy_on_sim_dates |>
    mutate(spy_equity = spy_shares_bh * adj_close)

  # mountain plot: include SPY series only when idle capital held in SPY
  if (invest_idle_in == "SPY") {
    plot_df <- daily_log |>
      select(date, equity) |>
      left_join(spy_series |> select(date, spy_equity), by = "date")

    mountain_plot <- ggplot(plot_df, aes(x = date)) +
      geom_area(aes(y = equity), fill = "#2c7fb8", alpha = 0.25) +
      geom_line(aes(y = equity), color = "#0868ac", linewidth = 1) +
      geom_line(
        aes(y = spy_equity),
        color = "#a50f15",
        size = 1,
        linetype = "dashed"
      ) +
      labs(
        title = paste0(
          "Strategy Equity (area) vs SPY Buy-and-Hold — idle: ",
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
        title = paste0("Strategy Equity (area) — idle: ", invest_idle_in),
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
  invest_idle_in = "SPY"
)

sim_out$mountain_plot
sim_out$trade_blotter
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

# compute capm stats for trades using sim_out$daily_series$equity and spy_series$spy_equity
capm_results <- compute_capm_stats(sim_out$daily_series, sim_out$spy_series)
print(capm_results)

# Compute CAPM stats
capm_results <- compute_capm_stats(sim_out$daily_series, sim_out$spy_series)

# Create annotated plot
plot_df <- sim_out$daily_series |>
  select(date, equity) |>
  left_join(sim_out$spy_series |> select(date, spy_equity), by = "date")

# Create annotation text
annotation_text <- sprintf(
  "Annual Alpha: %.2f%%\nBeta: %.2f\nR²: %.2f",
  capm_results$alpha * 100,
  capm_results$beta,
  capm_results$r_squared
)

mountain_plot <- ggplot(plot_df, aes(x = date)) +
  geom_area(aes(y = equity), fill = "#2c7fb8", alpha = 0.25) +
  geom_line(aes(y = equity), color = "#0868ac", linewidth = 1) +
  geom_line(
    aes(y = spy_equity),
    color = "#a50f15",
    linewidth = 1,
    linetype = "dashed"
  ) +
  annotate(
    "text",
    x = min(plot_df$date) + (max(plot_df$date) - min(plot_df$date)) * 0.02,
    y = max(plot_df$equity, plot_df$spy_equity, na.rm = TRUE) * 0.95,
    label = annotation_text,
    hjust = 0,
    vjust = 1,
    size = 4,
    color = "#252525",
    fontface = "bold"
  ) +
  labs(
    title = paste0("Strategy Equity (area) vs SPY Buy-and-Hold — idle cash: ", invest_idle_in),
    x = "Date",
    y = "Capital ($)"
  ) +
  theme_minimal()

mountain_plot

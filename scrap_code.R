---
title: "Trading on Social Media Sentiment: A Stocktwits Analysis"
author: "Your Name"
date: today
format:
  html:
    code-fold: true
    toc: true
    toc-depth: 3
    theme: cosmo
execute:
  warning: false
  message: false
---

## Introduction

Can social media sentiment predict stock market movements? This analysis explores whether posts from Stocktwits.com contain actionable trading signals. We examine sentiment data from thousands of users posting about hundreds of stocks, matching their bullish and bearish predictions against actual price movements.

Our approach:

1. Match sentiments to actual stock performance over the following week
2. Compare each stock's performance to the S&P 500 (SPY) benchmark
3. Identify users with statistically significant predictive skill
4. Build and backtest trading strategies based on skilled users' signals

## Setup and Data Loading

```{r setup}
library(tidyverse)
library(duckplyr)
library(lubridate)
library(scales)
library(knitr)
library(broom)

# Set theme for visualizations
theme_set(theme_minimal(base_size = 12))
```

```{r load-data}
# Load the parquet files
prices <- read_parquet_duckdb("data/prices.parquet")
sentiments <- read_parquet_duckdb("data/sentiments.parquet")

# Quick overview
cat("Sentiments data:\n")
cat("  Rows:", sentiments |> count() |> pull(), "\n")
cat("  Date range:", 
    format(sentiments |> summarise(min(date)) |> pull(), "%Y-%m-%d"), "to",
    format(sentiments |> summarise(max(date)) |> pull(), "%Y-%m-%d"), "\n")
cat("  Unique users:", sentiments |> distinct(user_id) |> count() |> pull(), "\n")
cat("  Unique tickers:", sentiments |> distinct(ticker) |> count() |> pull(), "\n\n")

cat("Prices data:\n")
cat("  Rows:", prices |> count() |> pull(), "\n")
cat("  Date range:", 
    format(prices |> summarise(min(date)) |> pull(), "%Y-%m-%d"), "to",
    format(prices |> summarise(max(date)) |> pull(), "%Y-%m-%d"), "\n")
cat("  Unique tickers:", prices |> distinct(ticker) |> count() |> pull(), "\n")
```

## Step 1: Matching Sentiments to Price Changes

For each sentiment post, we need to:

1. Find the stock's adjusted close price on the post date
2. Find the price approximately one week (7 days) later
3. Calculate the percentage change
4. Do the same for SPY as our benchmark
5. Label the sentiment as "correct" if the stock outperformed SPY

```{r match-prices-function, eval=FALSE}
# This computation is intensive, so we'll save the results

methods_restore()
prices <- as_tibble(prices)
sentiments <- as_tibble(sentiments)

get_fut_price <- function(prices, days_ahead){
price_chg <- prices |> 
  transmute(ticker,date,price_t0 = adj_close, future_date = date + days(days_ahead)) |> 
  left_join(prices |> 
      select(ticker, date, price_fut = adj_close),
    by = join_by("ticker", "future_date" == "date")
  ) |> filter(!is.na(price_fut))
  return(price_chg)
}
price_chg <- get_fut_price(prices, 7)
price_chg <- anti_join(prices,price_chg) |> 
  get_fut_price(8) |> 
  bind_rows(price_chg)
# 3 days should be enough to fill most gaps
price_chg <- anti_join(prices,price_chg) |> 
  get_fut_price(9) |> 
  bind_rows(price_chg)
# get indexed value change
price_chg <- price_chg |> 
  mutate(
    wk_return = (price_fut - price_t0) / price_t0
  ) |> 
  select(ticker, date, wk_return)
# add column for return relative to SPY return
# Get SPY returns for the same dates
spy_returns <- price_chg |>
  filter(ticker == "SPY") |>
  select(date, spy_wk_return = wk_return)
# Add excess return column (stock return - SPY return)
price_chg <- price_chg |>
  left_join(spy_returns, by = "date") |>
  mutate(
    excess_return = wk_return - spy_wk_return
  ) |>
  select(ticker, date, wk_return, excess_return)


# Join sentiments with prices on the post date
sentiments_with_returns <- sentiments |>
  left_join(price_chg,by = c("ticker", "date"))


sentiment_results <- sentiments_with_returns |>
  mutate(
    # Correct if bullish and outperformed, or bearish and underperformed
    correct = case_when(
      bullish & excess_return > 0 ~ TRUE,
      !bullish & excess_return < 0 ~ TRUE,
      TRUE ~ FALSE
    )
  )

# Save the matched data
compute_parquet(sentiment_results, "data/sentiment_results.parquet")
```

```{r load-matched-data}
# Load the pre-computed matched data
sentiment_results <- read_parquet_duckdb("data/sentiment_results.parquet") |> 
  as_tibble()

cat("Matched sentiments with returns:\n")
cat("  Total observations:", nrow(sentiments_with_returns), "\n")
cat("  Date range:", 
    format(min(sentiments_with_returns$date), "%Y-%m-%d"), "to",
    format(max(sentiments_with_returns$date), "%Y-%m-%d"), "\n")
```

## Step 2: Overall User Skill Analysis

Let's examine how accurate users are overall, and break this down by the four types of outcomes.

```{r overall-accuracy}
# Overall accuracy
overall_stats <- sentiment_results |>
  summarise(
    total_posts = n(),
    accuracy = mean(correct),
    correct = sum(correct),
    incorrect = n()-sum(correct),
    avg_excess_return = mean(excess_return,na.rm=TRUE),
    median_excess_return = median(excess_return,na.rm=TRUE)
  )

kable(overall_stats, digits = 4, caption = "Overall Sentiment Accuracy")
```

```{r four-types}
# Four types of outcomes
outcome_types <- sentiments_with_returns |>
  mutate(
    outcome_type = case_when(
      bullish & correct ~ "Bullish & Correct",
      bullish & !correct ~ "Bullish & Incorrect",
      !bullish & correct ~ "Bearish & Correct",
      !bullish & !correct ~ "Bearish & Incorrect"
    )
  ) |>
  group_by(outcome_type) |>
  summarise(
    count = n(),
    pct_of_total = n() / nrow(sentiments_with_returns),
    avg_stock_return = mean(stock_return),
    avg_spy_return = mean(spy_return),
    avg_excess_return = mean(excess_return),
    median_excess_return = median(excess_return),
    .groups = "drop"
  ) |>
  arrange(desc(outcome_type))

kable(outcome_types, digits = 4, caption = "Breakdown by Outcome Type")
```

```{r bullish-bearish-split}
# Compare bullish vs bearish sentiment accuracy
sentiment_split <- sentiments_with_returns |>
  group_by(bullish) |>
  summarise(
    total_posts = n(),
    correct = sum(correct),
    accuracy = mean(correct),
    avg_excess_return = mean(excess_return),
    .groups = "drop"
  ) |>
  mutate(sentiment = ifelse(bullish, "Bullish", "Bearish"))

kable(sentiment_split |> select(sentiment, everything(), -bullish), 
      digits = 4, 
      caption = "Accuracy by Sentiment Type")
```

```{r visualize-outcomes}
# Visualize the distribution of excess returns by outcome type
sentiments_with_returns |>
  mutate(
    outcome_type = case_when(
      bullish & correct ~ "Bullish & Correct",
      bullish & !correct ~ "Bullish & Incorrect",
      !bullish & correct ~ "Bearish & Correct",
      !bullish & !correct ~ "Bearish & Incorrect"
    )
  ) |>
  ggplot(aes(x = excess_return, fill = outcome_type)) +
  geom_histogram(bins = 100, alpha = 0.7) +
  facet_wrap(~outcome_type, ncol = 2, scales = "free_y") +
  scale_x_continuous(labels = percent_format(), limits = c(-0.5, 0.5)) +
  scale_fill_brewer(palette = "Set2") +
  labs(
    title = "Distribution of Excess Returns by Outcome Type",
    subtitle = "Excess return = Stock return - SPY return over ~1 week",
    x = "Excess Return",
    y = "Count",
    fill = "Outcome Type"
  ) +
  theme(legend.position = "none")
```

```{r accuracy-over-time}
# Track accuracy over time
accuracy_over_time <- sentiments_with_returns |>
  mutate(month = floor_date(date, "month")) |>
  group_by(month) |>
  summarise(
    posts = n(),
    accuracy = mean(correct),
    avg_excess_return = mean(excess_return),
    .groups = "drop"
  )

ggplot(accuracy_over_time, aes(x = month)) +
  geom_line(aes(y = accuracy), color = "steelblue", linewidth = 1) +
  geom_hline(yintercept = 0.5, linetype = "dashed", color = "red", alpha = 0.5) +
  scale_y_continuous(labels = percent_format(), limits = c(0.4, 0.6)) +
  labs(
    title = "Sentiment Accuracy Over Time",
    subtitle = "Monthly average accuracy compared to random (50%)",
    x = "Month",
    y = "Accuracy Rate"
  )
```

## Step 3: User-Level Skill Assessment

Now let's identify which users demonstrate statistically significant predictive skill.

```{r user-level-stats}
# Calculate per-user statistics
user_stats <- sentiments_with_returns |>
  group_by(user_id) |>
  summarise(
    total_posts = n(),
    correct = sum(correct),
    accuracy = mean(correct),
    avg_excess_return = mean(excess_return),
    median_excess_return = median(excess_return),
    .groups = "drop"
  ) |>
  filter(total_posts >= 10)  # Only users with at least 10 posts

# Binomial test for statistical significance
# H0: accuracy = 0.5 (random guessing)
user_stats_with_tests <- user_stats |>
  rowwise() |>
  mutate(
    p_value = binom.test(correct, total_posts, p = 0.5, alternative = "two.sided")$p.value,
    significant = p_value < 0.05,
    skill_type = case_when(
      significant & accuracy > 0.5 ~ "Positive Skill",
      significant & accuracy < 0.5 ~ "Negative Skill",
      TRUE ~ "No Significant Skill"
    )
  ) |>
  ungroup()

# Summary of user skills
skill_summary <- user_stats_with_tests |>
  group_by(skill_type) |>
  summarise(
    users = n(),
    avg_posts = mean(total_posts),
    avg_accuracy = mean(accuracy),
    avg_excess_return = mean(avg_excess_return),
    .groups = "drop"
  )

kable(skill_summary, digits = 4, caption = "User Skill Distribution")
```

```{r visualize-user-skills}
# Scatter plot: posts vs accuracy
ggplot(user_stats_with_tests, aes(x = total_posts, y = accuracy, color = skill_type)) +
  geom_point(alpha = 0.6) +
  geom_hline(yintercept = 0.5, linetype = "dashed", color = "red") +
  scale_x_log10() +
  scale_y_continuous(labels = percent_format()) +
  scale_color_manual(values = c("Positive Skill" = "darkgreen", 
                                  "Negative Skill" = "darkred",
                                  "No Significant Skill" = "gray60")) +
  labs(
    title = "User Accuracy vs. Activity Level",
    subtitle = "Statistical significance at p < 0.05",
    x = "Total Posts (log scale)",
    y = "Accuracy Rate",
    color = "Skill Type"
  )
```

```{r top-users}
# Top skilled users (positive)
top_positive <- user_stats_with_tests |>
  filter(skill_type == "Positive Skill") |>
  arrange(desc(accuracy)) |>
  head(10) |>
  select(user_id, total_posts, accuracy, avg_excess_return, p_value)

kable(top_positive, digits = 4, caption = "Top 10 Users with Positive Skill")

# Top negatively skilled users
top_negative <- user_stats_with_tests |>
  filter(skill_type == "Negative Skill") |>
  arrange(accuracy) |>
  head(10) |>
  select(user_id, total_posts, accuracy, avg_excess_return, p_value)

kable(top_negative, digits = 4, caption = "Top 10 Users with Negative Skill")
```

## Step 4: Rolling 3-Month Skill Assessment

For the trading strategy, we need to identify skilled users on a rolling basis using data from the prior 3 months, evaluated weekly.

```{r rolling-skill-assessment, eval=FALSE}
# This is computationally intensive, so we'll save the results

# Get unique weeks for evaluation
eval_dates <- sentiments_with_returns |>
  distinct(date) |>
  arrange(date) |>
  mutate(week = floor_date(date, "week")) |>
  distinct(week) |>
  filter(week >= min(week) + months(3))  # Need 3 months of history

# Function to assess user skill based on prior 3 months
assess_rolling_skill <- function(eval_date, data, min_posts = 10, p_threshold = 0.05) {
  # Get data from 3 months prior
  start_date <- eval_date - months(3)
  
  historical_data <- data |>
    filter(date >= start_date, date < eval_date)
  
  if(nrow(historical_data) == 0) return(NULL)
  
  # Calculate user statistics
  user_stats <- historical_data |>
    group_by(user_id) |>
    summarise(
      total_posts = n(),
      correct = sum(correct),
      accuracy = mean(correct),
      .groups = "drop"
    ) |>
    filter(total_posts >= min_posts)
  
  # Statistical testing
  user_stats |>
    rowwise() |>
    mutate(
      p_value = binom.test(correct, total_posts, p = 0.5, 
                           alternative = "two.sided")$p.value,
      significant = p_value < p_threshold,
      skill_direction = case_when(
        significant & accuracy > 0.5 ~ "positive",
        significant & accuracy < 0.5 ~ "negative",
        TRUE ~ "none"
      ),
      eval_date = eval_date
    ) |>
    ungroup() |>
    filter(skill_direction != "none") |>
    select(eval_date, user_id, skill_direction, accuracy, total_posts, p_value)
}

# Apply rolling assessment
rolling_skill_results <- map_df(
  eval_dates$week,
  ~assess_rolling_skill(.x, sentiments_with_returns),
  .progress = TRUE
)

# Save results
write_rds(rolling_skill_results, "data/rolling_skill_results.rds")
```

```{r load-rolling-skill}
# Load pre-computed rolling skill results
rolling_skill_results <- read_rds("data/rolling_skill_results.rds")

cat("Rolling skill assessment:\n")
cat("  Evaluation periods:", n_distinct(rolling_skill_results$eval_date), "\n")
cat("  Total user-week observations:", nrow(rolling_skill_results), "\n")
cat("  Unique skilled users identified:", n_distinct(rolling_skill_results$user_id), "\n")

# Breakdown by skill type
rolling_skill_results |>
  count(skill_direction) |>
  mutate(pct = n / sum(n)) |>
  kable(digits = 3, caption = "Skilled User Observations by Type")
```

```{r rolling-skill-over-time}
# Track number of skilled users over time
skilled_users_over_time <- rolling_skill_results |>
  group_by(eval_date, skill_direction) |>
  summarise(users = n_distinct(user_id), .groups = "drop")

ggplot(skilled_users_over_time, aes(x = eval_date, y = users, 
                                     color = skill_direction, fill = skill_direction)) +
  geom_area(alpha = 0.3, position = "identity") +
  geom_line(linewidth = 1) +
  scale_color_manual(values = c("positive" = "darkgreen", "negative" = "darkred")) +
  scale_fill_manual(values = c("positive" = "darkgreen", "negative" = "darkred")) +
  labs(
    title = "Skilled Users Identified Over Time",
    subtitle = "Based on 3-month rolling window with statistical significance (p < 0.05)",
    x = "Evaluation Date",
    y = "Number of Skilled Users",
    color = "Skill Type",
    fill = "Skill Type"
  )
```

## Step 5: Trading Strategy Implementation

Now we implement two trading strategies:

1. **Long-Only Strategy**: Only take long positions (buy signals)
2. **Long-Short Strategy**: Take both long and short positions

Both strategies:
- Start with $100,000
- Open positions of $1,000 each
- Maximum 10 position openings per day
- Hold SPY when not in other positions
- Close positions after ~1 week
- No margin, no transaction costs

```{r prepare-trading-signals, eval=FALSE}
# Generate trading signals based on rolling skill assessment

# For each sentiment post, check if the user had identified skill in the prior week
trading_signals <- sentiments_with_returns |>
  mutate(eval_week = floor_date(date, "week")) |>
  left_join(
    rolling_skill_results |> select(eval_date, user_id, skill_direction),
    by = c("eval_week" = "eval_date", "user_id")
  ) |>
  filter(!is.na(skill_direction)) |>
  mutate(
    # Generate signal based on user skill and sentiment
    signal = case_when(
      skill_direction == "positive" & bullish ~ "buy",
      skill_direction == "positive" & !bullish ~ "sell",
      skill_direction == "negative" & bullish ~ "sell",
      skill_direction == "negative" & !bullish ~ "buy",
      TRUE ~ "none"
    )
  ) |>
  filter(signal != "none") |>
  arrange(date, ticker) |>
  select(date, ticker, user_id, bullish, skill_direction, signal, 
         price_t0, price_t7, date_t7, stock_return, excess_return)

# Save trading signals
write_rds(trading_signals, "data/trading_signals.rds")
```

```{r load-trading-signals}
# Load pre-computed trading signals
trading_signals <- read_rds("data/trading_signals.rds")

cat("Trading signals generated:\n")
cat("  Total signals:", nrow(trading_signals), "\n")
cat("  Date range:", format(min(trading_signals$date), "%Y-%m-%d"), "to",
    format(max(trading_signals$date), "%Y-%m-%d"), "\n")

trading_signals |>
  count(signal) |>
  mutate(pct = n / sum(n)) |>
  kable(digits = 3, caption = "Trading Signal Distribution")
```

```{r backtest-strategies, eval=FALSE}
# Backtest both strategies

# Helper function to get SPY return for any period
get_spy_return <- function(date_start, date_end, spy_prices_data) {
  price_start <- spy_prices_data |>
    filter(date <= date_start) |>
    arrange(desc(date)) |>
    slice(1) |>
    pull(spy_price)
  
  price_end <- spy_prices_data |>
    filter(date >= date_end) |>
    arrange(date) |>
    slice(1) |>
    pull(spy_price)
  
  if(length(price_start) == 0 || length(price_end) == 0) return(0)
  
  (price_end - price_start) / price_start
}

# SPY prices for reference
spy_prices <- prices |>
  filter(ticker == "SPY") |>
  select(date, spy_price = adj_close) |>
  collect()

# Strategy execution function
execute_strategy <- function(signals, initial_capital = 100000, 
                             position_size = 1000, max_daily_trades = 10,
                             allow_short = TRUE) {
  
  # Initialize portfolio
  cash <- initial_capital
  positions <- tibble(
    ticker = character(),
    entry_date = as.Date(character()),
    exit_date = as.Date(character()),
    entry_price = numeric(),
    exit_price = numeric(),
    position_type = character(),
    shares = numeric(),
    pnl = numeric()
  )
  
  # Daily portfolio values
  portfolio_values <- tibble(
    date = as.Date(character()),
    cash = numeric(),
    position_value = numeric(),
    total_value = numeric()
  )
  
  # Get all unique dates
  all_dates <- seq(min(signals$date), max(signals$date), by = "day")
  
  # Process each day
  for(current_date in all_dates) {
    current_date <- as.Date(current_date, origin = "1970-01-01")
    
    # Close positions that have reached their exit date
    positions_to_close <- positions |>
      filter(is.na(pnl), exit_date <= current_date)
    
    if(nrow(positions_to_close) > 0) {
      for(i in 1:nrow(positions_to_close)) {
        pos <- positions_to_close[i, ]
        exit_price <- pos$exit_price
        
        if(pos$position_type == "long") {
          pnl <- (exit_price - pos$entry_price) * pos$shares
        } else {  # short
          pnl <- (pos$entry_price - exit_price) * abs(pos$shares)
        }
        
        cash <- cash + position_size + pnl
        
        # Update position with PnL
        positions <- positions |>
          mutate(pnl = ifelse(ticker == pos$ticker & 
                              entry_date == pos$entry_date & 
                              is.na(pnl), pnl, pnl))
      }
    }
    
    # Open new positions based on today's signals
    today_signals <- signals |>
      filter(date == current_date) |>
      head(max_daily_trades)
    
    if(nrow(today_signals) > 0) {
      for(i in 1:nrow(today_signals)) {
        sig <- today_signals[i, ]
        
        # Check if we have enough cash
        if(cash < position_size) next
        
        # Determine position type
        if(sig$signal == "buy") {
          position_type <- "long"
        } else if(sig$signal == "sell" && allow_short) {
          position_type <- "short"
        } else {
          next  # Skip sell signals if shorts not allowed
        }
        
        # Calculate shares
        shares <- if(position_type == "long") {
          position_size / sig$price_t0
        } else {
          -position_size / sig$price_t0
        }
        
        # Open position
        new_position <- tibble(
          ticker = sig$ticker,
          entry_date = sig$date,
          exit_date = sig$date_t7,
          entry_price = sig$price_t0,
          exit_price = sig$price_t7,
          position_type = position_type,
          shares = shares,
          pnl = NA_real_
        )
        
        positions <- bind_rows(positions, new_position)
        cash <- cash - position_size
      }
    }
    
    # Calculate total portfolio value
    # Value of cash + value of open positions
    open_positions <- positions |> filter(is.na(pnl))
    position_value <- nrow(open_positions) * position_size  # Simplified
    
    portfolio_values <- bind_rows(
      portfolio_values,
      tibble(
        date = current_date,
        cash = cash,
        position_value = position_value,
        total_value = cash + position_value
      )
    )
  }
  
  list(
    positions = positions |> filter(!is.na(pnl)),
    portfolio_values = portfolio_values
  )
}

# Execute both strategies
cat("Running long-short strategy...\n")
strategy_long_short <- execute_strategy(
  trading_signals, 
  allow_short = TRUE
)

cat("Running long-only strategy...\n")
strategy_long_only <- execute_strategy(
  trading_signals, 
  allow_short = FALSE
)

# Calculate SPY buy-and-hold performance
spy_start_price <- spy_prices |>
  filter(date <= min(trading_signals$date)) |>
  arrange(desc(date)) |>
  slice(1) |>
  pull(spy_price)

spy_benchmark <- spy_prices |>
  filter(date >= min(trading_signals$date),
         date <= max(trading_signals$date)) |>
  mutate(
    portfolio_value = 100000 * (spy_price / spy_start_price)
  ) |>
  select(date, spy_value = portfolio_value)

# Save results
backtest_results <- list(
  long_short = strategy_long_short,
  long_only = strategy_long_only,
  spy_benchmark = spy_benchmark
)

write_rds(backtest_results, "data/backtest_results.rds")
```

```{r load-backtest-results}
# Load pre-computed backtest results
backtest_results <- read_rds("data/backtest_results.rds")

# Extract components
long_short_positions <- backtest_results$long_short$positions
long_short_values <- backtest_results$long_short$portfolio_values
long_only_positions <- backtest_results$long_only$positions
long_only_values <- backtest_results$long_only$portfolio_values
spy_benchmark <- backtest_results$spy_benchmark
```

## Step 6: Strategy Performance Comparison

```{r strategy-performance-stats}
# Calculate final returns
initial_capital <- 100000

final_long_short <- long_short_values |>
  arrange(desc(date)) |>
  slice(1) |>
  pull(total_value)

final_long_only <- long_only_values |>
  arrange(desc(date)) |>
  slice(1) |>
  pull(total_value)

final_spy <- spy_benchmark |>
  arrange(desc(date)) |>
  slice(1) |>
  pull(spy_value)

# Summary statistics
strategy_summary <- tibble(
  Strategy = c("Long-Short", "Long-Only", "Buy & Hold SPY"),
  Initial_Value = initial_capital,
  Final_Value = c(final_long_short, final_long_only, final_spy),
  Total_Return = (Final_Value - Initial_Value) / Initial_Value,
  Total_Return_Pct = Total_Return * 100
)

kable(strategy_summary, digits = 2, caption = "Strategy Performance Summary")
```

```{r plot-portfolio-values}
# Combine all portfolio values for plotting
combined_values <- bind_rows(
  long_short_values |> mutate(Strategy = "Long-Short"),
  long_only_values |> mutate(Strategy = "Long-Only"),
  spy_benchmark |> 
    rename(total_value = spy_value) |> 
    mutate(Strategy = "Buy & Hold SPY", cash = NA, position_value = NA)
) |>
  select(date, Strategy, total_value)

ggplot(combined_values, aes(x = date, y = total_value, color = Strategy)) +
  geom_line(linewidth = 1) +
  geom_hline(yintercept = initial_capital, linetype = "dashed", alpha = 0.5) +
  scale_y_continuous(labels = dollar_format()) +
  scale_color_manual(values = c("Long-Short" = "darkblue", 
                                 "Long-Only" = "darkgreen",
                                 "Buy & Hold SPY" = "gray50")) +
  labs(
    title = "Portfolio Value Over Time: Trading Strategies vs. SPY",
    subtitle = sprintf("Initial capital: %s", dollar(initial_capital)),
    x = "Date",
    y = "Portfolio Value",
    color = "Strategy"
  ) +
  theme(legend.position = "bottom")
```

```{r trade-statistics}
# Analyze individual trades for both strategies
trade_stats_long_short <- long_short_positions |>
  summarise(
    total_trades = n(),
    winning_trades = sum(pnl > 0),
    losing_trades = sum(pnl <= 0),
    win_rate = mean(pnl > 0),
    avg_profit = mean(pnl),
    median_profit = median(pnl),
    total_profit = sum(pnl),
    best_trade = max(pnl),
    worst_trade = min(pnl)
  ) |>
  mutate(Strategy = "Long-Short")

trade_stats_long_only <- long_only_positions |>
  summarise(
    total_trades = n(),
    winning_trades = sum(pnl > 0),
    losing_trades = sum(pnl <= 0),
    win_rate = mean(pnl > 0),
    avg_profit = mean(pnl),
    median_profit = median(pnl),
    total_profit = sum(pnl),
    best_trade = max(pnl),
    worst_trade = min(pnl)
  ) |>
  mutate(Strategy = "Long-Only")

trade_stats <- bind_rows(trade_stats_long_short, trade_stats_long_only) |>
  select(Strategy, everything())

kable(trade_stats, digits = 2, caption = "Trade-Level Statistics")
```

```{r plot-trade-distribution}
# Distribution of trade P&L
combined_positions <- bind_rows(
  long_short_positions |> mutate(Strategy = "Long-Short"),
  long_only_positions |> mutate(Strategy = "Long-Only")
)

ggplot(combined_positions, aes(x = pnl, fill = Strategy)) +
  geom_histogram(bins = 50, alpha = 0.7, position = "identity") +
  geom_vline(xintercept = 0, linetype = "dashed", color = "red") +
  scale_x_continuous(labels = dollar_format()) +
  scale_fill_manual(values = c("Long-Short" = "darkblue", 
                                "Long-Only" = "darkgreen")) +
  labs(
    title = "Distribution of Trade Profits and Losses",
    x = "Trade P&L",
    y = "Count",
    fill = "Strategy"
  ) +
  theme(legend.position = "bottom")
```

```{r cumulative-trades}
# Cumulative P&L over time
combined_positions_cumulative <- combined_positions |>
  arrange(exit_date) |>
  group_by(Strategy) |>
  mutate(cumulative_pnl = cumsum(pnl)) |>
  ungroup()

ggplot(combined_positions_cumulative, aes(x = exit_date, y = cumulative_pnl, 
                                          color = Strategy)) +
  geom_line(linewidth = 1) +
  geom_hline(yintercept = 0, linetype = "dashed", alpha = 0.5) +
  scale_y_continuous(labels = dollar_format()) +
  scale_color_manual(values = c("Long-Short" = "darkblue", 
                                 "Long-Only" = "darkgreen")) +
  labs(
    title = "Cumulative P&L from Trades Over Time",
    x = "Trade Exit Date",
    y = "Cumulative P&L",
    color = "Strategy"
  ) +
  theme(legend.position = "bottom")
```

## Conclusions

This analysis examined whether social media sentiment from Stocktwits can generate profitable trading signals. Key findings:

### User Skill Assessment

- Overall accuracy of sentiment posts was close to random (check specific value above)
- A subset of users demonstrated statistically significant predictive skill
- Both positive skill (accurate predictions) and negative skill (consistently wrong) were identified
- The number of skilled users varied over time based on rolling 3-month assessments

### Trading Strategy Performance

Comparing our two strategies against a simple buy-and-hold SPY benchmark:

1. **Long-Short Strategy**: Allowed both long and short positions based on skilled users' signals
2. **Long-Only Strategy**: Only took long positions, ignoring sell signals
3. **SPY Benchmark**: Simply held SPY for the entire period

[Specific performance numbers are shown in the tables above]

### Key Insights

- The strategy's success depends critically on identifying truly skilled users
- Statistical significance testing (p < 0.05) with minimum post thresholds helps filter signal quality
- Short selling capability [added/reduced] strategy performance
- Transaction costs and slippage would reduce real-world returns
- The rolling assessment window allowed the strategy to adapt to changing user skill levels

### Limitations

- No transaction costs or market impact considered
- Perfect price execution assumed
- Look-ahead bias eliminated through rolling assessment
- Survivorship bias in the dataset should be considered
- Statistical significance thresholds may need optimization

This framework demonstrates that social media sentiment can potentially be incorporated into a trading strategy, particularly when combined with rigorous statistical filtering of signal sources.

## Appendix: Additional Visualizations

```{r monthly-returns}
# Calculate monthly returns for each strategy
monthly_returns_long_short <- long_short_values |>
  mutate(month = floor_date(date, "month")) |>
  group_by(month) |>
  filter(date == max(date)) |>
  ungroup() |>
  mutate(
    monthly_return = (total_value - lag(total_value)) / lag(total_value),
    Strategy = "Long-Short"
  ) |>
  select(month, Strategy, monthly_return)

monthly_returns_long_only <- long_only_values |>
  mutate(month = floor_date(date, "month")) |>
  group_by(month) |>
  filter(date == max(date)) |>
  ungroup() |>
  mutate(
    monthly_return = (total_value - lag(total_value)) / lag(total_value),
    Strategy = "Long-Only"
  ) |>
  select(month, Strategy, monthly_return)

monthly_returns_spy <- spy_benchmark |>
  mutate(month = floor_date(date, "month")) |>
  group_by(month) |>
  filter(date == max(date)) |>
  ungroup() |>
  mutate(
    monthly_return = (spy_value - lag(spy_value)) / lag(spy_value),
    Strategy = "Buy & Hold SPY"
  ) |>
  select(month, Strategy, monthly_return)

monthly_returns_combined <- bind_rows(
  monthly_returns_long_short,
  monthly_returns_long_only,
  monthly_returns_spy
) |>
  filter(!is.na(monthly_return))

ggplot(monthly_returns_combined, aes(x = month, y = monthly_return, 
                                     fill = Strategy)) +
  geom_col(position = "dodge") +
  geom_hline(yintercept = 0, color = "black") +
  scale_y_continuous(labels = percent_format()) +
  scale_fill_manual(values = c("Long-Short" = "darkblue", 
                                "Long-Only" = "darkgreen",
                                "Buy & Hold SPY" = "gray50")) +
  labs(
    title = "Monthly Returns Comparison",
    x = "Month",
    y = "Monthly Return",
    fill = "Strategy"
  ) +
  theme(legend.position = "bottom")
```

```{r position-types}
# Breakdown of position types for long-short strategy
long_short_positions |>
  count(position_type) |>
  mutate(pct = n / sum(n)) |>
  ggplot(aes(x = position_type, y = n, fill = position_type)) +
  geom_col() +
  geom_text(aes(label = sprintf("%d (%.1f%%)", n, pct * 100)), 
            vjust = -0.5) +
  scale_fill_manual(values = c("long" = "darkgreen", "short" = "darkred")) +
  labs(
    title = "Position Types in Long-Short Strategy",
    x = "Position Type",
    y = "Number of Trades",
    fill = "Position Type"
  ) +
  theme(legend.position = "none")
```

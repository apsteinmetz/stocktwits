# distributions of ticker mention rank vs number of mentions
library(ggplot2)
library(tidyverse)
library(duckplyr)

ticker_count <- read_parquet_duckdb("data/ticker_post_counts.parquet") |>
  as_tibble()

ggplot(ticker_count, aes(x = rank, y = post_count)) +
  geom_col()

# Test for power law distribution
# A power law would show as a straight line on a log-log plot

# Create log-log plot
ticker_count |>
  head(1000) |>
  ggplot(aes(x = rank, y = post_count)) +
  geom_point(alpha = 0.6, color = "steelblue") +
  scale_x_log10() +
  scale_y_log10() +
  labs(
    title = "Ticker Post Count vs Rank (log-log scale)",
    subtitle = "A straight line would indicate a power law distribution",
    x = "Rank (log scale)",
    y = "Post Count (log scale)"
  ) +
  theme_minimal() +
  geom_smooth(method = "lm", se = TRUE, color = "red", linewidth = 0.8)

# Fit a power law model: post_count = a * rank^(-b)
# Taking log: log(post_count) = log(a) - b * log(rank)
# This is a linear model in log space

power_law_model <- lm(log10(post_count) ~ log10(rank), data = ticker_count)

summary(power_law_model)

# Get the power law exponent
exponent <- -coef(power_law_model)[2]
intercept <- coef(power_law_model)[1]

cat("\nPower Law Fit:\n")
cat(sprintf("post_count ≈ %.0f × rank^%.3f\n", 10^intercept, -exponent))
cat(sprintf("R-squared: %.4f\n", summary(power_law_model)$r.squared))
cat(sprintf("Power law exponent: %.3f\n", exponent))
# test whether rank vs mentions follows a power law

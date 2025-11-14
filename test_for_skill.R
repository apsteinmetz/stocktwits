# test for skill
library(tidyverse)
library(gt)

# Perform binomial tests for each user
# H0: p = 0.5 (random chance)
# H1: p ≠ 0.5 (not random chance)

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
    ) |>
    select(-p_value)

  return(significant_posters)
}

# separate bulls from bears in plot
significant_posters <- get_significanct_posters(bear_win_rate) |>
  mutate(sentiment = "bearish") |>
  bind_rows(
    get_significanct_posters(bull_win_rate) |>
      mutate(sentiment = "bullish")
  ) |>
  mutate(sentiment = as.factor(sentiment))

gg <- significant_posters |>
  ggplot(aes(x = win_rate, fill = sentiment)) +
  geom_histogram(binwidth = 0.01, position = "stack", color = "black") +
  scale_fill_manual(
    name = "Sentiment",
    values = c("bearish" = "#D55E00", "bullish" = "#009E73")
  ) +
  labs(
    title = "There are far more Bullish Posts and Far More Posters with Negative Skill",
    subtitle = "Histogram of Batting Averages for Statistically Significant Posters",
    x = "Batting Average",
    y = "Count"
  ) +
  geom_vline(
    xintercept = 0.5,
    linetype = "dashed",
    color = "red",
    linewidth = 2
  ) +
  theme_minimal()
print(gg)

cat(
  "posters with win rates statistically different from random chance (p < 0.05):\n\n"
)
cat("\nSummary:\n")
cat(paste("Total posters tested:", summarise(track_record, n()), "\n"))
cat(paste(
  "posters significantly different from chance:",
  nrow(significant_posters),
  "\n"
))
cat(paste(
  "posters significantly better than chance (win_rate > 0.5):",
  nrow(filter(significant_posters, alpha_direction == 1)),
  "\n"
))
cat(paste(
  "posters significantly better than chance (win_rate > 0.5):",
  nrow(filter(significant_posters, alpha_direction == -1)),
  "\n"
))

# make a table of the summary using gt with bearish/bullish counts
total_tested <- track_record |>
  summarise(total = n()) |>
  collect() |>
  pull(total)
sentiment_count <- track_record |>
  summarise(.by = bullish, total = n()) |>
  collect()
bullish_total <- pull(
  filter(sentiment_count, bullish == TRUE),
  total
)
bearish_total <- pull(
  filter(sentiment_count, bullish == FALSE),
  total
)

significant_total <- nrow(significant_posters)
significant_bearish <- significant_posters |>
  filter(sentiment == "bearish") |>
  nrow()
significant_bullish <- significant_posters |>
  filter(sentiment == "bullish") |>
  nrow()

above_total <- significant_posters |>
  filter(alpha_direction == 1) |>
  nrow()
above_bearish <- significant_posters |>
  filter(alpha_direction == 1, sentiment == "bearish") |>
  nrow()
above_bullish <- significant_posters |>
  filter(alpha_direction == 1, sentiment == "bullish") |>
  nrow()

below_total <- significant_posters |>
  filter(alpha_direction == -1) |>
  nrow()
below_bearish <- significant_posters |>
  filter(alpha_direction == -1, sentiment == "bearish") |>
  nrow()
below_bullish <- significant_posters |>
  filter(alpha_direction == -1, sentiment == "bullish") |>
  nrow()

summary_table <- tibble(
  Metric = c(
    "Total posters tested",
    "posters significantly different from chance",
    "posters significantly better than .500",
    "posters significantly worse than .500"
  ),
  Count = c(
    total_tested,
    significant_total,
    above_total,
    below_total
  ),
  Bearish = c(
    bearish_total,
    significant_bearish,
    above_bearish,
    below_bearish
  ),
  Bullish = c(
    bullish_total,
    significant_bullish,
    above_bullish,
    below_bullish
  )
)

summary_table |>
  gt() |>
  tab_header(
    title = "Summary of User Win Rate Statistical Tests"
  ) |>
  fmt_number(
    columns = c(Count, Bearish, Bullish),
    decimals = 0
  ) |>
  print()

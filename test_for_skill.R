# test for skill
library(tidyverse)
library(gt)

# Perform binomial tests for each user
# H0: p = 0.5 (random chance)
# H1: p ≠ 0.5 (not random chance)

results <- bull_win_rate |>
  # compute(prudence = "lavish") |>
  # collect() |>
  as_tibble() |>
  rowwise() |>
  mutate(
    # Perform two-sided binomial test
    binom_test = list(binom.test(
      wins_absolute,
      total,
      p = 0.5,
      alternative = "two.sided"
    )),
    binom_test_rel = list(binom.test(
      wins_vs_SPY,
      total,
      p = 0.5,
      alternative = "two.sided"
    )),
    p_value = binom_test$p.value,
    conf_low = binom_test$conf.int[1],
    conf_high = binom_test$conf.int[2],
    p_value_rel = binom_test_rel$p.value,
    conf_low_rel = binom_test_rel$conf.int[1],
    conf_high_rel = binom_test_rel$conf.int[2]
  ) |>
  ungroup() |>
  select(-binom_test, -binom_test_rel, -starts_with("conf")) |>
  arrange(p_value)


# Filter for statistically significant results (p < 0.05)
significant_posters <- results |>
  filter(p_value < 0.05) |>
  mutate(
    direction = case_when(
      win_rate > 0.5 ~ "Above chance",
      win_rate < 0.5 ~ "Below chance",
      TRUE ~ "At chance"
    )
  )
significant_rel_posters <- results |>
  filter(p_value_rel < 0.05) |>
  mutate(
    direction = case_when(
      win_rate_vs_spy > 0.5 ~ "Above chance",
      win_rate_vs_spy < 0.5 ~ "Below chance",
      TRUE ~ "At chance"
    )
  )

significant_posters |>
  ggplot(aes(x = win_rate)) +
  geom_histogram(binwidth = 0.01, fill = "blue", color = "black") +
  labs(
    title = "Histogram of Batting Averages for Statistically Significant Posters",
    x = "Batting Average",
    y = "Count"
  ) +
  # add vertical line at 0.5
  geom_vline(
    xintercept = 0.5,
    linetype = "dashed",
    color = "red",
    linewidth = 2
  ) +
  theme_minimal()

cat(
  "posters with win rates statistically different from random chance (p < 0.05):\n\n"
)
significant_posters |>
  select(user_id, total, wins_absolute, win_rate, p_value, direction) |>
  print()

cat("\nSummary:\n")
cat(paste("Total posters tested:", nrow(results), "\n"))
cat(paste(
  "posters significantly different from chance:",
  nrow(significant_posters),
  "\n"
))
cat(paste(
  "posters significantly above chance (win_rate > 0.5):",
  sum(significant_posters$direction == "Above chance"),
  "\n"
))
cat(paste(
  "posters significantly below chance (win_rate < 0.5):",
  sum(significant_posters$direction == "Below chance"),
  "\n"
))

# make a table of the summary using gt
summary_table <- tibble(
  Metric = c(
    "Total posters tested",
    "posters significantly different from chance",
    "posters significantly above chance (win_rate > 0.5)",
    "posters significantly below chance (win_rate < 0.5)"
  ),
  Count = c(
    nrow(results),
    nrow(significant_posters),
    sum(significant_posters$direction == "Above chance"),
    sum(significant_posters$direction == "Below chance")
  )
)
summary_table |>
  gt() |>
  tab_header(
    title = "Summary of User Win Rate Statistical Tests"
  ) |>
  fmt_number(
    columns = c(Count),
    decimals = 0
  ) |>
  print()

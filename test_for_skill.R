# test for skill
library(tidyverse)

# Perform binomial tests for each user
# H0: p = 0.5 (random chance)
# H1: p ≠ 0.5 (not random chance)

results <- user_win_rate |>
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
    p_value = binom_test$p.value,
    conf_low = binom_test$conf.int[1],
    conf_high = binom_test$conf.int[2]
  ) |>
  ungroup() |>
  select(-binom_test) |>
  arrange(p_value)

# Show all results
results

# Filter for statistically significant results (p < 0.05)
significant_users <- results |>
  filter(p_value < 0.05) |>
  mutate(
    direction = case_when(
      win_rate > 0.5 ~ "Above chance",
      win_rate < 0.5 ~ "Below chance",
      TRUE ~ "At chance"
    )
  )

cat(
  "Users with win rates statistically different from random chance (p < 0.05):\n\n"
)
significant_users |>
  select(user_id, total, wins, win_rate, p_value, direction) |>
  print()

cat("\nSummary:\n")
cat(paste("Total users tested:", nrow(results), "\n"))
cat(paste(
  "Users significantly different from chance:",
  nrow(significant_users),
  "\n"
))
cat(paste(
  "Users significantly above chance (win_rate > 0.5):",
  sum(significant_users$direction == "Above chance"),
  "\n"
))
cat(paste(
  "Users significantly below chance (win_rate < 0.5):",
  sum(significant_users$direction == "Below chance"),
  "\n"
))

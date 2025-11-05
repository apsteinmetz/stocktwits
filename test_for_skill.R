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
    )

  return(significant_posters)
}

significant_posters <- get_significanct_posters(bear_win_rate)

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

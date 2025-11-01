library(dplyr)
library(lubridate)


# Filter track_record to only significant users
sig_user_ids <- significant_posters$user_id

track_record_sig <- track_record |>
  filter(user_id %in% sig_user_ids)

cat("Posts from significant users:\n")
cat("Total posts:", nrow(track_record_sig), "\n")
cat(
  "Date range:",
  as.character(min(track_record_sig$date)),
  "to",
  as.character(max(track_record_sig$date)),
  "\n"
)
cat("Number of unique users:", n_distinct(track_record_sig$user_id), "\n")

# For each significant user, create their own train/test split
user_windowed_data <- list()

for (user in sig_user_ids) {
  # Get posts for this user (collect to regular data frame)
  user_posts <- track_record_sig |>
    filter(user_id == user) |>
    arrange(date) |>
    collect()

  # Skip if too few posts
  if (nrow(user_posts) < 2) {
    next
  }

  # Get date range for this user
  user_min_date <- min(user_posts$date)
  user_max_date <- max(user_posts$date)

  # Calculate train/test split date
  # Train: first 4 months of activity
  train_end <- user_min_date + months(4) - days(1)
  test_start <- user_min_date + months(4)
  test_end <- user_min_date + months(6) - days(1)

  # Only keep users who have enough time span
  if (is.na(test_end) || test_end > user_max_date) {
    # Not enough data for full 6-month window, skip
    next
  }

  # Split into train and test
  train_posts <- user_posts |>
    filter(date >= user_min_date & date <= train_end) |>
    mutate(dataset = "train")

  test_posts <- user_posts |>
    filter(date >= test_start & date <= test_end) |>
    mutate(dataset = "test")

  # Only include if both train and test have posts
  if (nrow(train_posts) > 0 && nrow(test_posts) > 0) {
    user_windowed_data[[length(user_windowed_data) + 1]] <-
      bind_rows(train_posts, test_posts)
  }
}

# Combine all users
windowed_data_by_user <- bind_rows(user_windowed_data)

cat("Created individual train/test splits for each poster\n")
cat("Total posts:", nrow(windowed_data_by_user), "\n")
cat(
  "Number of users with splits:",
  n_distinct(windowed_data_by_user$user_id),
  "\n\n"
)

# Summary by user and dataset
summary_by_user <- windowed_data_by_user |>
  group_by(user_id, dataset) |>
  summarise(
    n_posts = n(),
    date_min = min(date),
    date_max = max(date),
    n_tickers = n_distinct(ticker),
    win_rate_vs_spy = mean(win_vs_SPY),
    .groups = "drop"
  ) |>
  arrange(user_id, desc(dataset))

cat("Summary of train/test splits by user:\n\n")
print(summary_by_user, n = 60)

cat("\n\nOverall summary:\n")
summary_by_user |>
  group_by(dataset) |>
  summarise(
    total_posts = sum(n_posts),
    n_users = n_distinct(user_id),
    avg_posts_per_user = mean(n_posts),
    avg_win_rate = mean(win_rate_vs_spy)
  )

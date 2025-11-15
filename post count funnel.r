# devtools::install_github("ropensci/plotly")
library(plotly)

labels <- c(
  "All posts",
  "Posts with ticker and sentiments<br>(Downloaded)",
  "No repeat sentiments in a single day",
  "Posts mentioning 500 top tickers",
  "Frequent posters only",
  "Valid price history available"
)

values <- c(500, 109, 51, 34, 23, 16)

# compute vertical midpoints in paper coords to align annotations with funnel slices
props <- values / sum(values)
bottoms <- c(0, head(cumsum(props), -1))
# mids <- (bottoms + props / 2) * .9  # adjust slightly to center vertically in slice
mids <- c(0.3, 0.63, 0.77, 0.85, 0.91, 0.96)
# build annotations to place labels on the left; x anchored to the right so text sits to the left of x
annotations <- Map(
  function(lbl, mid) {
    list(
      xref = "paper",
      yref = "paper",
      x = 0.08, # small paper x value near left; adjust as needed
      y = 1 - mid, # convert midpoint (measured top->down) to paper y (0 bottom -> 1 top)
      xanchor = "right",
      yanchor = "middle",
      text = paste0("<b>", lbl),
      showarrow = FALSE,
      font = list(size = 14), # label text size
      layer = "above"
    )
  },
  labels,
  mids
) |>
  unname()

fig <- plot_ly(
  title = list(text = "Post Count Funnel", font = list(size = 22)),
  type = "funnelarea",
  # show only numeric values inside each slice
  textinfo = "value",
  texttemplate = "%{value} MM",
  textfont = list(size = 20), # value text size inside slices
  text = labels, # still keep labels available if needed (annotations will show them)
  values = values
) %>%
  layout(
    annotations = annotations,
    margin = list(l = 260) # leave room on the left for the labels
  )
fig

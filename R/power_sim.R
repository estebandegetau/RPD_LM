
compute_mde <- function(data) {
  YR <- data |>
    select(duration_36, running) |>
    as.matrix()
    
  T <- data |> pull(take_up_12)

  mde <- rdmde(
    data = YR,
    fuzzy = T
  )
  return(mde)
}
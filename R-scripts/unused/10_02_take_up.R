#### RPD #######################################################################
#' 
#' @name 10_02_take_up.R
#' 
#' @description Produce tables and figures to show eligibility effect on take-up
#' 
#' @author Esteban Degetau
#' 
#' @created 2025-03-29
#' 
#### Take up #####################################################################

# Setup ------------------------------------------------------------------------
pacman::p_load(here)

source(here("R/RDD/09_rd_prep.R"))

# Perform RD -------------------------------------------------------------------

take_up <- to_compute |>
  filter(group == "Take up") |>
  mutate(
    rd = map(data, my_rd, .progress = T)
  )

# Plots ------------------------------------------------------------------------

# RD plots
take_up_plots <- take_up |>
  mutate(
    months = str_extract(name, "\\d+") |> as.numeric(),
  ) |>
  filter(months %in% c(2, 3, 6, 9, 12, 24)) |>
  arrange(months) |>
  mutate(
    plot = map(rd, "plot"),
    label = case_when(
        !is.na(months) ~ str_c("Take up - ", months, " months"),
        T ~ label
    ),
    plot = map2(plot, label, label_plots),
    plot = map(plot, ~ .x + coord_cartesian(ylim = c(0, 0.15)))
  ) |>
  select(plot, months, label)

# Treatment path
res <- take_up |>
  filter(str_detect(name, "take_up")) |>
  filter(!str_detect(name, "censored|days")) |>
  mutate(
    months = str_extract(name, "\\d+") |> as.numeric(),
    rd = map(rd, "output"),
    point = map(rd, "coef"),
    ci = map(rd, "ci"),
    lower = map_dbl(ci, 3),
    upper = map_dbl(ci, 6),
    point = map_dbl(point, 3)
    ) 

take_up_path <- res |>
  ggplot(aes(months, point)) +
  geom_pointrange(aes(ymin = lower, ymax = upper)) +
  geom_hline(yintercept = 0) +
  scale_x_continuous(breaks = seq(from = 3, to = 48, by = 3)) +
  labs(x = "Months since displacement",
       y = "Take Up Rate")

# Tables ---------------------------------------------------------------------


take_up_table <- take_up |>
  filter(str_detect(name, "take_up")) |>
  filter(!str_detect(name, "censored|days")) |>
  mutate(
    months = str_extract(name, "\\d+") |> as.numeric(),
    label = case_when(
      str_detect(name, "take_up") ~ str_c("Take up - ", months, " months")
    )
  ) |>
  filter(months %in% c(2, 3, 6, 9, 12, 24)) |>
  arrange(months) |>
  pluck_stats() |>
  select(!c(rd, months, name, data, col_type, levels, value_labels, group, pos, missing)) |>
  pivot_longer(cols = !label, names_to = "stat") |>
  pivot_wider(names_from = label) 

# Save results -----------------------------------------------------------------

save(
    take_up_plots,
    take_up_path,
    take_up_table,
    file = here("results/RD/take_up.RData")
)

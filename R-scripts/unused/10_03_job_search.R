#### RPD #######################################################################
#' 
#' @name 10_03_job_search.R
#' 
#' @description Produce tables and figures to show eligibility effect on 
#'              job search outcomes.
#' 
#' @author Esteban Degetau
#' 
#' @created 2025-03-29
#' 
#### Take up #####################################################################

# Setup ------------------------------------------------------------------------
pacman::p_load(here)

source(here("R/RDD/09_rd_prep.R"))

# Perform RDD ------------------------------------------------------------------

# Survival 
survival <- to_compute |>
  filter(group == "Survival") |>
  mutate(
    rd = map(data, my_rd, .progress = T)
  )


# Next job 
next_job <- to_compute |>
  filter(group == "Next job") |>
  mutate(
    rd = map(data, my_rd, .progress = T)
  )


# Perform Fuzzy RD -------------------------------------------------------------

survival_iv <- to_compute_fuzzy |>
  filter(group == "Survival") |>
  mutate(
    rd = map(data, my_fuzzy_rd, .progress = T)
  )


a <- to_compute_fuzzy |>
  filter(group == "Survival") |>
  filter(name == "survival_24") |>
  mutate(
    rd = map(data, my_fuzzy_rd, .progress = T)
  )


a |> my_modelsummary()
glimpse(a)
a$rd[[1]] |> pluck("output") |> glimpse()
a$rd[[1]] |> pluck("output") |> summary()



a |> my_modelsummary()

glance.rdrobust(a$rd[[1]]$output)

# Plots ------------------------------------------------------------------------

survival_plots <- survival |>
    filter(str_detect(name, "survival")) |>
    mutate(
        months = str_extract(name, "\\d+") |> as.numeric(),
        label = case_when(
            str_detect(name, "survival") ~ str_c("Survival - ", months, " months"),
            str_detect(name, "duration") ~ str_c("Duration - ", months, " months")
        )
    ) |>
    filter(months %in% c(2, 3, 6, 9, 12, 24)) |>
    arrange(months) |>
    mutate(
        plot = map(rd, "plot"),
        plot = map2(plot, label, label_plots)
    ) |>
    select(plot, months, label)

duration_plots <- survival |>
    filter(str_detect(name, "duration")) |>
    mutate(
        months = str_extract(name, "\\d+") |> as.numeric(),
        label = case_when(
            str_detect(name, "survival") ~ str_c("Survival - ", months, " months"),
            str_detect(name, "duration") ~ str_c("Duration - ", months, " months")
        )
    ) |>
    filter(months %in% c(2, 3, 6, 9, 12, 24)) |>
    arrange(months) |>
    mutate(
        plot = map(rd, "plot"),
        plot = map2(plot, label, label_plots)
    ) |>
    select(plot, months, label)

res <- survival |>
  filter(str_detect(name, "survival")) |>
  mutate(
    months = str_extract(name, "\\d+") |> as.numeric(),
    rd = map(rd, "output"),
    point = map(rd, "coef"),
    ci = map(rd, "ci"),
    lower = map_dbl(ci, 3),
    upper = map_dbl(ci, 6),
    point = map_dbl(point, 3)
    ) 

survival_path <- res |>
  ggplot(aes(months, point)) +
  geom_pointrange(aes(ymin = lower, ymax = upper)) +
  geom_hline(yintercept = 0) +
  scale_x_continuous(breaks = seq(from = 3, to = 48, by = 3)) +
  labs(x = "Months since displacement",
       y = "Survival Rate")


res <- survival |>
  filter(str_detect(name, "duration")) |>
  mutate(
    months = str_extract(name, "\\d+") |> as.numeric(),
    rd = map(rd, "output"),
    point = map(rd, "coef"),
    ci = map(rd, "ci"),
    lower = map_dbl(ci, 3),
    upper = map_dbl(ci, 6),
    point = map_dbl(point, 3)
   # ci = map(ci, as_tibble)
    ) 

duration_path <- res |>
  ggplot(aes(months, point)) +
  geom_pointrange(aes(ymin = lower, ymax = upper)) +
  geom_hline(yintercept = 0) +
  scale_x_continuous(breaks = seq(from = 3, to = 48, by = 3)) +
  labs(
    x = "Months since displacement",
    y = "Censored duration (weeks)"
  )

# Tables ---------------------------------------------------------------------

panel_a <- survival |>
  mutate(months = str_extract(name, "\\d+") |> as.numeric()) |>
  filter(months %in% c(3, 36)) |>
  arrange(name) |>
  mutate(label = case_when(
    str_detect(name, "survival") ~ str_c("Survival - ", months, " months"),
    str_detect(name, "duration") ~ str_c("Duration - ", months, " months")
  )) |>
  pluck_stats() |>
  select(!c(rd, months, name, data, col_type, levels, value_labels, group, pos, missing)) |>
  mutate(col = seq(1:4), panel = "Duration out of a Formal Job") |>
  pivot_longer(!c(col, panel), names_to = "stat") |>
  pivot_wider(id_cols = c(stat, panel), names_from = col) 
  
panel_b <- next_job |>
  arrange(name) |>
  mutate(
    label = case_when(
      name == "next_job_av_earnings" ~ "Monthly earnings",
      name == "next_job_cum_earnings" ~ "Total earnings",
      name == "next_job_duration" ~ "Job duration"
    )
  ) |>
  pluck_stats() |>
  select(!c(rd, name, data, col_type, levels, value_labels, group, pos, missing)) |>
  mutate(col = 2:4,
         panel = "Next Job Quality") |>
  pivot_longer(
    !c(col, panel),
    names_to = "stat"
  ) |>
  pivot_wider(
    id_cols = c(stat, panel),
    names_from = col
  ) 

job_search_table <- panel_a |>
  bind_rows(panel_b) |>
  mutate(
    stat = case_when(stat == "label" ~ "", T ~ stat)
  ) |>
  rename(" " = stat) |>
  group_by(panel)

# Save results -----------------------------------------------------------------

save(
    survival_plots,
    duration_plots,
    survival_path,
    duration_path,
    job_search_table,
    file = here("results/RD/job_search.RData")
)

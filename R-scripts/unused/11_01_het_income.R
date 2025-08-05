#### RPD #######################################################################
#' 
#' @name 08_heterogeneity.R
#' 
#' @description Estimate heterogeneous treatment effects.
#' 
#' @author Esteban Degetau
#' 
#' @created 2025-03-17
#' 
#### Heterogeneity #############################################################

# Setup --------------------------------------------------------------------

pacman::p_load(here)

source(here("R/RDD/09_rd_prep.R"))

# Split ----------------------------------------------------------------------

# Income
income <- rpd |>
    mutate(
        split = ntile(prev_job_av_earnings, 2),
    ) |>
    select(CVE_NSS, running, split, all_of(var_groups$variable)) |>
    mutate(across(where(is.Date), decimal_date)) |>
    pivot_longer(
        !matches("CVE|_id|running|split")
    ) |>
    nest(data = c(CVE_NSS, running, value)) |>
    left_join(var_groups, by = c("name" = "variable"))

# Perform RD -----------------------------------------------------------------

income_results <- income |>
  filter(str_detect(name, "survival|duration_")) |>
  mutate(months = str_extract(name, "\\d+") |> as.numeric()) |>
  filter(months %in% c(3, 12)) |>
  mutate(
    rd = map(data, my_rd, .progress = T)
  ) |>
  select(-data)

# Plots ------------------------------------------------------------------



res <- income_results |>
  filter(str_detect(name, "survival")) |>
  mutate(
    split = factor(split, levels = c("1", "2"), labels = c("Below median", "Above median")),
    months = str_extract(name, "\\d+") |> as.numeric(),
    rd = map(rd, "output"),
    point = map(rd, "coef"),
    ci = map(rd, "ci"),
    lower = map_dbl(ci, 3),
    upper = map_dbl(ci, 6),
    point = map_dbl(point, 3)
    ) 



survival_plot <- res |>
  ggplot(aes(months, point)) +
  geom_pointrange(
    aes(ymin = lower, ymax = upper, color = split),
    position = position_dodge(width = 0.5)
    ) +
  geom_hline(yintercept = 0) +
  scale_x_continuous(breaks = seq(from = 3, to = 48, by = 3)) +
  labs(x = "Months since displacement",
       y = "Survival Rate",
       color = ""
       )


res <- income_results |>
  filter(str_detect(name, "duration")) |>
  mutate(
    split = factor(split, levels = c("1", "2"), labels = c("Below median", "Above median")),
    months = str_extract(name, "\\d+") |> as.numeric(),
    rd = map(rd, "output"),
    point = map(rd, "coef"),
    ci = map(rd, "ci"),
    lower = map_dbl(ci, 3),
    upper = map_dbl(ci, 6),
    point = map_dbl(point, 3)
   # ci = map(ci, as_tibble)
    ) 

duration_plot <- res |>
  ggplot(aes(months, point)) +
  geom_pointrange(
    aes(ymin = lower, ymax = upper, color = split),
    position = position_dodge(width = 0.5)
    ) +
  geom_hline(yintercept = 0) +
  scale_x_continuous(breaks = seq(from = 3, to = 48, by = 3)) +
  labs(
    x = "Months since displacement",
    y = "Censored duration (weeks)",
    color = ""
  )

save(income_results, file = here("results/RD/het/survival_income.RData"))
rm(income, income_results)

# Tables ---------------------------------------------------------------------

panel_a <- income_results |>

  arrange(split, name) |>
  mutate(
    label = case_when(
      str_detect(name, "survival") ~ str_c("Survival - ", months, " months"),
      str_detect(name, "duration") ~ str_c("Duration - ", months, " months")
    )

    ) |>
    arrange(label) |>
  pluck_stats() |>
  select(!c(rd, name, months, col_type, levels, value_labels, group, pos, missing)) |>

  pivot_longer(
    !c(split, label),
    names_to = "stat"
  ) |>
  pivot_wider(
    id_cols = c(split, stat),
    names_from = label
  ) 
  

panel_a |>
  mutate(
    split = factor(split, levels = c(1, 2), labels = c("Below median", "Above median"))
  ) |>
  group_by(split) |>
  gt() |>
  sub_missing(columns = everything(),
              rows = everything(),
              missing_text = " - ")


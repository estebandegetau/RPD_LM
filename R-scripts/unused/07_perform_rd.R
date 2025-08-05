#### RPD #######################################################################
#' 
#' @name 08_perform_rd.R
#' 
#' @description Perform regression discontinuity analysis.
#' 
#' @author Esteban Degetau
#' 
#' @created 2025-02-05
#' 
#### Perform RD ################################################################

rm(list = ls())
gc()

#---- Libraries ----------------------------------------------------------------

pacman::p_load(tidyverse,
               here,
               gt,
               gtsummary,
               fixest,
               arrow,
               ggpubr,
               rddensity,
               rdrobust,
               labelled)

theme_set(theme_minimal())

#---- Functions ----------------------------------------------------------------

my_rd <- function(data, plot = T, ...) {
  y <- {{ data }} |> pull("value")
  x <- {{ data }} |> pull("running")

  if (is.Date(y)) y <- decimal_date(y)

  output <- rdrobust(
    y = y,
    x = x,
    c = 0,
    ...
  )

  if (plot) {
    plot <- rdplot(y = y, x = x, c = 0)

    plot <- plot |> pluck("rdplot")
    return(list(output = output, plot = plot))
  } else {
    return(list(output = output))
  }
}

#---- Load ---------------------------------------------------------------------

here("data/anon/rd_prep.RData") |>
  load()

running <- running |> select(!days_contributed) |> drop_na()

panel <- here("data/anon/rd") |>
  open_dataset(format = "parquet") |>
  filter(year(period) <= 2024) |>
  mutate(employed = as.numeric(density == 1),
         wage = case_when(
           employed == 1 ~ income / 30
         )) |>
  compute()

outcomes_pre_unemp <- panel |>
  filter(period <= floor_date(unemployment_date, "month")) |>
  select(worker_id, employed, density, income, wage) |>
  summarise(.by = worker_id,
            total_income = sum(income),
            monthly_income = mean(income),
            av_daily_wage = mean(wage, na.rm = T),
            months_worked = sum(employed),
            density = mean(density)
  ) |>
  collect() |>
  inner_join(running)

covariates <-  panel |>
  filter(period == floor_date(unemployment_date, "month")) |>
  select(
    worker_id,
    unemployment_date,
    female,
    age,
    began_working,
    matches("income_|density_")
  ) |>
  collect() |>
  inner_join(running)

#---- Summary ------------------------------------------------------------------

panel |> head() |> collect() |> generate_dictionary()

summary <- running |>
  distinct(worker_id) |>
  left_join(outcomes_pre_unemp) |>
  left_join(covariates) |>
  left_join(survival) |>
  left_join(next_job) |>
  left_join(
    medium |> 
      pivot_longer(
        cols = !c(worker_id, unemp_year)
      ) |>
      pivot_wider(
        id_cols = worker_id,
        names_from = c(name, unemp_year)
      )
    ) |>
  left_join(medium_totals, by = "worker_id", suffix = c("_pre", "_post")) |>
  mutate(across(where(is.Date), decimal_date)) |>
  pivot_longer(
    cols = !c(worker_id)
  ) |>
  summarise(
    .by = name,
    n = sum(!is.na(value)),
    Mean = mean(value, na.rm = T),
    Median = median(value, na.rm = T),
    Std.Dev. = sd(value, na.rm = T),
    Min = min(value, na.rm = T),
    Max = max(value, na.rm = T)
  )


summary |> save(file = here("results/summary.RData"))

glimpse(summary)

summary |> summary()

medium_totals |> glimpse()

medium_totals$worker_id |> summary()
running$worker_id |> summary()
#---- Density ------------------------------------------------------------------

rdd_object <- rddensity(
  X = running$running,
  c = 0
)

density_plot <- rdplotdensity(
  rdd_object,
  X = running$running,
  histFillCol = "lightgray",
  histFillShade = 0.8
  )

save(density_plot, file = here("results/density_plot.RData"))

#---- Covariates ---------------------------------------------------------------

a <- outcomes_pre_unemp |>
  pivot_longer(
    !c(worker_id, running)
  ) |>
  nest(data = c(worker_id, running, value)) |>
  mutate(
    rd = map(data, my_rd)
  ) |>
  select(name, rd)

b <- covariates |>
  mutate(
    across(where(is.Date), decimal_date)
  ) |>
  pivot_longer(
    !c(worker_id, running)
  ) |>
  nest(data = c(worker_id, running, value)) |>
  mutate(
    rd = map(data, my_rd)
  ) |>
  select(name, rd)

#---- Results ------------------------------------------------------------------

c <- survival |>
  inner_join(running) |>
  pivot_longer(matches("duration|survival")) |>
  nest(data = c(worker_id, running, value)) |>
  mutate(
    rd = map(data, my_rd)
  ) |>
  select(name, rd)


d <- next_job |>
  inner_join(running) |>
  pivot_longer(matches("next_job")) |>
  nest(data = c(worker_id, running, value)) |>
  mutate(
    rd = map(data, my_rd)
  ) |>
  select(name, rd)

e <- medium |>
  inner_join(running) |>
  pivot_longer(!c(worker_id, unemp_year, running)) |>
  nest(data = c(worker_id, running, value)) |>
  mutate(
    rd = map(data, my_rd)
  ) |>
  select(name, unemp_year, rd)

f <- medium_totals |>
  inner_join(running) |>
  pivot_longer(!c(worker_id, running)) |>
  nest(data = c(worker_id, running, value)) |>
  mutate(
    rd = map(data, my_rd)
  ) |>
  select(name, rd) |>
  mutate(unemp_year = "Total")

save(a, b, c, d, e, f, file = here("results/rdd.RData"))

#---- Covariates ------------------------------------------------------------

a_1 <- survival |>
  inner_join(running) |>
  left_join(covariates) |>
  left_join(outcomes_pre_unemp) |>
  pivot_longer(
    cols = matches("survival|duration"),
  ) |>
  select(!c(av_daily_wage, matches("_full|_year"))) |>
  mutate(across(where(is.Date), decimal_date)) |>
  nest(
    data = c(worker_id, running, value),
    covs = !c(name, worker_id, running, value)) |>
  mutate(
    covs = map(covs, as.matrix),
    rd = map2(data, covs, ~ my_rd(.x, covs = .y, plot = F), .progress = T)
  ) |>
  select(name, rd)


save(a_1, file = here("results/rdd_covs.RData"))

#---- Heterogeneity ------------------------------------------------------------

# Income quintiles
income_quintiles <- survival |>
  inner_join(running) |>
  left_join(outcomes_pre_unemp |> select(worker_id, av_daily_wage)) |>
  drop_na() |>
  mutate(
    income_quintile = ntile(av_daily_wage, 3)
  ) |>
  select(!av_daily_wage) |>
  pivot_longer(
    matches("survival|duration"),
  ) |>
  nest(data = c(worker_id, running, value)) |>
  mutate(
    rd = map(data, my_rd, plot = F, .progress = T)
  ) |>
  select(income_quintile, name, rd)


save(income_quintiles, file = here("results/rdd_income_quintiles.RData"))


# Gender
gender <- survival |>
  inner_join(running) |>
  left_join(covariates |> select(worker_id, female)) |>
  pivot_longer(
    matches("survival|duration"),
  ) |>
  nest(data = c(worker_id, running, value)) |>
  mutate(
    rd = map(data, my_rd, plot = F, .progress = T)
  ) |>
  select(female, name, rd)


save(gender, file = here("results/rdd_gender.RData"))

# Age
age <- survival |>
  inner_join(running) |>
  left_join(covariates |> select(worker_id, age)) |>
  mutate(
    above_age_median = as.numeric(age > median(age, na.rm = T))
  ) |>
  select(-age) |>
  pivot_longer(
    matches("survival|duration"),
  ) |>
  nest(data = c(worker_id, running, value)) |>
  mutate(
    rd = map(data, my_rd, plot = F, .progress = T)
  ) |>
  select(above_age_median, name, rd)

save(age, file = here("results/rdd_age.RData"))

#### SD ########################################################################
#' 
#' @name 07_prep_data.R
#' 
#' @description
#' Prepare data for analysis.
#' 
#' @author Esteban Degetau
#' 
#' @created 2025-02-01
#' 
#### DiD Data ##################################################################

rm(list = ls())
gc()

test <- 1
test_n <- 1000

# Test whether .sourced_in_run is defined
if (exists(".sourced_in_run")) {
  test <- 0
}

#---- Libraries ----------------------------------------------------------------

pacman::p_load(tidyverse, here, arrow, duckdb, labelled)

#---- Load Data ----------------------------------------------------------------

con <- DBI::dbConnect(duckdb::duckdb(), dbdir = "duckdb")

panel <- here("data/anon/rd") |>
  open_dataset(format = "parquet") |>
  to_duckdb(con) |>
  filter(year(period) <= 2024) |>
  compute()

load(here("data/anon/rd_workers.RData"))

workers <- workers |> to_duckdb(con)

ci <- here("data/working/rd_ci_final.feather") |>
  open_dataset(format = "feather") |>
  to_duckdb(con)

here("data/working/rd_keys.RData") |>
  load()

if (test) {
  set.seed(123)
  sample <- workers |>
    filter(got_unemployed == 1) |>
    distinct(worker_id) |>
    collect() |>
    sample_n(test_n) 

  workers <- workers |>
    inner_join(sample, copy = T) |>
    compute()

  panel <- panel |>
    inner_join(sample, copy = T) |>
    compute()

}


interest_workers <- workers |>
  inner_join(panel |> distinct(worker_id), copy = T) |>
  collect()

#---- Running variable ----------------------------------------------------------


running <- interest_workers |>
  distinct(worker_id, unemployment_date) |>
  left_join(keys) |>
  left_join(ci, copy = T) |>
  filter(tidy_end <= unemployment_date) |>
  mutate(
    days_contributed = citools::working_days(interval(tidy_start, tidy_end))
  ) |>
  summarise(
    .by = worker_id,
    days_contributed = sum(days_contributed)
  ) |>
  mutate(running = days_contributed - (365 * 2))

#---- Survival ------------------------------------------------------------------


survival <- interest_workers |>
  mutate(
    months_after_unemp = list(seq(3, 12 * 4, 3))
  ) |>
  unnest(cols = months_after_unemp) |>
  mutate(
    survival = as.numeric(unemployment_days / 30 >= months_after_unemp),
    duration = case_when(
      survival == 1 ~ months_after_unemp * 30 / 7,
      T ~ unemployment_days / 7
    )
  ) |>
  select(worker_id, months_after_unemp, survival, duration) |>
  compute() |>
  pivot_longer(
    cols = c(survival, duration)
  ) |>
  pivot_wider(
    names_from = c(name, months_after_unemp),
    values_from = value,
    names_sep = "_"
  ) 
  

description <- tibble(
  name = names(survival)
) |>
  mutate(
    months = str_extract(name, "\\d+"),
    label = 
      case_when(
        str_detect(name, "survival") ~ str_c(
          "Survival out of formal employment after ",
          months,
          " months"),
        str_detect(name, "duration") ~ str_c(
          "Weeks out of unemployment cansored at ",
          months,
          " months"
        
      )
  )) |>
  select(!months)


var_labels <- setNames(as.list(description$label), description$name)

survival <- survival |>
  set_variable_labels(.labels = var_labels, .strict = F)

#---- Next job quality ---------------------------------------------------------


post_unemp <- interest_workers |>
  distinct(worker_id, unemployment_date, unemployment_days) |>
  mutate(next_job_start = unemployment_date + days(unemployment_days)) |>
  left_join(panel |> select(worker_id, period, density, income), copy = T) |>
  filter(period >= unemployment_date) |>
  arrange(worker_id, period) |>
  mutate(
    employed = as.numeric(density == 1),
    wage = case_when(employed == 1 ~ income / 30) 
  ) |>
  compute()

next_job <- post_unemp |>
  filter(period >= floor_date(next_job_start, "month")) |>
  mutate(
    .by = worker_id,
    t = dense_rank(period),
  ) |>
  mutate(
    some_employment = case_when(
      t == 1 ~ 1,
      T ~ as.numeric(density == 1)
    ),
  ) |>
  mutate(
    .by = worker_id,
    cum_employment = cumsum(some_employment),
  ) |>
  filter(t == cum_employment) |>
  summarise(
    .by = worker_id,
    next_job_duration = max(cum_employment) * 30 / 7,
    next_job_cum_earnings = sum(income),
    next_job_av_earnings = mean(income)
  ) |>
  compute() |>
  set_variable_labels(
    next_job_duration = "Duration of next job (weeks)",
    next_job_cum_earnings = "Cumulative earnings in next job (2024 MXN)",
    next_job_av_earnings = "Monthly earnings in next job (2024 MXN)"
  )



#---- Medium term ---------------------------------------------------------------

medium <- post_unemp |>
  mutate(
    unemp_year = floor(as.numeric(period - unemployment_date) / 365) + 1,
  ) |>
  summarise(
    .by = c(worker_id, unemp_year),
    earnings = sum(income),
    months_worked = sum(density),
    av_earnings = mean(income),
    av_wage = mean(wage, na.rm = T),
  ) |>
  compute() |>
  set_variable_labels(
    unemp_year = "Year after unemployment",

    earnings = "Cumulative earnings (2024 MXN)",
    months_worked = "Months worked",
    av_earnings = "Monthly earnings (2024 MXN)",
    av_wage = "Average daily wage (2024 MXN)"
  )

medium_totals <- post_unemp |>
  summarise(
    .by = c(worker_id),
    earnings = sum(income),
    months_worked = sum(density),
    av_earnings = mean(income),
    av_wage = mean(wage, na.rm = T),
  ) |>
  compute() |>
  set_variable_labels(
    earnings = "Cumulative earnings (2024 MXN)",
    months_worked = "Months worked",
    av_earnings = "Monthly earnings (2024 MXN)",
    av_wage = "Average daily wage (2024 MXN)"
  )

#---- Save Data -----------------------------------------------------------------

if (!test) {
  save(
    survival,
    running,
    next_job,
    medium,
    medium_totals,
    file = here("data/anon/rd_prep.RData")
  )
}


# Close connection
DBI::dbDisconnect(con)

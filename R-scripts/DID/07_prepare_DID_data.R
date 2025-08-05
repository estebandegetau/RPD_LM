#### SD ########################################################################
#' 
#' @name prep_data.R
#' 
#' @description
#' Prepare data for analysis.
#' 
#' @author Esteban Degetau
#' 
#' @created 2024-10-27
#' 
#### DiD Data ##################################################################

rm(list = ls())
gc()

#---- Libraries ----------------------------------------------------------------

pacman::p_load(tidyverse, here, arrow, duckdb)

#---- Load Data ----------------------------------------------------------------

con <- DBI::dbConnect(duckdb::duckdb(), dbdir = "duckdb")

itt_raw <- here("data/anon/itt") |>
  open_dataset(format = "parquet") |>
  to_duckdb(con)
  

load(here("data/anon/workers.RData"))

workers <- workers |> to_duckdb(con)

# workers |> glimpse()

# workers |>
#   filter(got_unemployed) |>
#   summarise(.by = unemp_spells, n = n_distinct(worker_id)) 

#---- Wrangle ------------------------------------------------------------------


work <- itt_raw |>
  filter(!is.na(account_days)) |>
  collect() |>
  mutate(
 #   days_to_take_up = as.numeric(as_date(rpd_date) - as_date(unemployment_date)),
    employment = as.numeric(density == 1),
    wage = case_when(
      employment == 1 ~ income / 7
    ),
    # rpd_it = case_when(
    #   !is.na(rpd_date) & rpd_date <= period ~ 1,
    #   T ~ 0
    # ),

    t = dense_rank(period),
    post_wait = as.numeric(period >= unemployment_date + days(45)),
    unemployed = as.numeric(
      period >= unemployment_date &
        period <= unemployment_date + unemployment_days
    ),
    contributed_2_years = as.numeric(cumulative_days_contributed >= 2 * 365),
    eligible_a = as.numeric(contributed_2_years),
    eligible_b = as.numeric(account_days >= 5 * 365),
    eligible = as.numeric(eligible_a | eligible_b),
    unemp_elig = unemployed * eligible,
    full_elig  = post_wait * eligible * unemployed,

  ) |>
  mutate(
    .by = c(worker_id),
    full_elig_g = case_when(
      sum(full_elig) > 0 ~ min(t[full_elig == 1]) + 1,
      T ~ 0
    ),
    # rpd_no_cum = case_when(rpd_it > lag(rpd_it) ~ 1, T ~ 0),
    # rpd_g = case_when(
    #   sum(rpd_it) > 0 ~ min(t[rpd_it == 1]),
    #   T ~ 0
    # ),
    elig_g = min(t[eligible == 1]),
    unemp_g = min(t[unemployed == 1])
    
  ) |>
  mutate(
    weeks_to_elig_u = elig_g - unemp_g,
    post_full_elig = as.numeric(t >= full_elig_g & full_elig_g > 0),
    full_elig_t = t - full_elig_g,
    unemp_t = t - unemp_g,
    # rpd_t = t - rpd_g,
    elig_t = t - elig_g,
  ) |>
  relocate(
    worker_id, period, t, unemployment_date, unemployment_days, 
    ends_with("_g"), ends_with("_t"), everything()
  ) |>
  to_duckdb(con)



weeks <- work |>
  distinct(period, t) |>
  collect()


elig_dates <- work |>
  distinct(t, period) |>
  rename(elig_g = t, eligibility_date = period) |>
  collect()  

fully_employed_before <- work |> 
  select(worker_id, unemp_t, employment, unemployment_date) |>
  filter(unemp_t < -1) |>
  filter(employment == 1) |>
  mutate(
    consecutive =  unemp_t - lag(unemp_t) == 1
  ) |>
  compute()


pre_periods <- fully_employed_before|>
  filter(!consecutive) |>
  summarise(
    .by = worker_id,
    max_interruption = max(unemp_t)
  ) |>
  mutate(
    periods_worked_before = abs(max_interruption)
  ) |>
  select(worker_id, periods_worked_before) |>
  arrange(worker_id) |>
  compute()


# at_unemployment <- work |>
#   filter(unemp_t == 0) |>
# #   filter(
# #     is.na(rpd_date) | rpd_date > period,
# #     is.na(days_to_take_up) | days_to_take_up < 365,
# #     is.na(days_to_take_up) | days_to_take_up < unemployment_days
# #   ) |>
#   # Filter those whe were employed for at least 1 year before unemployment
#   inner_join(fully_employed_before, copy = T) |>
#   left_join(elig_dates, copy = T) |>
#   collect() |>
#   mutate(
#     days_to_elig = as.numeric(eligibility_date - unemployment_date),
#   ) 


#---- DiD Data -----------------------------------------------------------------

did_data <- work |>
  mutate(across(
    c(employment, density, income, wage),
    ~ ifelse(period >= began_working, .x, NA)
  )) |>
  compute() |>
  left_join(pre_periods, copy = T) |>
  left_join(elig_dates, copy = T) |>
  mutate(
    within_year = as.numeric(weeks_to_elig_u <= 26),
    cohort = case_when(within_year == 0 ~ 0, T ~ unemp_g - 7),
    treat = as.numeric(t >= cohort & cohort > 0 & within_year == 1),
    unemp_t = unemp_t - 7
  ) |>
  compute() 




dates <- did_data |>
  distinct(period, t) |>
  collect()

#---- At unemployment ----------------------------------------------------------

at_unemployment <- did_data |>
  filter(unemp_t == 0) |>
  collect()

#---- Write Data ---------------------------------------------------------------

# Save as parquet, using the partition variable
did_data |>
  to_arrow() |>
  write_dataset(
    here("data/anon/did"),
    format = "parquet",
    partition = "partition"
  )

dates |> save(file = here("data/anon/did_dates.RData"))

at_unemployment |>
  save(file = here("data/anon/at_unemployment.RData"))


# Close connection
DBI::dbDisconnect(con)

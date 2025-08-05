#### SD #######################################################################
#' 
#' @name weekly_outcomes.R
#' 
#' @description Compute weekly outcomes and eligibility criteria for a 
#'              subsample of workers close to the eligibility threshold.
#' 
#' @author Esteban Degetau
#' 
#' @created 2024-09-05
#' 
#### Weekly outcomes ############################################################

rm(list = ls())
gc()

test <- 0
n    <- 10000

#---- Libraries ----------------------------------------------------------------

pacman::p_load(data.table, tidyverse, here, citools, labelled, arrow, duckdb)

#---- Load data ----------------------------------------------------------------

con <- DBI::dbConnect(duckdb::duckdb())

load(here("data/working/unemployment.RData"))

load(here("data/working/pre_covid_features.RData"))

unemployment <- unemployment |> to_duckdb(con)
pre_covid_features <- pre_covid_features |> to_duckdb(con)

first_working_date <- pre_covid_features |>
  pull(began_working) |>
  first()

ci <- here::here("data/working/ci_final.feather") |>
  open_dataset(format = "feather") |>
  to_duckdb(con)


#---- Subsample ----------------------------------------------------------------

unemployed <- unemployment |>
  filter(got_unemployed) |>
  distinct(CVE_NSS, unemployment_date) |>
  left_join(pre_covid_features, by = "CVE_NSS") |>
  collect()

if (test) {
  set.seed(20240411)
  
  unemployed <- unemployed |>
    sample_n(n)
  
}

n_unemployed <- nrow(unemployed)

splits <- (n_unemployed / 1000) |> floor() 

samples <- unemployed |>
    select(CVE_NSS, unemployment_date, began_working) |>
    mutate(
        subsample = runif(n_unemployed) |> ntile(splits) |> as.numeric()
    ) |>
    nest(.by = subsample)


#---- Analysis periods ---------------------------------------------------------

#' Analysis periods are referenced as "periods", since this script was copied
#' from 05_outcomes.R, but every analysis period is in weeks.


last_queried <- file.info(here::here("data/raw/rpd_sample_ci.csv")) |>
  pluck("mtime") |>
  as_date()

period_length <- "week"

last_period <- last_queried |> lubridate::floor_date(unit = period_length)

first_period <- first_working_date |> lubridate::floor_date(unit = period_length)

analysis_periods <- seq(first_period, last_period, by = period_length)

analysis_intervals <- tibble(
  period = analysis_periods
) |>
  mutate(
    q_start = period,
    q_end = lead(period) - days(1),
  ) 


#---- Clean CI -----------------------------------------------------------------

tidy_ci <- ci |>
  filter(CVE_NSS %in% unemployed$CVE_NSS) |>
  mutate(
    FEC_MOV_FIN = ifelse(is.na(FEC_MOV_FIN), last_queried, FEC_MOV_FIN)
  ) |>
  filter(
    tidy_start <= tidy_end
  ) 


#---- Compute ------------------------------------------------------------------

merged <- samples |>
  mutate(
    analysis_intervals = list(analysis_intervals),
    queries = map2(
      data,
      analysis_intervals,
      ~ tibble(.y, list(.x)) |> unnest(cols = c(`list(.x)`))
    ),
    # Perform merge
    merge = map(
      queries,
      # This merge returns the external product CI x analysis_intervals
      ~ full_join(tidy_ci, .x, copy = T, relationship = "many-to-many") |>
        distinct(),
      .progress = T
    ),
    # Compute results
    results = map(
      merge,
      ~ .x |>
        collect() |>
        drop_na(period) |>
        mutate(
          # CI x analysis interval level computations
          query_interval = interval(q_start, q_end),
          tidy_interval = interval(tidy_start, tidy_end),
          within_interval = intersect(tidy_interval, query_interval),
          # Note cumulative variables are computed to end of period.
          career_interval = interval(began_working, q_end),
          before_query_interval = interval(began_working, first_period - 1),
          before_query = as.numeric(int_overlaps(before_query_interval, tidy_interval)),
          during_query = as.numeric(int_overlaps(query_interval, tidy_interval)),

          account_days = citools::working_days(career_interval),
          before_interval = intersect(tidy_interval, before_query_interval),
          days_contributed_0 = citools::working_days(before_interval),
    
          days_contributed = citools::working_days(within_interval),
          density = citools::working_days(within_interval) /
            citools::working_days(query_interval),
          work_interval = interval(FEC_MOV_INI, FEC_MOV_FIN),
          work_intersect = intersect(work_interval, query_interval),
          worked_days = citools::working_days(work_intersect),
          income = SAL_BAS * worked_days
        ) |>
        summarise(
          density = sum(density * during_query, na.rm = T),
          income = sum(income * during_query, na.rm = T),
          days_contributed_0 = sum(before_query * days_contributed_0, na.rm = T),
          days_contributed = sum(days_contributed * during_query, na.rm = T),
          account_days = first(account_days, na_rm = T),

          .by = c(CVE_NSS, unemployment_date, period)
        ),
      .progress = T
    )
  )


#---- Unnest and compute cumulative variables ----------------------------------

outcomes <- merged |>
  select(results) |>
  unnest(results) |>
  arrange(CVE_NSS, unemployment_date, period) |>
  group_by(CVE_NSS) |>
  ungroup() |>
  mutate(
    cumulative_days_contributed = days_contributed_0 + cumsum(days_contributed),
    .by = c(CVE_NSS, unemployment_date)
  ) |>
  mutate(
    cumulative_days_contributed = case_when(
        account_days < 0 ~ 0,
        T ~ cumulative_days_contributed
    )
  ) |>
  select(!c(days_contributed_0, days_contributed)) 


# rm(merged)

#---- Check --------------------------------------------------------------------




# Density check
if (test) {
  density_check <- outcomes |>
    filter(density > 1 | density < 0)

  if (nrow(density_check) > 0) {
    debug_density <- density_check |>
      arrange(desc(density)) |>
      pull(CVE_NSS) |>
      first()

    tidy_ci |>
      filter(CVE_NSS == debug_density) |>
      view()

    stop("Density check failed.")
  }




  # Days contributed check

#   days_contributed_check <- outcomes |>
#     filter(cumulative_days_contributed > account_days)



#   if (nrow(days_contributed_check) > 0) {
#     days_contributed_check |>
#       head(200) |>
#       view()

#     debug <- days_contributed_check |>
#       arrange(desc(cumulative_days_contributed - account_days)) |>
#       pull(CVE_NSS) |>
#       first()

#     tidy_ci |>
#       filter(CVE_NSS == debug) |>
#       view()

#     merged |>
#       select(results) |>
#       unnest(results) |>
#       arrange(CVE_NSS, unemployment_date, period) |>
#       group_by(CVE_NSS) |>
#       ungroup() |>
#       filter(CVE_NSS == debug) |>
#       view()

#     outcomes |>
#         filter(CVE_NSS == debug) |>
#         arrange(CVE_NSS, period) |>
#         view()

#     stop("Days contributed check failed.")
#  }
}

#---- Save ---------------------------------------------------------------------

if(!test) write_feather(outcomes, here("data/working/weekly_outcomes.feather"))

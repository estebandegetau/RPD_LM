#### SD ########################################################################
#' 
#' @name: 03_pre_covid_features.R
#' 
#' @description
#' Compute worker level features to show selection biases into unemployment.
#' 
#' @auhtor Esteban Degetau
#' 
#' @created 2024-05-03
#' 
#### Unemployment selection ####################################################

rm(list = ls())
gc()

#---- Setup --------------------------------------------------------------------

test <- 0
n    <- 1000

#---- Libraries ----------------------------------------------------------------

pacman::p_load(tidyverse, here, duckdb, arrow)


#---- Load data ----------------------------------------------------------------



con <- DBI::dbConnect(duckdb::duckdb(),
                      dbdir = "temp1.duckdb")

load(here("data/working/unemployment.RData"))

rm(unemployment)

interest_period_start <- int_start(interest_period)
interest_period_end   <- int_end(interest_period)


ci <- here::here("data/working/ci_final.feather") |>
  open_dataset(format = "feather") |>
  to_duckdb(con)


workers <- ci |> distinct(CVE_NSS) |> collect()

end_dates <- ci |> pull(tidy_end) |> summary()

#---- Subsample ----------------------------------------------------------------

if (test) {
  set.seed(20240411)
  
  workers <- workers |>
    collect() |>
    sample_n(n)
  
  ci <- ci |>
    filter(CVE_NSS %in% workers$CVE_NSS)
  
  compute(ci)

}



#---- Clean CI -----------------------------------------------------------------


ci_clean <- ci  |>
  filter(FEC_MOV_INI < interest_period_start) |>
  filter(FEC_MOV_FIN >= FEC_MOV_INI) |>
  mutate(
    FEC_MOV_FIN = ifelse(
      is.na(FEC_MOV_FIN),
      interest_period_start - days(1),
      FEC_MOV_FIN
    ),
    FEC_MOV_FIN = ifelse(
      FEC_MOV_FIN >= interest_period_start,
      interest_period_start - days(1),
      FEC_MOV_FIN
    ) |>
      as_date(),
    FEC_MOV_INI = ifelse(
      FEC_MOV_INI >= interest_period_start,
      interest_period_start - days(1),
      FEC_MOV_INI
    ) |>
      as_date()
  )

compute(ci_clean)

#---- Subsample ----------------------------------------------------------------

n_workers <- workers |> nrow()

splits <- (n_workers / 1000) |> floor()

samples <- workers |>
  collect() |>
  mutate(
    subsample = runif(n_workers) |> ntile(splits) |> as.numeric()
  ) 

#---- Query intervals ----------------------------------------------------------



intervals_query <- ci_clean |>
  summarise(
    first_job_start = min(FEC_MOV_INI, na.rm = T),
    last_job_end    = max(FEC_MOV_FIN, na.rm = T),
    .by = CVE_NSS
  ) 
  


intervals_query_ready <- intervals_query |>
  collect() |>
  mutate(
    
    first_year__start = first_job_start,
    first_year__end = first_job_start + days(365),
    first_year__end = case_when(
      first_year__end >= interest_period_start ~ interest_period_start - days(1),
      T ~ first_year__end
    ),
    
    last_year__start   = interest_period_start - days(365 + 1), 
    last_year__end = interest_period_start - days(1),
    
    full_career__start = first_job_start, 
    full_career__end = last_year__end
  ) |>
  select(CVE_NSS, matches("__")) |>
  pivot_longer(
    matches("__"),
    names_to = c("period", "start"),
    names_sep = "__"
  ) |>
  pivot_wider(
    id_cols = c(CVE_NSS, period),
    names_from = start
  ) |>
  mutate(
    across(c(start, end), as_date)
  )


# intervals_query_ready |>
#   group_by(period) |>
#   summarise(max(end))

# 
# intervals_query_ready <- intervals_query_ready |>
#   to_duckdb(con)




#---- Query intervals ----------------------------------------------------------



merged <- samples |>
  left_join(intervals_query_ready) |>
  nest(.by = subsample, intervals = !subsample) |>
  mutate(
    merged    = map(
      intervals,
      ~ full_join(ci_clean, .x, relationship = "many-to-many", copy = T) |>
        filter(tidy_start <= end, tidy_end >= start) |>
        distinct() 
    ) ,
    stats = map(
      merged,
      ~ .x |>
        collect()  |>
        drop_na(period) |>
        mutate(
          query_interval = interval(start, end),
          tidy_interval = interval(tidy_start, tidy_end),
          work_interval = interval(FEC_MOV_INI, FEC_MOV_FIN)
        ) |>
        filter(int_overlaps(query_interval, tidy_interval)) |>
        mutate(
          density_intersect = intersect(query_interval, tidy_interval),
          work_intersect = intersect(work_interval, query_interval),
          density = citools::working_days(density_intersect) / citools::working_days(query_interval),
          worked_days = citools::working_days(work_intersect),
          income = SAL_BAS * worked_days
        ) |>
        summarise(
          density = sum(density, na.rm = T),
          income = sum(income, na.rm = T),
          .by = c(CVE_NSS, period)
        ),
      .progress = T
    )
  )
  
#---- Compute stats ------------------------------------------------------------


stats <- merged |> select(stats) |> unnest(cols = c(stats)) |>
  complete(CVE_NSS, period) |>
  mutate(across(c(density, income), ~ ifelse(is.na(.x), 0, .x)))




#---- Validate -----------------------------------------------------------------

stats |>
  ggplot(aes(density)) +
  geom_histogram() +
  facet_wrap(~period)


debug <- stats |>
  filter(density > 1) |>
  arrange(desc(density)) |>
  pull(CVE_NSS)


ci_clean |>
  filter(CVE_NSS %in% debug) |>
  view()

stats |>
  ggplot(aes(income)) +
  geom_histogram() +
  facet_wrap(~period, scales = "free_x") +
  scale_x_log10(labels = scales::comma)

density_check <- stats |>
  filter(density > 1 | density < 0) 

  
stopifnot(nrow(density_check) == 0)

#---- First job stat -----------------------------------------------------------

fjs <- intervals_query_ready |>
  filter(period == "first_year") |>
  distinct(CVE_NSS, start) |>
  rename(began_working = start) |>
  collect()



#---- Merge to workers ---------------------------------------------------------

results <- stats |>
  left_join(fjs, copy = T)

#---- Save ---------------------------------------------------------------------

pre_covid_features <- results |>
  pivot_wider(
    names_from = period,
    values_from = c(density, income),
    names_sep = "_"
  ) |>
  collect()

#---- End ----------------------------------------------------------------------

if (!test) {
  pre_covid_features |>
    save(file = here("data/working/pre_covid_features.RData"))
  dbDisconnect(con)
}



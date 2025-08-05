#### SD ########################################################################
#' 
#' @name 01_identify_unemployment.R
#' 
#' @description Identify unemployment in the CI data. Also rewrites SAL_BAS as
#' a deflated wage.
#' 
#' @details 
#' Unemployment is identified when workers_unemployment are at least 45 days without a job.
#' 
#' @describeIn 
#' here("data/01_raw/ci.csv") is a random sample of workers_unemployment who had
#' some form of job in the year 2020.
#' 
#' @author Esteban Degetau
#' 
#' @created 2024-04-30
#' 
#### Identify unemployment ######################################################

rm(list = ls())
gc()

#---- Setup --------------------------------------------------------------------

test <- 0
n    <- 1000

#---- Libraries ----------------------------------------------------------------

pacman::p_load(data.table, tidyverse, here, lubridate, siebanxicor, arrow)

#---- Load data ----------------------------------------------------------------


con <- DBI::dbConnect(duckdb::duckdb(),
                      dbdir = "temp1.duckdb")

interest_period <- interval(ymd("2020-01-01"), ymd("2020-12-31"))


last_queried <- file.info(here::here("data/raw/rpd_sample_ci.csv")) |>
  pluck("mtime") |>
  as_date()


ci <- here::here("data/temp/tidy_ci.feather") |>
  open_dataset(format = "feather",
               partitioning = "CVE_NSS") |>
  mutate(
    across(matches("FEC_|tidy"), as_date),
    across(matches("CVE_", as.character))
    ) |>
  compute()
  
#---- Subset -------------------------------------------------------------------

if (test) {
  set.seed(20240411)
  
  sample_nss <- ci |>
    pull(CVE_NSS, as_vector = T) |>
    unique() |>
    sample(n) 
  
  ci <- ci |>
    filter(CVE_NSS %in% sample_nss) |>
    compute()
}

#---- Wrangle ------------------------------------------------------------------


# Definition of unemployment as days without a job
days <- 30

unemployed <- ci |>
  arrange(CVE_NSS, tidy_end, tidy_start ) |>
  group_by(CVE_NSS) |>
  mutate(unemployment_days = as.numeric(tidy_start - lag(tidy_end))) |>
  ungroup() |>
  # Definition of unemployment
  mutate(unemployment = unemployment_days >= days,
         unemployment_date = case_when(unemployment ~ lag(FEC_MOV_FIN))
         ) |>
  filter(unemployment_date %within% interest_period) |>
  distinct(CVE_NSS, unemployment_days, unemployment_date) 

#---- Validate -----------------------------------------------------------------

# Validate that the arrangement of intervals does not change the computation
unemployed_b <- ci |>
  arrange(CVE_NSS, tidy_start, tidy_end ) |>
  group_by(CVE_NSS) |>
  mutate(unemployment_days = as.numeric(tidy_start - lag(tidy_end))) |>
  ungroup() |>
  mutate(unemployment = unemployment_days >= days,
         unemployment_date = case_when(unemployment ~ lag(FEC_MOV_FIN))
  ) |>
  filter(unemployment_date %within% interest_period) |>
  distinct(CVE_NSS, unemployment_days, unemployment_date) 

# Checks whether arrangement of intervals changes unemployment computation
stopifnot(nrow(unemployed) == nrow(unemployed_b))

rm(unemployed_b)

not_unemployed <- ci |>
  distinct(CVE_NSS) |>
  anti_join(unemployed, by = "CVE_NSS")

unemployment <- unemployed |> 
    full_join(not_unemployed, by = "CVE_NSS", copy = T) |>
    mutate(got_unemployed = !is.na(unemployment_date)) |>
    arrange(CVE_NSS, unemployment_date)


unemployment |> 
  group_by(got_unemployed) |> 
  summarise(n = n()) |> 
  ungroup() |>
  mutate(per = n / sum(n))


#---- Save ---------------------------------------------------------------------

if (!test) {
 
  save(
    unemployment,
    interest_period,
    file = here("data/working/unemployment.RData")
  )
}

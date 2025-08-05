#### SD ########################################################################
#' 
#' @name 01_tidy_intervals.R
#' 
#' @description
#' Compute tidy work intervals compatible with contribution density computations.
#' 
#' @author Esteban Degetau
#' 
#' @created 2024-05-13
#' 
#### Tidy intervals ############################################################

rm(list = ls())
gc()

set.seed(20240411)

#---- Libraries ----------------------------------------------------------------

pacman::p_load(citools, tidyverse, here, duckdb, arrow, citools)

#---- Data ---------------------------------------------------------------------

con <- DBI::dbConnect(duckdb::duckdb(),
                      dbdir = "temp1.duckdb")


last_queried <- file.info(here("data/raw/rpd_sample_ci.csv")) |>
  pluck("mtime") |>
  as_date()

ci <- here::here("data/raw/rpd_sample_ci.csv") |>
  open_dataset(format = "csv") |>
  to_duckdb(con)


fix_dates <- function(x) {
  y <- x |>
    str_sub(1, 9) |>
    dmy() |>
    as_date()
  
  y <- ifelse(y > today() | is.na(y), last_queried, y) |>
    as_date()
  return(y)
}

workers <- ci |>
  distinct(CVE_NSS) |>
  collect() |>
  sample_frac(0.1) |>
  to_duckdb(con)


workers |>
  collect() |>
  write_csv(here("data/temp/workers_sub.csv"))

ci <- ci |>
  inner_join(workers, by = "CVE_NSS", copy = TRUE) |>
  compute()


  

#---- Clean CI -----------------------------------------------------------------

work_modes <- c(
  "10",
  "11",
  "12",
  "13",
  "14",
  "15",
  "16",
  "17",
  "18",
  "19",
  "21",
  "27",
  "28",
  "29",
  "30",
  "31",
<<<<<<<< HEAD:R-scripts/DID/01_tidy_intervals.R
  "35",
========
  "34",
  "35",
  "40",
>>>>>>>> 66a49c9a3f62a155dbe3d2a918d74d823b63435e:R/DID/01_tidy_intervals.R
  "42",
  "43",
  "44",
  "45"
)

ci_clean <- ci |>
  mutate(CVE_NSS = as.character(CVE_NSS),
         CVE_MODALIDAD = as.character(CVE_MODALIDAD)) |>
  filter(CVE_MODALIDAD %in% work_modes) |>
  collect() |>
  mutate(FEC_MOV_INI = fix_dates(FEC_MOV_INI),
         FEC_MOV_FIN = fix_dates(FEC_MOV_FIN)) |>
  to_duckdb(con) |>
  filter(FEC_MOV_INI <= FEC_MOV_FIN) |>
  compute()
    

# ci_clean |>
#   pull(FEC_MOV_FIN) |>
#   summary()

#---- Tidy intervals -----------------------------------------------------------

tidy_ci <- ci_clean |>
  arrange(CVE_NSS, FEC_MOV_INI, FEC_MOV_FIN) |>
  collect() |>
  mutate(
    .by = CVE_NSS,
    tidy_interval = citools::tidy_intervals(FEC_MOV_INI, FEC_MOV_FIN)
  ) |>
  mutate(tidy_start = int_start(tidy_interval) |> as_date(),
         tidy_end = int_end(tidy_interval) |> as_date(),
         tidy_end = case_when(
           tidy_start > tidy_end ~ tidy_start,
           T ~ tidy_end
         )
         ) |>
  to_duckdb(con) |>
  select(!tidy_interval) |>
  compute()

#---- Save ---------------------------------------------------------------------
tidy_ci |>
  to_arrow() |>
  write_feather(here("data/temp/tidy_ci.feather"))


dbDisconnect(con)

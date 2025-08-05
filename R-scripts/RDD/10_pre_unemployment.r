
rm(list = ls())
gc()

# Libraries ---------------------------------------------------------------------

pacman::p_load(
    tidyverse,
    here,
    arrow,
    duckdb
)

# Load data ------------------------------------------------------------------

ci_schema <- schema(
    CVE_NSS = double(),
    CVE_MODALIDAD = int64(),
    FEC_MOV_INI = date32(),
    FEC_MOV_FIN = date32(),
    CVE_PATRON = string(),
    SAL_BAS = double(),
    partition = double()
)

con <- DBI::dbConnect(duckdb::duckdb(), dbdir = "duckdb")

ci <- here::here("data/temp/rd_clean_ci") |>
    arrow::open_dataset(format = "parquet", schema = ci_schema) |>
    to_duckdb(con) 

load(here::here("data/working/rpd.RData"))

# Merge -------------------------------------------------------------------------



rpd_merged <- ci |>
    right_join(rpd, copy = T) |>
    filter(FEC_MOV_INI < unemployment_date) |>
    
    ungroup() |>
    mutate(
        contract_duration = as.numeric(FEC_MOV_FIN - FEC_MOV_INI) + 1,
        contract_start = as.numeric(FEC_MOV_INI - min(FEC_MOV_INI)),
        
        ) |>
    compute() 

rpd_merged |> pull(CVE_MODALIDAD) |> summary()

employers <- rpd_merged |>
    distinct(CVE_PATRON) |>
    collect() |>
    mutate(
        patron_id = row_number()
    )

outcomes <- c(
      "take_up_12",
      "survival_3",
      "duration_6",
      "duration_36",
      "next_job_duration",
      "next_job_av_earnings",
      "earnings_total",
      "months_worked_total",
      "av_earnings_total"
    )

rpd_final <- rpd_merged |>
    left_join(employers, copy = T) |>
    select(
        CVE_NSS,
        partition,
        contract_start,
        contract_duration,
        patron_id,
        CVE_MODALIDAD,
        SAL_BAS,
        elig,
        unemployment_date,
        any_of(outcomes)
    ) |>
    compute()


# Write as parquet ------------------------------------------------------------
rpd_final |>
    to_arrow() |>
    group_by(partition) |>
    write_parquet(here::here("data/working/ci_pre_unemployment"))

DBI::dbDisconnect(con)

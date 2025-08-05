#### RPD #######################################################################
#'
#' @name 08_subset.R
#' 
#' @description Apply final filters to the analysis dataset.
#' 
#' @author Esteban Degetau
#' 
#' @created 2025-03-29
#' 
#### Subset data ###############################################################

rm(list = ls())
gc()

# Libraries --------------------------------------------------------------------

pacman::p_load(tidyverse, here, labelled, duckdb, arrow)

# Load data --------------------------------------------------------------------

here("data/working/rd_prep.RData") |>
    load()


# Firms -----------------------------------------------------------------------

ci_schema <- schema(
    CVE_NSS = double(),
    CVE_MODALIDAD = int64(),
    FEC_MOV_INI = date32(),
    FEC_MOV_FIN = date32(),
    tidy_start = date32(),
    tidy_end = date32(),
    CVE_PATRON = string(),
    SAL_BAS = double(),
    partition = int64()
  )

con <- DBI::dbConnect(duckdb::duckdb(),
                       dbdir = here("data/temp/rd_prep.duckdb"))

ci <- here("data/temp/rd_tidy_ci") |>
  open_dataset(
    format = "parquet",
    schema = ci_schema
  ) 

firms <- ci |>
  group_by(CVE_PATRON) |>
  summarise(
    historic_workers = n_distinct(CVE_NSS),
    first_date = min(FEC_MOV_INI),
  ) |>
  ungroup() |>
  collect()

firms |>
  ggplot(aes(historic_workers)) +
  geom_histogram(bins = 100) 

firms$historic_workers |> summary()

not_self_employed <- firms |>
  filter(historic_workers > 1) |>
  select(CVE_PATRON) |>
  distinct()

# Sectors ----------------------------------------------------------------------

non_agrarian <- here("data/raw/patrones.csv") |>
  read_csv() |> 
  filter(
    CVE_DIVISION != "00"
  ) |>
  distinct(CVE_PATRON)



# Subset -----------------------------------------------------------------------

rpd <- outcomes |>
  mutate(
    elig = running > 0
  ) |>
  filter(contribution_days <= days_since_account_opened) |>
  filter(contribution_days < 3 * 365) |>
  filter(contribution_days > 365) |>
  filter(age < 65 | no_curp == 1) |>
  filter(age >= 18 | no_curp == 1) |>
  filter(last_modality == 10) |>
  # Remove top 1% of earners
  filter(unemployment_days < quantile(unemployment_days, 0.99, na.rm = T)) |>
  # Remove self-employed workers
  inner_join(
    not_self_employed,
    by = c("last_employer_id" = "CVE_PATRON")
  ) |>
  # Keep non-agrarian firms
  inner_join(
    non_agrarian,
    by = c("last_employer_id" = "CVE_PATRON")
  )


# Save data -------------------------------------------------------------------

save(rpd, file = here("data/working/rpd.RData"))

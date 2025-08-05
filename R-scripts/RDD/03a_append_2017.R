#### RPD #######################################################################
#' 
#' @name 03a_append_2017.R
#' 
#' @description Append workers who joined IMSS during 2017 to the whole data.
#'              Data is appended at the tidy_intervals stage.
#' 
#' @author Esteban Degetau
#' 
#' @created 2025-07-09
#' 
#### Append 2017 ##############################################################

rm(list = ls())
gc()

set.seed(20250709)

# Libraries -------------------------------------------------------------------

pacman::p_load(tidyverse, arrow, here, duckdb)

# Load data -------------------------------------------------------------------

con <- DBI::dbConnect(duckdb::duckdb(),
                      dbdir = "temp1.duckdb")

my_schema_ci <- schema(
  "CVE_NSS" = int64(),
  "CVE_PATRON" = string(),
  "CVE_MODALIDAD" = int32(),
  "SAL_BAS" = float(),
  "FEC_MOV_INI" = date32(),
  "FEC_MOV_FIN" = date32(),
  "tidy_start" = date32(),
  "tidy_end" = date32(),
  "partition" = int64()
  )

ci_2017 <- here("data/temp/rd_tidy_ci_2017") |>
  open_dataset(format = "parquet", schema = my_schema_ci) |>
  mutate(partition = partition + 20) |>
  compute() 

ci_2017 |>
  write_dataset(
    format = "parquet",
    path = here("data/temp/rd_tidy_ci_2017_20"),
    partitioning = "partition") 


ci_raw <- here("data/temp/rd_tidy_ci") |>
  open_dataset(format = "parquet", schema = my_schema_ci) 


# Redefine partitions -----------------------------------------------------------
# To preserve representativeness at each partition

new_partitions <- ci_raw |>
  distinct(CVE_NSS) |>
  collect() |>
  mutate(partition = sample(1:23, n(), replace = T)) |>
  as_arrow_table()

ci <- ci_raw |>
  select(-partition) |>
  left_join(new_partitions) |>
  compute()

ci |>
  write_dataset(
    format = "parquet",
    path = here("data/temp/rd_tidy_ci_appended"),
    partitioning = "partition"
  )
  
# Bind samples ------------------------------------------------------------------

# sample_a <- here("data/temp/rd_sample.feather") |>
#   open_dataset(format = "feather")

# sample_b <- here("data/temp/rd_sample_2017.feather") |>
#   open_dataset(format = "feather")

# sample_full <- 

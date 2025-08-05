#### RPD #######################################################################
#' 
#' @name 02_clean_ci.R
#' 
#' @description Clean the cuenta individual data set.
#' 
#' @author Esteban Degetau
#' 
#' @created 2025-02-27
#' 
#### Clean CI ##################################################################

rm(list = ls())
gc()

set.seed(20240411)

test <- 0
test_n <- 1000

# Test whether .sourced_in_run is defined
if (exists(".sourced_in_run")) {
  test <- 0
}

# Libraries --------------------------------------------------------------------

pacman::p_load(citools, tidyverse, here, duckdb, arrow, citools, tictoc)

# Data -------------------------------------------------------------------------

con <- DBI::dbConnect(duckdb::duckdb(),
                      dbdir = "temp1.duckdb")

last_queried <- file.info(here("data/raw/rd_rpd_sample_ci_2017.csv")) |>
  pluck("mtime") |>
  as_date()

ci_schema <- schema(
  CVE_NSS = double(),
  CVE_MODALIDAD = int64(),
  FEC_MOV_INI = string(),
  FEC_MOV_FIN = string(),
  CVE_PATRON = string(),
  SAL_BAS = double(),
  partition = int64()
)

ci <- here("data/raw/rd_rpd_sample_ci_2017") |>
  open_dataset(
    format = "parquet",
    schema = ci_schema
  ) |>
  to_duckdb(con)


sample <- here::here("data/raw/rd_rpd_sample_2017.csv") |>
  open_dataset(
    format = "csv"
    ) |>
  to_duckdb(con)

# Functions --------------------------------------------------------------------

fix_dates <- function(x) {
  y <- x |>
    str_sub(1, 9) |>
    dmy() |>
    as_date()
  
  y <- ifelse(y > last_queried | is.na(y), last_queried, y) |>
    as_date()
  return(y)
}

# Subsample --------------------------------------------------------------------

# Partitions are randomlly assigned
if (test) {
  ci <- ci |>
    filter(partition == 1) |>
    compute()

  sample <- sample |>
    inner_join(
      ci |>
        distinct(CVE_NSS),
      by = c("nss" = "CVE_NSS"),
      copy = T) |>
    collect() |>
    sample_n(test_n) |>
    to_duckdb(con)

  ci <- ci |>
    inner_join(
      sample |>
        distinct(nss),
      by = c("CVE_NSS" = "nss"),
      copy = T) |>
    compute()
}

# Clean CI ---------------------------------------------------------------------

tic()
clean_dates <- ci |>
  distinct(FEC_MOV_INI) |>
  collect() |>
  mutate(clean_date = fix_dates(FEC_MOV_INI)) |>
  rename(raw_date = FEC_MOV_INI)
toc()

tic()
ci <- ci |>
  # This choice of modalidades is the set that generates days of contribution
  filter(!CVE_MODALIDAD %in% c(0,16,20,22,23,24,25,26,32,33,36,38,39,41,40)) |>
  compute()
toc()

tic()
ci <- ci |>
  left_join(
    clean_dates,
    by = c("FEC_MOV_INI" = "raw_date"),
    copy = T
  ) |>
  mutate(FEC_MOV_INI = clean_date) |>
  select(-clean_date) |>
  left_join(
    clean_dates,
    by = c("FEC_MOV_FIN" = "raw_date"),
    copy = T
  ) |>
  mutate(
    FEC_MOV_FIN = clean_date,
    FEC_MOV_FIN = ifelse(is.na(FEC_MOV_FIN), last_queried, FEC_MOV_FIN) 

  ) |>
  select(-clean_date) |>
  compute()
toc()

glimpse(ci)

tic()
ci <- ci |>
  filter(FEC_MOV_INI <= FEC_MOV_FIN) |>
  compute()
toc()

# Write as parquet ------------------------------------------------------------

# conn2 <- dbConnect(duckdb(), dbdir = "rd_clean_ci.duckdb", read_only = F)
# 
# 
# query_sql <- dbplyr::sql_render(ci)
# 
# 
# dbExecute(con, "ATTACH
#           'clean_ci.duckdb' AS diskdb")
# 
# dbExecute(con, paste0("CREATE TABLE
#                       diskdb.ci_clean AS ", query_sql))


ci <- ci |>
  to_arrow()

ci |>
    write_dataset(
      format = "parquet",
      path = here("data/temp/rd_clean_ci_2017"),
      partitioning = "partition"
      )

sample |>
    to_arrow() |>
    write_feather(here("data/temp/rd_sample_2017.feather"))

# Disconnect
DBI::dbDisconnect(con)

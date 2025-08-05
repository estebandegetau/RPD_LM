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
  FEC_MOV_INI = date32(),
  FEC_MOV_FIN = date32(),
  CVE_PATRON = string(),
  SAL_BAS = double(),
  partition = int64()
)

ci <- here("data/temp/rd_clean_ci_2017") |>
  open_dataset(
    format = "parquet",
    schema = ci_schema
  ) |>
  to_duckdb(con)


sample <- here::here("data/temp/rd_sample_2017.feather") |>
  open_dataset(format = "feather") |>
  to_duckdb(con)


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
    sample_n(test_n) 

  ci <- ci |>
    inner_join(
      sample |>
        distinct(nss),
      by = c("CVE_NSS" = "nss"),
      copy = T) |>
    compute()
}


# Tidy intervals ---------------------------------------------------------------


tic()
tidy_ci <- ci |>
  distinct(CVE_NSS, partition) |>

  collect() |>
  mutate(
    .by = partition,
    subpartition = runif(n()) |> ntile(n = 10) 
  ) |>
  nest(data = CVE_NSS) |>
  mutate(
    data = map(data, to_duckdb, con)
  )
toc() 



if (0) {

  tic()
  test_ci <- tidy_ci |>
    filter(partition == 1, subpartition == 1) |>
    mutate(
      data = map(
        data,
        ~ .x |>
          left_join(ci, by = "CVE_NSS") |>
          collect() |>
          group_by(CVE_NSS) |>
          arrange(FEC_MOV_INI, FEC_MOV_FIN, .by_group = T) |>
          mutate(
            tidy_interval = tidy_intervals(FEC_MOV_INI, FEC_MOV_FIN)
          ) |>
          ungroup() |>
          mutate(
            tidy_start = int_start(tidy_interval) |> as_date(),
            tidy_end = int_end(tidy_interval) |> as_date(),
            tidy_end = case_when(
              tidy_start > tidy_end ~ tidy_start,
              T ~ tidy_end
            )
          ) |>
          select(!tidy_interval) |>
          as_arrow_table(),
        .progress = T
      )
    )
  toc()
  
}


tic()
tidy_ci <- tidy_ci |>
  
  mutate(
    data = map(
      data,
      ~ .x |>
        left_join(ci, by = "CVE_NSS") |>
        collect() |>
        group_by(CVE_NSS) |>
        arrange(FEC_MOV_INI, FEC_MOV_FIN, .by_group = T) |>
        mutate(
          tidy_interval = tidy_intervals(FEC_MOV_INI, FEC_MOV_FIN)
        ) |>
        ungroup() |>
        mutate(
          tidy_start = int_start(tidy_interval) |> as_date(),
          tidy_end = int_end(tidy_interval) |> as_date(),
          tidy_end = case_when(
            tidy_start > tidy_end ~ tidy_start,
            T ~ tidy_end
          )
        ) |>
        select(!tidy_interval) |>
        as_arrow_table(),
        .progress = T
    )
  )
toc()

tidy_ci |> glimpse()

# Save -------------------------------------------------------------------------

out_dir <- here("data/temp/rd_tidy_ci_2017")

tic()
tidy_ci |> 
  mutate(
    partition_path = here(out_dir, str_c("partition=", partition)),
    create_out_dir = map(partition_path, ~ .x |> dir.create(showWarnings = F)),
    file_path = here(partition_path, str_c("part-", subpartition, ".parquet")),
    
    out_data = map2(file_path, data,
                    ~ write_parquet(.y, .x))
  )
toc()

# Close connections
con |> dbDisconnect()

# Test reading parquet
if (0) {
  tidy_ci_schema <- schema(
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
  
  test_read_parquet <- open_dataset(here(out_dir), format = "parquet", schema = tidy_ci_schema)
  
  glimpse(test_read_parquet)

  test_read_parquet |>
    pull(partition, as_vector = T) |>
    table()

  test_read_parquet |>
    distinct(CVE_NSS, FEC_MOV_INI, CVE_PATRON) |>
    collect() |>
    glimpse()
  
  test_read_parquet |>
    distinct(CVE_NSS) |>
    collect() |>
    glimpse()

  test_read_parquet |>
    to_duckdb(con) |>
    filter(CVE_NSS == min(CVE_NSS)) |>
    collect() |>
    view()

  test_read_parquet |>
    summarise(.by = CVE_NSS, n = n()) |>
    collect() |>
    pull(n) |>
    summary()


  debug <- test_read_parquet |>
    summarise(.by = CVE_NSS, n = n()) |>
    collect() |>
    filter(n == max(n))

  test_read_parquet |>
    filter(CVE_NSS %in% debug$CVE_NSS) |>
    collect() |>
    view()

}

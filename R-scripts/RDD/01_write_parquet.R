#### RPD #######################################################################
#' 
#' @name 01_write_parquet.R
#' 
#' @description Write a partitioned parquet file from CI data.
#' 
#' @author Esteban Degetau
#' 
#' @created 2025-02-24
#' 
#### Write parquet #############################################################

rm(list = ls())
gc()

set.seed(20250224)

# Libraries --------------------------------------------------------------------

pacman::p_load(tidyverse, here, arrow)

# Load -------------------------------------------------------------------------

ci_csv <- here::here("data/raw/rd_rpd_sample_ci_2017.csv") |>
  open_dataset(
    format = "csv"
    ) 

sample <- here::here("data/raw/rd_rpd_sample_2017.csv") |>
  open_dataset(format = "csv")

# Partition --------------------------------------------------------------------

glimpse(sample)

partition <- sample |> 
  distinct(nss) |>
  collect() |>
  mutate(partition = sample(1:3, n(), replace = T)) |>
  rename(CVE_NSS = nss)  |>
  as_arrow_table()
 
pq_path <- here("data/raw/rd_rpd_sample_ci_2017")

ci_csv |>
  left_join(partition) |>
  group_by(partition) |>
  write_dataset(pq_path, format = "parquet")

tibble(
  files = list.files(pq_path, recursive = TRUE),
  size_MB = file.size(file.path(pq_path, files)) / 1024^2
)

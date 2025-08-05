#### RPD #######################################################################
#' 
#' @name 11_mr_perfect.R
#' 
#' @description
#' Prepare data for alternative specification: subset to *perfect* workers who
#' have been employed continously forever until the density eligibility threshold.
#' 
#' @author Esteban Degetau
#' 
#' @created 2025-07-14
#' 
#### Mr Perfect ################################################################

rm(list = ls())
gc()

test <- 1
test_n <- 1000

# Libraries --------------------------------------------------------------------

pacman::p_load(tidyverse, here, arrow, duckdb)

# Load data --------------------------------------------------------------------

con <- DBI::dbConnect(duckdb::duckdb(),
                      dbdir = "temp1.duckdb")

unemployment <- here("data/working/rd_unemployment_rpd") |>
  open_dataset(format = "parquet") |>
  to_duckdb(con)

glimpse(unemployment)

if(test) {
  unemployment <- unemployment |>
    filter(partition == 1) |>
    compute()

  sample <- unemployment |>
    distinct(CVE_NSS) |>
    collect() |>
    sample_n(size = test_n) 

  unemployment <- unemployment |>
    inner_join(sample, copy = T) |>
    compute()
}

unemployment |>
  filter(meets_unemp_days_criterion == 1) |>
  ggplot(aes(days_since_account_opened, contribution_days)) +
  geom_point() +
  scale_x_continuous(labels = ~.x / 365, breaks = seq(365, 365 *5, 365))

unemployment |>
  ggplot(aes(days_since_account_opened, contribution_days)) +
  geom_point()
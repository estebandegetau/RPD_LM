#### RPD #######################################################################
#' 
#' @name 00_run.R
#' 
#' @description Run R scripts that produce outputs of the RD design.
#' 
#' @author Esteban Degetau
#' 
#' @created 2025-02-06
#' 
#### Run #######################################################################

rm(list = ls())
gc()

.sourced_in_run <- T

#---- Libraries ----------------------------------------------------------------
if (!requireNamespace("pacman", quietly = TRUE)) {
    install.packages("pacman")
}

pacman::p_load(tidyverse, here)

#Check if citools is installed, if not, install from GitHub
if (!requireNamespace("citools", quietly = TRUE)) {
    pacman::p_load(remotes)
    remotes::install_github("estebandegetau/citools")
}

#---- Setup --------------------------------------------------------------------

.run_01_write_parquet      <- 0
.run_02_clean_ci           <- 0
.run_03_tidy_intervals     <- 0
.run_04_unemployment       <- 0
.run_05_merge_RPD          <- 0
.run_06_deflate_wages      <- 0
.run_07_outcomes           <- 0
.run_08_subset             <- 0
.run_09_rd_prep            <- 0
.run_10_01_covariates      <- 1
.run_10_02_take_up         <- 1
.run_10_03_job_search      <- 1



#---- Run ----------------------------------------------------------------------
if (.run_01_write_parquet) {
    source(here("R/RDD/01_write_parquet.R"), encoding = "UTF-8")
}

if (.run_02_clean_ci) {
    source(here("R/RDD/02_clean_ci.R"), encoding = "UTF-8")
}

if (.run_03_tidy_intervals) {
    source(here("R/RDD/03_tidy_intervals.R"), encoding = "UTF-8")
}

if (.run_04_unemployment) {
    source(here("R/RDD/04_unemployment.R"), encoding = "UTF-8")
}

if (.run_05_merge_RPD) {
    source(here("R/RDD/05_merge_RPD.R"), encoding = "UTF-8")
}

if (.run_06_deflate_wages) {
    source(here("R/RDD/06_deflate_wages.R"), encoding = "UTF-8")
}

if (.run_07_outcomes) {
    source(here("R/RDD/07_outcomes.R"), encoding = "UTF-8")
}

if (.run_08_subset) {
    source(here("R/RDD/08_subset.R"), encoding = "UTF-8")
}

if (.run_09_rd_prep) {
    source(here("R/RDD/09_perform_rd.R"), encoding = "UTF-8")
}

if (.run_10_01_covariates) {
    source(here("R/RDD/10_01_covariates.R"), encoding = "UTF-8")
}

if (.run_10_02_take_up) {
    source(here("R/RDD/10_02_take_up.R"), encoding = "UTF-8")
}

if (.run_10_03_job_search) {
    source(here("R/RDD/10_03_job_search.R"), encoding = "UTF-8")
}


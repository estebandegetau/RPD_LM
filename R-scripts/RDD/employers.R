#### RPD #######################################################################
#' 
#' @name employers.R
#' 
#' @description merge employers data with the RPD data
#' 
#' @author Esteban Degetau
#' 
#' @date 2025-04-17
#' 
#### Employers ##################################################################

rm(list = ls())
gc()

# Libraries --------------------------------------------------------------------

pacman::p_load(tidyverse, here, arrow, duckdb, haven)

# Load data --------------------------------------------------------------------


employers <- here("data/raw/patronesf2025131.sas7bdat") |>
    read_sas()

employers |> glimpse()





load(here("data/working/rpd.RData"))


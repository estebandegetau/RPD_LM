#### RPD #######################################################################
#' 
#' @name 09_rd_prep.R
#' 
#' @description Loads functions and data to perform and analyze RDD. 
#' 
#' @author Esteban Degetau
#' 
#' @created 2025-02-05
#' 
#### Perform RD ################################################################

rm(list = ls())
gc()

# Libraries --------------------------------------------------------------------

pacman::p_load(tidyverse,
               here,
               gt,
               gtsummary,
               fixest,
               arrow,
               ggpubr,
               rddensity,
               rdrobust,
               labelled,
               modelsummary,
               kableExtra
               )

theme_set(theme_minimal())

# Load data --------------------------------------------------------------------

load(here("data/working/rpd.RData"))

source(here("R/rd_functions.R"))

# Group variables ---------------------------------------------------------------

dict <- labelled::generate_dictionary(rpd)

var_groups <- dict |>
  mutate(
    group = case_when(
      str_detect(variable, "rpd") ~ "RPD",
      str_detect(variable, "no_curp|began_working|unemployment_date|days_since|covid|birth_date|female|age") ~ "Covariates",
      str_detect(variable, "prev_job") ~ "Previous job",
      str_detect(variable, "survival_|duration_") ~ "Survival",
      str_detect(variable, "next_job") ~ "Next job",
      str_detect(variable, "year_|_total") ~ "Medium term",
      str_detect(variable, "take_up") ~ "Take up"
      
    )
  ) |>
  drop_na(group) |>
  filter(!group == "RPD") 

var_groups |>
  write_csv(here("results/var_groups.csv"))

# Prep data to compute ---------------------------------------------------------


to_compute <- rpd |>
  select(CVE_NSS, running, all_of(var_groups$variable)) |>
  mutate(across(where(is.Date), decimal_date)) |>
  pivot_longer(
    !matches("CVE|_id|running")
  ) |>
  nest(data = c(CVE_NSS, running, value)) |>
  left_join(var_groups, by = c("name" = "variable"))


to_compute_fuzzy <- rpd |>
  select(CVE_NSS, running, all_of(var_groups$variable)) |>
  mutate(across(where(is.Date), decimal_date)) |>
  pivot_longer(
    !matches("CVE|_id|running|take_up_12")
  ) |>
  nest(data = c(CVE_NSS, running, value, take_up_12)) |>
  left_join(var_groups, by = c("name" = "variable"))


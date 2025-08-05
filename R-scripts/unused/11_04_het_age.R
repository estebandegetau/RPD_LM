#### RPD #######################################################################
#' 
#' @name 11_04_het_age.R
#' 
#' @description Estimate heterogeneous treatment effects.
#' 
#' @author Esteban Degetau
#' 
#' @created 2025-03-17
#' 
#### Heterogeneity #############################################################

# Setup --------------------------------------------------------------------

pacman::p_load(here)

source(here("R/RDD/09_rd_prep.R"))

# Split data ---------------------------------------------------------------
age <- rpd |>
    mutate(
        split = ntile(age, 2),
    ) |>
    select(CVE_NSS, running, split, all_of(var_groups$variable)) |>
    mutate(across(where(is.Date), decimal_date)) |>
    pivot_longer(
        !matches("CVE|_id|running|split")
    ) |>
    nest(data = c(CVE_NSS, running, value)) |>
    left_join(var_groups, by = c("name" = "variable"))

# Perform RD  ------------------------------------------------------------

age_results <- age |>
    filter(str_detect(name, "survival|duration_")) |>
    mutate(
        rd = map(data, my_rd, .progress = T)
    ) |>
    select(-data)

save(age_results, file = here("results/RD/het/survival_age.RData"))
rm(age, age_results)

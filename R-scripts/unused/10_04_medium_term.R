#### RPD #######################################################################
#' 
#' @name 10_04_medium_term.R
#' 
#' @description Produce tables and figures to show eligibility effect on 
#'              medium term outcomes.
#' 
#' @author Esteban Degetau
#' 
#' @created 2025-03-29
#' 
#### Medium term ###############################################################

# Setup ------------------------------------------------------------------------
pacman::p_load(here)

source(here("R/RDD/09_rd_prep.R"))

# Perform RD -------------------------------------------------------------------

medium_term <- to_compute |>
  filter(group == "Medium term") |>
  mutate(
    rd = map(data, my_rd, .progress = T)
  )

# Plots ------------------------------------------------------------------------

medium_table <- medium_term |>
  mutate(
    unemp_year = str_extract(name, "\\d+") |> as.numeric(),
    
    unemp_year = str_c("Year ", unemp_year),
    unemp_year = case_when(#   is.na(unemp_year) & str_detect(name, "av_") ~ "Average 3 years",
      is.na(unemp_year)  ~ "Total 3 years", T ~ unemp_year)
    
  ) |>
  mutate(
    label = case_when(
      str_detect(name, "av_earnings") ~ "Average monthly earnings (2024 MXN)",
      str_detect(name, "earnings") ~ "Earnings (2024 MXN)",
      str_detect(name, "months_worked") ~ "Months Worked",
      
      
    )
  ) |>
  pluck_stats() |>
  select(!c(
    rd,
    name,
    data,
    col_type,
    levels,
    value_labels,
    group,
    pos,
    missing
  )) |>
  pivot_longer(!c(unemp_year, label), names_to = "stat") |>
  pivot_wider(id_cols = c(stat, label), names_from = unemp_year) |>
  group_by(label) 

# Save ----------------------------------------------------------------------

save(medium_table, file = here("results/RD/medium_term.RData"))


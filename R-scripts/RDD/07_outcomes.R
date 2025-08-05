#### RPD #######################################################################
#' 
#' @title 04_prep_data.R
#' 
#' @description Prepare data for RD analysis
#' 
#' @author Esteban Degetau
#' 
#' @created 2025-02-20
#' 
#### Prep data ##################################################################

rm(list = ls())
gc()

set.seed(20240411)

# Setup ------------------------------------------------------------------------

test   <- 0
test_n <- 1000

# Test whether .sourced_in_run is defined
if (exists(".sourced_in_run")) {
  test <- 0
}

# Load libraries ---------------------------------------------------------------

pacman::p_load(tidyverse, here, arrow, duckdb, labelled)

# Load data --------------------------------------------------------------------

con <- DBI::dbConnect(duckdb::duckdb(), dbdir = "duckdb")

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

ci <- here("data/working/rd_ci") |>
  open_dataset(
    format = "parquet",
    schema = my_schema_ci
  ) |>
  to_duckdb(con) 

unemployment <- here("data/working/rd_unemployment_rpd") |>
  open_dataset(format = "parquet") |>
  to_duckdb(con)

unemployment_dict <- here("data/working/rd_unemployment_rpd_dictionary.csv") |>
  read_csv()

unemp_var_labs <- setNames(
  as.list(unemployment_dict$label),
  unemployment_dict$variable
)

# Subset -----------------------------------------------------------------------

if (test) {
  sample <- unemployment |>
    filter(partition == 1) |>
    distinct(CVE_NSS) |>
    collect() |>
    sample_n(test_n) |>
    to_duckdb(con)

  ci <- ci |>
    filter(partition == 1) |>
    inner_join(sample) |>
    compute()

  unemployment <- unemployment |>
    filter(partition == 1) |>
    inner_join(sample) |>
    compute()
}

# Interest events ---------------------------------------------------------------

ci <- ci |>
  filter(year(FEC_MOV_INI) >= 2010) |>
  compute()

# Unemployment events of interest. Defined in 04_unemployment.R
interest_events <- unemployment |>
  filter(interest_event == 1) |>
  compute()

# Use of RPD -------------------------------------------------------------------

take_up <- interest_events |>
  distinct(CVE_NSS, days_to_take_up) |>
    collect() |>
    mutate(
        months_after_unemp = list(seq(2, 12 * 3, 1))
    ) |>
    unnest(cols = months_after_unemp) |>
    mutate(
        take_up = as.numeric(days_to_take_up / 30 <= months_after_unemp),
        take_up = replace_na(take_up, 0)
    ) |>
    select(CVE_NSS, months_after_unemp, take_up) |>
    pivot_longer(
        cols = c(take_up)
    ) |>
    pivot_wider(
        id_cols = CVE_NSS,
        names_from = c(name, months_after_unemp),
        values_from = value,
        names_sep = "_"
    )

take_up_description <- tibble(
  name = names(take_up)
) |>
  mutate(
    months = str_extract(name, "\\d+"),
    label = 
      case_when(
        str_detect(name, "take_up") ~ str_c(
          "Take up of RPD after ",
          months,
          " months")
  )) |>
  select(!months)

outcomes <- interest_events |>
  left_join(take_up, copy = T) |>
  compute()


if(test) {

  outcomes |>
    select(running, take_up_3) |>
    mutate(elig = running >= 0) |>
    ggplot(aes(running, take_up_3, color = elig)) +
    geom_smooth() +
    lims(x = c(-365, 365))


}

rm(take_up)

# Survival ---------------------------------------------------------------------



survival <- interest_events |>
    distinct(CVE_NSS, unemployment_days) |>
    collect() |>
    mutate(
        months_after_unemp = list(seq(2, 12 * 3, 1))
    ) |>
    unnest(cols = months_after_unemp) |>
    mutate(
        survival = as.numeric(unemployment_days / 30 >= months_after_unemp),
        duration = case_when(
            survival == 1 ~ months_after_unemp * 30 / 7,
            T ~ unemployment_days / 7
        )
    ) |>
    select(CVE_NSS, months_after_unemp, survival, duration) |>
    pivot_longer(
        cols = c(survival, duration)
    ) |>
    pivot_wider(
        id_cols = CVE_NSS,
        names_from = c(name, months_after_unemp),
        values_from = value,
        names_sep = "_"
    ) 

description <- tibble(
  name = names(survival)
) |>
  mutate(
    months = str_extract(name, "\\d+"),
    label = 
      case_when(
        str_detect(name, "survival") ~ str_c(
          "Survival out of formal employment after ",
          months,
          " months"),
        str_detect(name, "duration") ~ str_c(
          "Weeks out of formal employment censored at ",
          months,
          " months"
        
      )
  )) |>
  select(!months)

var_labels <- setNames(as.list(description$label), description$name)

outcomes <- outcomes |>
  left_join(survival, copy = T) |>
  compute()

glimpse(outcomes)

rm(survival)



if(test) {
  outcomes |>
    collect() |>
    pivot_longer(matches("survival|duration"),
                 names_to = "outcome",
                 values_to = "value") |>
    mutate(
      month = str_extract(outcome, "\\d+") |> as.numeric(),
      outcome = str_remove(outcome, "\\d+") |> str_remove("_") |> str_to_title()
    ) |>
    ggplot(aes(month, value)) +
    stat_summary() +
    facet_wrap( ~ outcome, scales = "free_y")
}



# Next job quality -------------------------------------------------------------


# Post unemployment CI
post_unemp <- interest_events |>
  distinct(CVE_NSS, unemployment_date) |>
  left_join(ci, copy = T) |>
  filter(FEC_MOV_INI > unemployment_date) |>
  filter(year(FEC_MOV_INI) < 2025) |>
  arrange(CVE_NSS, FEC_MOV_INI) |>
  collect() |>
  group_by(CVE_NSS) |>
  arrange(tidy_end, tidy_start, .by_group = T) |>
  mutate(
    gap = as.numeric(tidy_start - lag(tidy_end)) - 1,
    gap = replace_na(gap, 0),
    movements_since_unemp = dense_rank(FEC_MOV_INI),
    continuous_movement_n = cumsum(gap == 0),
    first_employer = as.numeric(CVE_PATRON == first(CVE_PATRON)),
    ) |>
  ungroup() |>
  mutate(
    days = citools::working_days(interval(FEC_MOV_INI, FEC_MOV_FIN)),
    days_contributed = citools::working_days(interval(tidy_start, tidy_end)),
    income = days * SAL_BAS,
    wage = SAL_BAS
  ) |>  
  to_duckdb(con)

next_job <- post_unemp |>
  filter(movements_since_unemp == continuous_movement_n, first_employer == 1) |>
  summarise(
    .by = c(CVE_NSS, CVE_PATRON),
    next_job_duration = sum(days_contributed) / 7,
    next_job_cum_earnings = sum(income)
  ) |>
  mutate(next_job_av_earnings = (next_job_cum_earnings / next_job_duration) * (52 / 12)) |>
  collect() |>
  rename(next_employer_id = CVE_PATRON)


outcomes <- outcomes |>
  left_join(
    next_job,
    copy = T
  ) |>
  compute()

rm(next_job)

if(test) {
  outcomes |>
    ggplot(aes(next_job_duration)) +
    geom_histogram()
  
  outcomes |>
    ggplot(aes(next_job_cum_earnings)) +
    geom_histogram() +
    scale_x_log10(label = scales::dollar)
  
  
  outcomes |>
    ggplot(aes(next_job_av_earnings)) +
    geom_histogram() +
    scale_x_log10(label = scales::dollar)
}
# Medium term -------------------------------------------------------------------

years_nested <- tibble(year = 1:3) |>
  cross_join(
    interest_events |>
      distinct(CVE_NSS, unemployment_date, partition) |>
      collect()
  ) |>
  mutate(
    year_start = unemployment_date + years(year - 1),
    year_end = year_start + years(1) - days(1)
  ) |>
  arrange(partition, CVE_NSS, year) |>
  nest(data = !c(partition)) |>
  mutate(
    data = map(data, to_duckdb, con),
    data = map2(data, partition, ~ .x |>
      left_join(
        post_unemp |>
          filter(partition == .y),
        copy = T,
        relationship = "many-to-many",
        by = join_by(CVE_NSS, unemployment_date)
      ) |>
      compute(),
    .progress = T
    )
  )


# years_nested |> glimpse()
# 
# years_nested$data[[1]] |> collect() |> glimpse()
# 
# a <- years_nested$data[[1]] |> collect() |>
#   as_arrow_table()
# 
# a |> glimpse()

years <- years_nested |>
  mutate(
    data = map(data, ~ .x |>
      select(
        year,
        CVE_NSS, 
        FEC_MOV_INI, 
        FEC_MOV_FIN, 
        tidy_start, 
        tidy_end, 
        year_start,
        year_end,
        SAL_BAS) |>
      collect() |>
      mutate(
        ci_interval = interval(FEC_MOV_INI, FEC_MOV_FIN),
        tidy_interval = interval(tidy_start, tidy_end),
        year_interval = interval(year_start, year_end),
        ci_intersection = intersect(ci_interval, year_interval),
        tidy_intersection = intersect(tidy_interval, year_interval),
        days = citools::working_days(ci_intersection),
        income = days * SAL_BAS,
        days_worked = citools::working_days(tidy_intersection)
      ) |>
      summarise(
        .by = c(CVE_NSS, year),
        earnings = sum(income, na.rm = T),
        months_worked = sum(days, na.rm = T) / 30,
        av_earnings = (earnings / months_worked)
      )|>
      as_arrow_table(),
      .progress = T ) 
  ) 


years_final <- years |>
  mutate(
    data = map(data, ~ .x |> collect())
  ) |>
  unnest(data) |> 
  arrange(partition, CVE_NSS, year) |>
  to_duckdb(con)


if(test) {
  years_final |>
    ggplot(aes(year, earnings)) +
    stat_summary()
  
  years_final |>
    ggplot(aes(year, months_worked)) +
    stat_summary()
  
  years_final |>
    ggplot(aes(factor(year), av_earnings)) +
    geom_boxplot() +
    scale_y_log10()
}


medium_totals <- interest_events |>
  distinct(partition, CVE_NSS, unemployment_date) |>
  collect() |>
  mutate(
    start = unemployment_date,
    end = start + years(3)
  ) |>
  nest(data = !c(partition)) |>
  mutate(
    data = map(data, as_arrow_table),
    data = map(
      data,
      ~ .x |>
        to_duckdb(con) |>
        left_join(
          post_unemp,
          relationship = "many-to-many"
        ) |>
        compute(),
      .progress = T
    ),
    data = map(
      data,
      ~ .x |>
        collect() |>
        mutate(
          ci_interval = interval(FEC_MOV_INI, FEC_MOV_FIN),
          tidy_interval = interval(tidy_start, tidy_end),
          year_interval = interval(start, end),

          ci_intersection = intersect(ci_interval, year_interval),
          tidy_intersection = intersect(tidy_interval, year_interval),

          days = citools::working_days(ci_intersection),
          income = days * SAL_BAS,
          days_worked = citools::working_days(tidy_intersection)
        ) |>
        summarise(
          .by = c(CVE_NSS),
          earnings = sum(income, na.rm = T),
          months_worked = sum(days, na.rm = T) / 30,
          av_earnings = (earnings / months_worked)
        ) |>
        as_arrow_table(),
      .progress = T
    )
  )
   
medium_totals <- medium_totals |>
    mutate(
      data = map(data, collect)
    ) |>
    unnest(data)


medium <- years_final |>
  collect() |>
  pivot_longer(
    cols = c(earnings, months_worked, av_earnings),
    names_to = "outcome",
    values_to = "value"
  ) |>
  mutate(
    outcome = str_c(outcome, "year", year, sep = "_")
  ) |>
  select(CVE_NSS, outcome, value) |>
  pivot_wider(
    names_from = outcome,
    values_from = value
  ) |>
  left_join(
    medium_totals |>
      rename_with(~ str_c(.x, "_total"), .cols = !CVE_NSS),
  ) 

yearly_labs <- tibble(
  name = names(medium)
) |> 
  mutate(
    year = str_extract(name, "\\d+"),
    label = 
      case_when(
          str_detect(name, "av_earnings") ~ str_c(
          "Average monthly earnings in year ",
          year,
          " after displacement (2024 MXN)"),
        str_detect(name, "earnings") ~ str_c(
          "Total earnings in year ",
          year,
          " after displacement (2024 MXN)"),
        str_detect(name, "months_worked") ~ str_c(
          "Months worked in year ",
          year,
          " after displacement"),

      
  ),
    label = case_when(
      is.na(year) & str_detect(name, "av_earnings") ~ "Average monthly earnings after displacement (2024 MXN)",
      is.na(year) & str_detect(name, "earnings") ~ "Total earnings after displacement (2024 MXN)",
      is.na(year) & str_detect(name, "months_worked") ~ "Months worked after displacement",
      T ~ label
    )
  ) |>
  select(!year)

medium_val_labels <- setNames(as.list(yearly_labs$label), yearly_labs$name)

outcomes <- outcomes |>
  left_join(
    medium,
    copy = T
  ) |>
  compute()

rm(medium, medium_totals)

# Pre-unemployment outcomes ----------------------------------------------------

# Pre unemployment CI
pre_unemp <-  interest_events |>
  distinct(CVE_NSS, unemployment_date) |>
  left_join(ci, copy = T) |>
  filter(FEC_MOV_INI < unemployment_date) |>
  filter(year(FEC_MOV_INI) < 2025) |>
  arrange(CVE_NSS, FEC_MOV_INI) |>
  group_by(CVE_NSS) |>
  arrange(desc(tidy_end), desc(tidy_start), .by_group = T) |>
  collect() |>
  mutate(
    gap = as.numeric(lag(tidy_start) - tidy_end) - 1,
    gap = replace_na(gap, 0),
    movements_since_unemp = dense_rank(-as.numeric(FEC_MOV_INI)),
    continuous_movement_n = cumsum(gap == 0),
    last_employer = as.numeric(CVE_PATRON == first(CVE_PATRON)),
    ) |>
  ungroup() |>
  mutate(
    days = citools::working_days(interval(FEC_MOV_INI, FEC_MOV_FIN)),
    days_contributed = citools::working_days(interval(tidy_start, tidy_end)),
    income = days * SAL_BAS,
    wage = SAL_BAS
  ) |>  
  to_duckdb(con)


prev_job <- pre_unemp |>
  filter(movements_since_unemp == continuous_movement_n, last_employer == 1) |>
  summarise(
    .by = c(CVE_NSS, CVE_PATRON),
    prev_job_duration = sum(days_contributed) / 7,
    prev_job_cum_earnings = sum(income),
    last_sal_bas = first(SAL_BAS),
    last_modality = first(CVE_MODALIDAD)
  ) |>
  mutate(prev_job_av_earnings = (prev_job_cum_earnings / prev_job_duration) * (52 / 12)) |>
  collect() |>
  rename(last_employer_id = CVE_PATRON)

outcomes <- outcomes |>
  left_join(
    prev_job,
    copy = T
  ) |>
  compute()

rm(prev_job)

if(test) {
  outcomes |>
    ggplot(aes(prev_job_duration)) +
    geom_histogram()
  
  outcomes |>
    ggplot(aes(prev_job_cum_earnings)) +
    geom_histogram() +
    scale_x_log10(label = scales::dollar)
  
  outcomes |>
    ggplot(aes(prev_job_av_earnings)) +
    geom_histogram() +
    scale_x_log10(label = scales::dollar)
}
# Label variables -------------------------------------------------------------

outcomes <- outcomes |>
  collect() |>
  mutate(
    no_curp = as.numeric(is.na(age))
  ) |>
  set_variable_labels(.labels = unemp_var_labs, .strict = F) |>
  set_variable_labels(.labels = var_labels, .strict = F) |>
  set_variable_labels(
    next_job_duration = "Duration of next job (weeks)",
    next_job_cum_earnings = "Total earnings in next job (2024 MXN)",
    next_job_av_earnings = "Monthly earnings in next job (2024 MXN)",
    prev_job_duration = "Duration of previous job (weeks)",
    prev_job_cum_earnings = "Total earnings in previous job (2024 MXN)",
    prev_job_av_earnings = "Monthly earnings in previous job (2024 MXN)",
    last_sal_bas = "Last daily wage (2024 MXN)",
    last_modality = "Last modality",
    no_curp = "No CURP",
    .strict = F
  ) |>
  set_variable_labels(.labels = medium_val_labels, .strict = F) |>
  set_variable_labels(.labels = take_up_description, .strict = F)

generate_dictionary(outcomes) |>
  write_csv(here("data/working/rd_outcomes_dict.csv"))

# Save data --------------------------------------------------------------------

if(test){
  outcomes |>
    ggplot(aes(began_working)) + 
    stat_ecdf()

  outcomes |>
    ggplot(aes(unemployment_days)) + 
    stat_ecdf()
}

if (!test) {
outcomes |>
  save(file = here("data/working/rd_prep.RData"))
  DBI::dbDisconnect(con)
}

# Disconnect DB




#### SD ########################################################################
#' 
#' @name 02_unemployment.R
#' 
#' @description Identify unemployment in the CI data. 
#' 
#' @author Esteban Degetau
#' 
#' @created 2024-04-30
#' 
#### Identify unemployment ######################################################

rm(list = ls())
gc()

# Setup ------------------------------------------------------------------------

test    <- 1
test_n  <- 10000

# Test whether .sourced_in_run is defined
if (exists(".sourced_in_run")) {
  test <- 0
}

# Libraries --------------------------------------------------------------------

pacman::p_load(
  tidyverse,
  here,
  arrow,
  labelled
)

# Load data --------------------------------------------------------------------

con <- DBI::dbConnect(duckdb::duckdb(),
                      dbdir = "temp1.duckdb")

DBI::dbExecute(con, "PRAGMA max_temp_directory_size='100GB';")


sample <- here::here("data/temp/rd_sample") |>
  open_dataset(
    format = "feather"
    ) |> 
  collect() |>
  mutate(
    pricot = dmy(pricot)
  ) |>
  to_duckdb(con)


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

ci <- here::here("data/temp/rd_tidy_ci") |>
  open_dataset(format = "parquet",
               schema = my_schema_ci) |>
  to_duckdb(con)


if(0) {

  debug <- ci |>
    pull(FEC_MOV_INI) |>
    summary()

}

# Subset -----------------------------------------------------------------------

if (test) {
  set.seed(20240411)
  
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

# Unemployment -----------------------------------------------------------------

last_updated <- file.info(here("data/raw/rd_rpd_sample_ci_2017.csv")) |>
  pluck("mtime") |>
  as_date()

ci_clean <- ci |>
  filter(year(FEC_MOV_INI) >= 2010) |>
  group_by(CVE_NSS) |>
  arrange(tidy_end, tidy_start, .by_group = T) |>
  mutate(
    unemployment_days = as.numeric(tidy_start - lag(tidy_end)) - 1,
    unemployment_days = case_when(
      tidy_end == max(tidy_end) ~ as.numeric(last_updated - max(tidy_end)),
      T ~ unemployment_days
    ),
    unemployment_date = lag(FEC_MOV_FIN)
    
  ) |>
  ungroup() |>
  mutate(
    
    # According to Ley del Seguro Social
    meets_unemp_days_criterion = as.numeric(unemployment_days + 2 >= 46)
  ) |>
  compute()


unemployed <-  ci_clean |>
  # Allowing for spells of 1 day, to explore the assumption of 45 days
  filter(unemployment_days > 0) |>
  filter(year(unemployment_date) <= 2021) |>
  distinct(CVE_NSS,
           unemployment_days,
           unemployment_date,
           meets_unemp_days_criterion) |>
  left_join(sample |> select(nss, pricot), by = join_by(CVE_NSS == nss)) |>
  rename(began_working = pricot) |>
  mutate(
    days_since_account_opened = as.numeric(unemployment_date - began_working) + 1,
    meets_account_days_criterion = as.numeric(between(days_since_account_opened, 3 * 365, 4 * 365))
  ) |>
  filter(days_since_account_opened > 0) |>
  compute() 

if(test) {
  unemployed |>
    collect() |>
    drop_na(unemployment_days) |>
    ggplot(aes(unemployment_days)) +
    geom_histogram()
  
  
  unemployed |>
    ggplot(aes(unemployment_date)) +
    geom_histogram()
  
  unemployed |>
    filter(meets_unemp_days_criterion == 1) |>
    group_by(CVE_NSS) |>
    summarise(spells = n_distinct(unemployment_date)) |>
    ungroup() |>
    ggplot(aes(spells)) +
    geom_histogram()
  
  unemployed |>
    filter(meets_unemp_days_criterion == 1) |>
    ggplot(aes(days_since_account_opened)) +
    geom_histogram(binwidth = 365) +
    scale_x_continuous(label = ~ .x / 365, breaks = seq(0, 10, 1) * 365) +
    labs(x = "Years since account opened")
  
  
  }

# Append all workers ----------------------------------------------------------

unemployment <- sample |>
  distinct(nss, pricot) |>
  rename(CVE_NSS = nss, began_working = pricot) |>
  left_join(
    unemployed |>
      select(-began_working)
  ) |>
  arrange(CVE_NSS, unemployment_date) |>
  mutate(
    .by = CVE_NSS,
    ever_got_unemployed = as.numeric(any(meets_unemp_days_criterion == 1, na.rm = T)),
  ) |>
  to_arrow() |>
  mutate(
    covid = as.numeric(unemployment_date >= ymd("2020-03-01")),
    ever_got_unemployed = as.numeric(ifelse(is.na(ever_got_unemployed), 0, ever_got_unemployed)),
  ) |>
  filter(!is.na(unemployment_date)) |>
  compute() |>
  to_duckdb(con)

if (test) {
  glimpse(unemployment)
  
  unemployment |>
    group_by(meets_unemp_days_criterion) |>
    summarise(n = n_distinct(CVE_NSS)) |>
    ungroup() |>
    mutate(per = n / sum(n))
  
  unemployment |>
    ggplot(aes(unemployment_days, color = covid)) +
    stat_ecdf() +
    scale_x_log10() +
    geom_vline(xintercept = 47)
  
  unemployment |>
    ggplot(aes(unemployment_days, color = covid)) +
    geom_density() +
    scale_x_log10() +
    geom_vline(xintercept = 47)
  
  unemployment |>
    ggplot(aes(x = covid, y = unemployment_days, fill = factor(covid))) +
    geom_boxplot() +
    scale_y_log10()


}

# Define sample of interest ----------------------------------------------------

interest_workers <- unemployment |>
  filter(meets_unemp_days_criterion == 1, meets_account_days_criterion == 1) |>
  filter(
    .by = CVE_NSS,
    n_distinct(unemployment_date) == 1
  ) |>
  distinct(CVE_NSS, unemployment_date) |>
  mutate(interest_event = 1) |>
  compute()

# potential_mr_perfects <- unemployment |>
#   filter(
#     between(days_since_account_opened, 2 * 365, 4 * 365),
#     meets_unemp_days_criterion == 1
#   ) |>
#   distinct(CVE_NSS, unemployment_date) |>
#   slice_min(unemployment_date, by = CVE_NSS) |>
#   mutate(pot_mr_perfect = 1) |>
#   compute()



unemployment <- unemployment |>
  left_join(interest_workers) |>
  # left_join(potential_mr_perfects) |>
  mutate(
    interest_event = ifelse(is.na(interest_event), 0, interest_event),
    # pot_mr_perfect = ifelse(is.na(pot_mr_perfect), 0, pot_mr_perfect)
  ) |>
  mutate(
    .by = CVE_NSS,
    interest_worker = as.numeric(any(interest_event == 1 
      # | 
      # pot_mr_perfect == 1
  ))
  ) |>
  compute()

interest_workers <- interest_workers |>
  # full_join(potential_mr_perfects) |>
  distinct() |>
  mutate(interest_event = ifelse(is.na(interest_event), 0, interest_event)) |>
  arrange(CVE_NSS, unemployment_date) |>
  compute()

# Compute running variable -----------------------------------------------------

glimpse(unemployment)


if(test) {
  test_days_calc <- tibble(
    tidy_start = ymd("2020-01-01"),
    tidy_end = ymd("2020-12-31"),
    unemployment_date = ymd("2020-06-01")
  ) |>
    to_duckdb(con) |>
    mutate(days = tidy_end - tidy_start + 1) |>
    collect()
}

# Contribution days at the time of unemployment 
contribution_days <- interest_workers |>
  inner_join(
    ci,
    by = "CVE_NSS",
    copy = T
  ) |>
  filter(!FEC_MOV_INI > unemployment_date) |>
  mutate(days = as.numeric(tidy_end - tidy_start) + 1) |>
  summarise(.by = c(CVE_NSS, unemployment_date),
            contribution_days = sum(days)) |>
  compute()

# Running variable defined as days to completing 2 years of contributions
running <-  contribution_days |>
  mutate(running = contribution_days - (365 * 2)) |>
  arrange(CVE_NSS, unemployment_date) |>
  compute()

glimpse(running)


unemployment <- unemployment |>
  left_join(
    running
  ) |>
  filter(contribution_days <= days_since_account_opened) |>
  compute()

glimpse(unemployment)

# Explore running variable -----------------------------------------------------

if(test) {
  unemployment |> pull(1) |> length()
  # unemployment |>
  #   filter(pot_mr_perfect) |>
  #   ggplot(aes(days_since_account_opened, contribution_days)) +
  #   geom_point() +
  #   scale_x_continuous(labels = ~.x / 365, breaks = seq(365, 365 *5, 365))

  unemployment |>
    ggplot(aes(contribution_days)) +
    geom_histogram() +
    facet_wrap( ~ interest_worker)
  
  unemployment |>
    ggplot(aes(running)) +
    geom_histogram() +
    facet_wrap( ~ interest_worker)
  
  unemployment |>
    ggplot(aes(contribution_days)) +
    geom_histogram() +
    facet_wrap( ~ interest_event)
  
  unemployment |>
    ggplot(aes(running)) +
    geom_histogram() +
    facet_wrap( ~ interest_event)
  
  unemployment |>
    group_by(interest_worker) |>
    summarise(n = n_distinct(CVE_NSS)) |>
    ungroup() |>
    mutate(per = n / sum(n))
  
  unemployment |> pull(CVE_NSS) |> unique() |> length()
  
  glimpse(unemployment)
}

# Age at unemployment ----------------------------------------------------------

# From CURP
ages <- sample |>
  distinct(nss, CURP, sexo, pricot) |>
  collect() |>
  mutate(
    birth_date = substr(CURP, 5, 10) |> as_date(),
    birth_date = case_when(
      birth_date > pricot ~ birth_date - years(100),
      T ~ birth_date
    ),
    female = as.numeric(sexo == 2)
  ) |>
  select(CVE_NSS = nss, birth_date, female) |>
  to_duckdb(con)



unemployment <- unemployment |>
  left_join(
    ages
  ) |>
  mutate(
    age = as.numeric(unemployment_date - birth_date) / 365
  ) |>
  compute()

if(test){
  ages |>
  ggplot(aes(birth_date)) +
  geom_histogram()

unemployment |>
  ggplot(aes(age)) +
  geom_histogram() +
  facet_wrap(~ interest_event)

unemployment$age |> summary()}

# Partition data ---------------------------------------------------------------

partitions <- ci |>
  distinct(CVE_NSS, partition) |>
  compute()

unemployment <- unemployment |>
  left_join(
    partitions
  ) |>
  compute()

# Label variables --------------------------------------------------------------

unemp_labs <- unemployment |>
  head() |>
  collect() |>
  set_variable_labels(
    running = "Running variable: centered contribution days",
    interest_worker = "Worker with exactly one unemployment spell between 3 and 4 years after account opening",
    interest_event = "Unique unemployment spell between 3 and 4 years after account opening",
    contribution_days = "Contribution days at displacement date",
    days_since_account_opened = "Days since account opened at displacement date",
    unemployment_days = "Days without a job",
    unemployment_date = "Displacement date",
    began_working = "Date worker began working",
    meets_unemp_days_criterion = "Unemployment spell of at least 45 days",
    meets_account_days_criterion = "Worker has been active for at least 3 years",
    covid = "Displacement date is after March 2020",
    age = "Age at displacement date",
    female = "Female",
    birth_date = "Birth date"
  )

glimpse(unemp_labs)

generate_dictionary(unemp_labs)


rm(
  ages,
  partitions,
  contribution_days,
  running,
  interest_workers,
  ci,
  ci_clean,
  sample,
  unemployed
)

# Save -------------------------------------------------------------------------

# Unemployment is a table whose observation level is unemployment spells
if(!test) {
  unemployment <- unemployment |>
    to_arrow() 

  unemployment |>
    write_dataset(
      format = "parquet",
      path = here("data/working/rd_unemployment"),
      partitioning = "partition"
    )

  generate_dictionary(unemp_labs) |>
    write_csv(here("data/working/rd_unemployment_dictionary.csv"))
}

# Disconnect
DBI::dbDisconnect(con)
  
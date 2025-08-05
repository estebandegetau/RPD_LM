#### SD ########################################################################
#'
#' @name 11_prepare_ITT_data.R
#'
#' @description Prepare data for intention-to-treat analysis (Applied Research I)
#'
#' @author Esteban Degetau
#'
#' @created 2024-09-17
#'
#### Prepare ITT data ##########################################################

rm(list = ls())
gc()

#---- Libraries ----------------------------------------------------------------

pacman::p_load(
    tidyverse,
    here,
    citools,
    labelled,
    arrow,
    duckdb
)

#---- Load data ----------------------------------------------------------------

con <- DBI::dbConnect(duckdb::duckdb(), dbdir = "duckdb")

load(here("data/working/pre_covid_features.RData"))

load(here("data/working/unemployment.RData"))



# RPD data comes from Rodrigo's extraction.
# rpd <-
#   open_dataset(here("data/raw/extraccion_tabla_retiros.feather"),
#                format = "feather") |>
#   to_duckdb(con) |>
#   filter(!is.na(MONTO_RET)) |>
#   mutate(CVE_NSS = as.numeric(CVE_NSS))

features <- here("data/raw/rpd_sample.csv") |>
    open_dataset(format = "csv") |>
    to_duckdb(con) |>
    mutate(
        female = as.numeric(sexo == 2),
        edad_final = edad_final - 5, # Age in december 2019
        nss = as.character(nss)
    ) |>
    select(
        CVE_NSS = nss,
        female,
        age = edad_final # Age at last cotiza run. Currently December 2024.
    ) |>
    compute()

outcomes <- here("data/working/weekly_outcomes.feather") |>
    open_dataset(format = "feather") |>
    to_duckdb(con)

# Outcomes is a juggernaut. I'll save it as a feather file and load it in chunks.
path <- here("data/parquets/outcomes_yob")

outcomes |>
    collect() |>
    # Extract year of birth from CVE_NSS
    mutate(partition = case_when(
        str_sub(CVE_NSS, 5, 5) |> 
                as.numeric() == 9 ~ 1,
                T ~ 2
    )) |>
    group_by(partition) |>
    write_dataset(
        path, 
        format = "parquet",
        existing_data_behavior = "overwrite"
        )

tibble(
  files = list.files(path, recursive = TRUE),
  size_MB = file.size(file.path(path, files)) / 1024^2
)

outcomes <- path |>
    open_dataset(format = "parquet") |>
    to_duckdb(con)

#---- Merge --------------------------------------------------------------------

#' Note weekly outcomes are only defined for workers who had a 30 day
#' unemployment spell, and whose account is between 2 and 6 years old.

workers <- unemployment |>
    #   left_join(
    #     rpd,
    #     by = join_by(CVE_NSS),
    #     copy = T,
    #   ) |>
    left_join(
        features,
        by = join_by(CVE_NSS),
        copy = T,
    ) |>
    left_join(
        pre_covid_features,
        by = join_by(CVE_NSS),
        copy = T,
    ) |>
    drop_na(income_full_career) |>
    mutate(
        unemp_spells = n_distinct(unemployment_date, na.rm = T),
        .by = CVE_NSS
    ) |>
    compute()


# workers |>
#     select(!c(CVE_NSS)) |>
#     gtsummary::tbl_summary(
#         by = got_unemployed,
#         statistic = list(all_continuous() ~ "{mean} ({sd})")
#     )

#---- Define ITT sample --------------------------------------------------------
interest_workers <- workers |>
    # Defines period of unemployment

    collect() |>
    mutate(
    # These variables will determine eligibility. They have to be precise.
        days_since_opened = interval(began_working, unemployment_date) |>
            citools::working_days()
    ) |>
    # ITT requires all workers have accounts with more than 3 years of age.
    filter(days_since_opened >= 3 * 365) |>
    filter(days_since_opened <  5 * 365) |>
    filter(unemp_spells == 1) |>
    filter(unemployment_days > 45) |>
    to_duckdb(con)

#---- Wrangle ------------------------------------------------------------------


outcomes_names <- colnames(outcomes)
interest_workers_names <- colnames(interest_workers)


outcomes_names[outcomes_names %in% interest_workers_names]

work <- outcomes |>
    inner_join(
        interest_workers,
        copy = T
    ) |>
    compute()

glimpse(work)


#---- Tidy up ------------------------------------------------------------------


# Anonymize workers
keys <- unemployment |>
    distinct(CVE_NSS) |>
    collect() |>
    mutate(
        # Because sample was randomly drawn, we can use row number as worker ID
        # while preserving anonymity.
        worker_id = dense_rank(CVE_NSS)
    ) |>
    select(worker_id, CVE_NSS) |>
    arrange(worker_id)


anon <- work |>
    left_join(keys, copy = T) |>
    select(
        period,
        worker_id,
        density,
        income,
        account_days,
        cumulative_days_contributed,
        female,
        age,
        began_working,
        unemployment_date,
        unemployment_days,
        density_first_year,
        density_last_year,
        density_full_career,
        income_first_year,
        income_last_year,
        income_full_career,

        partition,

        # rpd,
        # retirement_balance = SALDO_RCV,
        # rpd_date = FEC_MOVTO,
        # days_lost = DIAS_RET,
        # rpd_withdrawal = MONTO_RET,
        # weeks_contributed = SEM_COT_97
    ) |>
    arrange(worker_id, period) |>
    compute()

workers_anon <- workers |>
    left_join(keys, copy = T) |>
    select(!CVE_NSS) |>
    relocate(worker_id, unemployment_date, .before = 1)

description <- tribble(
    ~name, ~label,
    "period", "Week start",
    "worker_id", "Worker ID",
    "density", "Weekly contribution density",
    "income", "Weekly contribution mass (2024 MXN)",
    "account_days", "Days since account was opened",
    "cumulative_days_contributed", "Days of contribution accumulated",
    "female", "Female",
    "began_working", "First contribution date",
    "unemployment_date", "Unemployment date",
    "unemployment_days", "Days unemployed",
    "unemp_spells", "Number of unemployment spells of at least 46 days in 2020",
    "density_first_year", "First year contribution density",
    "density_last_year", "2019 contribution density",
    "density_full_career", "Full career contribution density",
    "income_first_year", "First year contribution mass (2024 MXN)",
    "income_last_year", "2019 contribution mass (2024 MXN)",
    "income_full_career", "Full career contribution mass (2024 MXN)",
    "eligible", "Eligible to RPD at unemployment date",
    "rpd", "Made partial withdrawal",
    "retirement_balance", "Retirement account balance at moment of withdrawal (current MXN)",
    "rpd_date", "Date of partial withdrawal",
    "days_lost", "Contribution days lost to withdrawal",
    "rpd_withdrawal", "Amount withdrawn (current MXN)",
    "weeks_contributed", "Weeks contributed at moment of withdrawal",
    "age", "Age in December 2019"
)


#---- Save ---------------------------------------------------------------------


anon |>
    collect() |>
    group_by(partition) |>
    write_dataset(
        here("data/anon/itt"), 
        format = "parquet",
        existing_data_behavior = "overwrite"
        )

workers <- workers_anon

save(workers, file = here("data/anon/workers.RData"))


save(description, file = here("data/anon/itt_description.RData"))

save(keys, file = here("data/working/itt_keys.RData"))

write_csv(keys, here("data/working/itt_keys.csv"))

DBI::dbDisconnect(con)

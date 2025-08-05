#### SD ########################################################################
#' 
#' @name 05_final_sample.R
#' 
#' @description Build the final sample for the analysis, merging
#' 
#' @author Esteban Degetau
#' 
#' @created 2024-06-19
#' 
#### Final sample ##############################################################

rm(list = ls())
gc()

#---- Libraries ---------------------------------------------------------------

pacman::p_load(
    tidyverse,
    here,
    arrow,
    duckdb,
    labelled,
    data.table
)

#---- Load data ---------------------------------------------------------------


con <- DBI::dbConnect(duckdb::duckdb())

# Same sample as ci_raw with wages deflated.
ci_final <- 
  open_dataset(here("data/working/ci_final.feather"), format = "feather") |>
  to_duckdb(con)

# Same sample as ci_raw. NSS-unemployment_date level. A worker can have more
# than one unemployment events during 2020.
load(here("data/working/unemployment.RData"))

unemployment <- unemployment |> to_duckdb(con)

# Conditional on having pre-covid activity. 
load(here("data/working/pre_covid_features.RData"))

pre_covid_features <- pre_covid_features |> to_duckdb(con)

# Conditional on having gotten unemployed, as per unemployment dataset.
outcomes <- open_dataset(here("data/working/outcomes.feather"), format = "feather") |>
  to_duckdb(con)

# RPD data comes from Rodrigo's extraction.
# rpd <-
#   open_dataset(here("data/01_raw/extraccion_tabla_retiros.feather"),
#                format = "feather") |>
#   to_duckdb(con) |>
#   filter(!is.na(MONTO_RET)) |>
#   mutate(CVE_NSS = as.numeric(CVE_NSS))  



features <- here("data/raw/rpd_sample.csv") |>
    open_dataset(format = "csv") |>
    to_duckdb(con) |>
    mutate(
        female = as.numeric(sexo == 2),

        nss = as.character(nss)
    ) |>
    select(
        CVE_NSS = nss,
        female,
        age = edad_final # Age at last cotiza run. Currently December 2024.
    ) |>
    compute()

glimpse(features)

ymd(19630927) + years(61)

#---- Final sample ------------------------------------------------------------

# Unemployment events during 2020.
unemployment_feat <- unemployment |>
    collect() |>
 #   filter(got_unemployed) |>
    mutate(
        times_unemployed = n_distinct(unemployment_date, na.rm = T),
        .by = CVE_NSS
    ) |>
    inner_join(
        pre_covid_features,
        by = join_by(CVE_NSS),
        relationship = "many-to-many",
        copy = T
    ) |>
    # left_join(rpd,
    #     by = join_by(CVE_NSS),
    #     copy = T,
    #     relationship = "many-to-one"
    # ) |>

    left_join(
        features,
        by = join_by(CVE_NSS),
        copy = T,
        relationship = "many-to-one"
    ) |>
    drop_na(got_unemployed) |>
    distinct() |>
    mutate(
        unemp_worker_id = row_number()
    )

    # mutate(
    #     rpd = (sum(!is.na(FEC_MOVTO), na.rm = T) > 0) |> as.numeric(),
    #     .by = CVE_NSS
    # ) |>
    # mutate(treatment = factor(
    #     rpd,
    #     levels = c(0, 1),
    #     labels = c("Control", "Tratamiento")
    # ))

glimpse(unemployment_feat)


full_sample <- unemployment_feat |>
    inner_join(
        outcomes,
        by = join_by(CVE_NSS),
        relationship = "many-to-many",
        copy = T
    )


full_sample |> glimpse()

final_sample <- unemployed |>
    mutate(
        FEC_MOVTO = as_date(FEC_MOVTO),
        unemployment_date = as_date(unemployment_date),
        waiting_days = as.numeric(FEC_MOVTO - unemployment_date)
    ) |>
    filter(waiting_days >= 45 | is.na(waiting_days)) |>
    filter(
        unemployment_date == min(unemployment_date),
        .by = CVE_NSS
    ) |>
    inner_join(
        outcomes,
        by = join_by(CVE_NSS),
        relationship = "one-to-many",
        copy = T
    )

#---- Verify ------------------------------------------------------------------

final_sample |> pull(waiting_days) |> summary()

#---- Save data --------------------------------------------------------------

save(unemployed, file = here("data/02_temp/unemployed.RData"))

save(final_sample, file = here("data/02_temp/final_sample.RData"))

dbDisconnect(con)

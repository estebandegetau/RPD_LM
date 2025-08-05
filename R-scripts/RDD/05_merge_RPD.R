#### RPD #######################################################################
#' 
#' @name 09_merge_RPD.R
#' 
#' @description Find users of RPD withn the RD sample
#' 
#' @author Esteban Degetau
#' 
#' @created 2025-03-21
#' 
#### Merge RPD ##################################################################

rm(list = ls())
gc()

# Libraries ---------------------------------------------------------------------

pacman::p_load(tidyverse, arrow, here, duckdb, labelled)

# Data --------------------------------------------------------------------------

con <- DBI::dbConnect(duckdb::duckdb(),
                      dbdir = "temp1.duckdb")

DBI::dbExecute(con, "PRAGMA max_temp_directory_size='100GB';")

withdraws <- here("data/raw/copia_retiros.csv") |>
    open_dataset(format = "csv") |>
    to_duckdb(con)

unemployment <- here("data/working/rd_unemployment") |>
    open_dataset(format = "parquet") 

# Explore -----------------------------------------------------------------------

# glimpse(withdraws)

# withdraws |> pull(MONTO_RET) |>
#     summary()

# withdraws |> pull(SEM_COT_97) |>
#     summary()

# (withdraws |> pull(DIAS_RET) / 7) |>
#     summary()

# withdraws |>
#     group_by(CVE_NSS) |>
#     summarise(retiros = n()) |>
#     ungroup() |>
#     group_by(retiros) |>
#     summarise(n = n()) |>
#     ungroup() |>
#     ggplot(aes(x = retiros, y = n)) +
#     geom_point() 



# withdraws |>
#     inner_join(
#         rd_sample |> distinct(nss),
#         by = c("CVE_NSS" = "nss")
#     ) |>
#     group_by(SEM_COT_97) |>
#     summarise(n = n()) |>
#     ungroup() |>
#     arrange(SEM_COT_97) |>
#     mutate(n = n / sum(n),
#         n = cumsum(n)) |>
#     collect() |>
#     ggplot(aes(x = SEM_COT_97, y = n)) +
#     geom_step() +
#     geom_vline(xintercept = 104, linetype = "dashed") +
#     scale_x_log10() 

# Pull inflation data -----------------------------------------------------------

pacman::p_load(httr, jsonlite, rjson)

#Llamado al API
url <-"https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml/INDICATOR/910392/es/0700/false/BIE/2.0/ac23a0f8-1207-f404-3f62-9d669769b298?type=json"
respuesta<-GET(url)
datosGenerales<-content(respuesta,"text")
flujoDatos<-paste(datosGenerales,collapse = " ")

#Obtención de la lista de observaciones 
flujoDatos<-fromJSON(flujoDatos)
flujoDatos<-flujoDatos $Series
flujoDatos<-flujoDatos[[1]] $OBSERVATIONS



inpc <- tibble(
  fecha = map_chr(flujoDatos, "TIME_PERIOD") |> ym(),
  inpc = map_chr(flujoDatos, "OBS_VALUE") |> as.numeric()
)

inpc_base <- inpc |>
  filter(fecha == ym("2024-12")) |>
  pull(inpc)

inpc_earliest <- inpc |>
  filter(fecha == min(fecha)) |>
  pull(inpc)

inpc <- inpc |>
    mutate(
        deflactor = inpc / inpc_base
    )

# Clean witdrawls ---------------------------------------------------------------

withdraws_names <- withdraws |>
    janitor::clean_names() |>
    select(
        cve_nss, 
        rpd_date = fec_movto,
        contributed_weeks_rpd = sem_cot_97,
        amount_withdrawn = monto_ret,
        days_withdrawn = dias_ret,
        employer_rpd = cve_patron,
        modality_rpd = cve_modalidad,
        balance_rcv = saldo_rcv,
        wage_rpd = coc_o_sal_prom
    ) |>
    compute() 


clean_dates <- withdraws_names |>
    distinct(rpd_date) |>
    collect() |>
    mutate(
        clean_rpd_date = str_sub(rpd_date, 1, 9) |>
            dmy() |>
            as_date(),
        fecha = floor_date(clean_rpd_date, unit = "month")
    ) |>
    rename(raw_rpd_date = rpd_date) |>
    left_join(inpc) |>
    drop_na(inpc)


withdraws_clean <- withdraws_names |>
    left_join(
        clean_dates,
        by = c("rpd_date" = "raw_rpd_date"),
        copy = T
    ) |>
    filter(!is.na(inpc)) |>
    mutate(
        across(
            c(amount_withdrawn, balance_rcv, wage_rpd),
            ~ . / deflactor
        )
    ) |>
    mutate(rpd_date = clean_rpd_date) |>
    select(!c(clean_rpd_date, deflactor, fecha, inpc)) |>
    compute()

withdraws_clean |> glimpse()


# Merge -------------------------------------------------------------------------

unemp_rpd <- unemployment |> 
    filter(meets_unemp_days_criterion == 1) |>
    to_duckdb(con) |>
    left_join(
        withdraws_clean,
        by = c("CVE_NSS" = "cve_nss"),
        relationship = "many-to-many"
    ) |>
    compute()

unemp_rpd |> pull(1) |> length()
unemployment |> pull(1, as_vector = 1) |> length()

unemp_length_start <- unemployment |> pull(1, as_vector = T) |> length()

# Clean -------------------------------------------------------------------------

unemp_rpd_clean <- unemp_rpd |>
    mutate(
        days_to_take_up = as.numeric(rpd_date - unemployment_date),
        account_days_rpd = as.numeric(rpd_date - began_working)
    ) |>
    filter(amount_withdrawn > 0) |>
    filter(days_to_take_up >= 46) |>
    group_by(CVE_NSS, rpd_date ) |>
    slice_min(
        days_to_take_up,
        with_ties = F
        ) |>
    ungroup() |>
    filter(unemployment_days >= days_to_take_up) |>
    group_by(CVE_NSS, unemployment_date) |>
    slice_min(
        days_to_take_up,
        with_ties = F
        ) |>
    ungroup() |>
    compute()



unemp_rpd_clean |>
    arrange(CVE_NSS, unemployment_date, rpd_date) |>
    relocate(rpd_date, unemployment_days, days_to_take_up, .after = unemployment_date) |>
    head(500) |>
    collect()
   

unemp_rpd_clean |>
    ggplot(aes(x = days_to_take_up)) +
    stat_ecdf()

unemp_rpd_clean |> pull(1) |> length()

unemp_rpd_clean |>
    mutate(
        diff = unemployment_days - days_to_take_up
    ) |>
    ggplot(aes(x = diff)) +
    stat_ecdf()

unemp_rpd_clean |>
    ggplot(aes(x = age)) +
    stat_ecdf()

unemp_rpd_clean |> pull(1) |> length()
unemp_rpd_clean |> pull(CVE_NSS) |> unique() |> length()
unemployment |> pull(1, as_vector = 1) |> length()




# Bind all workers -------------------------------------------------------------

unemp_rpd_clean <- unemp_rpd_clean |>
    select(
        CVE_NSS,
        unemployment_date,
        rpd_date,
        days_to_take_up,
        account_days_rpd,
        contributed_weeks_rpd,
        amount_withdrawn,
        days_withdrawn,
        employer_rpd,
        modality_rpd,
        balance_rcv,
        wage_rpd
    ) |>
    compute() 


unemployment <- unemployment |>
  to_duckdb(con)

unemployment <- unemployment |>
  left_join(unemp_rpd_clean, by = c("CVE_NSS", "unemployment_date")) |>
  arrange(CVE_NSS, unemployment_date) |>
  compute()

glimpse(unemployment)

unemployment |>
    head(500) 

unemp_length_final <- unemployment |> pull(1) |> length()

stopifnot(unemp_length_start == unemp_length_final)

# Label variables ---------------------------------------------------------------

description <- here("data/working/rd_unemployment_dictionary.csv") |>
    read_csv() 

var_labels <- setNames(as.list(description$label), description$variable)

final_labs <- unemployment |>
    head() |>
    collect() |>
    set_variable_labels(.labels = var_labels, .strict = F) |>
    set_variable_labels(
        rpd_date = "Take up date",
        days_to_take_up = "Days to take up",
        account_days_rpd = "Days since account opened at take up date",
        contributed_weeks_rpd = "Contributed weeks at take up date",
        amount_withdrawn = "Amount withdrawn (2024 MXN)",
        days_withdrawn = "Days withdrawn",
        employer_rpd = "Employer (RPD)",
        modality_rpd = "Modality (RPD)",
        balance_rcv = "Pension account balance (RPD, 2024 MXN)",
        wage_rpd = "Wage (RPD, 2024 MXN)"
    )

final_labs |>
    generate_dictionary() |>
    write_csv(here("data/working/rd_unemployment_rpd_dictionary.csv"))

# Save --------------------------------------------------------------------------

unemployment <- unemployment |>
  to_arrow()

unemployment |>
    write_dataset(
        here("data/working/rd_unemployment_rpd"),
        format = "parquet",
        partitioning = "partition"
    )

labs <- get_variable_labels(final_labs)

withdraws_clean |>
    head() |>
    collect() |>
    set_variable_labels(labs, .strict = F) |>
    generate_dictionary() |>
    write_csv(here("data/temp/withdraws_clean_dictionary.csv"))

withdraws_clean <- withdraws_clean |>
    to_arrow() 

withdraws_clean |>
    write_feather(here("data/temp/withdraws_clean.feather"))

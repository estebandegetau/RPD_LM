#### SD #######################################################################
#' 
#' @name 03_deflate_wages.R
#' 
#' @description Deflate wages to 2023 MXN using the Consumer Price Index (CPI)
#' 
#' @author Esteban Degetau
#' 
#' @created 2024-09-04
#' 
#### Deflate wages #############################################################

rm(list = ls())
gc() 

#---- Setup --------------------------------------------------------------------

test <- 1
n    <- 1000

# Test whether .sourced_in_run is defined
if (exists(".sourced_in_run")) {
  test <- 0
}

#---- Libraries ----------------------------------------------------------------

pacman::p_load(tidyverse, here, lubridate, siebanxicor, arrow, duckdb)

#---- Load data ----------------------------------------------------------------


con <- DBI::dbConnect(duckdb::duckdb(),
                      dbdir = "temp1.duckdb")

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
  open_dataset(
    format = "parquet",
    schema = my_schema_ci
  ) |>
  to_duckdb(con)

#---- Subset -------------------------------------------------------------------

if (test) {
  set.seed(20240411)
  
  sample <- sample |>
    collect() |>
    sample_n(n) |>
    to_duckdb(con)
  
  ci <- ci |>
  inner_join(
    sample |> distinct(nss),
    by = join_by(CVE_NSS == nss),
    copy = T
  ) 


}

#Pull inflation data -----------------------------------------------------------

# setToken("574b5dedd17af5b0866d78637c2213ec3a6d8272babe9e41964f6b75ecb13903")

# banxico_ids <- c(
#   "SP1"     # INPC (Índice)
# )

# banxico_metadata <- getSeriesMetadata(banxico_ids)

# today <- today() |> as.character()

# banxico <- getSeriesData(
#   series = banxico_ids
#   # "1990-01-01",
#   # today
# )

# banxico_tb <- tibble(
#   serie = names(banxico),
#   fecha = map(banxico, 1),
#   valor = map(banxico, 2)
# )

# banxico_clean <- banxico_tb |>
#   unnest(!serie) |>
#   arrange(serie, desc(fecha)) |>
#   # Replace serie name with descriptions above
#   mutate(
#     serie = case_when(
#       serie == "SF29652" ~ "base_monetaria",
#       serie == "SP30603" ~ "precios_importaciones",
#       serie == "SG1" ~ "gasto_publico",
#       serie == "SG193" ~ "deuda_publica",
#       serie == "SF283" ~ "tiie_28",
#       serie == "SF17801" ~ "tiie_91",
#       serie == "SF221962" ~ "tiie_182",
#       serie == "SF17906" ~ "tipo_cambio",
#       serie == "SP1" ~ "infl_gen",
#       serie == "SP74625" ~ "infl_sub",
#       serie == "SL11439" ~ "wage_industry",
#       serie == "SP6" ~ "inpp",
#       serie == "SL11298" ~ "min_wage",
#       serie == "SR16734" ~ "igae",
#       serie == "SL1" ~ "tasa_desocupacion",
#       T ~ serie
#     ),
#     valor = case_when(
#       serie == "inpp" ~ ((valor / lead(valor, n = 12)) - 1) * 100,
#       T ~ valor
#     ),
#     valor = case_when(
#       serie == "tasa_desocupacion" & fecha >= ymd(20050101) ~ NA,
#       serie == "inpp" & fecha >= ymd(20100101) ~ NA,
#       T ~ valor
#     ),
#     fuente = "Banco de México"
#   ) |>
#   drop_na()

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

flujoDatos |> glimpse()


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

# Deflate wages ----------------------------------------------------------------

dates <- ci |>
  to_arrow() |>
  distinct(FEC_MOV_INI) |>
  collect() |>
  mutate(
    fecha = floor_date(FEC_MOV_INI, unit = "month")
  ) |>
  left_join(inpc) |>
  select(!fecha) |>
  to_duckdb(con)
  

ci_work <- ci |>
  left_join(dates) |> 
  compute()

ci_work <- ci_work |>
  mutate(
    inpc = case_when(
      is.na(inpc) ~ inpc_earliest,
      T ~ inpc
    ),
    SAL_BAS = SAL_BAS * (inpc_base / inpc),
    SAL_BAS = round(SAL_BAS, 2)
  ) |>
  select(!inpc) |>
  compute()

#---- Validate -----------------------------------------------------------------

errors <- ci_work |>
  filter(is.na(SAL_BAS)) |>
  collect()

ci_work |>
  pull(FEC_MOV_FIN) |>
  summary()

if (nrow(errors) > 0) {
  
  ci_work |>
    filter(is.na(SAL_BAS)) |>
    print()
  
  stop("Problems with deflator")}

#---- Save ---------------------------------------------------------------------

ci_work <- ci_work |>
  to_arrow() 
  # This CI has been deflated

ci_work |>
  write_dataset(
    path = here("data/working/rd_ci"),
    format = "parquet",
    partitioning = "partition" 
    )


DBI::dbDisconnect(con)

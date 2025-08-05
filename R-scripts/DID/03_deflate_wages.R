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

test <- 0
n    <- 1000

#---- Libraries ----------------------------------------------------------------

pacman::p_load(data.table, tidyverse, here, lubridate, siebanxicor, arrow)

#---- Load data ----------------------------------------------------------------


con <- DBI::dbConnect(duckdb::duckdb(),
                      dbdir = "temp1.duckdb")

interest_period <- interval(ymd("2020-01-01"), ymd("2020-12-31"))


last_queried <- file.info(here::here("data/raw/ci.csv")) |>
  pluck("mtime") |>
  as_date()


ci <- here::here("data/temp/tidy_ci.feather") |>
  open_dataset(format = "feather",
               partitioning = "CVE_NSS") |>
  to_duckdb(con) |>
  mutate(
    across(matches("FEC_|tidy"), as_date),
    across(matches("CVE_", as.character))
    ) |>
  compute()



 #---- Subset -------------------------------------------------------------------

if (test) {
  set.seed(20240411)
  
  sample_nss <- ci |>
    pull(CVE_NSS, as_vector = T) |>
    unique() |>
    sample(n) 
  
  ci <- ci |>
    filter(CVE_NSS %in% sample_nss) |>
    compute()
}

#---- Deflate wages ------------------------------------------------------------


setToken("574b5dedd17af5b0866d78637c2213ec3a6d8272babe9e41964f6b75ecb13903")

banxico_ids <- c(
  "SP1"     # INPC (Índice)
)

banxico_metadata <- getSeriesMetadata(banxico_ids)



today <- today() |> as.character()

banxico <- getSeriesData(
  series = banxico_ids
  # "1990-01-01",
  # today
)


banxico_tb <- tibble(
  serie = names(banxico),
  fecha = map(banxico, 1),
  valor = map(banxico, 2)
)



banxico_clean <- banxico_tb |>
  unnest(!serie) |>
  arrange(serie, desc(fecha)) |>
  # Replace serie name with descriptions above
  mutate(
    serie = case_when(
      serie == "SF29652" ~ "base_monetaria",
      serie == "SP30603" ~ "precios_importaciones",
      serie == "SG1" ~ "gasto_publico",
      serie == "SG193" ~ "deuda_publica",
      serie == "SF283" ~ "tiie_28",
      serie == "SF17801" ~ "tiie_91",
      serie == "SF221962" ~ "tiie_182",
      serie == "SF17906" ~ "tipo_cambio",
      serie == "SP1" ~ "infl_gen",
      serie == "SP74625" ~ "infl_sub",
      serie == "SL11439" ~ "wage_industry",
      serie == "SP6" ~ "inpp",
      serie == "SL11298" ~ "min_wage",
      serie == "SR16734" ~ "igae",
      serie == "SL1" ~ "tasa_desocupacion",
      T ~ serie
    ),
    valor = case_when(
      serie == "inpp" ~ ((valor / lead(valor, n = 12)) - 1) * 100,
      T ~ valor
    ),
    valor = case_when(
      serie == "tasa_desocupacion" & fecha >= ymd(20050101) ~ NA,
      serie == "inpp" & fecha >= ymd(20100101) ~ NA,
      T ~ valor
    ),
    fuente = "Banco de México"
  ) |>
  drop_na()



banxico_clean |>
  drop_na() |>
  group_by(serie) |>
  summarise(
    min = min(fecha),
    max = max(fecha))

inpc <- banxico_clean |>
  select(fecha, inpc = valor)

inpc_base <- inpc |>
  filter(fecha == ym("2024-12")) |>
  pull(inpc)

inpc_earliest <- inpc |>
  filter(fecha == min(fecha)) |>
  pull(inpc)

ci_work <- ci |>
  mutate(
    fecha = floor_date(FEC_MOV_INI, unit = "month")
  ) |>
  compute() |>
  left_join(inpc, copy = T) |>
  mutate(
    inpc = case_when(
      is.na(inpc) ~ inpc_earliest,
      T ~ inpc
    ),
    SAL_BAS = SAL_BAS * (inpc_base / inpc)
  ) |>
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

ci <- ci_work |>
  select(1:8) |>
  to_arrow()

if (!test) {
      # This CI has been deflated
      write_feather(ci, here("data/working/ci_final.feather"))
}

DBI::dbDisconnect(con)

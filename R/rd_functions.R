
make_density_plot <- function(data) {
  rdd_object <- rddensity::rddensity(
    X = data$running,
    c = 0
  )

  left <- min(data$running)
  right <- max(data$running)

  density_plot <- rddensity::rdplotdensity(
    rdd_object,
    X = data$running,
    histFillCol = "lightgray",
    histFillShade = 0.8,
    histBreaks = seq(-250, 200, 7)
  )

  density_plot$Estplot +
    labs(
      x = "Contribution days to eligibility"
    ) +
    theme_minimal() +
    theme(legend.position = "none") +
    coord_cartesian(xlim = c(-200, 200))
}

group_outcomes <- function(data) {
  dict <- labelled::generate_dictionary(data)

  var_groups <- dict |>
    dplyr::mutate(
      group = case_when(
        str_detect(variable, "no_curp|began_working|unemployment_date|days_since|covid|birth_date|female|age") ~ "Covariates",
        str_detect(variable, "prev_job") ~ "Previous job",
        str_detect(variable, "survival_|duration_") ~ "Survival",
        str_detect(variable, "next_job") ~ "Next job",
        str_detect(variable, "year_|_total") ~ "Medium term",
        str_detect(variable, "take_up_") ~ "Take up",
        str_detect(variable, "take_up|amount_withdrawn|contributed_weeks|days_withdrawn") ~ "RPD"
      )
    ) |>
    tidyr::drop_na(group) 

  return(var_groups)
}


extract_next_job_balance <- function(data) {

  var_groups <- group_outcomes(data)

  data |>
    drop_na(next_job_duration) |>
    select(CVE_NSS, running, prev_job_av_earnings, take_up_12) |>
    mutate(
      across(where(is.Date), decimal_date),
      instrument = take_up_12
      ) |>
    select(-take_up_12) |>
    pivot_longer(
      !matches("CVE|_id|running|instrument")
    ) |>
    nest(data = c(CVE_NSS, running, value, instrument)) |>
    mutate(
      group = "Next job",
    ) 
}




# prepare_rd_data <- function(data) {
  
#   var_groups <- group_outcomes(data)


#   data |>
#     select(CVE_NSS, running, all_of(var_groups$variable)) |>
#     mutate(across(where(is.Date), decimal_date)) |>
#     pivot_longer(
#       !matches("CVE|_id|running")
#     ) |>
#     nest(data = c(CVE_NSS, running, value)) |>
#     left_join(var_groups, by = c("name" = "variable")) |>

# }


prepare_rd_data <- function(data) {

  var_groups <- group_outcomes(data)  

  to_compute_fuzzy <- data |>
    select(CVE_NSS, running, all_of(var_groups$variable)) |>
    mutate(
      across(where(is.Date), decimal_date),
      instrument = take_up_12
      ) |>
    pivot_longer(
      !matches("CVE|_id|running|instrument")
    ) |>
    nest(data = c(CVE_NSS, running, value, instrument)) |>
    left_join(var_groups, by = c("name" = "variable"))
}


my_rd <- function(data, ...) {
  y <- {{ data }} |> pull("value")
  x <- {{ data }} |> pull("running")


  if (is.Date(y)) y <- decimal_date(y)

  output <- rdrobust(
    y = y,
    x = x,
    c = 0,
    masspoints = "off",
    vce = "hc0",
    level = 95,
    ...
  )


  return(list(output = output))
}

my_rd_plot <- function(data, ...) {
  y <- {{ data }} |> pull("value")
  x <- {{ data }} |> pull("running")

  if (is.Date(y)) y <- decimal_date(y)

  plot <- rdplot(
    y = y,
    x = x,
    c = 0,
    masspoints = "off",
    binselect = "es",
    nbins = c(26, 26),
    hide = T,
    # ci = 95,
    # shade = T,
    kernel = "tri",
    ...
  )

  return(list(plot = plot))
}



my_fuzzy_rd <- function(data, ...) {
  y <- {{ data }} |> pull("value")
  x <- {{ data }} |> pull("running")
  z <- {{ data }} |> pull("instrument")


  if (is.Date(y)) y <- decimal_date(y)

  output <- rdrobust(
    y = y,
    x = x,
    c = 0,
    fuzzy = z,
    masspoints = "off",
    vce = "hc0",
    level = 95,
    ...
  )

  return(list(output = output))
}


compute_rd_group <- function(data) {
  data |>
    mutate(
      rd = map(data, my_rd, .progress = T)
    ) |>
    select(-data)
}

compute_rd_plot <- function(data) {
  data |>
    mutate(
      rd = map(data, my_rd_plot, .progress = T)
    ) |>
    select(-data)
}

compute_fuzzy_rd_group <- function(data) {
  data |>
    mutate(
      rd = map(data, my_fuzzy_rd, .progress = T)
    ) |>
    select(-data)
}


label_plots <- function(plot, outcome) {
  p <- plot +
    labs(title = outcome, 
         x = "Contribution days to eligibility",
         y = "") +
    theme_minimal() 

  # q <- ggplot_build(p)
  # q$data[[5]]$fill <- "lightblue"
  # q$data[[5]]$alpha <- 0.3

  # p <- ggplot_gtable(q)

  return(p)

}

pluck_stats <- function(x) {
  x |>
    mutate(
      RPD = map_dbl(rd, ~ pluck(.x, "output", "coef", 3)) |>
        format(digits = 2, scientific = F) |>
        as.character(),
      SE = map_dbl(rd, ~ pluck(.x, "output", "se", 3)) |>
        format(digits = 2, scientific = F) |>
        as.character(),
      SE = str_c("(", SE, ")"),
      `Mean at cutoff - ineligible` = map_dbl(rd, ~ pluck(.x, "output", "tau_bc", 1)) |>
        format(digits = 2, big.mark = ",", scientific = F) |>
        as.character(),
      N = (map_dbl(rd, ~ pluck(.x, "output", "N", 2)) +
        map_dbl(rd, ~ pluck(.x, "output", "N", 1))) |>
        format(big.mark = ",", scientific = F) |>
        as.character()
      
    )
  
}



tidy.rdrobust <- function(model, ...) {
  ret <- data.frame(
    term = row.names(model$coef),
    estimate = model$coef[, 1],
    std.error = model$se[, 1],
    p.value = model$pv[, 1]
  ) |>
    filter(term == "Robust")
  row.names(ret) <- NULL
  ret$term[1] <- "RPD"
  ret
}


glance.rdrobust <- function(model, ...) {
  ret <- tibble(
    "Mean at cutoff - ineligible" = pluck(model, "tau_bc", 1) |>
      format(digits = 3, big.mark = ",", scientific = F) |>
      as.character(),
    Observations = (pluck(model, "N", 2) + pluck(model, "N", 1)) |>
      format(big.mark = ",", scientific = F) |>
      as.character()
    
  )
  ret
}


my_modelsummary <- function(x, digits = 2) {
  names <- x$label
  row.names(names) <- NULL
  models <- x$rd |> map("output")

  named_list <- as.list(setNames(models, names))

  y <- modelsummary::modelsummary(
    named_list,
    escape = FALSE,
    fmt = digits,
    output = "kableExtra"
  )
  y
}

split_data <- function(path, split_by) {
  load(path)

  dict <- labelled::generate_dictionary(rpd)

  outcomes <- dict |>
    filter(variable %in% c(
      "take_up_12",
      "survival_3",
      "duration_36",
      "next_job_duration",
      "next_job_av_earnings",
      "earnings_total",
      "months_worked_total",
      "av_earnings_total"
    )) |>
    select(variable, label)


  split <- rpd |>
    mutate(
      split = eval(split_by),
    ) |>
    select(CVE_NSS, running, split, all_of(outcomes$variable)) |>
    mutate(
      across(where(is.Date), decimal_date),
      instrument = take_up_12
      ) |>
    pivot_longer(
      !matches("CVE|_id|running|split|instrument")
    ) |>
    nest(data = c(CVE_NSS, running, value, instrument)) |>
    left_join(outcomes, by = c("name" = "variable"))
}
map_rd <- function(split_data, ...) {
  first_stage <- split_data |>
    group_by(split) |>
    slice(1) |>
    ungroup() |>
    mutate(
      data = map(data, ~ .x |>
        select(!value) |>
        rename(value = instrument)),
      rd = map(data, my_rd, .progress = T),
      name = "take_up_12",
      label = "Take up (First stage)",
    ) |>
    select(-data)

  iv <- split_data |>
    filter(!name == "take_up_12") |>
    mutate(
      rd = map(data, my_fuzzy_rd, .progress = T, ...),
    ) |>
    select(-data) 
    
    bind_rows(first_stage, iv)

}

prepare_scores_data <- function(scores, data) {

  dict <- labelled::generate_dictionary(data)

  outcomes <- dict |>
    filter(variable %in% c(
      "take_up_12",
      "survival_3",
      "duration_36",
      "next_job_duration",
      "next_job_av_earnings",
      "earnings_total",
      "months_worked_total",
      "av_earnings_total"
    )) |>
    select(variable, label)


  con <- DBI::dbConnect(duckdb::duckdb(),
                          dbdir = "temp1.duckdb")

    var_groups <- group_outcomes(data)

    my_schema <- schema(
    CVE_NSS = double(),
    contract_start = int32(),
    score = double()
  )

    scores <- here("data/working/take_up_predictions/") |>
        open_dataset(
            format = "parquet",
            schema = my_schema
            ) |>
        to_duckdb(con) |>
        group_by(CVE_NSS) |>
        slice_max(contract_start, n = 1) |>
        select(CVE_NSS, score) |>
        collect() 

  data <- data |>
    select(CVE_NSS, running, all_of(outcomes$variable)) |>
    left_join(scores, by = "CVE_NSS") |>
    mutate(across(where(is.Date), decimal_date)) |>
    pivot_longer(
      !matches("CVE|_id|running|score")
    ) |>
    nest(data = c(CVE_NSS, running, value, score)) |>
    left_join(var_groups, by = c("name" = "variable"))

  return(data)

}

my_owate_rd <- function(data) {


   y <- {{ data }} |> pull("value")
  x <- {{ data }} |> pull("running")
  z <- {{ data }} |> pull("score")


  if (is.Date(y)) y <- decimal_date(y)

  output <- rdrobust(
    y = y,
    x = x,
    c = 0,
    weights = z,
    masspoints = "off"
  )


  return(list(output = output))

}

my_osate_rd <- function(data) {

  data <- data |>
    filter(between(score, 0.1, 0.9))


   y <- {{ data }} |> pull("value")
  x <- {{ data }} |> pull("running")



  if (is.Date(y)) y <- decimal_date(y)

  output <- rdrobust(
    y = y,
    x = x,
    c = 0,

    masspoints = "off"
  )


  return(list(output = output))

}


map_owate <- function(data) {
  data |>
    mutate(
      rd = map(data, my_owate_rd, .progress = T)
    ) |>
    select(-data)

}

map_osate <- function(data) {

  data |>
    mutate(
      rd = map(data, my_osate_rd, .progress = T),
    ) |>
    select(-data)

}
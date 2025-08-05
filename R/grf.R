



select_predictors <- function(data) {
    
    data |>
        select(
            CVE_NSS,
            contract_start,
            contract_duration,
            patron_id,
            CVE_MODALIDAD,
            SAL_BAS
        )
}


train_takeup_forest <- function(path) {

    data <- path |>
        open_dataset(format = "parquet") |>
        filter(partition == 1) |>
        compute()

    set.seed(20250406)

    # Train on the elegible workers
    eligible <- data |>
        filter(elig) |>
        compute()

    W <- eligible |> pull(take_up_12, as_vector = T)
    X <- eligible |>
        select_predictors() |>
        collect() |>
        as.matrix()

    rf <- regression_forest(X, W)

    return(rf)
}



predict_take_up <- function(rf, data) {

    X <- data |>
        group_by(partition) |>
        select_predictors() |>
        collect() |>
        as.matrix()

    p.hat <- predict(rf, X)

 
    predictions <- data |>
        collect() |>
        mutate(
            score = p.hat$predictions
        ) 

    predictions |>
        as_arrow_table()

}


compute_take_up <- function(rf, path) {

    data <- path |>
        open_dataset(format = "parquet") 

    if(0) {
        data <- data |>
            filter(partition == 1)

        worker <- data |>
            distinct(CVE_NSS) |>
            collect() |>
            sample_n(100)
        
        data <- data |>
            inner_join(worker, by = "CVE_NSS", copy = T) |>
            group_by(partition) |>
            compute()
    }
    
    partitions <- data |>
        distinct(partition) |>
        collect() |>
        mutate(
            data = map(
                partition,
                ~ data |> 
                    filter(partition == .x) |> 
                    compute()
            ),
            data = map(
                data, 
                ~ predict_take_up(rf, .x),
                .progress = T
        ))

    out_dir <- here("data/working/take_up_predictions/")

    out <- partitions |>
        mutate(
            partition_path = here(out_dir, str_c("partition=", partition)),
            create_out_dir = map(partition_path, ~ .x |> dir.create(showWarnings = F)),
            file_path = here(partition_path, str_c("part-0", ".parquet")),
    
    out_data = map2(file_path, data,
                    ~ write_parquet(.y, .x))
        )
    return(1)
}

plot_scores <- function(scores) {

    con <- DBI::dbConnect(duckdb::duckdb(),
                          dbdir = "temp1.duckdb")

    my_schema <- schema(
    CVE_NSS = double(),
    contract_start = int32(),
    elig = int32(),
    partition = int32(),
    score = double()
  )

    data <- here("data/working/take_up_predictions/") |>
        open_dataset(
            format = "parquet",
            schema = my_schema
            ) |>
        to_duckdb(con) |>
        group_by(CVE_NSS) |>
        slice_max(contract_start, n = 1) |>
        ungroup() |>
        select(score, elig) |>
        collect() 
    
    data |>
        ggplot(aes(x = score, fill = factor(elig))) +
        geom_histogram(bins = 50) +
        labs(
            title = "Distribution of predicted take-up scores",
            x = "Predicted take-up score",
            y = "Count",
            fill = "Eligible"
        ) +
        theme_minimal()
}




compute_osate <- function(data) {
    data <- data |>
        filter(between(score, 0.1, 0.9))

    rdrobust(
        y = data$earnings_year_1,
        x = data$running,
        fuzzy = data$take_up_12,
    ) 
    
}


get_last_queried <- function(query) {
    file.info(query) |>
        pluck("mtime") |>
        as_date()
}

compute_censored_casual_forest <- function(data, last_queried) {
    set.seed(20250419)

    data <- data |> filter(partition <= 5)
    
    Y <- data$unemployment_days |> as.integer()

    W <- as.numeric(data$elig)
    D <- as.numeric(data$unemployment_date + data$unemployment_days < last_queried)

    X <- data |>
        select(
            contribution_days,
            age,
            female,
            days_since_account_opened,
            prev_job_duration,
            last_sal_bas,
            prev_job_cum_earnings
        ) |>
        as.matrix()


    # hist(Y[D == 1], main = "Histogram of Y", xlab = "")
    # hist(Y[D == 0], col = adjustcolor("red", 0.5), add = TRUE)
    # legend("topright", c("Event", "Censored"), col = c("gray", adjustcolor("red", 0.5)), lwd = 4)
    # abline(v = 5 * 365, lty = 2)



    cs.forest <- causal_survival_forest(
        X = X,
        Y = Y,
        W = W,
        D = D,
        horizon = 5 * 365
    )
    return(cs.forest)
}



compute_blp <- function(forest, data) {

    X <- data |>
        select(
            contribution_days,
            age,
            female,
            days_since_account_opened,
            prev_job_duration,
            last_sal_bas,
            prev_job_cum_earnings
        ) |>
        as.matrix()

    best_linear_projection(forest, X)
    }



plot_rate <- function(forest, tau.hat) {
    rate <- rank_average_treatment_effect(forest, tau.hat)


    plot(rate, main = "TOC: By decreasing CATE estimates")
}

#IV Forest ---------------------------------------------------------------------

compute_instrumental_forest <- function(data) {
  set.seed(20250419)
  
  data <- data |> filter(partition <= 5)
  
  Y <- data$duration_36 |> as.integer()
  
  W <- as.numeric(data$elig)
  
  X <- data |>
    select(
      contribution_days,
      age,
      female,
      days_since_account_opened,
      prev_job_duration,
      last_sal_bas,
      prev_job_cum_earnings
    ) |>
    as.matrix()
  
  iv.pred <- instrumental_forest(
    X = X,
    Y = Y,
    W = data$take_up_12,
    Z = data$elig,
  )
  
  return(iv.pred)
  
  
}



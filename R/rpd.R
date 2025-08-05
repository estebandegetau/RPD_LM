plot_rpd_usage <- function(path) {
    withdraws <- here(path) |>
        open_dataset(format = "feather") 


    left <- withdraws |>
        summarise(
            .by = rpd_date,
            n = n()
        ) |>
        collect() |>
        arrange(rpd_date) |>
        mutate(n = cumsum(n)) |>
        ggplot(aes(x = rpd_date, y = n)) +
        geom_step() +
        labs(
            title = "Cumulative number of withdraws",
            x = "Date",
            y = "Number of withdraws"
        ) +
        scale_y_continuous(labels = scales::comma)

    right <- withdraws |>
        summarise(
            .by = rpd_date,
            n = sum(amount_withdrawn)
        ) |>
        collect() |>
        arrange(rpd_date) |>
        mutate(n = cumsum(n)) |>
        ggplot(aes(x = rpd_date, y = n)) +
        geom_step() +
        labs(
            title = "Cumulative amount withdrawn",
            x = "Date",
            y = "Amount withdrawn (Millions MXN 2024)"
        ) +
        scale_y_continuous(labels = scales::dollar_format(scale = 1e-6))


    ggarrange(left, right)
}



compute_distinct_users <- function(path) {
withdraws <- here(path) |>
        open_dataset(format = "feather") 

    withdraws |>
        distinct(cve_nss) |>
        collect() |>
        nrow()

}

compute_usage <- function(path) {
withdraws <- here(path) |>
        open_dataset(format = "feather") 

    withdraws |>
        select(cve_nss) |>
        collect() |>
        nrow()
}
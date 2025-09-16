


rdd_footnote <- "This table reports the estimated coefficient of interest from Equation 4.1, obtained using a local linear regression with a triangular kernel and optimal bandwidth selection. Bias-corrected point estimates and robust standard errors are computed following the procedure of Calonico, Cattaneo, and Titiunik (2014)."

rdd_footnote_beamer <- "Note: This table presents the estimated coefficient of interest from Equation 1, obtained using a local linear regression with a triangular kernel and optimal bandwidth selection. Bias-corrected estimates and robust standard errors are computed following the method of Calonico, Cattaneo, and Titiunik (2014)."


fuzzy_rd_footnote <- "This table reports the estimated coefficient of interest from Equation 4.2, where treatment is defined as program take-up 12 months after displacement. Estimates are obtained using a local linear regression with a triangular kernel and optimal bandwidth selection. Bias-corrected point estimates and robust standard errors are computed following the method of Cattaneo, Idrobo, and Titiunik (2024)."

fuzzy_rd_footnote_beamer <- "This table presents the estimated coefficient of interest from Equation 3, using program take-up at 12 months after displacement as the treatment variable. Estimates are obtained using a local linear regression with a triangular kernel, optimal bandwidth selection, and a linear polynomial. Bias-corrected point estimates and robust standard errors are computed following the method of Cattaneo, Idrobo, and Titiunik (2024)."

make_eligibility_plot <- function() {
    ggplot() +
        geom_rect(aes(
            xmin = 3,
            xmax = 10,
            ymin = 2,
            ymax = 10,
            fill = "A"
        ), alpha = 0.5) +
        geom_rect(aes(
            xmin = 5,
            xmax = 10,
            ymin = 0,
            ymax = 10,
            fill = "B"
        ), alpha = 0.5) +
        geom_polygon(aes(
            x = c(0, 10, 0),
            y = c(0, 10, 10),
            fill = "Not feasible"
        ), alpha = 1) +
        labs(
            x = "Years since entry to labor market",
            y = "Years contributed to SS",
            fill = "Eligibility to RPD"
        ) +
        scale_fill_manual(
            values = c(
                "A" = "lightgreen",
                "B" = "lightblue",
                "Dropped*" = "lightcoral",
                "Not feasible" = "lightgray"
            )
        ) +
        scale_x_continuous(
            breaks = seq(0, 8, 1)
        ) +
        coord_fixed(
            xlim = c(0, 7),
            ylim = c(0, 5)
        )
}
make_sample_selection_plot <- function() {
    ggplot() +
        geom_rect(aes(
            xmin = 3,
            xmax = 10,
            ymin = 2,
            ymax = 10,
            fill = "A"
        ), alpha = 0.5) +
        geom_rect(aes(
            xmin = 5,
            xmax = 10,
            ymin = 0,
            ymax = 10,
            fill = "B"
        ), alpha = 0.5) +
        geom_polygon(aes(
            x = c(0, 10, 0),
            y = c(0, 10, 10),
            fill = "Not feasible"
        ), alpha = 1) +

        # geom_point(
        #     data = interest_events,
        #     aes(
        #         x = days_since_account_opened / 365,
        #         y = contribution_days / 365,
        #     ),
        #     alpha = 0.2
        # ) +
        geom_rect(
            aes(
                xmin = 3,
                xmax = 4,
                ymin = 1,
                ymax = 3,
            ),
            fill = "transparent",
            color = "red"
        ) +
        labs(
            title = "Workers at the start of unemployment spell",
            subtitle = "selected sample in red rectangle",
            x = "Years since entry to labor market",
            y = "Years contributed to SS",
            fill = "Eligibility to RPD"
        ) +
        scale_fill_manual(
            values = c(
                "A" = "lightgreen",
                "B" = "lightblue",
                "Dropped*" = "lightcoral",
                "Not feasible" = "lightgray"
            )
        ) +
        scale_x_continuous(
            breaks = seq(0, 8, 1)
        ) +
        coord_fixed(
            xlim = c(0, 6),
            ylim = c(0, 5)
        )
}


append_all_covariates <- function(x, y) {
    # Covariates
    covariates <- x

    # Previous job
    prev_job <- y

    all_covs <- bind_rows(covariates, prev_job) |>
        filter(name != "covid") |>
        mutate(label = factor(
            name,
            levels = c(
                "female",
                "birth_date",
                "began_working",
                "unemployment_date",
                "age",
                "days_since_account_opened",
                "no_curp",
                "prev_job_duration",
                "prev_job_cum_earnings",
                "prev_job_av_earnings"
            ),
            labels = c(
                "Female",
                "Birth date",
                "Began working",
                "Unemployment date",
                "Age",
                "Days since account opened",
                "No CURP",
                "Prev job duration (weeks)",
                "Prev job total earnings",
                "Prev job av monthly earnings"
            )
        )) |>
        arrange(label)
}

make_covariates_plots <- function(x, nrow = 3, ncol = 2) {
    plots <- x |>
        filter(group == "Covariates") |>
        filter(name != "wage_rpd") |>
        filter(
            str_detect(name, "female|age|birth_date|began_working|no_curp|days_since")
        ) |>
        compute_rd_plot()

    labeled <- plots |>
        arrange(label) |>
        mutate(
            plot = map(rd, "plot"),
            plot = map(plot, "rdplot"),
            plot = map2(plot, label, label_plots)
        ) |>
        select(name, label, plot) 
       

    ggarrange(plotlist = labeled$plot, nrow = nrow, ncol = ncol)
}

make_previous_job_plots <- function(x) {
    plots <- x |>
        filter(str_detect(name, "prev_job|unemployment_date")) |>
        compute_rd_plot()

    labeled <- plots |>
        arrange(label) |>
        mutate(
            plot = map(rd, "plot"),
            plot = map(plot, "rdplot"),
            plot = map2(plot, label, label_plots)
        ) |>
        select(name, label, plot)

    ggarrange(plotlist = labeled$plot, nrow = 2, ncol = 2)
}



make_take_up_plots <- function(x, nrow = 3, ncol = 2) {
    plots <- x |>
        filter(str_detect(name, "take_up")) |>
        filter(!str_detect(name, "days")) |>
        mutate(
            months = str_extract(name, "\\d+") |> as.numeric()
        ) |>
        filter(
            months %in% c(2, 3, 6, 9, 12, 24)
        ) |>
        arrange(months) |>
        compute_rd_plot()
    
    labeled <- plots |>
        mutate(
            plot = map(rd, "plot"),
            plot = map(plot, "rdplot"),
            label = case_when(
                !is.na(months) ~ str_c("Take up - ", months, " months"),
                T ~ label
            ),
            plot = map2(plot, label, label_plots),
            plot = map(plot, ~ .x + coord_cartesian(ylim = c(0, 0.15)))
        ) |>
        select(name, label, plot)

    ggarrange(plotlist = labeled$plot, nrow = nrow, ncol = ncol)
}

make_take_up_path <- function(x) {
    res <- x |>
        filter(str_detect(name, "take_up")) |>
        filter(!str_detect(name, "censored|days")) |>
        mutate(
            months = str_extract(name, "\\d+") |> as.numeric(),
            rd = map(rd, "output"),
            point = map(rd, "coef"),
            ci = map(rd, "ci"),
            lower = map_dbl(ci, 3),
            upper = map_dbl(ci, 6),
            point = map_dbl(point, 3)
        )

    res |>
        ggplot(aes(months, point)) +
        geom_pointrange(aes(ymin = lower, ymax = upper)) +
        geom_hline(yintercept = 0) +
        scale_x_continuous(breaks = seq(from = 3, to = 48, by = 3)) +
        labs(
            x = "Months since displacement",
            y = "Take Up Rate"
        )
}


make_survival_plots <- function(x, nrow = 3, ncol = 2) {
    plots <- x |>
        filter(str_detect(name, "survival")) |>
        mutate(
            months = str_extract(name, "\\d+") |> as.numeric(),
            label = case_when(
                str_detect(name, "survival") ~ str_c("Survival - ", months, " months"),
                str_detect(name, "duration") ~ str_c("Duration - ", months, " months")
            )
        ) |>
        filter(months %in% c(2, 3, 6, 9, 12, 24)) |>
        arrange(months) |>
        compute_rd_plot()

    labeled <- plots |>
        mutate(
            plot = map(rd, "plot"),
            plot = map(plot, "rdplot"),
            plot = map2(plot, label, label_plots)
        ) |>
        select(plot, months, label)

    ggarrange(plotlist = labeled$plot, ncol = ncol, nrow = nrow) 
}

make_duration_plots <- function(x, nrow = 3, ncol = 2) {
    plots <- x |>
        filter(str_detect(name, "duration")) |>
        mutate(
            months = str_extract(name, "\\d+") |> as.numeric(),
            label = case_when(
                str_detect(name, "survival") ~ str_c("Survival - ", months, " months"),
                str_detect(name, "duration") ~ str_c("Duration - ", months, " months")
            )
        ) |>
        filter(months %in% c(2, 3, 6, 9, 12, 24)) |>
        arrange(months) |>
        compute_rd_plot()

    labeled <- plots |>
        mutate(
            plot = map(rd, "plot"),
            plot = map(plot, "rdplot"),
            plot = map2(plot, label, label_plots)
        ) |>
        select(plot, months, label)

    ggarrange(plotlist = labeled$plot, ncol = ncol, nrow = nrow)
}

make_survival_path <- function(x) {
    survival <- x

    res <- survival |>
        filter(str_detect(name, "survival")) |>
        mutate(
            months = str_extract(name, "\\d+") |> as.numeric(),
            rd = map(rd, "output"),
            point = map(rd, "coef"),
            ci = map(rd, "ci"),
            lower = map_dbl(ci, 3),
            upper = map_dbl(ci, 6),
            point = map_dbl(point, 3)
        )

    survival_path <- res |>
        ggplot(aes(months, point)) +
        geom_pointrange(aes(ymin = lower, ymax = upper)) +
        geom_hline(yintercept = 0) +
        scale_x_continuous(breaks = seq(from = 3, to = 48, by = 3)) +
        labs(
            x = "Months since displacement",
            y = "Survival Rate"
        )


    res <- survival |>
        filter(str_detect(name, "duration")) |>
        mutate(
            months = str_extract(name, "\\d+") |> as.numeric(),
            rd = map(rd, "output"),
            point = map(rd, "coef"),
            ci = map(rd, "ci"),
            lower = map_dbl(ci, 3),
            upper = map_dbl(ci, 6),
            point = map_dbl(point, 3)
            # ci = map(ci, as_tibble)
        )

    duration_path <- res |>
        ggplot(aes(months, point)) +
        geom_pointrange(aes(ymin = lower, ymax = upper)) +
        geom_hline(yintercept = 0) +
        scale_x_continuous(breaks = seq(from = 3, to = 48, by = 3)) +
        labs(
            x = "Months since displacement",
            y = "Censored duration (weeks)"
        )

    ggarrange(survival_path, duration_path, nrow = 2)
}



make_fuzzy_path <- function(x) {
    res <- x |>
        filter(str_detect(name, "survival")) |>
        mutate(
            months = str_extract(name, "\\d+") |> as.numeric(),
            rd = map(rd, "output"),
            point = map(rd, "coef"),
            ci = map(rd, "ci"),
            lower = map_dbl(ci, 3),
            upper = map_dbl(ci, 6),
            point = map_dbl(point, 3)
        )

    survival_path <- res |>
        ggplot(aes(months, point)) +
        geom_pointrange(aes(ymin = lower, ymax = upper)) +
        geom_hline(yintercept = 0) +
        scale_x_continuous(breaks = seq(from = 3, to = 48, by = 3)) +
        labs(
            x = "Months since displacement",
            y = "Survival Rate"
        )


    res <- x |>
        filter(str_detect(name, "duration")) |>
        mutate(
            months = str_extract(name, "\\d+") |> as.numeric(),
            rd = map(rd, "output"),
            point = map(rd, "coef"),
            ci = map(rd, "ci"),
            lower = map_dbl(ci, 3),
            upper = map_dbl(ci, 6),
            point = map_dbl(point, 3)
            # ci = map(ci, as_tibble)
        )

    duration_path <- res |>
        ggplot(aes(months, point)) +
        geom_pointrange(aes(ymin = lower, ymax = upper)) +
        geom_hline(yintercept = 0) +
        scale_x_continuous(breaks = seq(from = 3, to = 48, by = 3)) +
        labs(
            x = "Months since displacement",
            y = "Censored duration (weeks)"
        )

    ggarrange(survival_path, duration_path, nrow = 2)
}


my_medium_table <- function(x) {
    x |>
        mutate(label = unemp_year) |>
        my_modelsummary() |>
        kable_styling(
            latex_options = "scale_down",
        ) |>
        column_spec(1, width = "11em") |>
        column_spec(2:5, width = "6em")
}


clean_hate <- function(x) {
    x |>
        mutate(
            label = case_when(
                str_detect(name, "take_up")              ~ "Take up (First stage)",
                str_detect(name, "survival")             ~ "Survival - 3 months",
                str_detect(name, "duration_36")          ~ "Duration - 36 months (wks)",
                str_detect(name, "next_job_duration")    ~ "Job duration (wks)",
                str_detect(name, "next_job_av_earnings") ~ "Monthly Earnings",
                str_detect(name, "av_earnings_total")    ~ "Monthly Earnings - 3 years",
                str_detect(name, "earnings_total")       ~ "Total Earnings - 3 years",
                str_detect(name, "months_worked_total")  ~ "Months Worked - 3 years",
            )
        ) |>
        select(-name) |>
        drop_na(label)
}

print_hate <- function(x) {
    x |>
        my_modelsummary() |>
        kable_styling(
            latex_options = "scale_down",
            font_size = 8
        ) |>
        column_spec(1, width = "12em") |>
        column_spec(2:9, width = "4.5em")
}



load_rpd_data <- function(path) {
    load(path)
    data <- rpd
    return(data)
}


make_balance_table <- function(data) {
 
    data |>
        dplyr::select(
            elig,
            unemployment_date,
            take_up_12,
            survival_3,
            duration_6,
            duration_36,
            days_since_account_opened,
            contribution_days,
            prev_job_duration,
            last_sal_bas,
            female,
            age,
            no_curp
        ) |>
        dplyr::mutate(
            across(where(is.Date), decimal_date),
            elig = factor(elig, levels = c(T, F), labels = c("Eligible", "Ineligible")),
        ) |>
        gtsummary::tbl_summary(
            by = elig,
            missing = "no",
            statistic = all_continuous() ~ "{mean} ({sd})",
            label = list(
                take_up_12 ~ "Take up - 12 months",
                survival_3 ~ "Survival - 3 months",
                duration_6 ~ "Unemp. Dur. - Cens. 6 months",
                duration_36 ~ "Unemp. Dur. - Cens. 36 months",
                prev_job_duration ~ "Tenure (weeks)",
                last_sal_bas ~ "Daily wage (MXN 2024)",
                unemployment_date ~ "Displacement date",
                days_since_account_opened ~ "Days since entry to formal labor market",
                contribution_days ~ "Days contributed to SS",
                female = "Female",
                age = "Age at displacement date (years)",
                no_curp = "No CURP"
            )
        ) |>
        gtsummary::add_p() 
}

make_summary_statistics <- function(data) {
   # browser()
    var_groups <- group_outcomes(data)

    summary <- data |>
        mutate(across(where(is.Date), decimal_date)) |>
        select(any_of(var_groups$variable)) |>
        select(!starts_with("take_up")) |>
        pivot_longer(
            !matches("CVE|id|employer_rpd")
        ) |>
        summarise(
            .by = name,
            n = sum(!is.na(value)),
            Mean = mean(value, na.rm = T),
            Median = median(value, na.rm = T),
            Std.Dev. = sd(value, na.rm = T),
            Min = min(value, na.rm = T),
            Max = max(value, na.rm = T)
        )


    summary |>
        left_join(var_groups, by = c("name" = "variable")) |>
        drop_na(group) |>
        filter(!str_detect(name, "partition|ever_")) |>
        mutate(
            month = case_when(
                str_detect(label, "Survival|censored|take_up") ~ (str_extract(label, "\\d+") |> as.numeric())
            ),
            label = case_when(
                name == "birth_date" ~ "Birth date",
                T ~ label
            )
        ) |>
        filter(is.na(month) | month %in% c(3, 36)) |>
        filter(!(group == "Medium term" & str_detect(label, "year"))) |>
        select(label, where(is.numeric), group) |>
        select(!c(month)) |>
        rename("Variable" = label) |>
        group_by(group) |>
        arrange(Variable, .by_group = T) 
    #   gt() |>
    #   tab_footnote(
    #     md("*Note:* This table shows summary statistics of the variables used in the analysis. The observations included were the final sample of interest. "),
    #   ) |>
    #   fmt_number(
    #     decimals = 1,
    #     drop_trailing_zeros = T
    #   ) |>
    #   cols_width(
    #     where(is.numeric) ~ pct(11.6),
    #     everything() ~ pct(30)
    #   ) |>
    #   tab_options(
    #     quarto.disable_processing = T
    #   )
}
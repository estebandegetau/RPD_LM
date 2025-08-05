
# Gender
gender <- rpd |>
    mutate(split = female) |>
    select(CVE_NSS, running, split, all_of(var_groups$variable)) |>
    mutate(across(where(is.Date), decimal_date)) |>
    pivot_longer(
        !matches("CVE|_id|running|split")
    ) |>
    nest(data = c(CVE_NSS, running, value)) |>
    left_join(var_groups, by = c("name" = "variable"))

gender_results <- gender |>
    filter(str_detect(name, "survival|duration_")) |>
    mutate(
        rd = map(data, my_rd, .progress = T)
    ) |>
    select(-data)



save(gender_results, file = here("results/RD/het/survival_gender.RData"))
rm(gender, gender_results)

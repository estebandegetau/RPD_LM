#### RPD #######################################################################
#' 
#' @name 11_03_het_covid.R
#' 
#' @description Estimate heterogeneous treatment effects.
#' 
#' @author Esteban Degetau
#' 
#' @created 2025-03-17
#' 
#### Het Covid  ################################################################

# Setup --------------------------------------------------------------------

pacman::p_load(here)

source(here("R/RDD/09_rd_prep.R"))


# Split data 3 ways ----------------------------------------------------------
covid <- rpd |>
    mutate(
        split = case_when(
            unemployment_date > ymd("2020-03-01") ~ "Full exposure",
            unemployment_date <= ymd("2020-03-01") - years(1) ~ "No exposure",
            T ~ "Partial exposure"
        ),
        split = factor(split, levels = c("No exposure", "Partial exposure", "Full exposure"))
        
        ) |>
    select(CVE_NSS, running, split, all_of(var_groups$variable)) |>
    mutate(across(where(is.Date), decimal_date)) |>
    pivot_longer(
        !matches("CVE|_id|running|split")
    ) |>
    nest(data = c(CVE_NSS, running, value)) |>
    left_join(var_groups, by = c("name" = "variable"))

# Perform RD ---------------------------------------------------------------

covid_results <- covid |>
    filter(str_detect(name, "survival|duration_")) |>
    mutate(months = str_extract(name, "\\d+") |> as.numeric()) |>
    filter(months <= 12) |>
    mutate(
        rd = map(data, my_rd, .progress = T)
    ) |>
    select(-data)

res <- covid_results |>
  filter(str_detect(name, "survival")) |>
  mutate(
    rd = map(rd, "output"),
    point = map(rd, "coef"),
    ci = map(rd, "ci"),
    lower = map_dbl(ci, 3),
    upper = map_dbl(ci, 6),
    point = map_dbl(point, 3)
    ) 

survival_plot <- res |>
  ggplot(aes(months, point)) +
  geom_pointrange(
    aes(ymin = lower, ymax = upper, color = split),
    position = position_dodge(width = 0.5)
    ) +
  geom_hline(yintercept = 0) +
  scale_x_continuous(breaks = seq(from = 3, to = 48, by = 3)) +
  labs(x = "Months since displacement",
       y = "Survival Rate",
       color = ""
       )

ggsave(here("results/RD/het/survival_covid_3.png"),
       plot = survival_plot,
       width = 8, height = 5, dpi = 300)

res <- covid_results |>
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

duration_plot <- res |>
  ggplot(aes(months, point)) +
  geom_pointrange(
    aes(ymin = lower, ymax = upper, color = split),
    position = position_dodge(width = 0.5)
    ) +
  geom_hline(yintercept = 0) +
  scale_x_continuous(breaks = seq(from = 3, to = 48, by = 3)) +
  labs(
    x = "Months since displacement",
    y = "Censored duration (weeks)",
    color = ""
  )

ggsave(here("results/RD/het/duration_covid_3.png"),
       plot = duration_plot,
       width = 8, height = 5, dpi = 300)

# Split data 2 ways ----------------------------------------------------------

covid <- rpd |>
    mutate(
        split = covid
    ) |>
    select(CVE_NSS, running, split, all_of(var_groups$variable)) |>
    mutate(across(where(is.Date), decimal_date)) |>
    pivot_longer(
        !matches("CVE|_id|running|split")
    ) |>
    nest(data = c(CVE_NSS, running, value)) |>
    left_join(var_groups, by = c("name" = "variable"))

covid_results <- covid |>
    filter(str_detect(name, "survival|duration_")) |>
    mutate(months = str_extract(name, "\\d+") |> as.numeric()) |>
    filter(months <= 12) |>
    mutate(
        rd = map(data, my_rd, .progress = T)
    ) |>
    select(-data)

# Plots ------------------------------------------------------------------

res <- covid_results |>
  filter(str_detect(name, "survival")) |>
  mutate(
    split = factor(split, levels = c("0", "1"), labels = c("No exposure", "Full exposure")),
    months = str_extract(name, "\\d+") |> as.numeric(),
    rd = map(rd, "output"),
    point = map(rd, "coef"),
    ci = map(rd, "ci"),
    lower = map_dbl(ci, 3),
    upper = map_dbl(ci, 6),
    point = map_dbl(point, 3)
    )

survival_plot <- res |>
  ggplot(aes(months, point)) +
  geom_pointrange(
    aes(ymin = lower, ymax = upper, color = split),
    position = position_dodge(width = 0.5)
    ) +
  geom_hline(yintercept = 0) +
  scale_x_continuous(breaks = seq(from = 3, to = 48, by = 3)) +
  labs(x = "Months since displacement",
       y = "Survival Rate",
       color = ""
       )

ggsave(here("results/RD/het/survival_covid_2.png"),
       plot = survival_plot,
       width = 8, height = 5, dpi = 300)

res <- covid_results |>
  filter(str_detect(name, "duration")) |>
  mutate(
    split = factor(split, levels = c("0", "1"), labels = c("No exposure", "Full exposure")),
    months = str_extract(name, "\\d+") |> as.numeric(),
    rd = map(rd, "output"),
    point = map(rd, "coef"),
    ci = map(rd, "ci"),
    lower = map_dbl(ci, 3),
    upper = map_dbl(ci, 6),
    point = map_dbl(point, 3)
    )

duration_path <- res |>
  ggplot(aes(months, point)) +
  geom_pointrange(
    aes(ymin = lower, ymax = upper, color = split),
    position = position_dodge(width = 0.5)
    ) +
  geom_hline(yintercept = 0) +
  scale_x_continuous(breaks = seq(from = 3, to = 48, by = 3)) +
  labs(
    x = "Months since displacement",
    y = "Censored duration (weeks)",
    color = ""
  )

ggsave(here("results/RD/het/duration_covid_2.png"),
       plot = duration_path,
       width = 8, height = 5, dpi = 300)
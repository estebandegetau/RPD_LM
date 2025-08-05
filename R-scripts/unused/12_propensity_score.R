#### RPD #######################################################################
#' 
#' @name 10_propensity_score.R
#' 
#' @description What pre-displacement features predict the likelihood of 
#'              take up of the RPD?
#' 
#' @author Esteban Degetau
#' 
#' @created 2025-03-23
#' 
#### Propensity Score ###########################################################

rm(list = ls())
gc()

# Libraries --------------------------------------------------------------------

pacman::p_load(tidyverse, here, fixest, rdrobust)

# Load data --------------------------------------------------------------------

here("data/working/rd_prep.RData") |>
    load()

# Subset data ------------------------------------------------------------------

outcomes$take_up_36 |> mean()

outcomes <- outcomes |>
  mutate(
    elig = running > 0
  ) |>
  filter(contribution_days <= days_since_account_opened) |>
  filter(contribution_days < 3 * 365) |>
  filter(contribution_days > 365) |>
  filter(age < 65) |>
  filter(age >= 18) |>
  filter(last_modality == 10) |>
  # Remove top 1% of earners
  filter(prev_job_av_earnings < quantile(prev_job_av_earnings, 0.995, na.rm = T)) |>
  filter(av_earnings_total < quantile(av_earnings_total, 0.995, na.rm = T))

outcomes$take_up_36 |> mean()
outcomes$elig |> mean()

# Eligible sample --------------------------------------------------------------

pre_displacement <- outcomes |>
    select(take_up_12,
    !c(
        CVE_NSS,
        unemployment_days,
        running,
        matches(
            "_id|rpd|interest|withdrawn|balance|take_up|duration_|survival|next_job|_year_|_total"),
        meets_unemp_days_criterion,
        meets_account_days_criterion,
        ever_got_unemployed,
        partition,
        last_modality,
        birth_date,
        unemployment_date,
        contribution_days
    )) |>
  mutate(across(where(is.Date), decimal_date))

pre_displacement |> glimpse()


eligible <- pre_displacement |>
    filter(elig) |>
    select(!elig)

glimpse(eligible)

# Propensity Score ------------------------------------------------------------

propensity_fit <- glm(
        data = eligible,
        take_up_12 ~ ., family = binomial()
    )


propensity <- predict(propensity_fit, newdata = pre_displacement) 

rd_data <- cbind(outcomes, propensity) |>
    mutate(
        weights = exp(propensity) / (1 + exp(propensity))
    )

rd_data$weights |> summary()

# RDD -------------------------------------------------------------------------

# Weighted RDD
b <- rdrobust(
    y = rd_data$duration_36,
    x = rd_data$running,
    weights = rd_data$weights,
    c = 0,
    masspoints = "off"
)

b |>
    summary()

rdplot(
    y = rd_data$earnings_total,
    x = rd_data$running,
    weights = rd_data$weights,
    c = 0,
    masspoints = "off",
    nbins = c(20, 20),
    binselect = "es"
)

covs <- pre_displacement |>
    select(
        !c(take_up_12, elig, began_working)
    ) |>
    as.matrix()

# Best specification so far
fuzzy <- rdrobust(
    y = rd_data$duration_36,
    x = rd_data$running,
    fuzzy = rd_data$take_up_12,
    weights = rd_data$weights,
    covs = covs,
    c = 0,
    masspoints = "adjust"
)

summary(fuzzy)

bw_fit <- rdbwselect(
    y = rd_data$duration_36,
    x = rd_data$running,
    fuzzy = rd_data$take_up_12,
    weights = rd_data$weights,
    covs = covs,
    c = 0,
    masspoints = "adjust"
)

bw_fit |>
    summary()


bw_1 <- rdrobust(
    y = rd_data$duration_36,
    x = rd_data$running,
    fuzzy = rd_data$take_up_12,
    weights = rd_data$weights,
    covs = covs,
    c = 0,
    masspoints = "adjust",
    h = 100
)

bw_1 |>
    summary()

# No covid
fuzzy <- rdrobust(
    y = rd_data$duration_36,
    x = rd_data$running,
    fuzzy = rd_data$take_up_12,
    weights = rd_data$weights,
    subset = rd_data$covid == 0,
    covs = covs,
    c = 0,
    masspoints = "adjust"
)

summary(fuzzy)

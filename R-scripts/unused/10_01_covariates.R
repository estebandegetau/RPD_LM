#### RPD #######################################################################
#' 
#' @title 09_01_covariates.R
#' 
#' @description Produce tables and figures to show continuity around the cutoff
#' 
#' @author Esteban Degetau
#' 
#' @created 2025-03-29
#' 
#### Covariates ################################################################

# Setup ------------------------------------------------------------------------
pacman::p_load(here)

source(here("R/RDD/09_rd_prep.R"))

# Perform RD -------------------------------------------------------------------

# Covariates
covariates <- to_compute |>
  filter(group == "Covariates") |>
  mutate(
    rd = map(data, my_rd, .progress = T)
  ) 


# Previous job 
prev_job <- to_compute |>
  filter(group == "Previous job") |>
  mutate(
    rd = map(data, my_rd, .progress = T)
  )

# Plots ------------------------------------------------------------------------

covs_plots <- covariates |>
  mutate(
    plot = map(rd, "plot"),
    plot = map2(plot, label, label_plots)) |>
  select(name, label, plot)

prev_job_plots <- prev_job |>
  mutate(
    plot = map(rd, "plot"),
    plot = map2(plot, label, label_plots)
    ) |>
  select(name, label, plot)


# Tables ---------------------------------------------------------------------




covariates_table <- covariates |> 
  filter(name != "covid") |>
  mutate(
    label = factor(
      name,
      levels = c(
        "female",
        "birth_date",
        "began_working",
        "unemployment_date",
        "age",
        "days_since_account_opened",
        "no_curp"

      ),
      labels = c(
        "Female",
        "Birth date",
        "Began working",
        "Unemployment date",
        "Age",
        "Days since account opened",
        "No CURP"
      )
    )
  ) |>
  arrange(label) |>
  my_modelsummary()


prev_job_table <- prev_job |>
    mutate(
        label = factor(
            name,
            levels = c(

                "prev_job_duration",
                "prev_job_cum_earnings",
                "prev_job_av_earnings"
            ),
            labels = c(

                "Prev job duration (weeks)",
                "Prev job total earnings",
                "Prev job av monthly earnings"
            )
        )
    ) |>
    arrange(label) |>
    my_modelsummary()


    
# Created by use_targets().
# Follow the comments below to fill in this target script.
# Then follow the manual to check and run the pipeline:
#   https://books.ropensci.org/targets/walkthrough.html#inspect-the-pipeline

# Load packages required to define the pipeline:
pacman::p_load(targets, here, quarto, tidyverse, tarchetypes)
# library(tarchetypes) # Load other packages as needed.

# Set target options:
tar_option_set(
  packages = c(
    "tibble",
    "grf",
    "policytree",
    "rdrobust",
    "tidyverse",
    "here",
    "gt",
    "gtsummary",
    "fixest",
    "labelled",
    "rddensity",
    "modelsummary",
    "kableExtra",
    "arrow",
    "cardx",
    "duckdb",
    "ggpubr",
    "rdpower"
    )
    
    # Packages that your targets need for their tasks.
  # format = "qs", # Optionally set the default storage format. qs is fast.
  #
  # Pipelines that take a long time to run may benefit from
  # optional distributed computing. To use this capability
  # in tar_make(), supply a {crew} controller
  # as discussed at https://books.ropensci.org/targets/crew.html.
  # Choose a controller that suits your needs. For example, the following
  # sets a controller that scales up to a maximum of two workers
  # which run as local R processes. Each worker launches when there is work
  # to do and exits if 60 seconds pass with no tasks to run.
  #
  #   controller = crew::crew_controller_local(workers = 2, seconds_idle = 60)
  #
  # Alternatively, if you want workers to run on a high-performance computing
  # cluster, select a controller from the {crew.cluster} package.
  # For the cloud, see plugin packages like {crew.aws.batch}.
  # The following example is a controller for Sun Grid Engine (SGE).
  # 
  #   controller = crew.cluster::crew_controller_sge(
  #     # Number of workers that the pipeline can scale up to:
  #     workers = 10,
  #     # It is recommended to set an idle time so workers can shut themselves
  #     # down if they are not running tasks.
  #     seconds_idle = 120,
  #     # Many clusters install R as an environment module, and you can load it
  #     # with the script_lines argument. To select a specific version of R,
  #     # you may need to include a version string, e.g. "module load R/4.3.2".
  #     # Check with your system administrator if you are unsure.
  #     script_lines = "module load R"
  #   )
  #
  # Set other options as needed.
)

# Debug using browser() in your function and run the pipeline with:
# targets::tar_make(callr_function = NULL, use_crew = FALSE, as_job = FALSE)
# Run the R scripts in the R/ folder with your custom functions:
tar_source()
theme_set(theme_minimal())

tar_option_set(
  #  memory = "transient",
  # error = "null",
  garbage_collection = 2 # Can be an integer in version >= 1.8.9003
)



list(
  # Data -------------------------------------------------------------------------------
  tar_target(
    name = path,
    command = "data/working/rpd.RData",
    format = "file"
  ),
  tar_target(
    name = eligibility_plot,
    command = make_eligibility_plot()
  ),
  tar_target(
    name = rpd_data,
    command = load_rpd_data(path)
  ),
  tar_target(
    name = sample_selection_plot,
    command = make_sample_selection_plot()
  ),
  tar_target(
    name = balance_table,
    command = make_balance_table(rpd_data)
  ),
  tar_target(
    name = summary_table,
    command = make_summary_statistics(rpd_data)
  ),
  # RPD Data --------------------------------------------------------------------------
  tar_target(
    name = rpd_data_path,
    command = "data/temp/withdraws_clean.feather"
  ),
  tar_target(
    name = rpd_usage_plot,
    command = plot_rpd_usage(rpd_data_path)
  ),
  tar_target(
    name = rpd_users,
    command = compute_distinct_users(rpd_data_path)
  ),
  tar_target(
    name = rpd_usage,
    command = compute_usage(rpd_data_path)
  ),


  # Regression Discontinuity ----------------------------------------------------------
  tar_target(
    name = density_plot,
    command = make_density_plot(rpd_data)
  ),
  tar_target(
    name = rd_data,
    command = prepare_rd_data(rpd_data)
  ),
  tar_target(
    name = covariates,
    command = rd_data |>
      filter(group == "Covariates") |>
      filter(name != "covid") |> 
      filter(name != "wage_rpd") |>
      compute_rd_group()
  ),
  tar_target(
    name = prev_job,
    command = rd_data |>
      filter(group == "Previous job") |>
      compute_rd_group()
  ),
  tar_target(
    name = survival,
    command = rd_data |>
      filter(group == "Survival") |>
      compute_rd_group()
  ),
  tar_target(
    name = next_job_balance,
    command = extract_next_job_balance(rpd_data)
  ),
  tar_target(
    name = next_job,
    command = rd_data |>
      filter(group == "Next job") |>
      bind_rows(next_job_balance) |>
      compute_rd_group()
  ),
  tar_target(
    name = medium_term,
    command = rd_data |>
      filter(group == "Medium term") |>
      compute_rd_group()
  ),
  tar_target(
    name = take_up,
    command = rd_data |>
      filter(group == "Take up") |>
      compute_rd_group()
  ),
  # IV

  tar_target(
    name = survival_iv,
    command = rd_data |>
      filter(group == "Survival") |>
      compute_fuzzy_rd_group()
  ),
  tar_target(
    name = next_job_iv,
    command = rd_data |>
      filter(group == "Next job") |>
      bind_rows(next_job_balance) |>
      compute_fuzzy_rd_group()
  ),
  tar_target(
    name = medium_term_iv,
    command = rd_data |>
      filter(group == "Medium term") |>
      compute_fuzzy_rd_group()
  ),


  # Random Forest ----------------------------------------------------------------------
  # tar_target(
  #   name = pre_unemp_data_path,
  #   command = here::here("data/working/ci_pre_unemployment")
  # ),
  # tar_target(
  #   name = takeup_forest,
  #   command = train_takeup_forest(pre_unemp_data_path)
  # ),
  # tar_target(
  #   name = scores,
  #   command = compute_take_up(takeup_forest, pre_unemp_data_path)
  # ),
  # tar_target(
  #   name = histogram,
  #   command = plot_scores(scores)
  # ),
  # tar_target(
  #   name = scores_data,
  #   command = prepare_scores_data(scores, rpd_data)
  # ),
  # tar_target(
  #   name = owate_results,
  #   command = map_owate(scores_data)
  # ),
  # tar_target(
  #   name = osate_results,
  #   command = map_osate(scores_data)
  # ),
  # Heterogeneous treatment effects (Old school) ------------------------
  
  tar_target(
    name = income,
    command = split_data(path, split_by = expression(ntile(prev_job_av_earnings, 2))),
  ),
  tar_target(
    name = income_results,
    command = map_rd(income)
  ),
  tar_target(
    name = age,
    command = split_data(path, split_by = expression(ntile(age, 2))),
  ),
  tar_target(
    name = age_results,
    command = map_rd(age)
  ),
  tar_target(
    name = gender,
    command = split_data(path, split_by = expression(female)),
    ),
  tar_target(
    name = gender_results,
    command = map_rd(gender)
  ),
  tar_target(
    name = covid,
    command = split_data(path, split_by = expression(
      case_when(
        unemployment_date < ymd("2019-03-01") ~ "No Exposure",
        unemployment_date >= ymd("2020-03-01") ~ "Full Exposure",
        T ~ "Partial Exposure"
      )
    )),
  ),
  tar_target(
    name = covid_results,
    command = map_rd(covid)
  ),
  tar_target(
    name = entry_year,
    command = split_data(path, split_by = expression(year(began_working) >= 2014))
  ),
  tar_target(
    name = entry_year_results,
    command = map_rd(entry_year)
  ),
  # Results ----------------------------------------------------------
  tar_target(
    name = all_covariates,
    command = append_all_covariates(covariates, prev_job)
  ),
  tar_target(
    name = covariates_plots,
    command = make_covariates_plots(rd_data)
  ),
  tar_target(
    name = prev_job_plots,
    command = make_previous_job_plots(rd_data)
  ),
  tar_target(
    name = take_up_plots,
    command = make_take_up_plots(rd_data)
  ),
  tar_target(
    name = take_up_path,
    command = make_take_up_path(take_up)
  ),

  tar_target(
    name = survival_plots,
    command = make_survival_plots(rd_data)
  ),
  tar_target(
    name = duration_plots,
    command = make_duration_plots(rd_data)
  ),
  tar_target(
    name = survival_path,
    command = make_survival_path(survival)
  ),
  tar_target(
    name = fuzzy_path,
    command = make_fuzzy_path(survival_iv)
  ),
  # Power simulation ----------------------------------------------------------
  # tar_target(
  #   name = mde,
  #   command = compute_mde(rpd_data)
  # ),

  

  # Presentation ----
  tar_target(
    name = covariates_plots_long,
    command = make_covariates_plots(rd_data, nrow = 2, ncol = 3)
  ),
  tar_target(
    name = take_up_plots_long,
    command = make_take_up_plots(rd_data, nrow = 2, ncol = 3)
  ),
  tar_target(
    name = survival_plots_long,
    command = make_survival_plots(rd_data, nrow = 2, ncol = 3)
  ),
  tar_target(
    name = duration_plots_long,
    command = make_duration_plots(rd_data, nrow = 2, ncol = 3)
  ),
  tar_quarto(manuscript, quiet = F, execute = T, cache_refresh = T)
  # tar_quarto(
  #   name = presentation,
  #   path = "presentations/04_final.qmd",
  #   quiet = F,
  #   cache_refresh = T
  # )
)

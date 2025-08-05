pacman::p_load(here, taskscheduleR, lubridate)


run_project_on <- "18:00"


# Run project
taskscheduler_create(
    taskname = "runproject",
    rscript = "R/RDD/00_run.R",
    schedule = "ONCE",
    starttime = run_project_on,
    exec_path = here()
)


# taskscheduler_delete(taskname = "runproject")

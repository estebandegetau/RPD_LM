


rdd_footnote <- "This table reports the estimated coefficient of interest from Equation 4.1, obtained using a local linear regression with a triangular kernel and optimal bandwidth selection. Bias-corrected point estimates and robust standard errors are computed following the procedure of Calonico, Cattaneo, and Titiunik (2014)."

rdd_footnote_beamer <- "Note: This table presents the estimated coefficient of interest from Equation 1, obtained using a local linear regression with a triangular kernel and optimal bandwidth selection. Bias-corrected estimates and robust standard errors are computed following the method of Calonico, Cattaneo, and Titiunik (2014)."


fuzzy_rd_footnote <- "This table reports the estimated coefficient of interest from Equation 4.2, where treatment is defined as program take-up 12 months after displacement. Estimates are obtained using a local linear regression with a triangular kernel and optimal bandwidth selection. Bias-corrected point estimates and robust standard errors are computed following the method of Cattaneo, Idrobo, and Titiunik (2024)."

fuzzy_rd_footnote_beamer <- "This table presents the estimated coefficient of interest from Equation 3, using program take-up at 12 months after displacement as the treatment variable. Estimates are obtained using a local linear regression with a triangular kernel, optimal bandwidth selection, and a linear polynomial. Bias-corrected point estimates and robust standard errors are computed following the method of Cattaneo, Idrobo, and Titiunik (2024)."


# --- tinytable helpers for the working paper ---------------------------------------
#
# The WP renders through Typst (see wp.qmd), where kableExtra cannot be used: it
# selects its backend with knitr::is_latex_output(), gets FALSE, and emits HTML,
# which halts the render. These wrappers standardise the tinytable equivalents of
# the kable_styling()/column_spec()/footnote() chains they replace.

#' Table note, formatted as the WP's tables expect it
#'
#' Replaces `kableExtra::footnote(general = , general_title = "Note:", ...)`.
#' The markup is Typst: `_x_` is emphasis and `#text(size:)` sets the size, both
#' of which pass through tinytable untouched because it does not escape cells.
my_note <- function(...) {
    paste0("#text(size: 0.85em)[_Note:_ ", paste0(..., collapse = ""), "]")
}

#' Make data-derived cell contents safe for Typst
#'
#' tinytable deliberately does not escape cells -- that is what lets the notes
#' and group labels above carry Typst markup -- so anything coming out of the
#' data has to be neutralised by hand. `<` is the one that actually bites: Typst
#' reads `<0.001` as the opening of a label and the render dies with "unclosed
#' label". The rest are included because they would misfire the same way if they
#' ever appeared. Do NOT apply this to columns that hold hand-written `$...$`
#' math -- those are meant to reach Typst intact.
my_escape <- function(x) {
    stringr::str_replace_all(as.character(x), "([\\\\#$<>@*_])", "\\\\\\1")
}

#' Attach a note to a table that has already been built
#'
#' `my_note()` covers the case where the note is passed to `tt(notes = )`; this
#' one covers tables that arrive already assembled, as everything coming out of
#' `my_modelsummary()` does.
my_add_note <- function(x, ...) {
    x@notes <- list(my_note(...))
    x
}

#' Column widths as fractions of the text width
#'
#' Replaces `column_spec(1, width = "11em")` etc. `first` is the share taken by
#' the outcome-label column; the remaining estimate columns split what is left
#' evenly. Setting explicit widths also does the job that
#' `kable_styling(latex_options = "scale_down")` used to do, since Typst has no
#' equivalent of scale_down: the table is laid out to the text width instead of
#' being shrunk to fit after the fact.
my_widths <- function(x, first = 0.28) {
    n <- x@ncol
    x@width <- c(first, rep((1 - first) / (n - 1), n - 1))
    x
}

make_eligibility_plot <- function() {
    a <- ggplot() +
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
    return(a)
}
make_sample_selection_plot <- function() {
    a <- ggplot() +
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
    return(a)
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

# --- RD panel figures ---------------------------------------------------------------
#
# The make_*_plots() functions below return a *named list of panels*, not an arranged
# grid. Arrangement and theme are the caller's business: the working paper wants 3x2 in
# TeX Gyre Heros, the conference deck wants 2x3 in Libertinus Sans, and both read the
# same target. Baking either choice into the target is what produced the
# `*_plots_long` duplicates that used to sit alongside these -- ~1 GB of _targets store
# whose only difference was nrow/ncol.
#
# Panels therefore leave label_plots() carrying no theme of their own (see
# R/rd_functions.R); arrange_rd_panels() supplies one. This is the arrangement the
# `_esp` family already uses -- cf. arrange_covariates_plots_esp() in R/amafore_esp.R.

#' Theme for the RD binned-scatter panels in the working paper
#'
#' Deliberately close to tema_esp() in R/amafore_esp.R. base_family is named
#' explicitly rather than left to theme_set(): these panels used to inherit a
#' complete theme_minimal() from label_plots(), and a complete theme is not merged
#' with the global default, so the `theme_set(theme_minimal(base_family = "TeX Gyre
#' Heros"))` in the section setup chunks was silently doing nothing to them. TeX Gyre
#' Heros is the WP's sansfont and is visible to both Typst and R (see CLAUDE.md).
#' `base_size = 11` matches theme_minimal()'s own default, which is what these panels
#' had before -- the point of this theme is the font, the gridlines and the title
#' weight, not a size change. The WP chunks draw at fig-width 9 and display at about
#' 6.5in, so 11pt lands near 8pt on the page; dropping it to 9 shrank that to 6.5pt.
theme_rd_panel <- function(base_size = 11, base_family = "TeX Gyre Heros") {
    theme_minimal(base_size = base_size, base_family = base_family) +
        theme(
            panel.grid.minor = element_blank(),
            plot.title = element_text(face = "bold", size = rel(1)),
            plot.title.position = "plot",
            strip.text = element_text(face = "bold")
        )
}

#' Theme a list of RD panels and arrange them into a grid
#'
#' `lapply()` rather than `purrr::map()` on purpose: this also runs inside the callr
#' child of fig_slides() in slides/rpd-talk.qmd, which attaches only ggplot2 and
#' ggpubr. Adding a complete theme replaces whatever rdplot() left on the panel, so
#' nothing leaks through from the stored object.
arrange_rd_panels <- function(plots, nrow = 3, ncol = 2, theme = theme_rd_panel()) {
    # Insist on a bare list of panels. A ggplot is itself list-like, so a target that
    # still holds the pre-refactor ggarrange sends lapply() over the *internals* of a
    # ggplot -- its layers, scales, coordinates -- and the render dies with "Cannot add
    # <ggproto> objects together", which points nowhere near the real problem. That is
    # not hypothetical: rendering while `targets` has not caught up is the normal state
    # of this project, because the panel targets each load rd_data and are rebuilt by
    # hand.
    if (!is.list(plots) || inherits(plots, "gg") ||
        !all(vapply(plots, inherits, logical(1), "ggplot"))) {
        stop(
            "arrange_rd_panels() expects a list of ggplot panels, got <",
            paste(class(plots), collapse = "/"), ">. If this came from tar_read(), the ",
            "target predates the panel-list refactor -- rebuild it with tar_make().",
            call. = FALSE
        )
    }
    ggarrange(
        plotlist = lapply(plots, function(p) p + theme),
        nrow = nrow, ncol = ncol
    )
}

make_covariates_plots <- function(x) {
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


    setNames(labeled$plot, as.character(labeled$label))
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

    setNames(labeled$plot, as.character(labeled$label))
}



make_take_up_plots <- function(x) {
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

    setNames(labeled$plot, as.character(labeled$label))
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

    a <- res |>
        ggplot(aes(months, point)) +
        geom_pointrange(aes(ymin = lower, ymax = upper)) +
        geom_hline(yintercept = 0) +
        scale_x_continuous(breaks = seq(from = 3, to = 48, by = 3)) +
        labs(
            x = "Months since displacement",
            y = "Take Up Rate"
        )
    return(a)
}


make_survival_plots <- function(x) {
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

    setNames(labeled$plot, as.character(labeled$label))
}

make_duration_plots <- function(x) {
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

    setNames(labeled$plot, as.character(labeled$label))
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

    a <- ggarrange(survival_path, duration_path, nrow = 2)
    return(a)
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

    a <- ggarrange(survival_path, duration_path, nrow = 2)
    return(a)
}


my_medium_table <- function(x) {
    x |>
        mutate(label = unemp_year) |>
        my_modelsummary() |>
        my_widths(first = 0.28) |>
        tinytable::style_tt(j = 1, align = "l") |>
        tinytable::style_tt(fontsize = 0.8)
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
    # Nine columns. A narrower label column and 0.7em type are what keep this
    # inside the text block now that scale_down is gone.
    x |>
        my_modelsummary() |>
        my_widths(first = 0.22) |>
        tinytable::style_tt(j = 1, align = "l") |>
        tinytable::style_tt(fontsize = 0.7)
}

# slides_en.R
#
# Functions for the *_slides / *_en targets that feed the English conference deck
# in slides/. This file ONLY defines new functions: it does not modify any function
# or target used by the thesis, the working paper, or the AMAFORE submission.
#
# Two things distinguish these from the print figures in R/results.R:
#   1. They are themed for projection, not for a page -- larger relative type,
#      heavier strokes, no minor gridlines, muted axis text.
#   2. They carry the deck's palette, which is the same palette as the beamer
#      theme in slides/beamer-rpd.tex. Keep the hex values below in sync with the
#      \definecolor block there.
#
# The numeric summary targets they consume (heterogeneidad_esp, muestra_stats_esp,
# resultados_principales_esp, ...) are the Spanish-labelled ones built for the
# AMAFORE companion paper. Only their numeric columns are used; English labels are
# applied here. This is the same arrangement iab/abstract.qmd uses, and it exists so
# that the deck and the paper cannot report different numbers for the same estimate.

# Palette and shared labels ------------------------------------------------------

slide_ink <- "#1A2028"
slide_mute <- "#6E7884"
slide_rule <- "#DADEE2"
slide_accent <- "#00506B"
slide_alert <- "#B4532A"

axis_days_en <- "Contribution days to the eligibility threshold"
axis_months_en <- "Months since displacement"

# For figures rendered narrower than about 2.6 inches -- a half-width column -- the
# full axis title is wider than the plot and gets cut off at the image edge.
axis_days_short_en <- "Days to the eligibility threshold"

# Projection theme. Deliberately close to tema_esp() in R/amafore_esp.R, with the
# axis text muted and the strokes thickened: at 5.5 inches wide on a projector the
# default theme_minimal() gridlines disappear and the axis labels read as noise.
#
# base_family names a font that only exists in the texmf tree, invisible to
# fontconfig. It resolves at draw time in the callr subprocess that renders each
# figure (see fig_slides() in slides/rpd-talk.qmd), which registers the OTFs with
# systemfonts first. Building the target itself never draws anything, so a missing
# registration here is harmless.
theme_slides <- function(base_size = 9, base_family = "Libertinus Sans") {
    theme_minimal(base_size = base_size, base_family = base_family) +
        theme(
            panel.grid.minor = element_blank(),
            panel.grid.major = element_line(color = slide_rule, linewidth = 0.3),
            plot.title = element_text(face = "bold", size = rel(1), color = slide_ink),
            plot.title.position = "plot",
            plot.subtitle = element_text(size = rel(0.9), color = slide_mute),
            axis.title = element_text(size = rel(0.9), color = slide_mute),
            axis.text = element_text(color = slide_mute),
            strip.text = element_text(face = "bold", color = slide_ink),
            legend.position = "top",
            legend.title = element_blank(),
            legend.margin = margin(b = -4),
            plot.margin = margin(2, 4, 2, 2)
        )
}

# The heterogeneity target is keyed on the Spanish labels of the AMAFORE paper.
# These translate at the point of use; the estimates are never recomputed.
groups_en <- c(
    "Ingreso previo"        = "Prior earnings",
    "Edad"                  = "Age",
    "Sexo"                  = "Sex",
    "Exposición a COVID-19" = "COVID-19 exposure"
)

subgroups_en <- c(
    "Por debajo de la mediana"        = "Below median",
    "Por arriba de la mediana"        = "Above median",
    "Hombres"                         = "Men",
    "Mujeres"                         = "Women",
    "Separación antes de marzo 2019"  = "Displaced before Mar 2019",
    "Separación después de marzo 2020" = "Displaced after Mar 2020"
)

# Re-skins of existing plot targets ----------------------------------------------
# These take the built plot object rather than rd_data, so they are cheap: no
# rdrobust refit, no 955 MB deserialisation.

make_eligibility_plot_slides <- function(eligibility_plot) {
    eligibility_plot +
        scale_fill_manual(
            values = c(
                "A"            = "#3E7A8C",
                "B"            = "#8FB4BF",
                "Dropped*"     = slide_alert,
                "Not feasible" = "#E6E9EB"
            )
        ) +
        labs(
            x = "Years since entry to the formal labor market",
            y = "Years contributed to social security",
            fill = NULL
        ) +
        theme_slides(base_size = 10)
}

make_sample_selection_plot_slides <- function(sample_selection_plot) {
    sample_selection_plot +
        scale_fill_manual(
            values = c(
                "A"            = "#3E7A8C",
                "B"            = "#8FB4BF",
                "Dropped*"     = slide_alert,
                "Not feasible" = "#E6E9EB"
            )
        ) +
        labs(
            title = NULL,
            subtitle = NULL,
            x = "Years since entry to the formal labor market",
            y = "Years contributed to social security",
            fill = NULL
        ) +
        theme_slides(base_size = 10)
}

make_density_plot_slides <- function(density_plot) {
    density_plot +
        labs(x = axis_days_short_en, y = "Density") +
        theme_slides(base_size = 9) +
        theme(legend.position = "none")
}

make_rpd_usage_plot_slides <- function(path) {
    withdraws <- here(path) |>
        open_dataset(format = "feather")

    left <- withdraws |>
        summarise(.by = rpd_date, n = n()) |>
        collect() |>
        arrange(rpd_date) |>
        mutate(n = cumsum(n)) |>
        ggplot(aes(x = rpd_date, y = n)) +
        geom_step(linewidth = 0.6, color = slide_accent) +
        scale_y_continuous(labels = scales::label_number(scale = 1e-6, suffix = "M")) +
        labs(title = "Withdrawals, cumulative", x = NULL, y = NULL) +
        theme_slides()

    right <- withdraws |>
        summarise(.by = rpd_date, n = sum(amount_withdrawn)) |>
        collect() |>
        arrange(rpd_date) |>
        mutate(n = cumsum(n)) |>
        ggplot(aes(x = rpd_date, y = n)) +
        geom_step(linewidth = 0.6, color = slide_accent) +
        scale_y_continuous(
            labels = scales::label_dollar(scale = 1e-9, accuracy = 1, suffix = "bn")
        ) +
        labs(title = "Amount withdrawn, cumulative (2024 MXN)", x = NULL, y = NULL) +
        theme_slides()

    ggarrange(left, right)
}

# Dynamic paths -------------------------------------------------------------------
# extraer_path_esp() (R/amafore_esp.R) does the extraction; only the labelling and
# the theme differ, so it is reused rather than duplicated.

make_take_up_path_slides <- function(take_up) {
    take_up |>
        extraer_path_esp("take_up") |>
        ggplot(aes(months, point)) +
        geom_hline(yintercept = 0, linewidth = 0.3, color = slide_mute) +
        geom_pointrange(
            aes(ymin = lower, ymax = upper),
            size = 0.35, linewidth = 0.5, color = slide_accent
        ) +
        scale_x_continuous(breaks = seq(from = 3, to = 48, by = 3)) +
        scale_y_continuous(labels = ~ .x * 100) +
        labs(x = axis_months_en, y = "Effect on RPD take-up (pp)") +
        theme_slides()
}

make_fuzzy_path_slides <- function(survival_iv) {
    survival <- survival_iv |>
        extraer_path_esp("survival") |>
        ggplot(aes(months, point)) +
        geom_hline(yintercept = 0, linewidth = 0.3, color = slide_mute) +
        geom_pointrange(
            aes(ymin = lower, ymax = upper),
            size = 0.3, linewidth = 0.45, color = slide_accent
        ) +
        scale_x_continuous(breaks = seq(from = 3, to = 48, by = 3)) +
        scale_y_continuous(labels = ~ .x * 100) +
        labs(
            title = "Probability of still being out of formal work",
            x = NULL, y = "Effect (pp)"
        ) +
        theme_slides(base_size = 8)

    duration <- survival_iv |>
        extraer_path_esp("duration") |>
        ggplot(aes(months, point)) +
        geom_hline(yintercept = 0, linewidth = 0.3, color = slide_mute) +
        geom_pointrange(
            aes(ymin = lower, ymax = upper),
            size = 0.3, linewidth = 0.45, color = slide_accent
        ) +
        scale_x_continuous(breaks = seq(from = 3, to = 48, by = 3)) +
        labs(
            title = "Cumulative time out of formal work",
            x = axis_months_en, y = "Effect (weeks)"
        ) +
        theme_slides(base_size = 8)

    ggarrange(survival, duration, nrow = 2)
}

# Binned-scatter RD plots ---------------------------------------------------------
# These two are the only slide targets that load rd_data. Build each in its own R
# process: see the note in _targets.R.

make_rd_take_up_plot_slides <- function(rd_data) {
    plots <- rd_data |>
        filter(name == "take_up_12") |>
        compute_rd_plot()

    plots$rd[[1]]$plot$rdplot +
        labs(title = NULL, x = axis_days_en, y = "RPD take-up within 12 months") +
        scale_y_continuous(labels = scales::label_percent(accuracy = 1)) +
        coord_cartesian(ylim = c(0, 0.15)) +
        theme_slides(base_size = 10)
}

make_rd_outcome_plot_slides <- function(rd_data) {
    plots <- rd_data |>
        filter(name %in% c("survival_3", "duration_36")) |>
        arrange(name != "survival_3") |>
        compute_rd_plot()

    left <- plots$rd[[1]]$plot$rdplot +
        labs(title = "Still out of formal work at 3 months", x = axis_days_en, y = NULL) +
        scale_y_continuous(labels = scales::label_percent(accuracy = 1)) +
        theme_slides(base_size = 8)

    right <- plots$rd[[2]]$plot$rdplot +
        labs(title = "Weeks out of formal work over 36 months", x = axis_days_en, y = NULL) +
        theme_slides(base_size = 8)

    ggarrange(left, right)
}

# Heterogeneity: the difference test, drawn ---------------------------------------
# The point of this figure is the distinction the working paper added in f804302:
# a large coefficient in one half of a split and a small one in the other does not
# establish that the halves differ. Subgroups whose difference is significant at 5%
# are drawn in the accent colour; the rest are muted, and every panel is labelled
# with the p-value of the difference so the eye cannot draw the wrong conclusion.

make_het_diff_plot_slides <- function(het, level = 0.95) {
    z <- stats::qnorm(1 - (1 - level) / 2)

    het |>
        mutate(
            group = unname(groups_en[grupo]),
            subgroup = unname(subgroups_en[subgrupo]),
            lower = dur_coef - z * dur_se,
            upper = dur_coef + z * dur_se,
            distinguishable = dif_pv < 0.05,
            panel = paste0(
                group, "\n(diff. p = ", formatC(dif_pv, format = "f", digits = 3), ")"
            )
        ) |>
        # Keep the split order of the paper, and put the first-listed subgroup on top.
        mutate(
            panel = factor(panel, levels = unique(panel[order(match(grupo, names(groups_en)))])),
            subgroup = factor(subgroup, levels = rev(unique(subgroup)))
        ) |>
        ggplot(aes(x = dur_coef, y = subgroup, color = distinguishable)) +
        geom_vline(xintercept = 0, linewidth = 0.3, color = slide_mute) +
        geom_pointrange(
            aes(xmin = lower, xmax = upper),
            size = 0.35, linewidth = 0.55
        ) +
        facet_grid(panel ~ ., scales = "free_y", space = "free_y", switch = "y") +
        scale_color_manual(
            values = c(`TRUE` = slide_accent, `FALSE` = slide_mute),
            labels = c(`TRUE` = "Halves are distinguishable (p < 0.05)",
                       `FALSE` = "Not distinguishable"),
            breaks = c(TRUE, FALSE)
        ) +
        labs(
            x = "Effect on weeks out of formal work over 36 months",
            y = NULL
        ) +
        theme_slides() +
        theme(
            strip.placement = "outside",
            strip.text.y.left = element_text(angle = 0, hjust = 0, size = rel(0.85)),
            panel.grid.major.y = element_blank()
        )
}

# Bandwidth sensitivity -----------------------------------------------------------

make_bw_plot_slides <- function(bw_sensitivity, outcome = "duration_36") {
    bw_sensitivity |>
        filter(name == outcome) |>
        mutate(
            lower = coef - 1.96 * se,
            upper = coef + 1.96 * se,
            optimal = multiplo == 1
        ) |>
        ggplot(aes(x = multiplo, y = coef, color = optimal)) +
        geom_hline(yintercept = 0, linewidth = 0.3, color = slide_mute) +
        geom_pointrange(aes(ymin = lower, ymax = upper), size = 0.35, linewidth = 0.5) +
        scale_color_manual(values = c(`TRUE` = slide_accent, `FALSE` = slide_mute)) +
        scale_x_continuous(breaks = sort(unique(bw_sensitivity$multiplo))) +
        labs(
            x = "Bandwidth, as a multiple of the MSE-optimal one",
            y = "Effect (weeks)"
        ) +
        theme_slides() +
        theme(legend.position = "none")
}

# Withdrawal size relative to the account -----------------------------------------
# English twins of fig_quintil_esp() and its two callers. The Spanish originals
# hard-code their axis titles and quintile labels, so these are twins rather than a
# parameterisation of the existing function -- which stays untouched.

fig_quintile_en <- function(shares, lo, mid, hi, y_lab, y_labels) {
    shares |>
        mutate(
            quintile_lab = factor(
                quintil,
                levels = 1:5,
                labels = c("Q1\n(lowest)", "Q2", "Q3", "Q4", "Q5\n(highest)")
            )
        ) |>
        ggplot(aes(x = quintile_lab, y = .data[[mid]])) +
        geom_linerange(
            aes(ymin = .data[[lo]], ymax = .data[[hi]]),
            linewidth = 0.7, color = slide_rule
        ) +
        geom_point(size = 2.4, color = slide_accent) +
        scale_y_continuous(labels = y_labels) +
        labs(x = "Quintile of the user's wage", y = y_lab) +
        theme_slides()
}

# Titles are kept short on purpose: these two go side by side at about 2.5 inches
# each, and a longer y-axis title is wider than the plot and gets cut off at the
# image edge. The slide's prose carries the fuller description.
make_fig_retiro_saldo_en <- function(shares) {
    fig_quintile_en(
        shares,
        lo = "p25", mid = "p50", hi = "p75",
        y_lab = "Share of the RCV balance",
        y_labels = scales::label_percent(accuracy = 1)
    )
}

make_fig_retiro_semanas_en <- function(shares) {
    fig_quintile_en(
        shares,
        lo = "sem_p25", mid = "sem_p50", hi = "sem_p75",
        y_lab = "Contribution weeks lost",
        y_labels = scales::label_number(accuracy = 1)
    )
}

# What the additional unemployment costs at retirement ----------------------------
# Reproduces the calculation in sections/results.qmd (the `pension-cost` chunk).
# It lives in a target rather than in a chunk of the deck so that the slide's inline
# numbers and its figure read the same object and cannot drift apart -- which is how
# presentations/english.qmd came to state four findings the paper no longer supports.
#
# Two channels, both compounding at the same real rate to age 65, so channel 2's
# share of the total is invariant to the return assumption:
#   Channel 1: the withdrawn capital stops earning inside the individual account.
#   Channel 2: contributions not made during the RPD-induced additional non-employment.
#
# Illustrative, not actuarial: it combines a complier LATE (the extra weeks) with
# sample means (the withdrawal and the prior wage), so it describes a worker of
# average characteristics induced by the eligibility rule, not an observed individual.

compute_pension_cost_en <- function(muestra_stats,
                                    resultados,
                                    rates = c(0.03, 0.04, 0.05),
                                    c_rates = c(0.065, 0.15),
                                    horizon = 40,
                                    weeks_per_month = 4.345) {
    W <- muestra_stats$w_retiro
    wbar <- muestra_stats$wbar
    delta <- resultados$coef[resultados$name == "duration_36"]
    delta_se <- resultados$se[resultados$name == "duration_36"]

    tidyr::crossing(rate = rates, c_rate = c_rates) |>
        mutate(
            withdrawal = W,
            wage = wbar,
            weeks = delta,
            extra_months = delta / weeks_per_month,
            fv = (1 + rate)^horizon,
            channel1 = W * fv,
            channel2 = wbar * c_rate * extra_months * fv,
            channel2_lo = wbar * c_rate * ((delta - 1.96 * delta_se) / weeks_per_month) * fv,
            channel2_hi = wbar * c_rate * ((delta + 1.96 * delta_se) / weeks_per_month) * fv,
            total = channel1 + channel2,
            total_lo = channel1 + channel2_lo,
            total_hi = channel1 + channel2_hi,
            multiple = total / W,
            multiple_lo = total_lo / W,
            multiple_hi = total_hi / W,
            share2 = channel2 / total
        )
}

make_fig_pension_channels_en <- function(pension_cost, c_rate_shown = 0.065) {
    d <- pension_cost |>
        filter(c_rate == c_rate_shown) |>
        select(rate, channel1, channel2, withdrawal) |>
        tidyr::pivot_longer(
            c(channel1, channel2),
            names_to = "channel", values_to = "amount"
        ) |>
        mutate(
            multiple = amount / withdrawal,
            # Short labels: the figure sits in a half-width column, where the full
            # description of each channel is wider than the plot and is cut off.
            # The surrounding slide text spells both channels out.
            channel = factor(
                channel,
                levels = c("channel2", "channel1"),
                labels = c("Contributions never made", "Withdrawal, uncompounded")
            ),
            rate_lab = scales::label_percent(accuracy = 1)(rate)
        )

    totals <- d |>
        summarise(.by = rate_lab, multiple = sum(multiple))

    d |>
        ggplot(aes(x = rate_lab, y = multiple, fill = channel)) +
        geom_col(width = 0.62) +
        geom_text(
            data = totals,
            aes(x = rate_lab, y = multiple, label = paste0(formatC(multiple, format = "f", digits = 1), "x")),
            inherit.aes = FALSE,
            vjust = -0.45, size = 2.9, fontface = "bold", color = slide_ink
        ) +
        scale_fill_manual(values = c(slide_alert, slide_accent)) +
        # Extra headroom so the total annotation above the tallest bar is not clipped.
        scale_y_continuous(
            expand = expansion(mult = c(0, 0.18)),
            labels = ~ paste0(.x, "x")
        ) +
        guides(fill = guide_legend(nrow = 2, reverse = TRUE)) +
        labs(
            x = "Real return assumption",
            y = "Loss at 65, × the withdrawal"
        ) +
        theme_slides() +
        theme(panel.grid.major.x = element_blank())
}

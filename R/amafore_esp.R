# amafore_esp.R
#
# Funciones para los targets *_esp que alimentan la postulación al Premio de
# Investigación sobre Pensiones en México 2026 (AMAFORE/ITAM), en amafore/.
# Este archivo SOLO define funciones nuevas: no modifica ninguna función ni
# target existente de la tesis o del working paper.

# Tema y etiquetas compartidas ---------------------------------------------------

tema_esp <- function(base_size = 11) {
    theme_minimal(base_size = base_size) +
        theme(
            panel.grid.minor = element_blank(),
            plot.title = element_text(face = "bold", size = rel(1)),
            plot.title.position = "plot",
            strip.text = element_text(face = "bold")
        )
}

eje_dias_esp <- "Días de cotización respecto al umbral de elegibilidad"
eje_meses_esp <- "Meses desde el despido"

etiquetas_covariables_esp <- c(
    "female"                    = "Mujer",
    "birth_date"                = "Fecha de nacimiento",
    "began_working"             = "Inicio de vida laboral formal",
    "unemployment_date"         = "Fecha de despido",
    "age"                       = "Edad",
    "days_since_account_opened" = "Días desde apertura de la cuenta",
    "no_curp"                   = "Sin CURP",
    "prev_job_duration"         = "Duración del empleo previo (semanas)",
    "prev_job_cum_earnings"     = "Ingresos totales, empleo previo",
    "prev_job_av_earnings"      = "Salario mensual, empleo previo"
)

label_plots_esp <- function(plot, outcome) {
    plot +
        labs(
            title = outcome,
            x = eje_dias_esp,
            y = ""
        ) +
        tema_esp(base_size = 9)
}

# Figuras descriptivas -------------------------------------------------------------

plot_rpd_usage_esp <- function(path) {
    withdraws <- here(path) |>
        open_dataset(format = "feather")

    left <- withdraws |>
        summarise(
            .by = rpd_date,
            n = n()
        ) |>
        collect() |>
        arrange(rpd_date) |>
        mutate(n = cumsum(n)) |>
        ggplot(aes(x = rpd_date, y = n)) +
        geom_step(linewidth = 0.5) +
        labs(
            title = "Número acumulado de retiros",
            x = "Fecha",
            y = "Retiros"
        ) +
        scale_y_continuous(labels = scales::label_comma()) +
        tema_esp()

    right <- withdraws |>
        summarise(
            .by = rpd_date,
            n = sum(amount_withdrawn)
        ) |>
        collect() |>
        arrange(rpd_date) |>
        mutate(n = cumsum(n)) |>
        ggplot(aes(x = rpd_date, y = n)) +
        geom_step(linewidth = 0.5) +
        labs(
            title = "Monto acumulado retirado",
            x = "Fecha",
            y = "Millones de pesos de 2024"
        ) +
        scale_y_continuous(
            labels = scales::label_dollar(scale = 1e-6, accuracy = 1, big.mark = ",")
        ) +
        tema_esp()

    ggarrange(left, right)
}

make_density_plot_esp <- function(density_plot) {
    density_plot +
        labs(
            x = eje_dias_esp,
            y = "Densidad"
        ) +
        tema_esp() +
        theme(legend.position = "none")
}

# Trayectorias dinámicas (paths) ----------------------------------------------------

extraer_path_esp <- function(x, patron) {
    x |>
        filter(str_detect(name, patron)) |>
        filter(!str_detect(name, "censored|days")) |>
        mutate(
            months = str_extract(name, "\\d+") |> as.numeric(),
            rd = map(rd, "output"),
            point = map_dbl(rd, ~ pluck(.x, "coef", 3)),
            lower = map_dbl(rd, ~ pluck(.x, "ci", 3)),
            upper = map_dbl(rd, ~ pluck(.x, "ci", 6))
        ) |>
        select(name, months, point, lower, upper)
}

make_take_up_path_esp <- function(take_up) {
    take_up |>
        extraer_path_esp("take_up") |>
        ggplot(aes(months, point)) +
        geom_hline(yintercept = 0, linewidth = 0.3, color = "gray40") +
        geom_pointrange(aes(ymin = lower, ymax = upper), size = 0.3, linewidth = 0.4) +
        scale_x_continuous(breaks = seq(from = 3, to = 48, by = 3)) +
        scale_y_continuous(labels = scales::label_percent(accuracy = 1)) +
        labs(
            x = eje_meses_esp,
            y = "Efecto sobre la tasa de uso del RPD"
        ) +
        tema_esp()
}

make_fuzzy_path_esp <- function(survival_iv) {
    survival_path <- survival_iv |>
        extraer_path_esp("survival") |>
        ggplot(aes(months, point)) +
        geom_hline(yintercept = 0, linewidth = 0.3, color = "gray40") +
        geom_pointrange(aes(ymin = lower, ymax = upper), size = 0.3, linewidth = 0.4) +
        scale_x_continuous(breaks = seq(from = 3, to = 48, by = 3)) +
        scale_y_continuous(labels = scales::label_percent(accuracy = 1)) +
        labs(
            x = NULL,
            title = "Probabilidad de seguir desempleado",
            y = "Efecto (puntos porcentuales)"
        ) +
        tema_esp()

    duration_path <- survival_iv |>
        extraer_path_esp("duration") |>
        ggplot(aes(months, point)) +
        geom_hline(yintercept = 0, linewidth = 0.3, color = "gray40") +
        geom_pointrange(aes(ymin = lower, ymax = upper), size = 0.3, linewidth = 0.4) +
        scale_x_continuous(breaks = seq(from = 3, to = 48, by = 3)) +
        labs(
            x = eje_meses_esp,
            title = "Duración acumulada del desempleo",
            y = "Efecto (semanas)"
        ) +
        tema_esp()

    ggarrange(survival_path, duration_path, nrow = 2)
}

# Gráficas de discontinuidad (binned scatter) ---------------------------------------

make_rd_take_up_plot_esp <- function(rd_data) {
    plots <- rd_data |>
        filter(name == "take_up_12") |>
        compute_rd_plot()

    plots$rd[[1]]$plot$rdplot +
        labs(
            title = NULL,
            x = eje_dias_esp,
            y = "Tasa de uso del RPD a 12 meses"
        ) +
        scale_y_continuous(labels = scales::label_percent(accuracy = 1)) +
        coord_cartesian(ylim = c(0, 0.15)) +
        tema_esp()
}

make_rd_outcome_plot_esp <- function(rd_data) {
    plots <- rd_data |>
        filter(name %in% c("survival_3", "duration_36")) |>
        arrange(name != "survival_3") |>
        compute_rd_plot()

    izq <- plots$rd[[1]]$plot$rdplot +
        labs(
            title = "Prob. de seguir desempleado a 3 meses",
            x = eje_dias_esp,
            y = ""
        ) +
        scale_y_continuous(labels = scales::label_percent(accuracy = 1)) +
        tema_esp(base_size = 9)

    der <- plots$rd[[2]]$plot$rdplot +
        labs(
            title = "Semanas de desempleo a 36 meses",
            x = eje_dias_esp,
            y = ""
        ) +
        tema_esp(base_size = 9)

    ggarrange(izq, der)
}

# Cada gráfica de covariable se construye en su propio target (una por proceso)
# para acotar el uso de memoria; la composición final no carga rd_data.
make_covariate_plot_esp <- function(rd_data, var) {
    plots <- rd_data |>
        filter(name == var) |>
        compute_rd_plot()

    label_plots_esp(plots$rd[[1]]$plot$rdplot, etiquetas_covariables_esp[var])
}

arrange_covariates_plots_esp <- function(plots, nrow = 3, ncol = 2) {
    orden <- order(map_chr(plots, ~ .x$labels$title))
    ggarrange(plotlist = plots[orden], nrow = nrow, ncol = ncol)
}

# Tablas (se regresan tibbles; el formato LaTeX se decide en amafore/) ---------------

extraer_rd_stats_esp <- function(x) {
    x |>
        mutate(
            coef = map_dbl(rd, ~ pluck(.x, "output", "coef", 3)),
            se = map_dbl(rd, ~ pluck(.x, "output", "se", 3)),
            pv = map_dbl(rd, ~ pluck(.x, "output", "pv", 3)),
            n_total = map_dbl(rd, ~ pluck(.x, "output", "N", 1)) +
                map_dbl(rd, ~ pluck(.x, "output", "N", 2)),
            n_efectivo = map_dbl(rd, ~ pluck(.x, "output", "N_h", 1)) +
                map_dbl(rd, ~ pluck(.x, "output", "N_h", 2)),
            # bws es una matriz 2x2 (filas h, b); pluck indexa por columna
            h = map_dbl(rd, ~ pluck(.x, "output", "bws", 1))
        ) |>
        select(
            any_of(c("name", "label", "multiplo")),
            coef, se, pv, n_total, n_efectivo, h
        )
}

make_balance_table_esp <- function(all_covariates) {
    all_covariates |>
        extraer_rd_stats_esp() |>
        mutate(covariable = etiquetas_covariables_esp[name]) |>
        select(covariable, coef, se, pv, n_efectivo, h) |>
        drop_na(covariable)
}

primera_etapa_stats_esp <- function(take_up) {
    take_up |>
        filter(name == "take_up_12") |>
        extraer_rd_stats_esp() |>
        mutate(t = coef / se, f = t^2)
}

make_itt_table_esp <- function(take_up, survival, next_job, medium_term) {
    principales <- c(
        "take_up_12"           = "Uso del RPD a 12 meses (primera etapa)",
        "survival_3"           = "Supervivencia en desempleo a 3 meses",
        "duration_36"          = "Duración del desempleo a 36 meses (semanas)",
        "next_job_av_earnings" = "Salario mensual, siguiente empleo (MXN 2024)",
        "next_job_duration"    = "Duración del siguiente empleo (semanas)",
        "earnings_total"       = "Ingresos totales acumulados a 3 años (MXN 2024)",
        "months_worked_total"  = "Meses trabajados a 3 años"
    )

    bind_rows(take_up, survival, next_job, medium_term) |>
        filter(name %in% names(principales)) |>
        extraer_rd_stats_esp() |>
        mutate(
            resultado = principales[name],
            resultado = factor(resultado, levels = unname(principales))
        ) |>
        arrange(resultado) |>
        select(name, resultado, coef, se, pv, n_efectivo, h)
}

# Sensibilidad al ancho de banda -----------------------------------------------------

make_bw_sensitivity_esp <- function(rd_data,
                                    outcomes = c("survival_3", "duration_36"),
                                    multiplos = c(0.5, 0.75, 1, 1.5, 2)) {
    etiquetas <- c(
        "survival_3" = "Supervivencia en desempleo a 3 meses",
        "duration_36" = "Duración del desempleo a 36 meses (semanas)"
    )

    rd_data |>
        filter(name %in% outcomes) |>
        mutate(
            fit0 = map(data, my_fuzzy_rd),
            h0 = map_dbl(fit0, ~ pluck(.x, "output", "bws", 1)),
            b0 = map_dbl(fit0, ~ pluck(.x, "output", "bws", 2))
        ) |>
        select(name, data, h0, b0) |>
        mutate(multiplo = list(multiplos)) |>
        unnest(multiplo) |>
        mutate(
            rd = pmap(
                list(data, h0, b0, multiplo),
                \(d, h0, b0, m) my_fuzzy_rd(d, h = m * h0, b = m * b0)
            )
        ) |>
        select(-data, -h0, -b0) |>
        extraer_rd_stats_esp() |>
        mutate(resultado = etiquetas[name]) |>
        select(name, resultado, multiplo, h, coef, se, pv, n_efectivo)
}

# Costeo agregado a partir del registro de retiros ------------------------------------
# Los montos en withdraws_clean.feather ya están deflactados a pesos de dic-2024
# (ver R-scripts/RDD/05_merge_RPD.R), por lo que la capitalización usa tasas reales.

compute_retiros_capitalizados_esp <- function(path,
                                              tasas = c(0.03, 0.04, 0.05),
                                              corte = as.Date("2024-12-31")) {
    diario <- here(path) |>
        open_dataset(format = "feather") |>
        summarise(
            .by = rpd_date,
            monto = sum(amount_withdrawn),
            n = n()
        ) |>
        collect() |>
        filter(rpd_date <= corte) |>
        mutate(anios = as.numeric(corte - rpd_date) / 365.25)

    tibble(tasa = tasas) |>
        mutate(
            monto_2024 = sum(diario$monto),
            n_retiros = sum(diario$n),
            capitalizado = map_dbl(
                tasa,
                \(r) sum(diario$monto * (1 + r)^diario$anios)
            ),
            multiplo = capitalizado / monto_2024
        )
}

compute_retiro_saldo_shares_esp <- function(path, corte = as.Date("2024-12-31")) {
    con <- DBI::dbConnect(duckdb::duckdb())
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

    here(path) |>
        open_dataset(format = "feather") |>
        to_duckdb(con) |>
        filter(
            balance_rcv > 0,
            amount_withdrawn > 0,
            wage_rpd > 0,
            rpd_date <= corte
        ) |>
        mutate(
            share = pmin(amount_withdrawn / balance_rcv, 1),
            quintil = ntile(wage_rpd, 5)
        ) |>
        group_by(quintil) |>
        summarise(
            p25 = quantile(share, 0.25),
            p50 = quantile(share, 0.50),
            p75 = quantile(share, 0.75),
            media = mean(share),
            prop_25 = mean(as.integer(share >= 0.25)),
            salario_mediano = median(wage_rpd),
            n = n()
        ) |>
        collect() |>
        arrange(quintil)
}

make_fig_retiro_saldo_esp <- function(shares) {
    shares |>
        mutate(
            quintil_lab = factor(
                quintil,
                levels = 1:5,
                labels = c(
                    "Q1\n(más bajo)", "Q2", "Q3", "Q4", "Q5\n(más alto)"
                )
            )
        ) |>
        ggplot(aes(x = quintil_lab, y = p50)) +
        geom_linerange(aes(ymin = p25, ymax = p75), linewidth = 0.6, color = "gray55") +
        geom_point(size = 2.2) +
        scale_y_continuous(labels = scales::label_percent(accuracy = 1)) +
        labs(
            x = "Quintil del salario previo al retiro (pesos de 2024)",
            y = "Retiro como proporción del saldo RCV"
        ) +
        tema_esp()
}

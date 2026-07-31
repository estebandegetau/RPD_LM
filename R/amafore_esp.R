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
eje_meses_esp <- "Meses desde la separación"

etiquetas_covariables_esp <- c(
    "female"                    = "Mujer",
    "birth_date"                = "Fecha de nacimiento",
    "began_working"             = "Inicio de vida laboral formal",
    "unemployment_date"         = "Fecha de separación",
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
        scale_y_continuous(labels = ~ .x * 100) +
        labs(
            x = eje_meses_esp,
            y = "Efecto sobre la tasa de uso del RPD (pp)"
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
        scale_y_continuous(labels = ~ .x * 100) +
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
        "next_job_av_earnings"  = "Salario mensual, siguiente empleo (MXN 2024)",
        # Espeja exactamente las filas de tbl-resultados-principales: el Apéndice
        # anuncia que la tabla de forma reducida acompaña a la de LATE, así que
        # omitir un resultado invitaría a preguntar por qué.
        "next_job_cum_earnings" = "Ingresos totales, siguiente empleo (MXN 2024)",
        "next_job_duration"     = "Duración del siguiente empleo (semanas)",
        "earnings_total"        = "Ingresos totales acumulados a 3 años (MXN 2024)",
        "months_worked_total"   = "Meses trabajados a 3 años"
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

# Resultados principales (LATE difuso) ------------------------------------------------
# Extrae, para cada resultado, el coeficiente difuso (fila "Robust"), su error
# estándar, valor-p, la media del lado no elegible (tau_bc[1], como en
# glance.rdrobust) y el N efectivo. Regresa numéricos crudos + una etiqueta de
# unidad `tipo`; el formato (estrellas, %, $) se decide en amafore/.

extraer_late_esp <- function(x) {
    x |>
        mutate(
            coef = map_dbl(rd, ~ pluck(.x, "output", "coef", 3)),
            se = map_dbl(rd, ~ pluck(.x, "output", "se", 3)),
            pv = map_dbl(rd, ~ pluck(.x, "output", "pv", 3)),
            media = map_dbl(rd, ~ pluck(.x, "output", "tau_bc", 1)),
            n_efectivo = map_dbl(rd, ~ pluck(.x, "output", "N_h", 1)) +
                map_dbl(rd, ~ pluck(.x, "output", "N_h", 2))
        )
}

make_resultados_principales_esp <- function(survival_iv, next_job_iv, medium_term_iv) {
    spec <- tibble::tribble(
        ~name,                   ~resultado,                                        ~tipo,
        "survival_3",            "Supervivencia en desempleo a 3 meses (pp)",       "pp",
        "duration_36",           "Duración del desempleo a 36 meses (semanas)",     "semanas",
        "next_job_av_earnings",  "Salario mensual, siguiente empleo (MXN 2024)",    "mxn",
        "next_job_cum_earnings", "Ingresos totales, siguiente empleo (MXN 2024)",   "mxn",
        "next_job_duration",     "Duración del siguiente empleo (semanas)",         "semanas",
        "earnings_total",        "Ingresos totales acumulados a 3 años (MXN 2024)", "mxn",
        "months_worked_total",   "Meses trabajados a 3 años",                       "meses"
    )

    bind_rows(survival_iv, next_job_iv, medium_term_iv) |>
        filter(name %in% spec$name) |>
        extraer_late_esp() |>
        select(name, coef, se, pv, media, n_efectivo) |>
        left_join(spec, by = "name") |>
        arrange(match(name, spec$name)) |>
        select(name, resultado, tipo, coef, se, pv, media, n_efectivo)
}

# Heterogeneidad (LATE difuso por subgrupo) -------------------------------------------
# Para cada target de corte (*_results) toma la fila del resultado pedido y el
# subgrupo indicado, devolviendo coef/se/pv/media crudos.

extraer_subgrupo_esp <- function(results, outcome, valor) {
    results |>
        filter(name == outcome, split == valor) |>
        extraer_late_esp() |>
        transmute(coef, se, pv, media) |>
        slice(1)
}

make_heterogeneidad_esp <- function(income_results, age_results,
                                    gender_results, covid_results) {
    filas <- tibble::tribble(
        ~grupo,                  ~subgrupo,                          ~target,          ~valor,
        "Ingreso previo",        "Por debajo de la mediana",         "income_results", "1",
        "Ingreso previo",        "Por arriba de la mediana",         "income_results", "2",
        "Edad",                  "Por debajo de la mediana",         "age_results",    "1",
        "Edad",                  "Por arriba de la mediana",         "age_results",    "2",
        "Sexo",                  "Hombres",                          "gender_results", "0",
        "Sexo",                  "Mujeres",                          "gender_results", "1",
        "Exposición a COVID-19", "Separación antes de marzo 2019",   "covid_results",  "No Exposure",
        "Exposición a COVID-19", "Separación después de marzo 2020", "covid_results",  "Full Exposure"
    )

    targets <- list(
        income_results = income_results,
        age_results = age_results,
        gender_results = gender_results,
        covid_results = covid_results
    )

    # El tipo de la columna `split` difiere (entero, lógico, texto); comparamos
    # sobre su representación como texto para casar el valor solicitado.
    saca <- function(target, outcome, valor) {
        res <- targets[[target]] |> mutate(split = as.character(split))
        extraer_subgrupo_esp(res, outcome, valor)
    }

    filas |>
        mutate(
            dur  = pmap(list(target, valor), \(t, v) saca(t, "duration_36", v)),
            ing  = pmap(list(target, valor), \(t, v) saca(t, "earnings_total", v)),
            wage = pmap(list(target, valor), \(t, v) saca(t, "next_job_av_earnings", v))
        ) |>
        mutate(
            dur_coef = map_dbl(dur, "coef"), dur_se = map_dbl(dur, "se"), dur_pv = map_dbl(dur, "pv"),
            ing_coef = map_dbl(ing, "coef"), ing_se = map_dbl(ing, "se"), ing_pv = map_dbl(ing, "pv"),
            wage_coef = map_dbl(wage, "coef"), wage_se = map_dbl(wage, "se"),
            wage_pv = map_dbl(wage, "pv"), wage_media = map_dbl(wage, "media")
        ) |>
        # Prueba de diferencia entre los dos subgrupos de cada corte. Las
        # estimaciones provienen de submuestras disjuntas, así que la varianza de
        # la diferencia es la suma de las varianzas. Sin esto, el texto sólo
        # podría contrastar magnitudes de coeficientes, que es justo lo que no
        # informa si los subgrupos difieren.
        mutate(
            .by = grupo,
            dif_coef = dur_coef[1] - dur_coef[2],
            dif_se   = sqrt(dur_se[1]^2 + dur_se[2]^2),
            dif_pv   = 2 * pnorm(-abs(dif_coef / dif_se))
        ) |>
        select(grupo, subgrupo,
               dur_coef, dur_se, dur_pv,
               ing_coef, ing_se, ing_pv,
               wage_coef, wage_se, wage_pv, wage_media,
               dif_coef, dif_se, dif_pv)
}

# Estadísticas descriptivas de la muestra ---------------------------------------------
# Equivalente en español de make_summary_statistics() (R/data.R), que alimenta la
# tabla del apéndice. Se calcula del mismo `rpd_data`, así que las medias coinciden
# por construcción con las de make_muestra_stats_esp() usadas en el costeo de §05.

# Orden de las filas y de los bloques dentro de la tabla. Las fechas se reportan
# en años decimales, como en la versión en inglés.
etiquetas_resumen_esp <- tibble::tribble(
    ~variable,                     ~etiqueta,                                        ~grupo,
    "female",                      "Mujer (proporción)",                             "Covariables",
    "birth_date",                  "Fecha de nacimiento (año)",                      "Covariables",
    "began_working",               "Inicio de vida laboral formal (año)",            "Covariables",
    "unemployment_date",           "Fecha de separación (año)",                      "Covariables",
    "age",                         "Edad a la separación (años)",                    "Covariables",
    "days_since_account_opened",   "Días desde la apertura de la cuenta",            "Covariables",
    "no_curp",                     "Sin CURP (proporción)",                          "Covariables",
    "prev_job_duration",           "Duración del empleo previo (semanas)",           "Empleo previo",
    "prev_job_av_earnings",        "Salario mensual, empleo previo (MXN 2024)",      "Empleo previo",
    "prev_job_cum_earnings",       "Ingresos totales, empleo previo (MXN 2024)",     "Empleo previo",
    "amount_withdrawn",            "Monto retirado (MXN 2024)",                      "Retiro (RPD)",
    "balance_rcv",                 "Saldo RCV al momento del retiro (MXN 2024)",     "Retiro (RPD)",
    "contributed_weeks_rpd",       "Semanas cotizadas al momento del retiro",        "Retiro (RPD)",
    "days_to_take_up",             "Días entre la separación y el retiro",           "Retiro (RPD)",
    "days_withdrawn",              "Días de salario retirados",                      "Retiro (RPD)",
    "survival_3",                  "Sigue desempleado a 3 meses (proporción)",       "Búsqueda de empleo",
    "survival_36",                 "Sigue desempleado a 36 meses (proporción)",      "Búsqueda de empleo",
    "duration_3",                  "Semanas de desempleo, censuradas a 3 meses",     "Búsqueda de empleo",
    "duration_36",                 "Semanas de desempleo, censuradas a 36 meses",    "Búsqueda de empleo",
    "next_job_duration",           "Duración del siguiente empleo (semanas)",        "Siguiente empleo",
    "next_job_av_earnings",        "Salario mensual, siguiente empleo (MXN 2024)",   "Siguiente empleo",
    "next_job_cum_earnings",       "Ingresos totales, siguiente empleo (MXN 2024)",  "Siguiente empleo",
    "earnings_total",              "Ingresos totales a 3 años (MXN 2024)",           "Mediano plazo",
    "av_earnings_total",           "Salario mensual promedio a 3 años (MXN 2024)",   "Mediano plazo",
    "months_worked_total",         "Meses trabajados a 3 años",                      "Mediano plazo"
)

make_tabla_resumen_esp <- function(rpd_data, especificacion = etiquetas_resumen_esp) {
    # Se resume columna por columna en lugar de pivotear a formato largo (como
    # hace la versión en inglés): con 877 mil filas, materializar el largo
    # multiplica la memoria sin necesidad.
    resumir <- function(variable) {
        x <- rpd_data[[variable]]
        if (inherits(x, "Date")) x <- decimal_date(x)
        x <- as.numeric(x)
        x <- x[!is.na(x)]

        tibble(
            variable = variable,
            n        = length(x),
            media    = mean(x),
            mediana  = median(x),
            de       = sd(x),
            min      = min(x),
            max      = max(x)
        )
    }

    especificacion |>
        filter(variable %in% names(rpd_data)) |>
        mutate(stats = map(variable, resumir)) |>
        unnest(stats, names_sep = "_") |>
        select(grupo, etiqueta,
               n = stats_n, media = stats_media, mediana = stats_mediana,
               de = stats_de, min = stats_min, max = stats_max)
}

# Estadísticos de muestra y parámetros de costeo --------------------------------------

make_muestra_stats_esp <- function(rpd_data) {
    tibble(
        n_muestra       = nrow(rpd_data),
        wbar            = mean(rpd_data$prev_job_av_earnings, na.rm = TRUE),
        mediana_ingreso = median(rpd_data$prev_job_av_earnings, na.rm = TRUE),
        mediana_edad    = median(rpd_data$age, na.rm = TRUE),
        edad_media      = mean(rpd_data$age, na.rm = TRUE),
        # Monto promedio retirado entre los usuarios de la muestra (W del costeo §05).
        w_retiro        = mean(rpd_data$amount_withdrawn, na.rm = TRUE),
        # Tasas de uso a 12 meses por elegibilidad (niveles, no el salto en el umbral).
        tu_elig         = mean(rpd_data$take_up_12[rpd_data$elig], na.rm = TRUE),
        tu_no_elig      = mean(rpd_data$take_up_12[!rpd_data$elig], na.rm = TRUE)
    )
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

# Costeo agregado de dos canales -------------------------------------------------------
# `retiros_capitalizados_esp` cuantifica solo el Canal 1: el capital que sale de la
# cuenta y el rendimiento que deja de generar. Este target le suma el Canal 2, las
# cotizaciones que el desempleo adicional causado por el programa impidió realizar.
#
# La razón para reportarlo como múltiplo (`markup`) y no como una proyección a la
# jubilación es algebraica: ambos canales se capitalizan por el mismo factor, de modo
# que el cociente total/canal1 = 1 + (wbar * c * delta) / W es invariante tanto a la
# tasa real como al horizonte. El costo agregado puede entonces expresarse sin suponer
# nada sobre cuándo se jubila cada usuario, que es justo el dato que el registro no
# tiene (no incluye fecha de nacimiento).
#
# El salario sale del propio registro (`wage_rpd`, SBC diario, deflactado a dic-2024 en
# el mismo paso que los montos), no de las medias de la muestra de estimación: así el
# único parámetro importado del RD es delta. Importa porque la muestra se sitúa en el
# umbral de dos años, donde el monto retirado es pequeño frente al salario mensual y el
# múltiplo sale más alto que en el registro completo.
#
# La agregación diaria es exacta, no una aproximación: todos los retiros de una misma
# fecha comparten factor de capitalización, así que la suma de montos y la suma de
# salarios por fecha son estadísticos suficientes. Deja el cálculo en ~10k filas.

compute_costo_agregado_esp <- function(path,
                                       resultados,
                                       tasas    = c(0.03, 0.04, 0.05),
                                       c_rates  = c(0.065, 0.15),
                                       dias_mes = 30.4,
                                       corte    = as.Date("2024-12-31")) {
    delta    <- resultados$coef[resultados$name == "duration_36"]
    delta_se <- resultados$se[resultados$name == "duration_36"]

    # 4.345 semanas/mes y 30.4 días/mes (= 4.345 * 7): las mismas conversiones que usan
    # los paneles de @tbl-costo-pension, para que el agregado y la ilustración
    # individual no difieran por convención.
    meses <- c(
        central = delta,
        lo      = delta - 1.96 * delta_se,
        hi      = delta + 1.96 * delta_se
    ) / 4.345

    # Mismo filtro que compute_retiros_capitalizados_esp() (solo la fecha): así
    # `n_retiros` y `canal1` reproducen ese target al peso y el texto puede citar el
    # mismo denominador en ambos lugares.
    diario <- here(path) |>
        open_dataset(format = "feather") |>
        summarise(
            .by = rpd_date,
            monto   = sum(amount_withdrawn),
            salario = sum(wage_rpd, na.rm = TRUE),
            n = n()
        ) |>
        collect() |>
        filter(rpd_date <= corte) |>
        mutate(anios = as.numeric(corte - rpd_date) / 365.25)

    crossing(tasa = tasas, c_rate = c_rates) |>
        mutate(
            n_retiros  = sum(diario$n),
            monto_2024 = sum(diario$monto),
            canal1 = map_dbl(tasa, \(r) sum(diario$monto * (1 + r)^diario$anios)),
            # Masa salarial mensual capitalizada: el Canal 2 es esta base por la tasa de
            # cotización y por los meses de desempleo adicional.
            base2  = map_dbl(tasa, \(r) sum(diario$salario * dias_mes * (1 + r)^diario$anios)),
            canal2    = base2 * c_rate * meses[["central"]],
            canal2_lo = base2 * c_rate * meses[["lo"]],
            canal2_hi = base2 * c_rate * meses[["hi"]],
            total     = canal1 + canal2,
            total_lo  = canal1 + canal2_lo,
            total_hi  = canal1 + canal2_hi,
            markup    = total / canal1,
            markup_lo = total_lo / canal1,
            markup_hi = total_hi / canal1,
            # Variante sin capitalizar: costo en pesos de 2024 al momento del retiro.
            canal2_nom = sum(diario$salario) * dias_mes * c_rate * meses[["central"]],
            total_nom  = monto_2024 + canal2_nom,
            markup_nom = total_nom / monto_2024
        ) |>
        select(!base2)
}

# Flujo anual de retiros ---------------------------------------------------------------
# El costeo de las propuestas 2 y 3 (§06) necesita el flujo reciente de retiros, no el
# acumulado desde 1997: el uso del programa está muy cargado hacia los últimos años, de
# modo que un promedio sobre las 28 cohortes subestimaría el costo actual por un factor
# grande. `n_bajo`/`monto_bajo` acotan el grupo objetivo del componente solidario
# (usuarios por debajo de la mediana salarial del propio registro).

compute_retiros_anuales_esp <- function(path, corte = as.Date("2024-12-31")) {
    con <- DBI::dbConnect(duckdb::duckdb())
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

    retiros <- here(path) |>
        open_dataset(format = "feather") |>
        to_duckdb(con) |>
        filter(amount_withdrawn > 0, wage_rpd > 0, rpd_date <= corte)

    mediana <- retiros |>
        summarise(w = quantile(wage_rpd, 0.5)) |>
        collect() |>
        pull(w)

    # Se agrega por día en duckdb y se anualiza en R: evita depender de la traducción
    # de funciones de fecha al motor SQL, y el resultado diario cabe holgadamente en RAM.
    diario <- retiros |>
        mutate(bajo = as.integer(wage_rpd < mediana)) |>
        group_by(rpd_date) |>
        summarise(
            n = n(),
            monto = sum(amount_withdrawn),
            n_bajo = sum(bajo),
            monto_bajo = sum(bajo * amount_withdrawn)
        ) |>
        collect()

    diario |>
        mutate(anio = as.integer(format(rpd_date, "%Y"))) |>
        summarise(
            .by = anio,
            n = sum(n),
            monto = sum(monto),
            n_bajo = sum(n_bajo),
            monto_bajo = sum(monto_bajo)
        ) |>
        arrange(anio) |>
        mutate(mediana_salario = mediana)
}

compute_retiro_saldo_shares_esp <- function(path, corte = as.Date("2024-12-31")) {
    con <- DBI::dbConnect(duckdb::duckdb())
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

    retiros <- here(path) |>
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
        # `share` no se puede reusar dentro del mismo mutate() en dbplyr: va aparte.
        # Art. 198 LSS: el retiro descuenta semanas en la misma proporción que el saldo.
        mutate(semanas_perdidas = share * contributed_weeks_rpd)

    # Medianas de todo el registro, no de un quintil: se citan en el texto y no
    # son recuperables a partir de las medianas por quintil.
    totales <- retiros |>
        summarise(
            sem_p50_total = quantile(semanas_perdidas, 0.50),
            p50_total = quantile(share, 0.50)
        ) |>
        collect()

    retiros |>
        group_by(quintil) |>
        summarise(
            p25 = quantile(share, 0.25),
            p50 = quantile(share, 0.50),
            p75 = quantile(share, 0.75),
            media = mean(share),
            prop_25 = mean(as.integer(share >= 0.25)),
            sem_p25 = quantile(semanas_perdidas, 0.25),
            sem_p50 = quantile(semanas_perdidas, 0.50),
            sem_p75 = quantile(semanas_perdidas, 0.75),
            sem_media = mean(semanas_perdidas),
            # Acervo de semanas al momento del retiro: explica por qué el conteo
            # absoluto de semanas perdidas crece con el salario aunque la razón caiga.
            sem_acum_p50 = quantile(contributed_weeks_rpd, 0.50),
            salario_mediano = median(wage_rpd),
            n = n()
        ) |>
        collect() |>
        arrange(quintil) |>
        mutate(
            sem_p50_total = totales$sem_p50_total,
            p50_total = totales$p50_total
        )
}

# Los dos paneles de @fig-retiro-saldo comparten geometría y solo cambian de unidad,
# así que se construyen con el mismo helper: mediana como punto, RIC como barra.
fig_quintil_esp <- function(shares, lo, mid, hi, y_lab, y_labels) {
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
        ggplot(aes(x = quintil_lab, y = .data[[mid]])) +
        geom_linerange(
            aes(ymin = .data[[lo]], ymax = .data[[hi]]),
            linewidth = 0.6, color = "gray55"
        ) +
        geom_point(size = 2.2) +
        scale_y_continuous(labels = y_labels) +
        labs(
            # Título corto: los paneles van a media plana y uno más largo se desborda.
            # El pie de figura precisa que es el salario diario real al momento del retiro.
            x = "Quintil del salario del usuario",
            y = y_lab
        ) +
        tema_esp()
}

make_fig_retiro_saldo_esp <- function(shares) {
    fig_quintil_esp(
        shares,
        lo = "p25", mid = "p50", hi = "p75",
        y_lab = "Retiro como proporción del saldo RCV",
        y_labels = scales::label_percent(accuracy = 1)
    )
}

make_fig_retiro_semanas_esp <- function(shares) {
    fig_quintil_esp(
        shares,
        lo = "sem_p25", mid = "sem_p50", hi = "sem_p75",
        y_lab = "Semanas de cotización descontadas",
        y_labels = scales::label_number(accuracy = 1)
    )
}

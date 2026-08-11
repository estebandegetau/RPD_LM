# Insuring Unemployment Out of Your Own Pension

**Causal Evidence from Mexico's *Retiro Parcial por Desempleo***

Mexico's *Retiro Parcial por Desempleo* (RPD) lets unemployed workers withdraw part of their
own pension savings — a quasi unemployment insurance financed by the worker's retirement
account rather than by a common pool. This repository holds the data pipeline, estimation
code and manuscripts for a paper that asks what that arrangement does to labor market
outcomes, using administrative social-security records and a fuzzy regression discontinuity
design around the eligibility threshold of two years of contributions.

Eligibility raises program take-up by 3.6 percentage points twelve months after displacement.
For compliers, RPD use prolongs time out of formal employment by about 36 weeks over three
years, with no detectable gain in reemployment wages, job stability, or cumulative formal
earnings — and because those are weeks without pension contributions, the cost compounds
until retirement.

## Read the paper

- **Working paper (HTML and PDF):** <https://www.estebandegetau.com/RPD_LM/>

Rendered output is not tracked here — a clone contains only source. The built paper is
published from the `gh-pages` branch.

## Data availability

**The data is not in this repository and cannot be shared.** The analysis runs on
confidential administrative microdata from the Instituto Mexicano del Seguro Social (IMSS),
covering individual social-security contribution histories. All of `data/` is gitignored.

The scripts in `R-scripts/` document exactly how the raw extracts are turned into the two
analysis-ready artifacts the pipeline consumes — `data/working/rpd.RData` and
`data/temp/withdraws_clean.feather` — so the construction of the sample is auditable even
though the inputs are not distributable. Questions about the data or the sample construction
are welcome at <estebandegetau@gmail.com>.

## Repository map

```
_targets.R            the analysis DAG (~100 targets): data -> RD estimates -> figures -> manuscripts
R/                    function library, sourced into the pipeline by tar_source()
  rd_functions.R        core of the project: my_rd() / my_fuzzy_rd() wrapping {rdrobust}
  data.R                load the analysis sample; balance and summary tables
  results.R             result figures and tables
  rpd.R                 program-usage statistics
  amafore_esp.R         Spanish-language targets for the AMAFORE submission
  slides_en.R           targets for the English conference deck
  grf.R, power_sim.R    exploratory (causal forests, power simulation); disabled in the pipeline
R-scripts/            one-off preparation of the raw IMSS extracts -- NOT part of the pipeline
  RDD/00_run.R          entry point; stages 01-11 build the RD analysis dataset
  DID/, unused/         an exploratory diff-in-diff build, and superseded code
sections/             the prose, shared by the thesis and the working paper
wp.qmd, thesis.qmd    working paper; MS thesis (adds sections/lit-review.qmd)
amafore/, iab/        prize and conference submissions (see below)
slides/               conference talk (Beamer)
_quarto*.yml          one Quarto profile per output document
.devcontainer/        pinned build environment
```

Two things are worth knowing before reading further.

**`R/` and `R-scripts/` play opposite roles.** `R/` is a library of functions that the
`targets` pipeline calls. `R-scripts/` is a sequence of one-off scripts that run *outside* the
pipeline to build its inputs from the raw administrative files.

**One analysis, many documents.** `_targets.R` builds a single store of estimates, figures
and tables. Every manuscript is then a view on that store, selected by a Quarto profile: the
working paper, the master's thesis it grew out of, a Spanish adaptation submitted to the
AMAFORE/ITAM pensions research prize (`amafore/`), an extended abstract for the IAB GradAB
PhD workshop (`iab/`), and a conference deck (`slides/`).

## Running it

```r
renv::restore()          # install the pinned library
targets::tar_make()      # run the pipeline (requires the confidential data)
targets::tar_visnetwork()  # inspect the DAG and what is outdated
```

```sh
quarto render --profile wp        # also: thesis, amafore, iab, slides
```

`tar_make()` will not get far without the IMSS data. Everything else — the pipeline
definition, the estimators, the prose — is readable and reviewable as-is.

## Environment

A VS Code Dev Container in `.devcontainer/` pins the whole toolchain: `rocker/verse:4.5.1`
(R 4.5.1, matching `renv.lock`), Quarto 1.9.38, and the LaTeX and system libraries the
renders need. `renv` is activated automatically by `.Rprofile`; the lockfile is the source of
truth for R packages.

## Citation

> Degetau, Esteban (2026). *Insuring Unemployment Out of Your Own Pension: Causal Evidence
> from Mexico's Retiro Parcial por Desempleo.* Working paper.
> <https://www.estebandegetau.com/RPD_LM/>

A machine-readable version is in [CITATION.cff](CITATION.cff).

## License

The **code** in this repository (`R/`, `R-scripts/`, `_targets.R`) is released under the MIT
License — see [LICENSE](LICENSE). The **manuscript text and figures** (`sections/`, the
`.qmd` documents) are licensed
[CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/).

## Contact

Esteban Degetau — Instituto Mexicano del Seguro Social and Barcelona School of Economics
<estebandegetau@gmail.com> · [ORCID 0009-0004-4095-8819](https://orcid.org/0009-0004-4095-8819)

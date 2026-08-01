# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

An empirical economics research paper (master's thesis + working paper): *The Labor Market Outcomes of (Quasi) Unemployment Insurance — Evidence from the Retiro Parcial por Desempleo (RPD) in Mexico*. The analysis estimates the causal effect of Mexico's RPD pension-withdrawal program on unemployment duration and reemployment outcomes using a **fuzzy regression discontinuity design** (eligibility threshold = 2 years of social-security contributions).

This is an R project, not application code. There is no build/lint/test suite — "running" the project means executing a reproducible analysis pipeline that produces figures, tables, and rendered manuscripts.

## Toolchain

- **`renv`** — dependency management. `.Rprofile` auto-activates it. Restore the library with `renv::restore()` before anything else.
- **`targets`** / **`tarchetypes`** — pipeline orchestration. The entire analysis is a DAG defined in `_targets.R`.
- **`quarto`** — manuscript rendering. The pipeline renders Quarto documents as final targets.
- Packages are loaded with `pacman::p_load(...)`, not `library()`. The custom package `citools` is installed from GitHub (`estebandegetau/citools`).

## Common commands

```r
renv::restore()                  # install pinned dependencies
targets::tar_make()              # run the full pipeline (data → estimates → figures → manuscripts)
targets::tar_visnetwork()        # visualize the DAG and what's outdated
targets::tar_read(rpd_data)      # read a single target's value
targets::tar_load(rd_data)       # load a target into the global env

# Debug a target interactively (no separate R process, honors browser()):
targets::tar_make(callr_function = NULL, use_crew = FALSE, as_job = FALSE)
```

Render a manuscript directly (outside the pipeline) by selecting a Quarto profile:

```sh
quarto render thesis.qmd --profile thesis   # PDF thesis
quarto render wp.qmd --profile wp           # HTML + PDF working paper
```

## Architecture

The flow is **raw admin data → prep scripts → analysis-ready `.RData`/`.feather` → targets pipeline → manuscript**.

### Two distinct R directories — do not confuse them

- **`R/`** — the function library. `tar_source()` sources all of it so every function is available to `tar_target()` commands. Edit these when changing *analysis logic*. Files:
  - `data.R` — load `rpd.RData`, build balance/summary tables (`gtsummary`).
  - `rd_functions.R` — **core of the project.** `my_rd()` / `my_fuzzy_rd()` wrap `rdrobust`; `prepare_rd_data()` reshapes outcomes into a nested long tibble grouped by domain (Covariates, Previous job, Survival, Next job, Medium term, Take up); `compute_rd_group()` / `compute_fuzzy_rd_group()` map the estimators over each group; `split_data()` + `map_rd()` produce heterogeneity splits; includes custom broom methods `tidy.rdrobust` / `glance.rdrobust` for `modelsummary`.
  - `results.R` — figures and result tables (eligibility, sample selection, covariate/survival/duration plots, RD "path" plots across bandwidths).
  - `rpd.R` — RPD program-usage stats/plots, read from a `.feather` arrow dataset.
  - `grf.R` — causal/instrumental forest exploration (`grf`, `policytree`). **Mostly commented out** in `_targets.R`.
  - `power_sim.R` — MDE power simulation. Also commented out in the pipeline.
- **`R-scripts/`** — one-off raw-data preparation scripts that are **NOT part of the targets pipeline**. They read confidential IMSS administrative CSVs and write the analysis-ready artifacts the pipeline consumes. Run them via `R-scripts/RDD/00_run.R`, which uses `.run_NN <- 0/1` toggle flags to select stages.
  - `R-scripts/RDD/01..11` — the main RDD prep pipeline (parquet → clean → tidy intervals → unemployment spells → merge RPD → deflate wages → outcomes → subset → rd prep).
  - `R-scripts/DID/` — an alternative difference-in-differences data build (exploratory).
  - `R-scripts/unused/` — superseded code; ignore unless explicitly asked.

### Pipeline structure (`_targets.R`)

Targets group into: data loading & descriptive tables → RD assumption checks (density/manipulation, covariate balance) → sharp RD by outcome group → fuzzy/IV RD → heterogeneity splits (income, age, gender, COVID exposure, entry year) → result figures → two `tar_quarto()` manuscript renders (`thesis`, `wp`). Commented-out blocks (random forest, power sim, presentation render) are intentionally disabled.

### Manuscript layout

- `thesis.qmd` — assembles the thesis from `sections/*.qmd` via `{{< include >}}`. `wp.qmd` is the working-paper variant.
- `sections/` — prose chapters (intro, lit-review, background, data, empirical-strategy, results, conclusion, appendix).
- Quarto **profiles** drive format: `_quarto.yml` is the base (default profile `wp`); `_quarto-thesis.yml` adds PDF thesis settings; `_quarto-wp.yml` adds HTML+PDF working-paper settings. `tar_quarto(..., profile = "...")` picks one.
- `notebooks/`, `presentations/` — supplementary Quarto docs and Beamer slides.
- `references.bib` — bibliography.

## Development container

The environment is pinned in `.devcontainer/` (VS Code Dev Containers). Requires the `ms-vscode-remote.remote-containers` extension — *not* `ms-azuretools.vscode-containers`, which is a different extension and cannot attach.

- **Base image `rocker/verse:4.5.1`** — matches the R version in `renv.lock`. It is Ubuntu noble, so the repo's existing `renv/library/linux-ubuntu-noble/R-4.5` binaries are reused directly; the Dockerfile asserts the codename and fails the build if the base ever changes.
- **The image adds** system libraries for the packages that need them (`libv8-dev`, the freetype/harfbuzz font stack, `gfortran` for `grf`/`policytree`/`rdrobust`/`nloptr`), the Quarto CLI pinned to 1.9.38, and `poppler-utils` for inspecting rendered PDFs. Anything previously installed ad hoc belongs in the Dockerfile now.
- **LaTeX: install collections, not individual packages.** The base image's TeX Live is minimal enough to be missing basic math font metrics (`cmex7.tfm`), so adding packages one at a time turns into whack-a-mole. The Dockerfile installs `collection-fontsrecommended` + `collection-latexrecommended` (~435 MB total), plus the `libertinus` family for the `fontfamily: libertinus` setting and the `gt_packages.sty` dependencies. If a render reports a missing `.sty`/`.tfm`, or logs `installing <pkg>`, add it to the Dockerfile — do not `tlmgr install` inside a running container, since that is lost on rebuild.
- **Renders must work with no network.** Quarto's TinyTeX silently downloads missing LaTeX packages mid-render, which hides dependencies and breaks offline/reproducible builds. The package list was derived by rendering until no auto-installs remained; verify with `docker run --network none ... quarto render`.
- **Paths inside the container are identical to the host.** The repo is bind-mounted at `/home/estebandegetau/projects/RPD_LM` (not `/workspace`), and the container user is renamed to `estebandegetau` with home `/home/estebandegetau`. This is required, not cosmetic: the renv library is ~217 **absolute symlinks** into `~/.cache/R/renv/cache/...`, so a different home dangles all of them and renv reports ~198 packages missing. Path parity also keeps the absolute `store:` paths in both `_targets.yaml` files valid and Claude Code's project memory continuous.
- **`remoteUser` is `estebandegetau` (uid/gid 1000), not root** — matching the host account, so bind-mounted files keep correct ownership. A root `remoteUser` is what leaves root-owned files behind in the project directory.
- **R packages are not baked into the image.** `renv.lock` stays the source of truth; `renv::restore()` runs as `postCreateCommand` against the host's bind-mounted renv cache, so it is effectively a no-op. It is wrapped in `try()` because `renv.lock` still records `taskscheduleR`, a Windows-only leftover that cannot install on Linux.
- Rebuild after editing the Dockerfile with "Dev Containers: Rebuild Container", or `docker build -t rpd_lm:dev .devcontainer/`.

## Important constraints

- **Data is not in the repo, but is present locally on this machine.** All `data/`, `*.RData`, `*.feather`, `*.parquet`, `*.csv` are gitignored — this is confidential IMSS administrative data — but `data/working/rpd.RData` and `data/temp/withdraws_clean.feather` (produced by the `R-scripts/` prep step) already exist on disk here. `tar_make()` can run downstream targets on this machine; do not assume the data is missing.
- **Memory is the binding constraint, and the container does not relax it.** Docker runs inside the same WSL2 VM as the host and shares its ~7.6 GB ceiling — containers get no extra headroom. Heavy targets that load `rd_data` must still run one per R process, and `rdrobust` figures must still be rendered in `callr` subprocesses (see `fig_esp()` in `amafore/index.qmd`). If a render dies with no error message, assume the R process was OOM-killed and split the build.
- **`_targets.yaml`'s `store:` path is absolute** (`/home/estebandegetau/projects/RPD_LM/_targets`, a populated store — not the Windows OneDrive path from earlier history). Because the container mounts the repo at that same path, it resolves both on the host and inside the container with no setup. A machine that mounts the repo elsewhere would still need this updated.

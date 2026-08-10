# Rendering from inside slides/ starts R here, so the repo-root .Rprofile (which
# calls renv/activate.R) is never sourced and .libPaths() collapses to the system
# library -- targets, ggpubr and rdrobust all disappear. Rendering from the repo
# root is unaffected, but this makes `cd slides && quarto render` work too.
# Same file as amafore/.Rprofile.
root_lib <- normalizePath(
  file.path("..", "renv", "library", "linux-ubuntu-noble", "R-4.5", "x86_64-pc-linux-gnu"),
  mustWork = FALSE
)
if (dir.exists(root_lib)) {
  .libPaths(c(root_lib, .libPaths()))
}

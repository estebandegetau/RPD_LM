# Environment for the RPD_LM analysis pipeline.
#
# Base tag matches the R version pinned in renv.lock (4.5.1) so that the
# project's existing renv/library/linux-ubuntu-noble/R-4.5 binaries are
# reusable as-is when the repo is bind-mounted in.
FROM rocker/verse:4.5.1

# The bind-mounted renv library is keyed on the OS codename. If the base image
# ever stops being Ubuntu noble, renv would silently rebuild every package from
# source instead of reusing it -- fail the build loudly instead.
RUN . /etc/os-release && [ "$VERSION_CODENAME" = "noble" ] \
    || { echo "ERROR: expected Ubuntu noble, got '$VERSION_CODENAME'"; exit 1; }

# System libraries for packages in renv.lock that need them:
#   V8 -> libv8-dev; curl/openssl/xml2/stringi -> their dev headers;
#   systemfonts/ragg/textshaping -> the freetype/fontconfig/harfbuzz stack;
#   grf, policytree, rdrobust, nloptr -> a Fortran compiler;
#   arrow -> cmake, only used if it falls back to a source build.
# poppler-utils provides pdfinfo/pdftotext for verifying rendered PDFs.
RUN apt-get update && apt-get install -y --no-install-recommends \
      libv8-dev \
      libcurl4-openssl-dev \
      libssl-dev \
      libxml2-dev \
      libicu-dev \
      libfreetype6-dev \
      libfontconfig1-dev \
      libharfbuzz-dev \
      libfribidi-dev \
      libpng-dev \
      libtiff-dev \
      libjpeg-dev \
      gfortran \
      cmake \
      poppler-utils \
    && rm -rf /var/lib/apt/lists/*

# Quarto CLI is not managed by renv; pin it to the version used on the host.
ARG QUARTO_VERSION=1.9.38
RUN curl -fsSL -o /tmp/quarto.deb \
      "https://github.com/quarto-dev/quarto-cli/releases/download/v${QUARTO_VERSION}/quarto-${QUARTO_VERSION}-linux-amd64.deb" \
    && dpkg -i /tmp/quarto.deb \
    && rm /tmp/quarto.deb
#
# LaTeX. The base image ships a minimal TeX Live that is missing even basic
# math font metrics (cmex7.tfm), so installing individual packages turns into
# whack-a-mole. The two standard collections cover the manuscripts' needs and
# only bring TeX Live to ~435 MB.
#
# On top of them: the libertinus family (fontfamily: libertinus in
# _quarto-{thesis,wp}.yml; Quarto renders with LuaTeX, so the otf variant is
# needed as well as type1), plus the packages the gt-generated gt_packages.sty
# pulls in. array, longtable, lscape, geometry and amsmath are already present
# in the base image.
#
# lualatex-math, libertinus-otf, libertine, anyfontsize and multirow are here
# because Quarto's TinyTeX silently downloads them mid-render otherwise. That
# makes renders depend on the network and fail in an offline container, so the
# list was derived by rendering until no further auto-installs occurred. If a
# render starts reporting "installing <pkg>", add it here.
RUN tlmgr install \
      collection-fontsrecommended \
      collection-latexrecommended \
      libertinus \
      libertinus-type1 \
      libertinus-otf \
      libertinus-fonts \
      libertine \
      fontaxes \
      anyfontsize \
      lualatex-math \
      booktabs \
      caption \
      float \
      colortbl \
      multirow \
      ulem \
      makecell \
      siunitx \
    && tlmgr path add

# Mirror the host account exactly. This is not cosmetic: the project's renv
# library consists of ~217 ABSOLUTE symlinks into
# /home/estebandegetau/.cache/R/renv/cache/... Giving the container user the
# same name and home directory means renv resolves the same cache path and
# those symlinks stay valid. With a different home (e.g. /home/rstudio) every
# link dangles, renv reports ~198 packages missing, and a restore would
# rebuild them from source into the bind-mounted library.
#
# The base image's rstudio user is already uid/gid 1000, matching the host, so
# renaming preserves correct ownership of bind-mounted files.
RUN usermod -l estebandegetau -d /home/estebandegetau -m rstudio \
    && groupmod -n estebandegetau rstudio

# Pre-create the bind-mount points with the right ownership; otherwise Docker
# creates any missing ones as root inside the user's home.
RUN mkdir -p /home/estebandegetau/projects/RPD_LM \
             /home/estebandegetau/.claude \
             /home/estebandegetau/.cache/R/renv \
    && chown -R estebandegetau:estebandegetau /home/estebandegetau

# Note: no USER instruction. devcontainer.json sets remoteUser instead, so that
# Dev Container features still install as root.

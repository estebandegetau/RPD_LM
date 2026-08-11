// Official ORCID iD icon, embedded so that the template does not depend on
// the fontawesome Typst package. https://info.orcid.org/brand-guidelines/
#let orcid-icon = ```
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 256 256">
  <path fill="#A6CE39" d="M256 128c0 70.7-57.3 128-128 128S0 198.7 0 128 57.3 0 128 0s128 57.3 128 128z"/>
  <path fill="#FFF" d="M86.3 186.2H70.9V79.1h15.4v107.1z"/>
  <path fill="#FFF" d="M108.9 79.1h41.6c39.6 0 57 28.3 57 53.6 0 27.5-21.5 53.6-56.8 53.6h-41.8V79.1zm15.4 93.3h24.5c34.9 0 42.9-26.5 42.9-39.7 0-21.5-13.7-39.7-43.7-39.7h-23.7v79.4z"/>
  <path fill="#FFF" d="M88.7 56.8c0 5.5-4.5 10.1-10.1 10.1-5.6 0-10.1-4.6-10.1-10.1 0-5.6 4.5-10.1 10.1-10.1 5.6 0 10.1 4.6 10.1 10.1z"/>
</svg>
```.text

#let article(
  // Document metadata
  title: none,
  subtitle: none,
  authors: none,
  date: none,
  abstract: none,
  abstract-title: "ABSTRACT",
  // PDF Metadata
  title-meta: none,
  author-meta: none,
  keywords-meta: none,
  date-meta: none,
  // Custom document metadata
  header: none,
  code-repo: none,
  keywords: none,
  custom-keywords: none,
  thanks: none,
  // Layout settings
  margin: (x: 1.25in, y: 1.25in),
  paper: "us-letter",
  // Typography settings
  lang: "en",
  region: "US",
  font: "libertinus serif",
  fontsize: 11pt,
  sansfont: "libertinus sans",
  mathfont: "New Computer Modern Math",
  link-color: rgb("#483d8b"),
  // Structure settings
  sectionnumbering: none,
  pagenumbering: "1",
  toc: false,
  cols: 1,
  doc,
) = {
  set document(
    title: title-meta,
    author: author-meta,
    keywords: keywords-meta,
    date: date-meta,
  )
  set page(
    paper: paper,
    margin: margin,
    numbering: pagenumbering,
  )
  set par(justify: true)
  set text(
    lang: lang,
    region: region,
    font: font,
    size: fontsize,
  )
  show math.equation: set text(font: mathfont)
  set heading(numbering: sectionnumbering)
  show heading: set text(font: sansfont, weight: "semibold")

  // Table cells are narrow, and the document-wide `par(justify: true)` above
  // turns that into stretched word spacing and broken words -- a results-table
  // header reading "Dura-tion - 36 months" looks like a typesetting fault
  // rather than a line break. Cells wrap at spaces instead.
  show table: set text(hyphenate: false)
  show table: set par(justify: false)

  // Explanatory notes written inside a `::: {#fig-…}` div are part of the figure
  // body, and Typst centres that body. Centring suits the image and not a
  // paragraph of prose: it leaves the note's last line floating mid-column.
  // Figure bodies are set flush left, with images re-centred on their own.
  show figure: it => {
    show image: img => align(center, img)
    set align(left)
    it
  }

  show figure.caption: it => context [
    #set text(font: sansfont, size: 0.9em)
    #if it.supplement == [Figure] {
      set align(left)
      text(weight: "semibold")[#it.supplement #it.counter.display(it.numbering): ]
      it.body
    } else {
      text(weight: "semibold")[#it.supplement #it.counter.display(it.numbering): ]
      it.body
    }

  ]

  show ref: it => {
    let eq = math.equation
    let el = it.element
    if el == none {
      it
    } else if el.func() == eq {
      // Built with `link(loc, body)` rather than a bracketed markup block: a
      // block written across several lines contributes both a leading and a
      // trailing space, which surfaces as "Table 1 ." wherever a reference is
      // followed by punctuation. Colour comes from the `show link` rule below.
      link(el.location(), numbering(el.numbering, ..counter(eq).at(el.location())))
    } else if el.func() == figure and type(el.numbering) == str {
      // Top-level floats only. Quarto gives subfloats a *closure* for
      // `numbering` which reads `quartosubfloatcounter` from the surrounding
      // context; calling it from the reference site (as this branch does)
      // evaluates that counter at the wrong place and renders every subfloat in
      // a group as "(a)". Those fall through to Quarto's own ref handling.
      el.supplement.text + " "
      link(el.location(), numbering(el.numbering, ..el.counter.at(el.location())))
    } else {
      it
    }
  }

  show link: set text(fill: link-color)
  set bibliography(title: "References")

  if date != none {
    align(left)[#block()[
        #text(weight: "semibold", font: sansfont, size: 0.8em)[
          #date
          #if header != none {
            h(3em)
            text(weight: "regular")[#header]
          }
        ]
      ]]
  }

  if code-repo != none {
    align(left)[#block()[
        #text(weight: "regular", font: sansfont, size: 0.8em)[
          #code-repo
        ]
      ]]
  }

  if title != none {
    align(left)[#block(spacing: 4em)[
        #text(weight: "semibold", size: 1.5em, font: sansfont)[
          #title
          #if thanks != none {
            footnote(numbering: "*", thanks)
          }\
          #if subtitle != none {
            text(weight: "regular", style: "italic", size: 0.8em)[#subtitle]
          }
        ]
      ]]
  }
  
  if authors != none {
    let count = authors.len()
    let ncols = calc.min(count, 3)
    grid(
      columns: (1fr,) * ncols,
      row-gutter: 1.5em,
      ..authors.map(author => align(left)[
        #text(size: 1.2em, font: sansfont)[#author.name]
        #if author.orcid != [] {
          link("https://orcid.org/" + author.orcid.text)[
            #box(
              baseline: 15%,
              image(bytes(orcid-icon), format: "svg", width: 0.85em),
            )
          ]
        } \
        #text(size: 0.85em, font: sansfont)[#author.affiliation] \
        #text(size: 0.7em, font: sansfont, fill: link-color)[
          #link("mailto:" + author.email.children.map(email => email.text).join())[#author.email]
        ]
      ])
    )
  }

  if abstract != none {
    block(inset: 2em)[
      #text(weight: "semibold", font: sansfont, size: 0.9em)[#abstract-title] #h(0.5em)
      #text(font: sansfont)[#abstract]
      #if keywords != none {
        text(weight: "semibold", font: sansfont, size: 0.9em)[\ Keywords:]
        h(0.5em)
        text(font: sansfont)[#keywords]
      }
      #if custom-keywords != none {
        for it in custom-keywords {
          text(weight: "semibold", font: sansfont, size: 0.9em)[\ #it.name:]
          h(0.5em)
          text(font: sansfont)[#it.values]
        }
      }
    ]
  }

  if toc {
    block(above: 0em, below: 2em)[
      #outline(
        title: auto,
        depth: none,
      );
    ]
  }

  if cols == 1 {
    doc
  } else {
    columns(cols, doc)
  }
}

#let appendix(content) = {
  // Reset Numbering
  set heading(numbering: "A.1.1")
  counter(heading).update(0)
  counter(figure.where(kind: "quarto-float-fig")).update(0)
  counter(figure.where(kind: "quarto-float-tbl")).update(0)

  // Figure & Table Numbering
  set figure(
    numbering: it => {
      [A.#it]
    },
  )

  // Appendix Start
  pagebreak(weak: true)
  // Emitted as a heading rather than as plain sized text so that the document's
  // `show heading` rule applies and the title is set in the same face as every
  // other heading. `numbering: none` keeps it out of the A/B sequence that the
  // rule above starts, which belongs to the appendix's own sections.
  block(
    below: 1.5em,
    text(size: 1.8em, heading(level: 1, numbering: none, outlined: false)[Appendix]),
  )
  content
}

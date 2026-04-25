-- Replace remote-only images (e.g. GitHub CI badges) with plain text so LaTeX PDF builds do not fail on SVG/HTTP.
function Image(el)
  if el.src:match("^https?://") then
    return pandoc.Para({ pandoc.Emph(pandoc.Str("[Remote image omitted in PDF]")) })
  end
  return el
end

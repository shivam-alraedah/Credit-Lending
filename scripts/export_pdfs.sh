#!/usr/bin/env bash
# Export key Markdown deliverables to PDF (pandoc + LaTeX engine).
# On macOS without a TeX install, install BasicTeX or MacTeX, or rely on CI artifacts.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${ROOT}/exports/pdf"
mkdir -p "${OUT}"

PDF_ENGINE=""
for e in xelatex pdflatex lualatex; do
  if command -v "${e}" >/dev/null 2>&1; then
    PDF_ENGINE="${e}"
    break
  fi
done

FILES=(
  docs/design_criteria_map.md
  docs/architecture.md
  docs/tradeoffs_production_readiness.md
  docs/execution_plan_30_60_90.md
  ifrs9/DESIGN.md
)

if [[ -z "${PDF_ENGINE}" ]]; then
  echo "No LaTeX engine (xelatex/pdflatex/lualatex) found in PATH." >&2
  echo "Install TeX (e.g. macOS: brew install --cask basictex) or download PDFs from the GitHub Actions artifact." >&2
  echo "Writing HTML fallbacks to ${OUT} …" >&2
  for f in "${FILES[@]}"; do
    [[ -f "${ROOT}/${f}" ]] || { echo "missing ${f}" >&2; exit 1; }
    base="$(basename "${f}" .md)"
    pandoc "${ROOT}/${f}" \
      -f markdown \
      -t html5 \
      --standalone \
      --metadata title="${base}" \
      -o "${OUT}/${base}.html"
  done
  exit 0
fi

for f in "${FILES[@]}"; do
  [[ -f "${ROOT}/${f}" ]] || { echo "missing ${f}" >&2; exit 1; }
  base="$(basename "${f}" .md)"
  pandoc "${ROOT}/${f}" \
    -f markdown \
    -o "${OUT}/${base}.pdf" \
    --pdf-engine="${PDF_ENGINE}" \
    -V geometry:margin=1in \
    -V fontsize=11pt \
    --toc
done

echo "PDFs written under ${OUT}"

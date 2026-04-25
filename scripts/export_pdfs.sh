#!/usr/bin/env bash
# Build only the three submission PDFs (page-capped). Docker (pandoc/latex) or local pandoc+xelatex.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${ROOT}/exports/pdf"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMG_FILTER="${SCRIPT_DIR}/pdf_strip_remote_images.lua"
TMPDIR="${OUT}/.tmp"
mkdir -p "${TMPDIR}"

PDF_ENGINE=""
for e in xelatex pdflatex lualatex; do
  command -v "${e}" >/dev/null 2>&1 && PDF_ENGINE="${e}" && break
done
USE_DOCKER=""
[[ -z "${PDF_ENGINE}" ]] && command -v docker >/dev/null 2>&1 && USE_DOCKER=1

run_pandoc() {
  local src_rel="$1"
  local out_rel="$2"
  if [[ -n "${PDF_ENGINE}" ]]; then
    pandoc "${ROOT}/${src_rel}" -f markdown -o "${ROOT}/${out_rel}" \
      --lua-filter="${IMG_FILTER}" --pdf-engine="${PDF_ENGINE}" \
      -V geometry:margin=0.75in -V fontsize=10pt --toc --toc-depth=2
  elif [[ -n "${USE_DOCKER}" ]]; then
    docker run --platform linux/amd64 --rm -v "${ROOT}:/data" --entrypoint pandoc pandoc/latex:3.6 \
      "/data/${src_rel}" -o "/data/${out_rel}" \
      -f markdown --lua-filter="/data/scripts/pdf_strip_remote_images.lua" \
      --pdf-engine=xelatex -V geometry:margin=0.75in -V fontsize=10pt --toc --toc-depth=2
  else
    echo "Install Docker or MacTeX/BasicTeX (xelatex)." >&2
    exit 1
  fi
}

trim_pages() {
  local src_pdf="$1"
  local dst_pdf="$2"
  local maxp="$3"
  command -v gs >/dev/null 2>&1 || { cp "${src_pdf}" "${dst_pdf}"; return; }
  gs -q -sDEVICE=pdfwrite -dNOPAUSE -dBATCH -dSAFER \
    -dFirstPage=1 "-dLastPage=${maxp}" \
    -sOutputFile="${dst_pdf}" "${src_pdf}" 2>/dev/null || cp "${src_pdf}" "${dst_pdf}"
}

rm -f "${OUT}"/*.pdf
mkdir -p "${TMPDIR}"

echo "Building architecture (max 8 pages)…"
run_pandoc "docs/architecture.md" "exports/pdf/.tmp/_arch.pdf"
trim_pages "${TMPDIR}/_arch.pdf" "${OUT}/architecture.pdf" 8

echo "Building 30/60/90 plan (max 3 pages)…"
run_pandoc "docs/execution_plan_30_60_90.md" "exports/pdf/.tmp/_plan.pdf"
trim_pages "${TMPDIR}/_plan.pdf" "${OUT}/plan_30_60_90.pdf" 3

echo "Building trade-offs / product analysis (max 5 pages)…"
run_pandoc "docs/tradeoffs_production_readiness.md" "exports/pdf/.tmp/_trade.pdf"
trim_pages "${TMPDIR}/_trade.pdf" "${OUT}/tradeoffs_and_product_analysis.pdf" 5

rm -rf "${TMPDIR}"
echo "Done: ${OUT}/architecture.pdf  ${OUT}/plan_30_60_90.pdf  ${OUT}/tradeoffs_and_product_analysis.pdf"

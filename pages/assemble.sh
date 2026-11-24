#!/usr/bin/env bash
# Assemble the Jekyll source for GitHub Pages from the experiment write-ups.
# Usage: bash pages/assemble.sh <dest-dir>
set -euo pipefail

DEST=${1:?destination directory required}
GD=experiments/gliner-decide

rm -rf "$DEST"
mkdir -p "$DEST"
cp pages/_config.yml "$DEST/"
cp -r pages/_layouts pages/_includes "$DEST/"

# copy <src.md> <dest/index.md> <title> <description>
copy() {
  local src=$1 dst=$2 title=$3 desc=$4
  mkdir -p "$(dirname "$dst")"
  {
    printf -- '---\nlayout: doc\ntitle: "%s"\ndescription: "%s"\n---\n\n' "$title" "$desc"
    cat "$src"
  } > "$dst"
}

# index.md already carries front matter; copy verbatim
cp pages/index.md "$DEST/index.md"

copy "$GD/gliner2-decide-text-to-sql-finetuning.md" \
  "$DEST/blog/text-to-sql-finetuning/index.md" \
  'Why we taught a 300M-parameter model to route SQL questions — GLiNER2.5-Decide text-to-SQL fine-tuning' \
  'How we took a small GLiNER2.5-Decide classifier from 0% to 96.6% exact skeletons on a text-to-SQL task, and why the remaining 3.4% is a router problem.'

copy "$GD/BLOGPOST.md" \
  "$DEST/blog/25-questions/index.md" \
  'Teaching a decision model to write SQL: fine-tuning GLiNER2.5-Decide on 25 questions' \
  'Part 2 of the ARR-QA text-to-SQL series: LoRA fine-tuning a 340M decision model to emit SQL skeletons, a scorer bug, and what 25 labeled examples can and cannot do.'

copy "$GD/gliner2-decide-filter-generalization-report.md" \
  "$DEST/report/filter-generalization/index.md" \
  'Report: scaling GLiNER2.5-Decide text-to-SQL from 25 to 1,703 training questions' \
  'Per-head skeleton accuracy at scale: select, group, order and join heads reach 0.98–1.00; the filter decision head stays collapsed at 0.23 despite 100x more training data.'

copy "$GD/REPORT.md" \
  "$DEST/report/zero-shot-lora/index.md" \
  'ARR QA to SQL-skeleton classification with GLiNER2.5-Decide: zero-shot and 25-question LoRA experiment' \
  'Zero-shot GLiNER2.5-Decide fails (0% exact skeletons); a 25-question LoRA fine-tune reaches 54.8% exact skeletons and 41.9% execution accuracy on an A. R. Rahman song dataset.'

# diagrams sit next to the blog posts
cp -r "$GD/diagrams" "$DEST/blog/text-to-sql-finetuning/diagrams"
cp -r "$GD/diagrams" "$DEST/blog/25-questions/diagrams"

# rewrite in-document .md links to their site URLs
while IFS= read -r f; do
  sed -i \
    -e 's#](gliner2-decide-text-to-sql-finetuning\.md)#](/blog/text-to-sql-finetuning/)#g' \
    -e 's#](gliner2-decide-filter-generalization-report\.md)#](/report/filter-generalization/)#g' \
    -e 's#](REPORT\.md)#](/report/zero-shot-lora/)#g' \
    -e 's#](BLOGPOST\.md)#](/blog/25-questions/)#g' \
    "$f"
done < <(find "$DEST" -name '*.md')

echo "assembled site source in $DEST"

# Reporting Layer

Power BI semantic models and reports built on the Gold layer.

## Intended contents

- `<name>.SemanticModel` — one Power BI semantic model per consumer group (e.g., `main`, `business_dq_checks`)
- `<name>.Report` — Power BI reports bound to one of the semantic models

## Conventions

- Semantic models bind to gold lakehouse tables via shortcuts or direct lakehouse refs.
- Measures and calculation groups are managed via Tabular Editor scripts in `scripts/measure-derivatives-*.csx`.
- Keep one semantic model per *audience* (e.g., business users vs. ops/DQ) rather than one per dashboard.

The template ships `main.SemanticModel/` here as the starting semantic model. Add more as siblings (e.g., `business_dq_checks.SemanticModel/`).

# Ontology Alignment Curation App

This repository supports a pairwise + SQLite ontology alignment workflow and now includes a Streamlit app to run the full pipeline with intermediate previews and exports.

## Install dependencies (uv)

```bash
uv sync
```

## Run the app

```bash
uv run streamlit run streamlit_app.py
```

## Modules

- Overview
- Fetch schemas and ontologies - Download external sources + browse OLS catalog
- Extract term TSV from TTL
- Generate pairwise candidates (local/local or local/OLS)
- Curate candidate decisions (quick actions + batch edits)
- Review curated dataset and export TTL
- View schema documentation (pyLODE), interactive graph, RDFGlance linkout, and Mermaid export before/after curation
- Background sync - candidate TSVs are synced automatically to SQLite and reconciled exports
- Inspect SQLite tables and run SQL queries

## Ontology Terms Glossary

- `Ontologies`: distinct vocabularies/knowledge models loaded in OLS (for example CHEBI, EDAM, BIOLINK).
- `Classes`: concept types in those ontologies (for example `chemical entity`, `analysis`, `sample`).
- `Properties`: relationship or attribute predicates used to connect/describe entities (for example `part_of`, `has_role`, `label`).
- `Individuals`: concrete instances (named entities) rather than concept types.

## Core outputs

- `registry/reconciled_mappings.tsv`
- `registry/reconciled_canonical_groups.tsv`
- `registry/alignment_curation.sqlite`

## Collaboration workflow (git + PR)

Use shared schema files (no per-curator filenames) and collaborate through branches/PRs.

1. Pull latest `main` and create a branch: `curation/<schema>-<short-topic>`.
2. In the app sidebar, select:
   - `Source ID` (schema you curate)
   - `Curator` (your name/id)
3. Curate in the shared file for that schema:
   - `registry/pair_alignment_candidates_<source>.tsv`
4. Commit only relevant changes for your curation scope.
5. Open a PR with a short summary:
   - schema curated
   - terms reviewed
   - notable manual additions/rejections
6. Reviewer checks diff + app preview, then merges.

Notes:
- Reviewer attribution is stored in TSV `reviewer`/`date_reviewed` fields.
- SQLite/reconciled exports are auto-synced by the app and should not be manually edited.

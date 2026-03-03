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
- Step 0 - Download external sources
- Step 1 - Extract term TSV from TTL
- Step 2 - Generate pairwise candidates (local/local or local/OLS)
- Step 3 - Curate candidate decisions (quick actions + batch edits)
- Step 4-5 - Review curated dataset and export TTL
- Step 6 - Sync TSV to SQLite and export reconciled outputs
- Step 7 - Inspect SQLite tables and run SQL queries

## Ontology Terms Glossary

- `Ontologies`: distinct vocabularies/knowledge models loaded in OLS (for example CHEBI, EDAM, BIOLINK).
- `Classes`: concept types in those ontologies (for example `chemical entity`, `analysis`, `sample`).
- `Properties`: relationship or attribute predicates used to connect/describe entities (for example `part_of`, `has_role`, `label`).
- `Individuals`: concrete instances (named entities) rather than concept types.

## Core outputs

- `registry/reconciled_mappings.tsv`
- `registry/reconciled_canonical_groups.tsv`
- `registry/alignment_curation.sqlite`

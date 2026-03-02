# Pairwise + SQLite Workflow

This is the only supported workflow in this repo.

## Goal
Curate mappings from source ontology terms to canonical ontology terms, then export:
- `registry/reconciled_mappings.tsv` (source -> canonical)
- `registry/reconciled_canonical_groups.tsv` (canonical with associated source terms)
- `registry/alignment_curation.sqlite` (SQLite source of truth)

## 0) Download external ontology/vocabulary sources

```bash
scripts/download_external_sources.py --source-id emi
```

Or download all enabled sources defined in `registry/external_sources.tsv`:

```bash
scripts/download_external_sources.py --all
```

## 1) Extract EMI terms from TTL

```bash
scripts/extract_ttl_terms.py \
  registry/downloads/emi.ttl \
  https://w3id.org/emi# \
  registry/imports/emi_terms.tsv
```

The extracted TSV now includes curator context columns:
- `definition`
- `comment`
- `example`
- `domain_iris`
- `range_iris`
- `parent_iris`

## 2) Generate candidate matches with OLS

```bash
scripts/suggest_pairwise_alignments.py \
  --left-terms registry/imports/emi_terms.tsv \
  --left-source EMI \
  --use-ols-api \
  --ontologies chebi,obi,ms,chmo,edam \
  --ols-fetch-metadata \
  --request-timeout 3 \
  --max-left-terms 100 \
  --output registry/pair_alignment_candidates.tsv
```

Notes:
- `--ols-fetch-metadata` tries to pull `definition/comment/example` from OLS term endpoint.
- Metadata availability depends on the source ontology in OLS; some terms have none.

Candidate table includes:
- OLS suggestion columns (`right_*`)
- `ols_search_url` and `bioportal_search_url` for manual lookup

## 3) Curate 3 rows (manual edit in TSV)

Open `registry/pair_alignment_candidates.tsv` and curate three rows:

1. `EMI canonical is correct`:
- set `status=approved`
- set `canonical_from=left`
- leave `canonical_term_*` empty (auto-filled during finalize)

2. `OLS suggestion is correct`:
- set `status=approved`
- set `canonical_from=right`
- leave `canonical_term_*` empty (auto-filled during finalize)

3. `Need a new canonical term found via lookup URL`:
- use `ols_search_url` (or `bioportal_search_url`) from that row
- pick the correct external term
- set `status=approved`
- set `canonical_from=manual`
- set:
  - `canonical_term_iri`
  - `canonical_term_label`
  - `canonical_term_source`

Optional:
- set `reviewer`
- adjust `relation`
- update `notes`

Alternative (recommended) lightweight reviewer UI in terminal:

```bash
scripts/review_pair_candidates.py \
  --candidates-file registry/pair_alignment_candidates.tsv \
  --status-filter needs_review \
  --reviewer your_name
```

Actions per row:
- `1` approve-left
- `2` approve-right
- `3` approve-manual (prompts canonical IRI/label/source)
- `4` reject
- `5` skip

## 4) Finalize approved rows

```bash
scripts/finalize_pair_alignment_candidates.py --statuses approved
```

What this does:
- moves approved candidate rows to `registry/pair_alignments.tsv`
- creates `registry/pair_alignments.tsv` automatically if missing
- assigns stable `ALIGN_*` ids
- fills `date_reviewed` if empty
- normalizes/cleans review notes
- auto-fills canonical fields when `canonical_from=left|right`

## 5) Validate

```bash
scripts/validate_pair_alignments.py registry/pair_alignment_candidates.tsv --kind candidate
scripts/validate_pair_alignments.py registry/pair_alignments.tsv --kind curated
```

## 6) Sync into SQLite + export canonical outputs

```bash
scripts/sync_alignment_sqlite.py \
  --db registry/alignment_curation.sqlite \
  --pair-candidates registry/pair_alignment_candidates.tsv \
  --pair-alignments registry/pair_alignments.tsv \
  --status approved \
  --reconciled-output registry/reconciled_mappings.tsv \
  --grouped-output registry/reconciled_canonical_groups.tsv
```

## 7) Inspect canonical groups in SQLite

```bash
sqlite3 registry/alignment_curation.sqlite \
  "SELECT canonical_term_iri, canonical_term_label, mapped_term_count FROM reconciled_canonical_groups ORDER BY mapped_term_count DESC;"
```

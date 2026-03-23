# Pairwise + SQLite Workflow

This is the supported curation workflow in this repo.

## Goal
Keep one review TSV per schema in Git:

- `registry/pair_alignment_candidates_<source>.tsv`

This shared ledger is intentionally minimal. It stores only approved source-to-canonical mappings plus review provenance:

- `source_term_source`
- `source_term_iri`
- `source_term_label`
- `source_term_kind`
- `canonical_term_iri`
- `canonical_term_label`
- `canonical_term_source`
- `canonical_term_kind`
- `relation` (optional semantic mapping metadata)
- `status`
- `curator`
- `curator_name`
- `reviewer`
- `reviewer_name`
- `date_reviewed`
- `curation_comment`

Keep the local queue outside Git:

- `registry/work/pair_alignment_candidates_<source>.tsv`

Generate local cache/export artefacts when needed:

- `registry/reconciled_mappings.tsv` (source -> canonical)
- `registry/reconciled_canonical_groups.tsv` (canonical with associated source terms)
- `registry/alignment_curation.sqlite` (SQLite cache for inspection/app features)

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

## 2) Generate or refresh candidate matches with OLS

```bash
scripts/suggest_pairwise_alignments.py \
  --left-terms registry/imports/emi_terms.tsv \
  --left-source EMI \
  --use-ols-api \
  --ontologies chebi,obi,ms,chmo,edam \
  --ols-fetch-metadata \
  --request-timeout 3 \
  --max-left-terms 100 \
  --curated-alignments registry/pair_alignment_candidates_emi.tsv \
  --output registry/work/pair_alignment_candidates_emi.tsv
```

Notes:
- `--ols-fetch-metadata` tries to pull `definition/comment/example` from OLS term endpoint.
- Metadata availability depends on the source ontology in OLS; some terms have none.
- The output file is local-only. It can be focused to a subset of labels without shrinking the shared Git-tracked ledger.
- Approved review decisions are synced from the local queue into `registry/pair_alignment_candidates_<source>.tsv`.

Candidate table includes:
- OLS suggestion columns (`right_*`)
- `ols_search_url` and `bioportal_search_url` for manual lookup

## 3) Curate 3 rows (manual edit in TSV)

Open `registry/work/pair_alignment_candidates_emi.tsv` and curate rows there:

1. `EMI canonical is correct`:
- set `status=approved`
- set `canonical_from=left`
- fill `canonical_term_*` from the left term, or use the curation UI quick action which fills them automatically

2. `OLS suggestion is correct`:
- set `status=approved`
- set `canonical_from=right`
- fill `canonical_term_*` from the right term, or use the curation UI quick action which fills them automatically

3. `Need a new canonical term found via lookup URL`:
- use `ols_search_url` (or `bioportal_search_url`) from that row
- pick the correct external term
- set `status=approved`
- set `canonical_from=manual`
- set:
  - `canonical_term_iri`
  - `canonical_term_label`
  - `canonical_term_source`
  - `canonical_term_kind`

Optional:
- set `reviewer` to a valid ORCID
- ensure `reviewer_name` matches the public ORCID name
- adjust `relation`
- update `notes`

Alternative (recommended) lightweight reviewer UI in terminal:

```bash
scripts/review_pair_candidates.py \
  --candidates-file registry/work/pair_alignment_candidates_emi.tsv \
  --status-filter needs_review \
  --reviewer 0000-0002-1825-0097
```

Actions per row:
- `1` approve-left
- `2` approve-right
- `3` approve-manual (prompts canonical IRI/label/source)
- `4` reject
- `5` skip

Reviewer identity rules:
- `curator` may be `auto` for generated rows, or a valid ORCID for manual additions.
- `reviewer` must be a valid ORCID for any reviewed row.
- `curator_name` and `reviewer_name` store the public display names resolved from ORCID.

## 4) Validate

```bash
scripts/validate_pair_alignments.py registry/pair_alignment_candidates_emi.tsv --kind candidate
```

Validate the local queue before syncing decisions when needed:

```bash
scripts/validate_pair_alignments.py registry/work/pair_alignment_candidates_emi.tsv --kind candidate
```

## 5) Sync into SQLite + export canonical outputs

```bash
scripts/sync_alignment_sqlite.py \
  --db registry/alignment_curation.sqlite \
  --pair-candidates registry/pair_alignment_candidates_emi.tsv \
  --pair-alignments registry/pair_alignment_candidates_emi.tsv \
  --status approved \
  --reconciled-output registry/reconciled_mappings.tsv \
  --grouped-output registry/reconciled_canonical_groups.tsv
```

## 6) Inspect canonical groups in SQLite

```bash
sqlite3 registry/alignment_curation.sqlite \
  "SELECT canonical_term_iri, canonical_term_label, mapped_term_count FROM reconciled_canonical_groups ORDER BY mapped_term_count DESC;"
```

## Git contract

Commit:

- `registry/pair_alignment_candidates_<source>.tsv`
- `registry/external_sources.tsv` when the manifest changes

Keep local only:

- queue files under `registry/work/`
- downloads/import extracts
- SQLite DB
- reconciled exports
- schema documentation
- `registry/pair_alignments.tsv` legacy file if you still generate it locally

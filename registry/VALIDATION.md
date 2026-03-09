# Validation

This repository uses a one-TSV-per-schema review workflow plus local derived exports.

## Active files
- Versioned review file:
  - `registry/pair_alignment_candidates_<source>.tsv`
- Local queue file:
  - `registry/work/pair_alignment_candidates_<source>.tsv`
- Local derived files:
  - `registry/reconciled_mappings.tsv`
  - `registry/reconciled_canonical_groups.tsv`
  - `registry/alignment_curation.sqlite`

## Required columns
Shared review ledger:
- `alignment_id`
- `source_term_source`
- `source_term_iri`
- `source_term_label`
- `source_term_kind`
- `canonical_term_iri`
- `canonical_term_label`
- `canonical_term_source`
- `canonical_term_kind`
- `relation`
- `status`
- `curator`
- `curator_name`
- `reviewer`
- `reviewer_name`
- `date_reviewed`
- `curation_comment`

Local queue:
- full candidate-generation/review columns, including `right_*`, matching metadata, search URLs, and review fields

## Allowed values
- `alignment_id`:
  - review ledger / local queue rows: `CAND_0001`, `CAND_0002`, ...
- `relation`: `exact|close|broad|narrow|related|owl:*|rdfs:*|skos:*` from the supported mapping set
- `status`: `needs_review|approved|rejected|deprecated`
- `canonical_from`: `left|right|manual` (or empty before approval)
- `curator`: `auto` or a valid ORCID
- `reviewer`: empty on open rows, valid ORCID on reviewed rows
- `source_term_kind` / `canonical_term_kind`: `class|property|individual`

## Decision rules
- If `status=approved`, canonical fields are required:
  - `canonical_term_iri`
  - `canonical_term_label`
  - `canonical_term_source`
  - `canonical_term_kind`
- If `curator` is an ORCID, `curator_name` is required.
- If `reviewer` is set, `reviewer_name` is required.
- If `status` is `approved|rejected|deprecated`, `reviewer` must be present and must be a valid ORCID.
- Approved rows require identical `source_term_kind` and `canonical_term_kind`.
- In local queue files, `match_score` must be numeric in `[0,1]`.
- In local queue files, `right_term_iri` may be empty only for placeholder `needs_review` rows.

## Timestamp format
- Accepted:
  - `YYYY-MM-DD`
  - `YYYY-MM-DDTHH:MM:SSZ` (preferred)
  - `YYYY-MM-DDTHH:MM:SS+HH:MM`

## Commands
Validate:

```bash
scripts/validate_pair_alignments.py registry/pair_alignment_candidates_emi.tsv --kind candidate
```

Local queue validation:

```bash
scripts/validate_pair_alignments.py registry/work/pair_alignment_candidates_emi.tsv --kind candidate
```

Sync TSV -> SQLite + canonical exports:

```bash
scripts/sync_alignment_sqlite.py \
  --db registry/alignment_curation.sqlite \
  --pair-candidates registry/pair_alignment_candidates_emi.tsv \
  --pair-alignments registry/pair_alignment_candidates_emi.tsv
```

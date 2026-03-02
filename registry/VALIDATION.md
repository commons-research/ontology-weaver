# Validation

This repository now uses only the pairwise + SQLite workflow.

## Active files
- `registry/pair_alignment_candidates.tsv`
- `registry/pair_alignments.tsv`
- `registry/reconciled_mappings.tsv`
- `registry/reconciled_canonical_groups.tsv`
- `registry/alignment_curation.sqlite`

## Required columns (`pair_*` TSV)
- `alignment_id`
- `left_source`
- `left_term_iri`
- `left_label`
- `right_source`
- `right_term_iri`
- `right_label`
- `match_method`
- `match_score`
- `relation`
- `suggestion_source`
- `canonical_from`
- `canonical_term_iri`
- `canonical_term_label`
- `canonical_term_source`
- `status`
- `curator`
- `date_added`

## Allowed values
- `alignment_id`:
  - candidates: `CAND_0001`, `CAND_0002`, ...
  - curated: `ALIGN_0001`, `ALIGN_0002`, ...
- `relation`: `exact|close|broad|narrow|related`
- `status`: `needs_review|approved|rejected|deprecated`
- `canonical_from`: `left|right|manual` (or empty before approval)

## Decision rules
- If `status=approved`, canonical fields are required:
  - `canonical_from`
  - `canonical_term_iri`
  - `canonical_term_label`
  - `canonical_term_source`
- `match_score` must be numeric in `[0,1]`.
- `left_term_iri` and `right_term_iri` must be present.

## Timestamp format
- Accepted:
  - `YYYY-MM-DD`
  - `YYYY-MM-DDTHH:MM:SSZ` (preferred)
  - `YYYY-MM-DDTHH:MM:SS+HH:MM`

## Commands
Validate:

```bash
scripts/validate_pair_alignments.py registry/pair_alignment_candidates.tsv --kind candidate
scripts/validate_pair_alignments.py registry/pair_alignments.tsv --kind curated
```

Finalize approved candidate rows:

```bash
scripts/finalize_pair_alignment_candidates.py --statuses approved
```

Sync TSV -> SQLite + canonical exports:

```bash
scripts/sync_alignment_sqlite.py --db registry/alignment_curation.sqlite
```

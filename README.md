# Ontology Alignment Curation App

This repository supports a pairwise + SQLite ontology alignment workflow and now includes a Streamlit app to run the full pipeline with intermediate previews and exports.

## Install dependencies 

### System dependencies

#### Raptor


**Linux**
```bash
# Debian/Ubuntu
sudo apt-get install libraptor2-dev

# Fedora/RHEL/CentOS
sudo yum install raptor2-devel

# Arch Linux
sudo pacman -S raptor

# openSUSE
sudo zypper install raptor-devel
```

**macOS**
```bash
brew install raptor
```

**Windows**

No native package manager support. Options:
- Install via [Conda](https://anaconda.org/conda-forge/raptor2): `conda install -c conda-forge raptor2`
- Or use [WSL](https://learn.microsoft.com/en-us/windows/wsl/) and follow the Linux instructions above

### Python (uv)

```bash
uv sync
```

## System requirements

- Required: `rapper` CLI (from Raptor RDF toolkit) for TTL extraction.

Install examples:

```bash
# macOS (Homebrew)
brew install raptor

# Ubuntu/Debian
sudo apt-get install -y raptor2-utils
```

## Run the app

```bash
uv run streamlit run streamlit_app.py
```

## Modules

- Overview
- Fetch schemas and ontologies - Download external sources + browse OLS catalog
- Extract term TSV from TTL
- Generate pairwise candidates (local/local, local/OLS, or local/Wikidata classes and properties)
- Curate candidate decisions (quick actions + batch edits)
- Review curated dataset and export TTL
- View schema documentation (pyLODE), interactive graph, RDFGlance linkout, and Mermaid export before/after curation
- Background sync - candidate TSVs are synced automatically to SQLite and reconciled exports
- Inspect SQLite tables and run SQL queries

### Wikidata alignment

In **Alignment workflow → Generate candidates**, select a pivot schema such as EMI and choose **Wikidata** as the target. Extract its terms first. The app searches English Wikidata items (Q IDs) for schema classes and Wikidata properties (P IDs) for schema properties, ranks hits by label/alias similarity, and keeps up to five per term by default. Other term kinds are not searched. Adjust the top-N limit to inspect more candidates; low-scoring hits remain suggestions, not assertions. This uses the live Wikidata API and may take time for a full schema. Network failures leave the existing local queue unchanged.

Candidates appear in the `registry/work/pair_alignment_candidates_<source>_wikidata.tsv` queue under their own alignment batch. Review descriptions and Wikidata entity pages, then choose an appropriate mapping relation (or keep the source term) in **Curate candidates** before validating. Wikidata Q items are not OWL classes; a lexical match does not justify `owl:equivalentClass`. Generation never approves matches or commits the queue to the shared ledger; only reviewed decisions are synced to `registry/pair_alignment_candidates_<source>_wikidata.tsv`.

The sidebar **Alignment batch** controls the target shown in **Curate candidates**. For an EMI term such as `InChIKey`, select `emi_wikidata`; then either generate the full batch with the focus filter empty, or use **Find Wikidata matches for this term** beside an unmatched term. This on-demand lookup appends hits to the local queue without replacing other decisions. EMI's `InChIKey` is a class, so it searches Q items (for example [Q21445422](https://www.wikidata.org/wiki/Q21445422)); [P235](https://www.wikidata.org/wiki/Property:P235) is a Wikidata identifier property, not a same-kind class candidate. A newly added candidate still requires review.

## Ontology Terms Glossary

- `Ontologies`: distinct vocabularies/knowledge models loaded in OLS (for example CHEBI, EDAM, BIOLINK).
- `Classes`: concept types in those ontologies (for example `chemical entity`, `analysis`, `sample`).
- `Properties`: relationship or attribute predicates used to connect/describe entities (for example `part_of`, `has_role`, `label`).
- `Individuals`: concrete instances (named entities) rather than concept types.

## Versioned curation files

Keep these in Git:

- `registry/external_sources.tsv`
- `registry/alignment_batches.tsv`
- `registry/pair_alignment_candidates_<source>.tsv`

Each schema should have one TSV that contains:

- approved review decisions

This versioned TSV is the shared review ledger and SSOT for exports.
It is kept deterministically sorted by `source_term_iri` and written with LF line endings so Git diffs stay row-local.

Minimal shared ledger columns:

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

## Local generated artefacts

Do not version these generated caches/exports:

- `registry/alignment_curation.sqlite`
- `registry/reconciled_mappings.tsv`
- `registry/reconciled_canonical_groups.tsv`
- `registry/pair_alignments.tsv`
- `registry/downloads/`
- `registry/imports/*_terms.tsv`
- `registry/exports/`
- `registry/work/`
- `registry/ols_ontologies.tsv`
- `registry/ols_ontologies_meta.json`
- `registry/mapping_relations_catalog.json`
- `registry/schema_docs/`

`registry/work/pair_alignment_candidates_<source>.tsv` is the local working queue.
Use it for focused regeneration and in-progress curation. Only approved decisions are synced back to the shared ledger.

On **Curate candidates**, the default `needs_review` filter also lists extracted source terms
with no generated matches, even if the local queue is empty. These terms can be kept
as their own canonical term or given a manual match; selecting **Validate** saves
the decision to the local queue and shared batch ledger. Terms already approved
in the shared batch ledger are hidden by default. The progress bar counts unique
source terms, not candidate pairs; generating candidates only suggests matches
above the chosen similarity threshold and does not curate every source term.

## Collaboration workflow (git + PR)

Use shared schema files (no per-curator filenames) and collaborate through branches/PRs.

1. Pull latest `main` and create a branch: `curation/<schema>-<short-topic>`.
2. In the app sidebar, select:
   - `Source ID` (schema you curate)
   - `Curator ORCID` (must resolve to a public ORCID name)
3. The shared review ledger for that schema is:
   - `registry/pair_alignment_candidates_<source>.tsv`
4. Generate and curate locally in:
   - `registry/work/pair_alignment_candidates_<source>.tsv`
5. Commit only the finalized shared ledger plus any intentional manifest edits:
   - `registry/pair_alignment_candidates_<source>.tsv`
6. Open a PR with a short summary:
   - schema curated
   - terms reviewed
   - notable manual additions/rejections
7. Reviewer checks diff + app preview, then merges.

Notes:
- `curator` may be `auto` or a valid ORCID. When it is an ORCID, `curator_name` stores the public ORCID name.
- `reviewer` must be a valid ORCID on reviewed rows. `reviewer_name` stores the public ORCID name.
- Reviewer attribution is stored in TSV `reviewer`/`reviewer_name`/`date_reviewed` fields.
- SQLite/reconciled exports are local cache files and should not be manually edited or committed.
- Focused local regeneration is safe because `needs_review` queue rows stay under `registry/work/`.

## Summary

- Describe the curation change in 1-3 lines.

## Changed Source Terms

- List the `source_term_iri` values changed in this PR.
- One per line.

Example:

```text
https://w3id.org/emi#LCMSAnalysis
https://w3id.org/emi#ChemicalEntity
```

## Review Notes

- State the intended canonical decision for each changed source term.
- Mention whether the source term was:
  - kept as canonical
  - mapped to an external ontology term
  - revised from a previous canonical choice

## Evidence / Rationale

- Briefly justify each change.
- Include ontology or term URLs when relevant.
- Mention if a prior decision in the shared ledger was replaced.

## Side-by-Side Diagram Review

- Attach screenshots of the side-by-side Mermaid plots for the changed terms, or paste links to generated review artifacts.
- If no diagram is needed, state why.

## Checks

- [ ] I changed only the intended shared-ledger rows.
- [ ] The `source_term_iri` values above match the actual TSV diff.
- [ ] Reviewer ORCID/name fields are correct.
- [ ] `curation-ci` passes.

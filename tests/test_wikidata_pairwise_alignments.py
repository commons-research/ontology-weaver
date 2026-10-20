"""Behavioral tests for Wikidata candidate search without live network access."""

import csv
import io
import json
import tempfile
import unittest
import urllib.parse
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from curation_app.pages.curate_candidates import _add_wikidata_term_candidates
from scripts import suggest_pairwise_alignments as pairs


class WikidataCandidateTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.left = self.root / "terms.tsv"
        self.output = self.root / "candidates.tsv"
        with self.left.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, delimiter="\t")
            writer.writerow(["iri", "label", "type"])
            writer.writerow(["urn:class", "Heavy water", "http://www.w3.org/2002/07/owl#Class"])
            writer.writerow(["urn:property", "has mass", "http://www.w3.org/2002/07/owl#ObjectProperty"])
            writer.writerow(["urn:individual", "Ignored", "http://www.w3.org/2002/07/owl#NamedIndividual"])

    def run_cli(self, *extra):
        args = [
            "suggest_pairwise_alignments.py", "--left-terms", str(self.left),
            "--left-source", "EMI", "--use-wikidata-api", "--output", str(self.output),
            *extra,
        ]
        stdout, stderr = io.StringIO(), io.StringIO()
        with patch("sys.argv", args), redirect_stdout(stdout), redirect_stderr(stderr):
            code = pairs.main()
        return code, stdout.getvalue(), stderr.getvalue()

    def rows(self):
        with self.output.open(encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle, delimiter="\t"))

    def test_kind_filtering_alias_ranking_and_review_only_output(self):
        requests = []

        def search(request, timeout):
            params = urllib.parse.parse_qs(urllib.parse.urlparse(request.full_url).query)
            requests.append((params, request.get_header("User-agent"), timeout, request.full_url))
            if params["type"] == ["item"]:
                payload = {"search": [
                    {"id": "P1", "label": "Heavy water"},
                    {"id": "Q2", "label": "Unrelated mineral", "description": "a mineral"},
                    {"id": "Q1", "label": "Deuterium oxide", "description": "isotope of water",
                     "match": {"type": "alias", "language": "en", "text": "Heavy water"}},
                    {"id": "Q1", "label": "Deuterium oxide", "aliases": ["Heavy water"]},
                ]}
            else:
                payload = {"search": [
                    {"id": "Q3", "label": "has mass"},
                    {"id": "P2", "label": "mass", "description": "mass of subject",
                     "match": {"type": "alias", "language": "en", "text": "has mass"}},
                ]}
            return io.BytesIO(json.dumps(payload).encode())

        with patch.object(pairs.urllib.request, "urlopen", side_effect=search):
            code, progress, warnings = self.run_cli(
                "--top-n-wikidata", "2", "--request-timeout", "7", "--emit-progress"
            )
        self.assertEqual(code, 0)
        self.assertIn("PROGRESS\t3\t3\tlocal-vs-wikidata", progress)
        self.assertIn("unsupported term kind individual", warnings)
        self.assertEqual(len(requests), 2)
        self.assertEqual(
            [(r[0]["search"], r[0]["type"], r[0]["language"], r[2]) for r in requests],
            [(["Heavy water"], ["item"], ["en"], 7.0),
             (["has mass"], ["property"], ["en"], 7.0)],
        )
        self.assertTrue(all("OntologyWeaver/" in r[1] for r in requests))
        self.assertIn("search=Heavy+water", requests[0][3])
        self.assertNotIn(" ", requests[0][3])
        rows = self.rows()
        self.assertEqual(
            [(r["left_term_iri"], r["right_term_iri"], r["right_term_kind"]) for r in rows],
            [("urn:class", "http://www.wikidata.org/entity/Q1", "class"),
             ("urn:class", "http://www.wikidata.org/entity/Q2", "class"),
             ("urn:property", "http://www.wikidata.org/entity/P2", "property")],
        )
        self.assertEqual(rows[0]["match_score"], "1.00")
        self.assertEqual(rows[0]["match_method"], "wikidata_alias_exact_normalized")
        self.assertEqual(rows[0]["right_label"], "Deuterium oxide")
        self.assertEqual(rows[0]["right_definition"], "isotope of water")
        self.assertLess(float(rows[1]["match_score"]), 0.82)
        for row in rows:
            self.assertEqual(row["right_source"], "WIKIDATA")
            self.assertEqual(row["relation"], "")
            self.assertEqual(row["canonical_from"], "")
            self.assertEqual(row["canonical_term_iri"], "")
            self.assertEqual(row["status"], "needs_review")
            self.assertEqual(row["suggestion_source"], "wikidata_api")

    def test_empty_search_is_valid_and_preserves_manual_rows(self):
        self.output.write_text(
            "alignment_id\tleft_source\tleft_term_iri\tright_source\tright_term_iri\tstatus\tsuggestion_source\n"
            "CAND_0009\tEMI\turn:manual\tWIKIDATA\thttp://www.wikidata.org/entity/Q7\tneeds_review\tmanual_entry\n",
            encoding="utf-8",
        )
        with patch.object(pairs.urllib.request, "urlopen", return_value=io.BytesIO(b'{"search": []}')):
            code, _, _ = self.run_cli("--focus", "heavy water")
        self.assertEqual(code, 0)
        self.assertEqual(len(self.rows()), 1)
        self.assertEqual(self.rows()[0]["alignment_id"], "CAND_0009")

    def test_api_failure_after_partial_search_keeps_queue_unchanged(self):
        original = "alignment_id\tstatus\nCAND_0001\tapproved\n"
        self.output.write_text(original, encoding="utf-8")
        responses = [io.BytesIO(b'{"search": [{"id": "Q1", "label": "Heavy water"}]}')]

        def search(request, timeout):
            if responses:
                return responses.pop()
            raise OSError("network unavailable")

        with patch.object(pairs.urllib.request, "urlopen", side_effect=search):
            with self.assertRaisesRegex(SystemExit, "Wikidata API failure; output left untouched"):
                self.run_cli()
        self.assertEqual(self.output.read_text(encoding="utf-8"), original)

    def test_existing_curated_and_preserved_pairs_not_duplicated(self):
        header = "alignment_id\tleft_source\tleft_term_iri\tright_source\tright_term_iri\tstatus\tsuggestion_source\n"
        self.output.write_text(
            header + "CAND_0012\tEMI\turn:class\tWIKIDATA\thttp://www.wikidata.org/entity/Q1\tneeds_review\tmanual_add\n",
            encoding="utf-8",
        )
        curated = self.root / "curated.tsv"
        curated.write_text(
            header + "CUR_1\tEMI\turn:class\tWIKIDATA\thttp://www.wikidata.org/entity/Q2\tapproved\tmanual_add\n",
            encoding="utf-8",
        )

        def search(request, timeout):
            return io.BytesIO(json.dumps({"search": [
                {"id": "Q1", "label": "Heavy water"},
                {"id": "Q2", "label": "Heavy water"},
                {"id": "Q3", "label": "Heavy water"},
            ]}).encode())

        with patch.object(pairs.urllib.request, "urlopen", side_effect=search):
            self.run_cli("--focus", "heavy water", "--curated-alignments", str(curated))
        rows = self.rows()
        self.assertEqual([row["right_term_iri"] for row in rows],
                         ["http://www.wikidata.org/entity/Q1", "http://www.wikidata.org/entity/Q3"])
        self.assertEqual(rows[1]["alignment_id"], "CAND_0013")

    def test_invalid_api_payload_fails_and_invalid_top_n_rejected(self):
        with patch.object(pairs.urllib.request, "urlopen", return_value=io.BytesIO(b'{"error": "bad request"}')):
            with self.assertRaisesRegex(SystemExit, "Wikidata API failure"):
                self.run_cli("--focus", "heavy water")
        busy_error = {"error": {
            "code": "cirrussearch-too-busy-error",
            "info": "Search is currently too busy.",
            "*": "Long API help text should not fill the curation screen.",
        }}
        with patch.object(pairs.urllib.request, "urlopen",
                          return_value=io.BytesIO(json.dumps(busy_error).encode())):
            with self.assertRaisesRegex(SystemExit, "cirrussearch-too-busy-error: Search is currently too busy") as caught:
                self.run_cli("--focus", "heavy water")
        self.assertNotIn("Long API help text", str(caught.exception))
        self.assertFalse(self.output.exists())
        with self.assertRaisesRegex(SystemExit, "--top-n-wikidata must be > 0"):
            self.run_cli("--top-n-wikidata", "0")


    def test_on_demand_inchikey_lookup_preserves_existing_queue(self):
        with self.left.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, delimiter="\t")
            writer.writerow(["iri", "label", "term_kind", "comment"])
            writer.writerow(["https://w3id.org/emi#InChIKey", "InChIKey", "class",
                             "A chemical structure represented by its InChIKey."])
        ctx = SimpleNamespace(
            terms_tsv=self.left, batch_id="emi_wikidata", pivot_source="emi",
            target_id="wikidata", target_backend="wikidata",
        )
        previous = pd.DataFrame([{
            "alignment_id": "CAND_0007",
            "left_source": "EMI",
            "left_term_iri": "urn:other",
            "right_term_iri": "http://www.wikidata.org/entity/Q8",
            "status": "approved",
        }])
        payload = {"search": [{
            "id": "Q21445422", "label": "InChIKey",
            "description": "chemical identifier",
        }]}
        with patch.object(pairs.urllib.request, "urlopen",
                          return_value=io.BytesIO(json.dumps(payload).encode())):
            updated, added = _add_wikidata_term_candidates(
                previous, ctx, "https://w3id.org/emi#InChIKey",
            )
        self.assertEqual(added, 1)
        self.assertEqual(updated.loc[0, "status"], "approved")
        self.assertEqual(updated.loc[1, "alignment_id"], "CAND_0008")
        self.assertEqual(updated.loc[1, "right_term_iri"], "http://www.wikidata.org/entity/Q21445422")
        self.assertEqual(updated.loc[1, "status"], "needs_review")
        self.assertEqual(updated.loc[1, "relation"], "")
        with patch.object(pairs.urllib.request, "urlopen",
                          return_value=io.BytesIO(json.dumps(payload).encode())):
            repeated, added = _add_wikidata_term_candidates(
                updated, ctx, "https://w3id.org/emi#InChIKey",
            )
        self.assertEqual(added, 0)
        pd.testing.assert_frame_equal(repeated, updated)

if __name__ == "__main__":
    unittest.main()

"""Schema documentation preview page using pyLODE."""

from __future__ import annotations

from pathlib import Path
import json
import hashlib
import csv
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import webbrowser
import os
import io
import zipfile
from urllib.parse import quote_plus

import streamlit as st
import streamlit.components.v1 as components
from rdflib import BNode, Graph, Literal, URIRef
from rdflib.namespace import OWL, RDF, RDFS, SKOS

from curation_app.context import active_source_context
from curation_app.helpers import file_to_bytes, to_relpath


def _doc_output_path(source_slug: str, variant: str) -> Path:
    suffix = "before" if variant == "before" else "after"
    return Path("registry/schema_docs") / f"{source_slug}_{suffix}_pylode.html"


def _graph_output_path(source_slug: str, variant: str) -> Path:
    suffix = "before" if variant == "before" else "after"
    return Path("registry/schema_docs") / f"{source_slug}_{suffix}_graph.html"


def _list_ttl_candidates() -> list[Path]:
    roots = [Path("registry"), Path("lib")]
    extensions = {".ttl", ".rdf", ".owl", ".xml", ".jsonld", ".nt", ".trig"}
    files: set[Path] = set()
    for root in roots:
        if not root.is_dir():
            continue
        for path in root.rglob("*"):
            if path.is_file():
                if path.suffix.lower() not in extensions:
                    continue
                files.add(path.resolve())
    return sorted(files, key=lambda p: to_relpath(p).lower())


def _doc_output_path_for_input(input_ttl: Path) -> Path:
    return _doc_output_path_for_input_with_generator(input_ttl, "pylode")


def _doc_output_path_for_input_with_generator(input_ttl: Path, generator: str) -> Path:
    rel = to_relpath(input_ttl)
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", rel).strip("._")
    digest = hashlib.sha1(rel.encode("utf-8")).hexdigest()[:8]
    stem = safe.rsplit(".", 1)[0] if "." in safe else safe
    return Path("registry/schema_docs") / f"{stem}_{digest}_{generator}.html"


def _widoco_output_dir_for_input(input_ttl: Path) -> Path:
    rel = to_relpath(input_ttl)
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", rel).strip("._")
    digest = hashlib.sha1(rel.encode("utf-8")).hexdigest()[:8]
    stem = safe.rsplit(".", 1)[0] if "." in safe else safe
    return Path("registry/schema_docs") / f"{stem}_{digest}_widoco"


def _find_widoco_index_html(output_dir: Path) -> Path | None:
    candidates = [
        output_dir / "index-en.html",
        output_dir / "index.html",
        output_dir / "doc" / "index-en.html",
        output_dir / "doc" / "index.html",
    ]
    for path in candidates:
        if path.is_file():
            return path
    return None


def _find_widoco_webvowl_html(output_dir: Path) -> Path | None:
    candidates = [
        output_dir / "webvowl" / "index.html",
        output_dir / "webvowl" / "webvowl.html",
        output_dir / "webvowl" / "index-en.html",
    ]
    for path in candidates:
        if path.is_file():
            return path
    return None


def _widoco_embedded_webvowl_html(output_dir: Path) -> Path | None:
    webvowl_dir = output_dir / "webvowl"
    d3_path = webvowl_dir / "js" / "d3.min.js"
    core_path = webvowl_dir / "js" / "webvowl.js"
    app_path = webvowl_dir / "js" / "webvowl.app.js"
    data_path = webvowl_dir / "data" / "ontology.json"
    if not (d3_path.is_file() and core_path.is_file() and app_path.is_file() and data_path.is_file()):
        return None

    try:
        d3_inline = d3_path.read_text(encoding="utf-8", errors="replace").replace("</script>", "<\\/script>")
        core_inline = core_path.read_text(encoding="utf-8", errors="replace").replace("</script>", "<\\/script>")
        app_inline = app_path.read_text(encoding="utf-8", errors="replace").replace("</script>", "<\\/script>")
        data_inline = data_path.read_text(encoding="utf-8", errors="replace").replace("</script>", "<\\/script>")
    except OSError:
        return None

    html = f"""<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>WIDOCO WebVOWL (Embedded)</title>
    <style>
      html, body, #graph {{
        margin: 0; padding: 0; width: 100%; height: 100%;
        background: #ffffff; overflow: hidden; font-family: sans-serif;
      }}
      #status {{
        position: absolute; top: 8px; left: 8px; z-index: 9999;
        background: rgba(255,255,255,0.92); border: 1px solid #ddd;
        border-radius: 6px; padding: 6px 8px; font-size: 12px;
      }}
    </style>
  </head>
  <body>
    <div id="status">Loading WebVOWL...</div>
    <div id="graph"></div>
    <script>{d3_inline}</script>
    <script>{core_inline}</script>
    <script>{app_inline}</script>
    <script id="ontology-json" type="application/json">{data_inline}</script>
    <script>
      const statusEl = document.getElementById("status");
      function setStatus(text) {{ statusEl.textContent = text; }}
      function patchD3MissingIdSelectors() {{
        if (!window.d3 || !d3.select || d3.__pfPatchedSelect) return;
        const originalSelect = d3.select;
        d3.select = function(selector) {{
          if (typeof selector === "string" && selector.startsWith("#")) {{
            const id = selector.slice(1);
            if (id && !document.getElementById(id)) {{
              const el = document.createElement("div");
              el.id = id;
              el.style.display = "none";
              document.body.appendChild(el);
            }}
          }}
          return originalSelect.call(d3, selector);
        }};
        d3.__pfPatchedSelect = true;
      }}
      function boot() {{
        try {{
          const text = document.getElementById("ontology-json").textContent;
          const data = JSON.parse(text);
          if (!window.webvowl) {{
            throw new Error("WebVOWL library not available");
          }}
          patchD3MissingIdSelectors();
          if (typeof webvowl.graph === "function") {{
            const graph = webvowl.graph();
            const options = graph.graphOptions();
            options.graphContainerSelector("#graph");
            options.width(window.innerWidth);
            options.height(window.innerHeight);
            graph.start();
            options.data(data);
            graph.load();
          }} else if (typeof webvowl.app === "function") {{
            const app = webvowl.app();
            app.initialize();
            if (typeof app.getOptions === "function" && typeof app.getGraph === "function") {{
              const opts = app.getOptions();
              const gr = app.getGraph();
              if (opts && gr) {{
                opts.data(data);
                gr.load();
              }}
            }}
          }} else {{
            throw new Error("WebVOWL graph API not available");
          }}
          setStatus("WebVOWL loaded");
          setTimeout(() => statusEl.style.display = "none", 1200);
        }} catch (err) {{
          setStatus("Error: " + err.message);
        }}
      }}
      boot();
    </script>
  </body>
</html>"""
    out = output_dir / "webvowl_embedded.html"
    try:
        out.write_text(html, encoding="utf-8")
    except OSError:
        return None
    return out


def _widoco_standalone_webvowl_html(output_dir: Path) -> Path | None:
    webvowl_dir = output_dir / "webvowl"
    d3_path = webvowl_dir / "js" / "d3.min.js"
    core_path = webvowl_dir / "js" / "webvowl.js"
    data_path = webvowl_dir / "data" / "ontology.json"
    if not (d3_path.is_file() and core_path.is_file() and data_path.is_file()):
        return None

    try:
        d3_inline = d3_path.read_text(encoding="utf-8", errors="replace").replace("</script>", "<\\/script>")
        core_inline = core_path.read_text(encoding="utf-8", errors="replace").replace("</script>", "<\\/script>")
        data_inline = data_path.read_text(encoding="utf-8", errors="replace").replace("</script>", "<\\/script>")
    except OSError:
        return None

    html = f"""<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>WebVOWL Standalone</title>
    <style>
      html, body, #graph {{
        margin: 0; padding: 0; width: 100%; height: 100%;
        background: #ffffff; overflow: hidden; font-family: sans-serif;
      }}
      #status {{
        position: absolute; top: 8px; left: 8px; z-index: 9999;
        background: rgba(255,255,255,0.92); border: 1px solid #ddd;
        border-radius: 6px; padding: 6px 8px; font-size: 12px;
      }}
    </style>
  </head>
  <body>
    <div id="status">Loading WebVOWL...</div>
    <div id="graph"></div>
    <script>{d3_inline}</script>
    <script>{core_inline}</script>
    <script id="ontology-json" type="application/json">{data_inline}</script>
    <script>
      const statusEl = document.getElementById("status");
      function setStatus(text) {{ statusEl.textContent = text; }}
      function patchD3MissingIdSelectors() {{
        if (!window.d3 || !d3.select || d3.__pfPatchedSelect) return;
        const originalSelect = d3.select;
        d3.select = function(selector) {{
          if (typeof selector === "string" && selector.startsWith("#")) {{
            const id = selector.slice(1);
            if (id && !document.getElementById(id)) {{
              const el = document.createElement("div");
              el.id = id;
              el.style.display = "none";
              document.body.appendChild(el);
            }}
          }}
          return originalSelect.call(d3, selector);
        }};
        d3.__pfPatchedSelect = true;
      }}
      function boot() {{
        try {{
          const text = document.getElementById("ontology-json").textContent;
          const data = JSON.parse(text);
          if (!window.webvowl || typeof webvowl.graph !== "function") {{
            throw new Error("WebVOWL graph API not available");
          }}
          patchD3MissingIdSelectors();
          const graph = webvowl.graph();
          const options = graph.graphOptions();
          function ensureModule(name) {{
            if (!options || typeof options[name] !== "function") {{
              options[name] = function() {{
                return {{
                  enabled: function() {{ return false; }},
                  filter: function() {{}},
                  apply: function() {{}},
                  reset: function() {{}}
                }};
              }};
            }}
          }}
          [
            "literalFilter",
            "datatypeFilter",
            "objectPropertyFilter",
            "subclassFilter",
            "disjointFilter",
            "setOperatorFilter",
            "nodeDegreeFilter"
          ].forEach(ensureModule);
          if (options && typeof options.loadingModule === "function") {{
            try {{
              options.loadingModule({{
                successfullyLoadedOntology: function() {{}},
                requestServerTimeStamp: function() {{}},
                requestOntologyFromServer: function() {{}},
                setErrorMessage: function() {{}},
                showLoadingIndicator: function() {{}},
                hideLoadingIndicator: function() {{}},
                setOntologyIri: function() {{}},
                setLoadingStatusInfo: function() {{}},
                clearLoadingStatusInfo: function() {{}},
                append_loadingMessage: function() {{}},
                resetLoadingInfo: function() {{}}
              }});
            }} catch (e) {{}}
          }}
          options.graphContainerSelector("#graph");
          options.width(window.innerWidth);
          options.height(window.innerHeight);
          graph.start();
          options.data(data);
          graph.load();
          setStatus("WebVOWL loaded");
          setTimeout(() => statusEl.style.display = "none", 1200);
        }} catch (err) {{
          setStatus("Error: " + err.message);
        }}
      }}
      boot();
    </script>
  </body>
</html>"""

    out = output_dir / "webvowl_standalone.html"
    try:
        out.write_text(html, encoding="utf-8")
    except OSError:
        return None
    return out


def _zip_directory_bytes(directory: Path) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in sorted(directory.rglob("*")):
            if not path.is_file():
                continue
            arcname = str(path.relative_to(directory.parent))
            zf.write(path, arcname=arcname)
    return buffer.getvalue()


def _widoco_jar_candidates() -> list[Path]:
    raw_env = os.environ.get("WIDOCO_JAR", "").strip()
    candidates = [
        Path(raw_env) if raw_env else None,
        Path("tools/widoco.jar"),
        Path("tools/widoco/widoco.jar"),
        Path("registry/tools/widoco.jar"),
    ]
    out: list[Path] = []
    for c in candidates:
        if c is None:
            continue
        p = c.resolve() if not c.is_absolute() else c
        if p.is_file():
            out.append(p)
    return out


def _widoco_available() -> tuple[bool, str]:
    if shutil.which("widoco"):
        return True, "`widoco` command found in PATH."
    if _widoco_jar_candidates():
        return True, "WIDOCO jar found in repository."
    return False, "WIDOCO not found (`widoco` command or `tools/widoco.jar`)."


def _run_widoco(input_ttl: Path, output_dir: Path, output_html: Path) -> tuple[bool, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    errors: list[str] = []

    cmd_candidates: list[list[str]] = []
    widoco_bin = shutil.which("widoco")
    if widoco_bin:
        cmd_candidates.append(
            [
                widoco_bin,
                "-ontFile",
                str(input_ttl),
                "-outFolder",
                str(output_dir),
                "-rewriteAll",
                "-webVowl",
            ]
        )

    java = shutil.which("java")
    jars = _widoco_jar_candidates()
    if java and jars:
        for jar in jars:
            cmd_candidates.append(
                [
                    java,
                    "-jar",
                    str(jar),
                    "-ontFile",
                    str(input_ttl),
                    "-outFolder",
                    str(output_dir),
                    "-rewriteAll",
                    "-webVowl",
                ]
            )

    if not cmd_candidates:
        return (
            False,
            "WIDOCO is not available. Install `widoco` in PATH or place a jar at `tools/widoco.jar`.",
        )

    for cmd in cmd_candidates:
        completed = subprocess.run(cmd, capture_output=True, text=True, check=False)
        index_html = _find_widoco_index_html(output_dir)
        if completed.returncode == 0 and index_html is not None:
            out = (completed.stdout or "").strip()
            return True, out or f"WIDOCO documentation generated with `{shlex.join(cmd)}`."
        err = (completed.stderr or completed.stdout or "").strip()
        if err:
            errors.append(f"$ {shlex.join(cmd)}\n{err}")

    detail = "\n\n".join(errors[-2:]) if errors else "No WIDOCO command could be executed."
    return False, "WIDOCO failed in this environment.\n\n" + detail


def _resolve_doc_html(generator: str, input_ttl: Path, output_html: Path) -> Path | None:
    if generator == "widoco":
        index_html = _find_widoco_index_html(_widoco_output_dir_for_input(input_ttl))
        if index_html is not None:
            return index_html
        if output_html.is_file():
            return output_html
        return None
    if output_html.is_file():
        return output_html
    return None


def _widoco_inapp_html(index_html: Path) -> str:
    text = index_html.read_text(encoding="utf-8", errors="replace")
    base_dir = index_html.parent

    # Inline CSS resources to avoid broken relative paths in embedded view.
    css_link_re = re.compile(r'<link[^>]+href="(resources/[^"]+\.css)"[^>]*/?>', re.IGNORECASE)
    def _css_repl(match: re.Match[str]) -> str:
        rel = match.group(1)
        css_path = base_dir / rel
        if not css_path.is_file():
            return ""
        css_text = css_path.read_text(encoding="utf-8", errors="replace")
        return f"<style>\n{css_text}\n</style>"
    text = css_link_re.sub(_css_repl, text)

    # Remove JS resources/loaders that depend on local file fetches in browser context.
    text = re.sub(r'<script[^>]+src="resources/[^"]+"[^>]*>\s*</script>', "", text, flags=re.IGNORECASE)
    text = re.sub(r"<script>\s*function loadHash\(\).*?</script>", "", text, flags=re.IGNORECASE | re.DOTALL)

    # Inline section fragments so cross-reference and descriptions are visible in-app.
    section_files = {
        "abstract": "sections/abstract-en.html",
        "introduction": "sections/introduction-en.html",
        "nstable": "sections/ns-en.html",
        "overview": "sections/overview-en.html",
        "description": "sections/description-en.html",
        "crossref": "sections/crossref-en.html",
        "references": "sections/references-en.html",
    }
    for div_id, rel_path in section_files.items():
        section_path = base_dir / rel_path
        if section_path.is_file():
            section_html = section_path.read_text(encoding="utf-8", errors="replace")
            text = re.sub(
                rf'<div id="{re.escape(div_id)}"></div>',
                section_html,
                text,
                flags=re.IGNORECASE,
            )
    return text


def _run_pylode(input_ttl: Path, output_html: Path) -> tuple[bool, str]:
    def _has_doc_root(path: Path) -> bool:
        g = Graph()
        try:
            g.parse(str(path))
        except Exception:
            return False
        has_ontology = any(True for _ in g.subjects(RDF.type, OWL.Ontology))
        has_scheme = any(True for _ in g.subjects(RDF.type, SKOS.ConceptScheme))
        prof_profile = URIRef("http://www.w3.org/ns/dx/prof/Profile")
        has_profile = any(True for _ in g.subjects(RDF.type, prof_profile))
        return has_ontology or has_scheme or has_profile

    def _inject_synthetic_ontology(path: Path) -> tuple[Path, str]:
        g = Graph()
        g.parse(str(path))
        synthetic = URIRef(f"urn:local:pylode:{path.stem}")
        g.add((synthetic, RDF.type, OWL.Ontology))
        g.add((synthetic, RDFS.label, Literal(f"Synthetic ontology wrapper for {path.name}")))
        tmp_dir = Path("registry/schema_docs")
        tmp_dir.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix="_pylode_wrapped.ttl",
            delete=False,
            dir=tmp_dir,
            encoding="utf-8",
        ) as handle:
            tmp_path = Path(handle.name)
        g.serialize(destination=str(tmp_path), format="turtle")
        msg = (
            f"Input has no `owl:Ontology` / `skos:ConceptScheme` / `prof:Profile`; "
            f"used temporary synthetic wrapper `{to_relpath(tmp_path)}` for pyLODE."
        )
        return tmp_path, msg

    output_html.parent.mkdir(parents=True, exist_ok=True)
    pylode_input = input_ttl
    prep_msg = ""
    try:
        if not _has_doc_root(input_ttl):
            pylode_input, prep_msg = _inject_synthetic_ontology(input_ttl)
    except Exception:
        pylode_input = input_ttl
        prep_msg = ""

    candidates: list[list[str]] = []
    pylode_path = shutil.which("pylode")
    if pylode_path:
        candidates.append([pylode_path, str(pylode_input), "-o", str(output_html)])
    venv_pylode = Path(".venv/bin/pylode")
    if venv_pylode.is_file():
        candidates.append([str(venv_pylode), str(pylode_input), "-o", str(output_html)])
    candidates.append([sys.executable, "-m", "pylode", str(pylode_input), "-o", str(output_html)])

    errors: list[str] = []
    for cmd in candidates:
        completed = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode == 0 and output_html.is_file():
            out = (completed.stdout or "").strip()
            base = out or f"Documentation generated with `{shlex.join(cmd)}`."
            if prep_msg:
                base = prep_msg + "\n" + base
            return True, base
        err = (completed.stderr or completed.stdout or "").strip()
        if err:
            errors.append(f"$ {shlex.join(cmd)}\n{err}")

    detail = "\n\n".join(errors[-2:]) if errors else "No pyLODE executable could be run."
    return (
        False,
        "pyLODE failed in this environment. "
        "Try `uv sync` and ensure `pylode` is installed in the same interpreter used by Streamlit.\n\n"
        f"{detail}",
    )


def _short_iri(graph: Graph, iri: URIRef) -> str:
    text = str(iri)
    if text.startswith("https://w3id.org/emi#"):
        return ":" + text.rsplit("#", 1)[-1]
    if text.startswith("http://www.w3.org/ns/prov#"):
        return "prov:" + text.rsplit("#", 1)[-1]
    match = re.match(r"^https?://purl\.obolibrary\.org/obo/([A-Za-z][A-Za-z0-9]*)_(.+)$", text)
    if match:
        return f"{match.group(1).lower()}:{match.group(2)}"
    match = re.match(r"^https?://semanticscience\.org/resource/([A-Za-z][A-Za-z0-9]*)_(.+)$", text)
    if match:
        return f"{match.group(1).lower()}:{match.group(2)}"
    try:
        normalized = graph.namespace_manager.normalizeUri(iri)
        if normalized and normalized != text and not normalized.startswith("<"):
            return normalized
    except Exception:
        pass
    return text


def _build_graph_html(input_ttl: Path, output_html: Path, max_nodes: int, node_scale: float = 1.0, search_query: str = "") -> tuple[bool, str]:
    try:
        from pyvis.network import Network
    except Exception as exc:
        return False, f"pyvis is not available: {exc}. Run `uv sync`."

    g = Graph()
    try:
        g.parse(str(input_ttl))
    except Exception as exc:
        return False, f"Failed to parse TTL: {exc}"

    classes = set(g.subjects(RDF.type, OWL.Class)) | set(g.subjects(RDF.type, RDFS.Class))
    properties = (
        set(g.subjects(RDF.type, RDF.Property))
        | set(g.subjects(RDF.type, OWL.ObjectProperty))
        | set(g.subjects(RDF.type, OWL.DatatypeProperty))
    )

    # Focus on class/property graph for readability.
    nodes = [n for n in classes | properties if isinstance(n, URIRef)]
    q = (search_query or "").strip().lower()
    if q:
        nodes = [
            n for n in nodes
            if q in _short_iri(g, n).lower() or q in str(n).lower()
        ]
    nodes = sorted(nodes, key=lambda x: str(x))[:max_nodes]
    node_set = set(nodes)

    net = Network(height="900px", width="100%", directed=True, bgcolor="#ffffff", font_color="#111827")
    net.barnes_hut()

    for n in nodes:
        is_class = n in classes
        color = "#93c5fd" if is_class else "#86efac"
        shape = "dot" if is_class else "box"
        base_size = 22 if is_class else 18
        net.add_node(
            str(n),
            label=_short_iri(g, n),
            title=str(n),
            color=color,
            shape=shape,
            size=int(base_size * node_scale),
            font={"size": int(16 * node_scale)},
        )

    def add_edge(s: URIRef, o: URIRef, label: str, color: str) -> None:
        if s in node_set and o in node_set:
            net.add_edge(str(s), str(o), label=label, color=color, arrows="to")

    for s, _, o in g.triples((None, RDFS.subClassOf, None)):
        if isinstance(s, URIRef) and isinstance(o, URIRef):
            add_edge(s, o, "subClassOf", "#2563eb")
    for s, _, o in g.triples((None, RDFS.subPropertyOf, None)):
        if isinstance(s, URIRef) and isinstance(o, URIRef):
            add_edge(s, o, "subPropertyOf", "#16a34a")
    for p, _, d in g.triples((None, RDFS.domain, None)):
        if isinstance(p, URIRef) and isinstance(d, URIRef):
            add_edge(p, d, "domain", "#f59e0b")
    for p, _, r in g.triples((None, RDFS.range, None)):
        if isinstance(p, URIRef) and isinstance(r, URIRef):
            add_edge(p, r, "range", "#ef4444")

    output_html.parent.mkdir(parents=True, exist_ok=True)
    net.save_graph(str(output_html))
    return True, f"Interactive graph generated ({len(nodes)} nodes)."


def _mermaid_output_path(source_slug: str, variant: str) -> Path:
    suffix = "before" if variant == "before" else "after"
    return Path("registry/schema_docs") / f"{source_slug}_{suffix}_schema.mmd"


def _ns_base(iri: str) -> str:
    if "#" in iri:
        return iri.rsplit("#", 1)[0] + "#"
    if "/" in iri:
        return iri.rsplit("/", 1)[0] + "/"
    return iri


def _build_mermaid(
    input_ttl: Path,
    mode: str,
    max_nodes: int,
    include_external: bool,
    focus_entity_iri: str | None = None,
    focus_max_hops: int = 0,
) -> tuple[bool, str, str]:
    g = Graph()
    try:
        g.parse(str(input_ttl))
    except Exception as exc:
        return False, "", f"Failed to parse TTL: {exc}"

    classes_typed = set(g.subjects(RDF.type, OWL.Class)) | set(g.subjects(RDF.type, RDFS.Class))
    properties_typed = (
        set(g.subjects(RDF.type, RDF.Property))
        | set(g.subjects(RDF.type, OWL.ObjectProperty))
        | set(g.subjects(RDF.type, OWL.DatatypeProperty))
    )
    typed_nodes = [n for n in classes_typed | properties_typed if isinstance(n, URIRef)]
    if not typed_nodes:
        return False, "", "No class/property nodes found."

    edge_rows_all: list[tuple[URIRef, URIRef, str]] = []
    for s1, _, o1 in g.triples((None, RDFS.subClassOf, None)):
        if isinstance(s1, URIRef) and isinstance(o1, URIRef):
            edge_rows_all.append((s1, o1, "subClassOf"))
    for s1, _, o1 in g.triples((None, RDFS.subPropertyOf, None)):
        if isinstance(s1, URIRef) and isinstance(o1, URIRef):
            edge_rows_all.append((s1, o1, "subPropertyOf"))
    if mode == "schema":
        for p1, _, d1 in g.triples((None, RDFS.domain, None)):
            if isinstance(p1, URIRef) and isinstance(d1, URIRef):
                edge_rows_all.append((p1, d1, "domain"))
        for p1, _, r1 in g.triples((None, RDFS.range, None)):
            if isinstance(p1, URIRef) and isinstance(r1, URIRef):
                edge_rows_all.append((p1, r1, "range"))
        for s1, _, o1 in g.triples((None, OWL.equivalentClass, None)):
            if isinstance(s1, URIRef) and isinstance(o1, URIRef):
                edge_rows_all.append((s1, o1, "equivalentClass"))
        for s1, _, o1 in g.triples((None, OWL.equivalentProperty, None)):
            if isinstance(s1, URIRef) and isinstance(o1, URIRef):
                edge_rows_all.append((s1, o1, "equivalentProperty"))
        for s1, _, o1 in g.triples((None, OWL.sameAs, None)):
            if isinstance(s1, URIRef) and isinstance(o1, URIRef):
                edge_rows_all.append((s1, o1, "sameAs"))
        for s1, _, o1 in g.triples((None, RDFS.seeAlso, None)):
            if isinstance(s1, URIRef) and isinstance(o1, URIRef):
                edge_rows_all.append((s1, o1, "seeAlso"))
        for s1, _, o1 in g.triples((None, SKOS.exactMatch, None)):
            if isinstance(s1, URIRef) and isinstance(o1, URIRef):
                edge_rows_all.append((s1, o1, "skos:exactMatch"))
        for s1, _, o1 in g.triples((None, SKOS.closeMatch, None)):
            if isinstance(s1, URIRef) and isinstance(o1, URIRef):
                edge_rows_all.append((s1, o1, "skos:closeMatch"))
        for s1, _, o1 in g.triples((None, SKOS.broadMatch, None)):
            if isinstance(s1, URIRef) and isinstance(o1, URIRef):
                edge_rows_all.append((s1, o1, "skos:broadMatch"))
        for s1, _, o1 in g.triples((None, SKOS.narrowMatch, None)):
            if isinstance(s1, URIRef) and isinstance(o1, URIRef):
                edge_rows_all.append((s1, o1, "skos:narrowMatch"))
        for s1, _, o1 in g.triples((None, SKOS.relatedMatch, None)):
            if isinstance(s1, URIRef) and isinstance(o1, URIRef):
                edge_rows_all.append((s1, o1, "skos:relatedMatch"))
        for s1, _, o1 in g.triples((None, SKOS.mappingRelation, None)):
            if isinstance(s1, URIRef) and isinstance(o1, URIRef):
                edge_rows_all.append((s1, o1, "skos:mappingRelation"))

    edge_nodes = {n for s1, o1, _ in edge_rows_all for n in (s1, o1)}
    all_nodes = list(set(typed_nodes) | edge_nodes)

    focus_requested: URIRef | None = None
    if focus_entity_iri:
        try:
            focus_requested = URIRef(focus_entity_iri)
        except Exception:
            focus_requested = None

    # When a focus entity is requested, do not prune namespaces up front.
    # Otherwise the focus can disappear on one side and silently look "unfiltered".
    if not include_external and focus_requested is None:
        freq: dict[str, int] = {}
        for n in typed_nodes:
            base = _ns_base(str(n))
            freq[base] = freq.get(base, 0) + 1
        keep_ns = {ns for ns, _ in sorted(freq.items(), key=lambda kv: kv[1], reverse=True)[:2]}
        all_nodes = [n for n in all_nodes if _ns_base(str(n)) in keep_ns]

    node_pool = set(all_nodes)
    edge_rows_all = [(s1, o1, e) for s1, o1, e in edge_rows_all if s1 in node_pool and o1 in node_pool]

    focus_node: URIRef | None = None
    if focus_requested is not None:
        if focus_requested in node_pool:
            focus_node = focus_requested
        else:
            return (
                True,
                "flowchart LR\n",
                f"Focused entity `{focus_requested}` not found in this TTL under current mode.",
            )

    if focus_node is not None:
        adjacency: dict[URIRef, set[URIRef]] = {n: set() for n in all_nodes}
        for s1, o1, _ in edge_rows_all:
            adjacency.setdefault(s1, set()).add(o1)
            adjacency.setdefault(o1, set()).add(s1)
        ordered: list[URIRef] = []
        seen: set[URIRef] = set()
        queue: list[tuple[URIRef, int]] = [(focus_node, 0)]
        while queue:
            cur, dist = queue.pop(0)
            if cur in seen:
                continue
            seen.add(cur)
            ordered.append(cur)
            if focus_max_hops > 0 and dist >= focus_max_hops:
                continue
            for nxt in sorted(adjacency.get(cur, set()), key=lambda x: _short_iri(g, x)):
                if nxt not in seen:
                    queue.append((nxt, dist + 1))
        nodes = ordered[:max_nodes]
    else:
        nodes = sorted(all_nodes, key=lambda x: _short_iri(g, x))[:max_nodes]

    node_set = set(nodes)
    node_ids = {n: f"n{i}" for i, n in enumerate(nodes)}
    edge_rows = [(s1, o1, e) for s1, o1, e in edge_rows_all if s1 in node_set and o1 in node_set]

    lines: list[str] = ["flowchart LR"]
    for n in nodes:
        nid = node_ids[n]
        label = _short_iri(g, n).replace('"', "'")
        if n in properties_typed:
            lines.append(f'  {nid}{{"{label}"}}')
        else:
            lines.append(f'  {nid}["{label}"]')
    for s1, o1, elabel in edge_rows:
        lines.append(f'  {node_ids[s1]} -->|{elabel}| {node_ids[o1]}')
    for n in nodes:
        iri = str(n).replace('"', "%22")
        lines.append(f'  click {node_ids[n]} href "{iri}" "Open IRI" _blank')

    text = "\n".join(lines) + "\n"
    if focus_node is not None:
        if len(edge_rows) == 0:
            return (
                True,
                "flowchart LR\n",
                "No connected edges found for the selected focus entity under current filters. "
                "Try enabling 'Include external IRIs'.",
            )
        return (
            True,
            text,
            f"Generated Mermaid connected graph for `{_short_iri(g, focus_node)}` "
            f"(hops={'all' if focus_max_hops == 0 else focus_max_hops}; "
            f"{len(nodes)} nodes, {len(edge_rows)} edges).",
        )
    return True, text, f"Generated Mermaid ({len(nodes)} nodes, {len(edge_rows)} edges)."


def _mermaid_entity_options(input_ttl: Path, include_external: bool) -> list[tuple[str, str]]:
    g = Graph()
    g.parse(str(input_ttl))
    classes = set(g.subjects(RDF.type, OWL.Class)) | set(g.subjects(RDF.type, RDFS.Class))
    properties = (
        set(g.subjects(RDF.type, RDF.Property))
        | set(g.subjects(RDF.type, OWL.ObjectProperty))
        | set(g.subjects(RDF.type, OWL.DatatypeProperty))
    )
    nodes = [n for n in classes | properties if isinstance(n, URIRef)]
    if not include_external:
        freq: dict[str, int] = {}
        for n in nodes:
            base = _ns_base(str(n))
            freq[base] = freq.get(base, 0) + 1
        keep_ns = {ns for ns, _ in sorted(freq.items(), key=lambda kv: kv[1], reverse=True)[:2]}
        nodes = [n for n in nodes if _ns_base(str(n)) in keep_ns]
    out = []
    for n in sorted(nodes, key=lambda x: (_short_iri(g, x), str(x))):
        iri = str(n)
        out.append((f"{_short_iri(g, n)}  [{iri}]", iri))
    return out


def _mermaid_embed_html(mermaid_text: str) -> str:
    mermaid_safe = (
        mermaid_text.replace("</script>", "<\\/script>")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )
    return f"""<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <style>
      html, body {{ margin: 0; padding: 0; background: #fff; }}
      #status {{
        font-family: sans-serif;
        font-size: 12px;
        color: #555;
        padding: 6px 10px;
      }}
      .mermaid {{
        padding: 8px 10px;
      }}
    </style>
  </head>
  <body>
    <div id="status">Rendering Mermaid...</div>
    <pre class="mermaid">{mermaid_safe}</pre>
    <script>
      function loadScript(src) {{
        return new Promise((resolve, reject) => {{
          const s = document.createElement("script");
          s.src = src;
          s.onload = resolve;
          s.onerror = reject;
          document.head.appendChild(s);
        }});
      }}
      async function boot() {{
        const status = document.getElementById("status");
        const urls = [
          "https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js",
          "https://unpkg.com/mermaid@10/dist/mermaid.min.js"
        ];
        for (const u of urls) {{
          try {{
            await loadScript(u);
            if (window.mermaid) break;
          }} catch (_e) {{}}
        }}
        if (!window.mermaid) {{
          status.textContent = "Mermaid JS could not be loaded (network blocked).";
          return;
        }}
        try {{
          mermaid.initialize({{ startOnLoad: true, securityLevel: "loose", theme: "default" }});
          await mermaid.run({{ querySelector: ".mermaid" }});
          status.textContent = "Rendered.";
        }} catch (e) {{
          status.textContent = "Mermaid render error: " + (e && e.message ? e.message : e);
        }}
      }}
      boot();
    </script>
  </body>
</html>
"""


def _lookup_alignment_mapped_iri(focus_iri: str, target_side: str = "right", source_slug: str = "") -> str:
    if not focus_iri:
        return ""
    src_slug = (source_slug or "").strip().lower()
    candidate_paths = [
        Path("registry/reconciled_mappings.tsv"),
        Path("registry/pair_alignments.tsv"),
        Path("registry") / f"pair_alignment_candidates_{source_slug}.tsv",
    ]
    for path in candidate_paths:
        if not path.is_file():
            continue
        try:
            with path.open("r", encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh, delimiter="\t")
                for row in reader:
                    status = (
                        (row.get("mapping_status") or "").strip().lower()
                        or (row.get("status") or "").strip().lower()
                    )
                    if status != "approved":
                        continue
                    left_source = (
                        (row.get("source_term_source") or "").strip().lower()
                        or (row.get("left_source") or "").strip().lower()
                    )
                    if src_slug and left_source and left_source != src_slug:
                        continue
                    left_iri = (
                        (row.get("source_term_iri") or "").strip()
                        or (row.get("left_term_iri") or "").strip()
                    )
                    right_iri = (
                        (row.get("canonical_term_iri") or "").strip()
                        or (row.get("right_term_iri") or "").strip()
                    )
                    if target_side == "right":
                        if left_iri == focus_iri and right_iri:
                            return right_iri
                    else:
                        if right_iri == focus_iri and left_iri:
                            return left_iri
        except OSError:
            continue
    return ""


def _lookup_alignment_mapping_detail(
    focus_iri: str, target_side: str = "right", source_slug: str = ""
) -> tuple[str, str]:
    if not focus_iri:
        return "", ""
    src_slug = (source_slug or "").strip().lower()
    candidate_paths = [
        Path("registry/reconciled_mappings.tsv"),
        Path("registry/pair_alignments.tsv"),
        Path("registry") / f"pair_alignment_candidates_{source_slug}.tsv",
    ]
    for path in candidate_paths:
        if not path.is_file():
            continue
        try:
            with path.open("r", encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh, delimiter="\t")
                for row in reader:
                    status = (
                        (row.get("mapping_status") or "").strip().lower()
                        or (row.get("status") or "").strip().lower()
                    )
                    if status != "approved":
                        continue
                    left_source = (
                        (row.get("source_term_source") or "").strip().lower()
                        or (row.get("left_source") or "").strip().lower()
                    )
                    if src_slug and left_source and left_source != src_slug:
                        continue
                    left_iri = (
                        (row.get("source_term_iri") or "").strip()
                        or (row.get("left_term_iri") or "").strip()
                    )
                    right_iri = (
                        (row.get("canonical_term_iri") or "").strip()
                        or (row.get("right_term_iri") or "").strip()
                    )
                    relation = (
                        (row.get("relation") or "").strip()
                        or (row.get("mapping_relation") or "").strip()
                    )
                    if target_side == "right":
                        if left_iri == focus_iri and right_iri:
                            return right_iri, relation
                    else:
                        if right_iri == focus_iri and left_iri:
                            return left_iri, relation
        except OSError:
            continue
    return "", ""


def _ttl_contains_iri(input_ttl: Path, iri: str) -> bool:
    if not iri or not input_ttl.is_file():
        return False
    g = Graph()
    try:
        g.parse(str(input_ttl))
    except Exception:
        return False
    target = URIRef(iri)
    for s, p, o in g:
        if s == target or p == target or o == target:
            return True
    return False




def _collect_prefixes(input_ttl: Path) -> dict[str, str]:
    g = Graph()
    g.parse(str(input_ttl))
    prefixes: dict[str, str] = {}
    for pref, ns in g.namespace_manager.namespaces():
        label = pref if pref else ":"
        prefixes[label] = str(ns)
    return dict(sorted(prefixes.items(), key=lambda kv: kv[0]))


def _prefix_filtered_ttl_path(source_slug: str, variant: str, prefix: str) -> Path:
    suffix = "before" if variant == "before" else "after"
    safe = prefix.replace(":", "").replace("/", "_").replace("#", "")
    return Path("registry/schema_docs") / f"{source_slug}_{suffix}_{safe}_filtered.ttl"


def _merged_ttl_output_path(source_slug: str, tag: str) -> Path:
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", (tag or "merged"))
    return Path("registry/schema_docs") / f"{source_slug}_{safe}.ttl"


def _write_merged_ttl(inputs: list[Path], output_ttl: Path) -> tuple[bool, str]:
    g = Graph()
    loaded = 0
    for p in inputs:
        if not p.is_file():
            continue
        try:
            g.parse(str(p))
            loaded += 1
        except Exception as exc:
            return False, f"Failed to parse `{to_relpath(p)}`: {exc}"
    if loaded == 0:
        return False, "No input TTL files available to merge."
    output_ttl.parent.mkdir(parents=True, exist_ok=True)
    g.serialize(destination=str(output_ttl), format="turtle")
    return True, f"Merged {loaded} TTL file(s) into `{to_relpath(output_ttl)}`."


def _write_prefix_filtered_ttl(input_ttl: Path, prefix_iri: str, output_ttl: Path) -> tuple[bool, str]:
    g = Graph()
    try:
        g.parse(str(input_ttl))
    except Exception as exc:
        return False, f"Failed to parse TTL: {exc}"

    focus: set[URIRef] = set()
    for s, p, o in g:
        if isinstance(s, URIRef) and str(s).startswith(prefix_iri):
            focus.add(s)
        if isinstance(p, URIRef) and str(p).startswith(prefix_iri):
            focus.add(p)
        if isinstance(o, URIRef) and str(o).startswith(prefix_iri):
            focus.add(o)

    if not focus:
        return False, f"No entities found for namespace `{prefix_iri}`."

    out = Graph()
    for pref, ns in g.namespace_manager.namespaces():
        out.bind(pref, ns)

    kept = 0
    for s, p, o in g:
        keep = False
        if isinstance(s, URIRef) and s in focus:
            keep = True
        if isinstance(o, URIRef) and o in focus:
            keep = True
        if isinstance(p, URIRef) and p in focus:
            keep = True
        if keep:
            out.add((s, p, o))
            kept += 1

    output_ttl.parent.mkdir(parents=True, exist_ok=True)
    out.serialize(destination=str(output_ttl), format="turtle")
    return True, f"Filtered TTL written with {kept} triple(s)."




def _vega_edge_output_path(source_slug: str, variant: str, scenario: str = "") -> Path:
    suffix = "before" if variant == "before" else "after"
    scen = (scenario or "default").replace(" ", "_").replace("/", "_")
    return Path("registry/schema_docs") / f"{source_slug}_{suffix}_{scen}_vega_edge_bundling.json"




def _discover_uri_prefixes(input_ttl: Path, limit: int = 5000) -> list[str]:
    g = Graph()
    g.parse(str(input_ttl))
    out: set[str] = set()
    seen = 0
    for s1, p1, o1 in g:
        for t in (s1, p1, o1):
            if isinstance(t, URIRef):
                out.add(_ns_base(str(t)))
                seen += 1
                if seen >= limit:
                    return sorted(out)
    return sorted(out)


def _normalize_excluded_prefixes(prefixes: list[str] | None) -> list[str]:
    norm: set[str] = set()
    for raw in prefixes or []:
        p = (raw or "").strip()
        if not p:
            continue
        norm.add(p)
        if p.endswith("#") or p.endswith("/"):
            norm.add(p[:-1])
        else:
            norm.add(p + "#")
            norm.add(p + "/")
    return sorted(norm)


def _build_vega_edge_bundling_spec(
    input_ttl: Path,
    max_nodes: int,
    include_external: bool,
    scenario: str = "class_domain_range",
    max_links: int = 20000,
    excluded_prefixes: list[str] | None = None,
    include_blank_nodes: bool = False,
) -> tuple[bool, dict, str]:
    g = Graph()
    try:
        g.parse(str(input_ttl))
    except Exception as exc:
        return False, {}, f"Failed to parse TTL: {exc}"

    excluded = _normalize_excluded_prefixes(excluded_prefixes)

    def _is_excluded_uri(term) -> bool:
        if not isinstance(term, URIRef):
            return False
        val = str(term)
        return any(val.startswith(pref) for pref in excluded)

    def _label(term) -> str:
        if isinstance(term, URIRef):
            return _short_iri(g, term)
        if isinstance(term, Literal):
            txt = str(term)
            return txt if len(txt) <= 48 else (txt[:45] + "...")
        if isinstance(term, BNode):
            return "_:bnode"
        return str(term)

    def _term_id(term) -> str:
        if isinstance(term, URIRef):
            return str(term)
        if isinstance(term, Literal):
            h = hashlib.sha1(str(term).encode("utf-8", errors="ignore")).hexdigest()[:12]
            return f"lit:{h}"
        h = hashlib.sha1(str(term).encode("utf-8", errors="ignore")).hexdigest()[:12]
        return f"bn:{h}"

    def _parent_for_term(term) -> str:
        if isinstance(term, URIRef):
            return "ns:" + _ns_base(str(term))
        if isinstance(term, Literal):
            return "ns:_literal"
        return "ns:_bnode"

    diagnostics: dict[str, object] = {"scenario": scenario}

    # Scenario A/B: class-centered hierarchy
    if scenario in {"class_domain_range", "class_all_predicates"}:
        classes = set(g.subjects(RDF.type, OWL.Class)) | set(g.subjects(RDF.type, RDFS.Class))
        class_nodes = [n for n in classes if isinstance(n, URIRef) and not _is_excluded_uri(n)]
        if not class_nodes:
            return False, {}, "No classes found in TTL."

        if not include_external:
            freq: dict[str, int] = {}
            for n in class_nodes:
                base = _ns_base(str(n))
                freq[base] = freq.get(base, 0) + 1
            keep_ns = {ns for ns, _ in sorted(freq.items(), key=lambda kv: kv[1], reverse=True)[:2]}
            class_nodes = [n for n in class_nodes if _ns_base(str(n)) in keep_ns]

        class_nodes = sorted(class_nodes, key=lambda n: _short_iri(g, n))[:max_nodes]
        node_set = set(class_nodes)

        parent_candidates: dict[URIRef, list[URIRef]] = {n: [] for n in class_nodes}
        for child, _, parent in g.triples((None, RDFS.subClassOf, None)):
            if isinstance(child, URIRef) and isinstance(parent, URIRef):
                if child in node_set and parent in node_set and child != parent:
                    parent_candidates[child].append(parent)

        rows = [{"id": "root", "parent": None, "name": "root"}]
        parent_of: dict[URIRef, str] = {}
        for n in class_nodes:
            parents = sorted(parent_candidates.get(n, []), key=lambda x: str(x))
            parent_id = str(parents[0]) if parents else "root"
            parent_of[n] = parent_id
            rows.append({"id": str(n), "parent": parent_id, "name": _short_iri(g, n)})

        children_map: dict[str, set[str]] = {}
        for n in class_nodes:
            nid = str(n)
            pid = parent_of[n]
            children_map.setdefault(pid, set()).add(nid)
            children_map.setdefault(nid, set())

        leaf_ids = {str(n) for n in class_nodes if not children_map.get(str(n))}
        memo: dict[str, set[str]] = {}

        def leaf_descendants(node_id: str) -> set[str]:
            if node_id in memo:
                return memo[node_id]
            kids = children_map.get(node_id, set())
            if not kids:
                out = {node_id} if node_id in leaf_ids else set()
                memo[node_id] = out
                return out
            out: set[str] = set()
            for k in kids:
                out |= leaf_descendants(k)
            memo[node_id] = out
            return out

        deps: dict[tuple[str, str], str] = {}
        raw_pairs = 0
        dropped_no_leaf = 0

        if scenario == "class_domain_range":
            predicates = (
                set(g.subjects(RDF.type, OWL.ObjectProperty))
                | set(g.subjects(RDF.type, OWL.DatatypeProperty))
                | set(g.subjects(RDF.type, RDF.Property))
            )
            for p in predicates:
                if not isinstance(p, URIRef) or _is_excluded_uri(p):
                    continue
                domains = [d for d in g.objects(p, RDFS.domain) if isinstance(d, URIRef) and d in node_set and not _is_excluded_uri(d)]
                ranges = [r for r in g.objects(p, RDFS.range) if isinstance(r, URIRef) and r in node_set and not _is_excluded_uri(r)]
                for d in domains:
                    for r in ranges:
                        if d == r:
                            continue
                        raw_pairs += 1
                        d_leaves = leaf_descendants(str(d)) or ({str(d)} if str(d) in leaf_ids else set())
                        r_leaves = leaf_descendants(str(r)) or ({str(r)} if str(r) in leaf_ids else set())
                        if not d_leaves or not r_leaves:
                            dropped_no_leaf += 1
                            continue
                        for dl in d_leaves:
                            for rl in r_leaves:
                                if dl != rl:
                                    deps[(dl, rl)] = _short_iri(g, p)
                                if len(deps) >= max_links:
                                    break
                            if len(deps) >= max_links:
                                break
                        if len(deps) >= max_links:
                            break
                    if len(deps) >= max_links:
                        break
                if len(deps) >= max_links:
                    break
        else:
            # all predicates between classes
            for s1, p1, o1 in g:
                if not (isinstance(s1, URIRef) and isinstance(o1, URIRef)):
                    continue
                if _is_excluded_uri(s1) or _is_excluded_uri(p1) or _is_excluded_uri(o1):
                    continue
                if s1 not in node_set or o1 not in node_set or s1 == o1:
                    continue
                raw_pairs += 1
                edge_class = _short_iri(g, p1) if isinstance(p1, URIRef) else "link"
                s_leaves = leaf_descendants(str(s1)) or ({str(s1)} if str(s1) in leaf_ids else set())
                o_leaves = leaf_descendants(str(o1)) or ({str(o1)} if str(o1) in leaf_ids else set())
                if not s_leaves or not o_leaves:
                    dropped_no_leaf += 1
                    continue
                for sl in s_leaves:
                    for ol in o_leaves:
                        if sl != ol:
                            deps[(sl, ol)] = edge_class
                        if len(deps) >= max_links:
                            break
                    if len(deps) >= max_links:
                        break
                if len(deps) >= max_links:
                    break

        dep_rows = [
            {"source": s1, "target": t1, "edge_class": c1}
            for (s1, t1), c1 in sorted(deps.items())
        ]
        class_count = len(class_nodes)
        leaf_count = len(leaf_ids)
        diagnostics.update(
            {
                "endpoint_mode": "leaf_projected",
                "raw_pairs": raw_pairs,
                "dropped_no_leaf": dropped_no_leaf,
                "kept_edges": len(dep_rows),
                "class_nodes": class_count,
                "leaf_nodes": leaf_count,
            }
        )

    else:
        # Scenario C: all entities and all links (full triple expansion)
        entities_by_id: dict[str, object] = {}
        dep_rows_raw: list[dict[str, str]] = []
        triples_seen = 0
        triples_skipped_annotation = 0
        triples_skipped_literal_object = 0
        skip_predicates = {
            str(RDFS.comment),
            str(RDFS.label),
            str(RDFS.seeAlso),
            str(OWL.versionInfo),
            "http://purl.org/dc/terms/description",
            "http://purl.org/dc/terms/abstract",
            "http://www.w3.org/2004/02/skos/core#definition",
            "http://www.w3.org/2004/02/skos/core#note",
        }

        for s1, p1, o1 in g:
            triples_seen += 1
            if not include_blank_nodes and (isinstance(s1, BNode) or isinstance(p1, BNode) or isinstance(o1, BNode)):
                continue
            if _is_excluded_uri(s1) or _is_excluded_uri(p1) or _is_excluded_uri(o1):
                continue
            if isinstance(p1, URIRef) and str(p1) in skip_predicates:
                triples_skipped_annotation += 1
                continue
            if isinstance(o1, Literal):
                triples_skipped_literal_object += 1
                continue

            sid = _term_id(s1)
            pid = _term_id(p1)
            oid = _term_id(o1)
            entities_by_id[sid] = s1
            entities_by_id[pid] = p1
            entities_by_id[oid] = o1

            if sid != oid:
                edge_class = _short_iri(g, p1) if isinstance(p1, URIRef) else "link"
                dep_rows_raw.append({"source": sid, "target": oid, "edge_class": edge_class})
            if len(dep_rows_raw) >= max_links:
                break

        entities = list(entities_by_id.values())
        if not include_external:
            uri_entities = [e for e in entities if isinstance(e, URIRef)]
            freq: dict[str, int] = {}
            for e in uri_entities:
                base = _ns_base(str(e))
                freq[base] = freq.get(base, 0) + 1
            keep_ns = {ns for ns, _ in sorted(freq.items(), key=lambda kv: kv[1], reverse=True)[:2]}
            entities = [
                e for e in entities
                if (not isinstance(e, URIRef)) or (_ns_base(str(e)) in keep_ns)
            ]

        # Keep nodes that participate in links first.
        used_ids = {d["source"] for d in dep_rows_raw} | {d["target"] for d in dep_rows_raw}
        prioritized = [entities_by_id[i] for i in used_ids if i in entities_by_id]
        others = [e for e in entities if _term_id(e) not in used_ids]
        entities = (prioritized + others)[:max_nodes]
        idset = {_term_id(e) for e in entities}

        rows = [{"id": "root", "parent": None, "name": "root"}]
        namespace_ids: set[str] = set()
        for e in entities:
            pid = _parent_for_term(e)
            if pid not in namespace_ids:
                namespace_ids.add(pid)
                rows.append({"id": pid, "parent": "root", "name": pid.replace("ns:", "")})
            rows.append({"id": _term_id(e), "parent": pid, "name": _label(e)})

        dep_rows = [
            d for d in dep_rows_raw
            if d["source"] in idset and d["target"] in idset and d["source"] != d["target"]
        ][:max_links]

        class_count = len(entities)
        leaf_count = len(entities)
        diagnostics.update(
            {
                "endpoint_mode": "entity_leaf",
                "triples_seen": triples_seen,
                "raw_edges": len(dep_rows_raw),
                "kept_edges": len(dep_rows),
                "skipped_annotation_predicate": triples_skipped_annotation,
                "skipped_literal_object": triples_skipped_literal_object,
                "entity_nodes": class_count,
            }
        )

    edge_interpolate = "bundle"
    edge_opacity = 0.28 if scenario != "all_entities_all_links" else 0.45
    edge_width = 1.2 if scenario != "all_entities_all_links" else 1.1

    spec = {
        "$schema": "https://vega.github.io/schema/vega/v6.json",
        "description": f"Ontology hierarchical edge bundling ({scenario}).",
        "usermeta": diagnostics,
        "padding": 5,
        "width": 720,
        "height": 720,
        "autosize": "none",
        "signals": [
            {"name": "tension", "value": 0.85, "bind": {"input": "range", "min": 0, "max": 1, "step": 0.01}},
            {"name": "radius", "value": 280, "bind": {"input": "range", "min": 20, "max": 400}},
            {"name": "extent", "value": 360, "bind": {"input": "range", "min": 0, "max": 360, "step": 1}},
            {"name": "rotate", "value": 0, "bind": {"input": "range", "min": 0, "max": 360, "step": 1}},
            {"name": "textSize", "value": 8, "bind": {"input": "range", "min": 2, "max": 20, "step": 1}},
            {"name": "textOffset", "value": 2, "bind": {"input": "range", "min": 0, "max": 10, "step": 1}},
            {"name": "layout", "value": "cluster", "bind": {"input": "radio", "options": ["tidy", "cluster"]}},
            {"name": "colorIn", "value": "firebrick"},
            {"name": "colorOut", "value": "forestgreen"},
            {"name": "originX", "update": "width / 2"},
            {"name": "originY", "update": "height / 2"},
            {
                "name": "active", "value": None,
                "on": [
                    {"events": "text:pointerover", "update": "datum.id"},
                    {"events": "pointerover[!event.item]", "update": "null"}
                ]
            }
        ],
        "data": [
            {
                "name": "tree",
                "values": rows,
                "transform": [
                    {"type": "stratify", "key": "id", "parentKey": "parent"},
                    {"type": "tree", "method": {"signal": "layout"}, "size": [1, 1], "as": ["alpha", "beta", "depth", "children"]},
                    {"type": "formula", "expr": "(rotate + extent * datum.alpha + 270) % 360", "as": "angle"},
                    {"type": "formula", "expr": "inrange(datum.angle, [90, 270])", "as": "leftside"},
                    {"type": "formula", "expr": "originX + radius * datum.beta * cos(PI * datum.angle / 180)", "as": "x"},
                    {"type": "formula", "expr": "originY + radius * datum.beta * sin(PI * datum.angle / 180)", "as": "y"}
                ]
            },
            {"name": "leaves", "source": "tree", "transform": [{"type": "filter", "expr": "!datum.children && datum.id !== 'root'"}]},
            {
                "name": "dependencies",
                "values": dep_rows,
                "transform": [
                    {"type": "formula", "expr": "treePath(\'tree\', datum.source, datum.target)", "as": "treepath", "initonly": True},
                    {"type": "filter", "expr": "datum.treepath && length(datum.treepath) > 1"}
                ]
            },
            {
                "name": "dependency_paths",
                "source": "dependencies",
                "transform": [
                    {"type": "identifier", "as": "dep_id"},
                    {"type": "flatten", "fields": ["treepath"], "as": ["node"], "index": "path_index"},
                    {"type": "formula", "expr": "isObject(datum.node) ? datum.node.id : datum.node", "as": "node_id"},
                    {"type": "lookup", "from": "tree", "key": "id", "fields": ["node_id"], "as": ["node_row"]},
                    {"type": "formula", "expr": "isObject(datum.node) ? datum.node.x : (datum.node_row ? datum.node_row.x : null)", "as": "x"},
                    {"type": "formula", "expr": "isObject(datum.node) ? datum.node.y : (datum.node_row ? datum.node_row.y : null)", "as": "y"},
                    {"type": "filter", "expr": "isValid(datum.x) && isValid(datum.y)"},
                ],
            },
            {"name": "selected", "source": "dependencies", "transform": [{"type": "filter", "expr": "datum.source === active || datum.target === active"}]}
        ],
        "scales": [
            {
                "name": "edgeColor",
                "type": "ordinal",
                "domain": {"data": "dependencies", "field": "edge_class"},
                "range": {"scheme": "category20"},
            }
        ],
        "legends": [
            {"stroke": "edgeColor", "title": "Edge class", "orient": "right"}
        ],
        "marks": [
            {
                "type": "text",
                "from": {"data": "leaves"},
                "encode": {
                    "enter": {"text": {"field": "name"}, "baseline": {"value": "middle"}},
                    "update": {
                        "x": {"field": "x"},
                        "y": {"field": "y"},
                        "dx": {"signal": "textOffset * (datum.leftside ? -1 : 1)"},
                        "angle": {"signal": "datum.leftside ? datum.angle - 180 : datum.angle"},
                        "align": {"signal": "datum.leftside ? 'right' : 'left'"},
                        "fontSize": {"signal": "textSize"},
                        "fontWeight": [
                            {"test": "indata('selected', 'source', datum.id)", "value": "bold"},
                            {"test": "indata('selected', 'target', datum.id)", "value": "bold"},
                            {"value": None}
                        ],
                        "fill": [
                            {"test": "datum.id === active", "value": "black"},
                            {"test": "indata('selected', 'source', datum.id)", "signal": "colorIn"},
                            {"test": "indata('selected', 'target', datum.id)", "signal": "colorOut"},
                            {"value": "#111827"}
                        ]
                    }
                }
            },
            {
                "type": "group",
                "from": {
                    "facet": {
                        "name": "path",
                        "data": "dependency_paths",
                        "groupby": ["dep_id", "source", "target", "edge_class"],
                    }
                },
                "marks": [
                    {
                        "type": "line",
                        "interactive": False,
                        "from": {"data": "path"},
                        "sort": {"field": "path_index", "order": "ascending"},
                        "encode": {
                            "enter": {"interpolate": {"value": edge_interpolate}, "strokeWidth": {"value": edge_width}},
                            "update": {
                                "stroke": [
                                    {"test": "parent.source === active", "signal": "colorOut"},
                                    {"test": "parent.target === active", "signal": "colorIn"},
                                    {"scale": "edgeColor", "field": "parent.edge_class"}
                                ],
                                "strokeOpacity": [
                                    {"test": "parent.source === active || parent.target === active", "value": 1},
                                    {"value": edge_opacity}
                                ],
                                "tension": {"signal": "tension"},
                                "x": {"field": "x"},
                                "y": {"field": "y"}
                            }
                        }
                    }
                ]
            }
        ]
    }

    msg = (
        f"Generated Vega spec with {class_count} node(s), "
        f"{leaf_count} leaf node(s), and {len(dep_rows)} dependency edge(s)."
    )
    if scenario in {"class_domain_range", "class_all_predicates"}:
        msg += (
            f" Raw candidate links: {int(diagnostics.get('raw_pairs', 0))}; "
            f"dropped (non-leaf endpoint): {int(diagnostics.get('dropped_no_leaf', 0))}."
        )
    else:
        msg += (
            f" Raw edges: {int(diagnostics.get('raw_edges', 0))}; "
            f"filtered by node cap/filters: "
            f"{max(0, int(diagnostics.get('raw_edges', 0)) - int(diagnostics.get('kept_edges', 0)))}."
        )
    if scenario == "all_entities_all_links":
        msg += " (full-entity mode keeps bundling; increase max links if sparse)"
    if len(dep_rows) >= max_links:
        msg += f" (capped at {max_links} links)"
    return True, spec, msg



def _vega_embed_html(spec: dict) -> str:
    spec_json = json.dumps(spec).replace("</script>", "<\\/script>")
    return f"""<!doctype html>
<html>
  <head><meta charset="utf-8" /></head>
  <body>
    <div id="status" style="font-family:Arial,sans-serif;font-size:12px;color:#555;padding:6px 8px;">Rendering Vega...</div>
    <div id="vis"></div>
    <script>
      function loadScript(src) {{
        return new Promise((resolve, reject) => {{
          const s = document.createElement('script');
          s.src = src;
          s.onload = resolve;
          s.onerror = reject;
          document.head.appendChild(s);
        }});
      }}
      async function boot() {{
        const status = document.getElementById('status');
        const sets = [
          [
            'https://cdn.jsdelivr.net/npm/vega@5',
            'https://cdn.jsdelivr.net/npm/vega-lite@5',
            'https://cdn.jsdelivr.net/npm/vega-embed@6'
          ],
          [
            'https://unpkg.com/vega@5',
            'https://unpkg.com/vega-lite@5',
            'https://unpkg.com/vega-embed@6'
          ]
        ];
        let loaded = false;
        for (const urls of sets) {{
          try {{
            for (const u of urls) await loadScript(u);
            if (window.vegaEmbed) {{ loaded = true; break; }}
          }} catch (_e) {{}}
        }}
        if (!loaded) {{
          status.textContent = 'Could not load Vega libraries (network blocked).';
          return;
        }}
        const spec = {spec_json};
        try {{
          const result = await vegaEmbed('#vis', spec, {{actions: true, renderer: 'svg'}});
          const view = result && result.view ? result.view : null;
          if (view) {{
            const depCount = (view.data('dependencies') || []).length;
            const pathCount = (view.data('dependency_paths') || []).length;
            status.textContent = 'Rendered. dependencies=' + depCount + ', dependency_paths=' + pathCount;
          }} else {{
            status.textContent = 'Rendered.';
          }}
        }} catch (e) {{
          status.textContent = 'Vega render error: ' + (e && e.message ? e.message : e);
        }}
      }}
      boot();
    </script>
  </body>
</html>"""


def render() -> None:
    st.title("View schema")

    ctx = active_source_context()
    if ctx is None:
        st.warning("No source slug selected. Pick one in the sidebar first.")
        return

    variant_key = f"view_schema_variant_{ctx.source_id}"
    if variant_key not in st.session_state:
        st.session_state[variant_key] = "before"
    variant = str(st.session_state.get(variant_key, "before"))
    if variant not in {"before", "after"}:
        variant = "before"
        st.session_state[variant_key] = variant

    if variant == "before":
        input_ttl = ctx.download_ttl
    else:
        input_ttl = Path("registry/exports") / f"{ctx.source_id}_updated.ttl"
    output_html = _doc_output_path(ctx.source_id, variant)
    graph_html = _graph_output_path(ctx.source_id, variant)
    mermaid_path = _mermaid_output_path(ctx.source_id, variant)

    input_ttl_exists = input_ttl.is_file()
    if not input_ttl_exists:
        st.warning("Selected TTL file is missing. Generate/download it first.")

    prefix_map: dict[str, str] = {}
    if input_ttl_exists:
        try:
            prefix_map = _collect_prefixes(input_ttl)
        except Exception as exc:
            st.error(f"Could not read prefixes from TTL: {exc}")
            prefix_map = {}

    prefix_key = f"view_schema_prefix_{ctx.source_id}_{variant}"
    if prefix_key not in st.session_state:
        st.session_state[prefix_key] = "(all)"
    selected_prefix = str(st.session_state.get(prefix_key, "(all)"))
    if selected_prefix != "(all)" and selected_prefix not in prefix_map:
        selected_prefix = "(all)"
        st.session_state[prefix_key] = "(all)"

    effective_input_ttl = input_ttl
    if selected_prefix != "(all)" and selected_prefix in prefix_map:
        filtered_ttl = _prefix_filtered_ttl_path(ctx.source_id, variant, selected_prefix)
        if filtered_ttl.is_file():
            effective_input_ttl = filtered_ttl

    tab_doc, tab_graph, tab_rdfglance, tab_mermaid, tab_vega = st.tabs(
        ["Documentation", "Interactive graph", "RDFGlance", "Mermaid", "Vega Bundling"]
    )

    with tab_doc:
        st.caption("Preview ontology documentation generated with pyLODE or WIDOCO.")
        ttl_options = _list_ttl_candidates()
        if input_ttl.is_file() and input_ttl.resolve() not in ttl_options:
            ttl_options = [input_ttl.resolve(), *ttl_options]
        if effective_input_ttl.is_file() and effective_input_ttl.resolve() not in ttl_options:
            ttl_options = [effective_input_ttl.resolve(), *ttl_options]
        if not ttl_options:
            st.info("No TTL files found in `registry/` or `lib/`.")
        else:
            default_ttl = effective_input_ttl.resolve() if effective_input_ttl.is_file() else ttl_options[0]
            default_idx = ttl_options.index(default_ttl) if default_ttl in ttl_options else 0
            selected_doc_ttl = st.selectbox(
                "Input TTL (searchable)",
                options=ttl_options,
                index=default_idx,
                format_func=lambda p: to_relpath(p),
                key=f"doc_ttl_select_{ctx.source_id}_{variant}",
                help="Choose any local TTL file to generate/view documentation.",
            )
            widoco_ok, widoco_msg = _widoco_available()
            selected_doc_generator_label = st.selectbox(
                "Documentation generator",
                options=["pyLODE", "WIDOCO"],
                index=0,
                key=f"doc_generator_{ctx.source_id}_{variant}",
                help="Choose which generator to use for documentation HTML.",
            )
            selected_doc_generator = selected_doc_generator_label.lower()
            selected_doc_output = _doc_output_path_for_input_with_generator(selected_doc_ttl, selected_doc_generator)
            st.caption(f"Selected input: `{to_relpath(selected_doc_ttl)}`")
            st.caption(f"Generator: `{selected_doc_generator_label}`")
            if selected_doc_generator == "widoco":
                st.caption(f"WIDOCO folder: `{to_relpath(_widoco_output_dir_for_input(selected_doc_ttl))}`")
            st.caption(f"Doc HTML: `{to_relpath(selected_doc_output)}`")
            if selected_doc_generator == "widoco" and not widoco_ok:
                st.warning(widoco_msg)
            elif selected_doc_generator == "widoco":
                st.caption(widoco_msg)

            dcol1, dcol2 = st.columns(2)
            with dcol1:
                if st.button("Generate / refresh documentation", type="primary", key=f"doc_generate_{ctx.source_id}_{variant}"):
                    if selected_doc_generator == "widoco":
                        ok, message = _run_widoco(
                            selected_doc_ttl,
                            _widoco_output_dir_for_input(selected_doc_ttl),
                            selected_doc_output,
                        )
                    else:
                        ok, message = _run_pylode(selected_doc_ttl, selected_doc_output)
                    if ok:
                        st.success(message)
                    else:
                        st.error(message)
            with dcol2:
                cached_html = _resolve_doc_html(selected_doc_generator, selected_doc_ttl, selected_doc_output)
                if cached_html is not None and st.button("Use cached documentation", key=f"doc_cached_{ctx.source_id}_{variant}"):
                    st.info(f"Using cached {selected_doc_generator_label} output.")

            current_doc_html = _resolve_doc_html(selected_doc_generator, selected_doc_ttl, selected_doc_output)
            if current_doc_html is None:
                st.info("No documentation generated yet for this version. Click 'Generate / refresh documentation'.")
            else:
                action_col1, action_col2 = st.columns(2)
                with action_col1:
                    if st.button("Open documentation in browser", use_container_width=True):
                        try:
                            ok = webbrowser.open_new_tab(current_doc_html.resolve().as_uri())
                            if ok:
                                st.success("Opened documentation in your default browser.")
                            else:
                                st.warning("Could not open browser automatically. Use 'Download HTML' instead.")
                        except Exception as exc:
                            st.warning(f"Could not open browser automatically: {exc}")
                with action_col2:
                    st.download_button(
                        "Download HTML",
                        data=file_to_bytes(current_doc_html),
                        file_name=current_doc_html.name,
                        mime="text/html",
                        key=f"download_schema_doc_{ctx.source_id}_{variant}",
                        use_container_width=True,
                    )

                if selected_doc_generator == "widoco":
                    try:
                        html_text = _widoco_inapp_html(current_doc_html)
                        components.html(html_text, height=900, scrolling=True)
                    except Exception:
                        components.iframe(current_doc_html.resolve().as_uri(), height=900, scrolling=True)
                else:
                    html_text = current_doc_html.read_text(encoding="utf-8", errors="replace")
                    components.html(html_text, height=900, scrolling=True)

                if selected_doc_generator == "widoco":
                    widoco_dir = _widoco_output_dir_for_input(selected_doc_ttl)
                    webvowl_html = _widoco_standalone_webvowl_html(widoco_dir) or _find_widoco_webvowl_html(widoco_dir)
                    webvowl_data = widoco_dir / "webvowl" / "data" / "ontology.json"
                    st.markdown("**WIDOCO graph (WebVOWL)**")
                    if webvowl_html is None:
                        st.info("No WIDOCO WebVOWL graph found. Re-run WIDOCO generation for this TTL.")
                    else:
                        st.caption(
                            "In-app WebVOWL rendering is disabled for stability. "
                            "Use the standalone HTML below. The raw WIDOCO bundle may require `http://` serving."
                        )
                        gcol1, gcol2, gcol3 = st.columns(3)
                        with gcol1:
                            if st.button("Open WIDOCO WebVOWL in browser", key=f"open_widoco_webvowl_{ctx.source_id}_{variant}"):
                                try:
                                    ok = webbrowser.open_new_tab(webvowl_html.resolve().as_uri())
                                    if ok:
                                        st.success("Opened WIDOCO WebVOWL graph in your default browser.")
                                    else:
                                        st.warning("Could not open browser automatically.")
                                except Exception as exc:
                                    st.warning(f"Could not open browser automatically: {exc}")
                        with gcol2:
                            st.download_button(
                                "Download WIDOCO WebVOWL HTML",
                                data=file_to_bytes(webvowl_html),
                                file_name=webvowl_html.name,
                                mime="text/html",
                                key=f"download_widoco_webvowl_{ctx.source_id}_{variant}",
                                use_container_width=True,
                            )
                        with gcol3:
                            if webvowl_data.is_file():
                                st.download_button(
                                    "Download ontology.json",
                                    data=file_to_bytes(webvowl_data),
                                    file_name=webvowl_data.name,
                                    mime="application/json",
                                    key=f"download_widoco_webvowl_json_{ctx.source_id}_{variant}",
                                    use_container_width=True,
                                )
                        if (widoco_dir / "webvowl").is_dir():
                            st.download_button(
                                "Download raw WebVOWL bundle (.zip)",
                                data=_zip_directory_bytes(widoco_dir / "webvowl"),
                                file_name=f"{widoco_dir.name}_webvowl.zip",
                                mime="application/zip",
                                key=f"download_widoco_webvowl_zip_{ctx.source_id}_{variant}",
                                use_container_width=True,
                            )

    with tab_graph:
        max_nodes = st.slider(
            "Max nodes",
            min_value=50,
            max_value=1000,
            value=350,
            step=50,
            help="Limits rendered graph size for responsiveness.",
        )
        graph_search = st.text_input(
            "Search nodes (label or IRI contains)",
            value="",
            key=f"graph_search_{ctx.source_id}_{variant}",
            help="Filters nodes before rendering.",
        )
        node_scale = st.slider(
            "Node size scale",
            min_value=1.0,
            max_value=3.0,
            value=1.6,
            step=0.1,
            help="Increase node and label sizes for readability.",
            key=f"graph_node_scale_{ctx.source_id}_{variant}",
        )
        gcol1, gcol2 = st.columns(2)
        with gcol1:
            if st.button("Generate / refresh interactive graph", type="primary"):
                ok, message = _build_graph_html(effective_input_ttl, graph_html, max_nodes=max_nodes, node_scale=node_scale, search_query=graph_search)
                if ok:
                    st.success(message)
                else:
                    st.error(message)
        with gcol2:
            if graph_html.is_file():
                st.download_button(
                    "Download graph HTML",
                    data=file_to_bytes(graph_html),
                    file_name=graph_html.name,
                    mime="text/html",
                    key=f"download_schema_graph_{ctx.source_id}_{variant}",
                    use_container_width=True,
                )

        if graph_html.is_file():
            graph_text = graph_html.read_text(encoding="utf-8", errors="replace")
            components.html(graph_text, height=900, scrolling=True)
        else:
            st.info("No interactive graph yet. Click 'Generate / refresh interactive graph'.")

    with tab_rdfglance:
        st.caption(
            "RDFGlance is an external WASM viewer. It can preload a dataset only from a public URL "
            "that allows CORS (`access-control-allow-origin: *`)."
        )
        base = "https://xdobry.github.io/rdfglance/"
        ttl_public_url = st.text_input(
            "Public TTL URL (optional)",
            value="",
            placeholder="https://example.org/schema.ttl",
            help="If set, RDFGlance is opened with `?url=<your-ttl-url>`.",
            key=f"rdfglance_ttl_url_{ctx.source_id}_{variant}",
        ).strip()
        if ttl_public_url:
            rdfglance_url = f"{base}?url={quote_plus(ttl_public_url)}"
        else:
            rdfglance_url = base

        rg_col1, rg_col2 = st.columns(2)
        with rg_col1:
            if st.button("Open RDFGlance in browser", use_container_width=True):
                try:
                    ok = webbrowser.open_new_tab(rdfglance_url)
                    if ok:
                        st.success("Opened RDFGlance in your default browser.")
                    else:
                        st.warning("Could not open browser automatically.")
                except Exception as exc:
                    st.warning(f"Could not open browser automatically: {exc}")
        with rg_col2:
            st.link_button("Open RDFGlance here", rdfglance_url, use_container_width=True)

        components.iframe(rdfglance_url, height=900, scrolling=True)

    with tab_mermaid:
        st.caption("Generate a compact Mermaid graph from TTL (best for structural ontology views).")
        with st.expander("Plot single graph", expanded=False):
            st.radio(
                "Schema version",
                options=["before", "after"],
                horizontal=True,
                format_func=lambda v: (
                    "Before curation (downloaded TTL)"
                    if v == "before"
                    else "After curation (exported TTL)"
                ),
                key=variant_key,
            )
            st.caption(f"Input TTL: `{to_relpath(input_ttl)}`")
            st.caption(f"Doc HTML: `{to_relpath(output_html)}`")
            st.caption(f"Graph HTML: `{to_relpath(graph_html)}`")

            st.markdown("**Step 1: Prefix Filter (optional)**")
            prefix_options = ["(all)"] + list(prefix_map.keys())
            st.selectbox(
                "Entity prefix",
                options=prefix_options,
                help="Restrict schema views to entities from one namespace prefix (e.g. `emi`).",
                key=prefix_key,
            )
            selected_prefix = str(st.session_state.get(prefix_key, "(all)"))
            if selected_prefix != "(all)" and selected_prefix in prefix_map:
                filtered_ttl = _prefix_filtered_ttl_path(ctx.source_id, variant, selected_prefix)
                pcol1, pcol2 = st.columns([1, 2])
                with pcol1:
                    if st.button("Apply prefix filter", type="primary", key=f"apply_prefix_{ctx.source_id}_{variant}"):
                        ok, msg = _write_prefix_filtered_ttl(input_ttl, prefix_map[selected_prefix], filtered_ttl)
                        if ok:
                            st.success(msg)
                        else:
                            st.error(msg)
                with pcol2:
                    st.caption(f"Namespace: `{selected_prefix}` -> `{prefix_map[selected_prefix]}`")
                if filtered_ttl.is_file():
                    st.caption(f"Effective TTL: `{to_relpath(filtered_ttl)}`")
                else:
                    st.info("Apply the prefix filter to generate a scoped TTL for this view.")
            else:
                st.caption(f"Effective TTL: `{to_relpath(effective_input_ttl)}`")

            mcol1, mcol2, mcol3 = st.columns(3)
            with mcol1:
                mermaid_mode = st.radio(
                    "Mode",
                    options=["taxonomy", "schema"],
                    format_func=lambda m: (
                        "Taxonomy (subClassOf/subPropertyOf)"
                        if m == "taxonomy"
                        else "Schema relations (+ domain/range)"
                    ),
                    key=f"mermaid_mode_{ctx.source_id}_{variant}",
                )
            with mcol2:
                mermaid_max_nodes = st.slider(
                    "Max nodes",
                    min_value=50,
                    max_value=1500,
                    value=300,
                    step=50,
                    key=f"mermaid_max_nodes_{ctx.source_id}_{variant}",
                )
            with mcol3:
                include_external = st.checkbox(
                    "Include external IRIs",
                    value=False,
                    key=f"mermaid_include_external_{ctx.source_id}_{variant}",
                )
            focus_options = [("All entities", "")]
            try:
                focus_options.extend(_mermaid_entity_options(effective_input_ttl, include_external))
            except Exception:
                pass
            focus_labels = [label for label, _ in focus_options]
            focus_label_to_iri = {label: iri for label, iri in focus_options}
            selected_focus_label = st.selectbox(
                "Focus entity (connected graph)",
                options=focus_labels,
                index=0,
                key=f"mermaid_focus_entity_{ctx.source_id}_{variant}",
                help="Select one entity to render only its connected component.",
            )
            selected_focus_iri = focus_label_to_iri.get(selected_focus_label, "")
            focus_hops = st.slider(
                "Focus depth (hops)",
                min_value=0,
                max_value=10,
                value=0,
                step=1,
                key=f"mermaid_focus_hops_{ctx.source_id}_{variant}",
                help="0 = all hops from focused entity, 1 = direct neighbors, 2 = neighbors of neighbors, etc.",
            )

            if st.button("Generate / refresh Mermaid", type="primary", key=f"gen_mermaid_{ctx.source_id}_{variant}"):
                ok, text, msg = _build_mermaid(
                    input_ttl=effective_input_ttl,
                    mode=mermaid_mode,
                    max_nodes=mermaid_max_nodes,
                    include_external=include_external,
                    focus_entity_iri=selected_focus_iri or None,
                    focus_max_hops=focus_hops,
                )
                if ok:
                    mermaid_path.parent.mkdir(parents=True, exist_ok=True)
                    mermaid_path.write_text(text, encoding="utf-8")
                    st.success(msg)
                else:
                    st.error(msg)

            if mermaid_path.is_file():
                st.caption(f"Mermaid: `{to_relpath(mermaid_path)}`")
                mtext = mermaid_path.read_text(encoding="utf-8", errors="replace")
                st.download_button(
                    "Download Mermaid (.mmd)",
                    data=mtext.encode("utf-8"),
                    file_name=mermaid_path.name,
                    mime="text/plain",
                    key=f"download_mermaid_{ctx.source_id}_{variant}",
                    use_container_width=True,
                )
                st.code(mtext, language="mermaid")
                if st.toggle(
                    "Render Mermaid in app",
                    value=True,
                    key=f"render_mermaid_{ctx.source_id}_{variant}",
                ):
                    components.html(_mermaid_embed_html(mtext), height=800, scrolling=True)
            else:
                st.info("No Mermaid output yet. Click 'Generate / refresh Mermaid'.")

        with st.expander("Plot side by side", expanded=False):
            st.caption("Render side-by-side Mermaid graphs for downloaded TTL (before) and exported TTL (after).")

            left_ttl = ctx.download_ttl
            right_ttl = Path("registry/exports") / f"{ctx.source_id}_updated.ttl"
            mapping_ttl = Path("registry/exports") / f"{ctx.source_id}_mappings.ttl"
            before_exists = left_ttl.is_file()
            after_exists = right_ttl.is_file()
            mapping_exists = mapping_ttl.is_file()

            use_original_plus_mappings = st.toggle(
                "Use original + mappings on right graph",
                value=False,
                key=f"mermaid_compare_right_original_plus_mappings_{ctx.source_id}_{variant}",
                help=(
                    "When enabled, right graph is built from downloaded source TTL plus exported mapping TTL, "
                    "instead of the canonicalized updated TTL."
                ),
            )
            right_effective_ttl = right_ttl
            if use_original_plus_mappings:
                if before_exists and mapping_exists:
                    merged_right_ttl = _merged_ttl_output_path(ctx.source_id, "right_overlay_original_plus_mappings")
                    ok_merge, merge_msg = _write_merged_ttl([left_ttl, mapping_ttl], merged_right_ttl)
                    if ok_merge:
                        right_effective_ttl = merged_right_ttl
                        st.caption(f"Right graph source: `{to_relpath(left_ttl)}` + `{to_relpath(mapping_ttl)}`")
                    else:
                        st.warning(merge_msg)
                else:
                    st.info(
                        "Right overlay needs both files: "
                        f"`{to_relpath(left_ttl)}` and `{to_relpath(mapping_ttl)}`."
                    )

            st.caption(
                f"Before file: `{to_relpath(left_ttl)}` ({'found' if before_exists else 'missing'}) | "
                f"After file: `{to_relpath(right_ttl)}` ({'found' if after_exists else 'missing'})"
            )
            if use_original_plus_mappings:
                st.caption(
                    f"Mappings file: `{to_relpath(mapping_ttl)}` ({'found' if mapping_exists else 'missing'}) | "
                    f"Effective right TTL: `{to_relpath(right_effective_ttl)}`"
                )
            if not before_exists:
                st.warning("Before TTL is missing for the selected source.")
            if not after_exists:
                st.info("After TTL not found yet. Run export/finalization first, then compare.")

            compare_mode = st.radio(
                "Comparison mode",
                options=["taxonomy", "schema"],
                horizontal=True,
                key=f"mermaid_compare_mode_{ctx.source_id}_{variant}",
                format_func=lambda m: (
                    "Taxonomy (subClassOf/subPropertyOf)"
                    if m == "taxonomy"
                    else "Schema relations (+ domain/range)"
                ),
            )
            compare_max_nodes = st.slider(
                "Comparison max nodes",
                min_value=50,
                max_value=1500,
                value=300,
                step=50,
                key=f"mermaid_compare_max_nodes_{ctx.source_id}_{variant}",
            )
            compare_hops = st.slider(
                "Comparison focus depth (hops)",
                min_value=0,
                max_value=10,
                value=0,
                step=1,
                key=f"mermaid_compare_hops_{ctx.source_id}_{variant}",
                help="0 = all hops from focused entity, 1 = direct neighbors, 2 = neighbors of neighbors, etc.",
            )
            sync_compare_controls = st.toggle(
                "Sync left/right controls",
                value=True,
                key=f"mermaid_compare_sync_controls_{ctx.source_id}_{variant}",
                help="When enabled, left and right graphs use the same max nodes and focus depth.",
            )
            if sync_compare_controls:
                left_compare_max_nodes = compare_max_nodes
                right_compare_max_nodes = compare_max_nodes
                left_compare_hops = compare_hops
                right_compare_hops = compare_hops
            else:
                lcol, rcol = st.columns(2)
                with lcol:
                    left_compare_max_nodes = st.slider(
                        "Left max nodes",
                        min_value=50,
                        max_value=1500,
                        value=compare_max_nodes,
                        step=50,
                        key=f"mermaid_compare_max_nodes_left_{ctx.source_id}_{variant}",
                    )
                    left_compare_hops = st.slider(
                        "Left focus depth (hops)",
                        min_value=0,
                        max_value=10,
                        value=compare_hops,
                        step=1,
                        key=f"mermaid_compare_hops_left_{ctx.source_id}_{variant}",
                    )
                with rcol:
                    right_compare_max_nodes = st.slider(
                        "Right max nodes",
                        min_value=50,
                        max_value=1500,
                        value=compare_max_nodes,
                        step=50,
                        key=f"mermaid_compare_max_nodes_right_{ctx.source_id}_{variant}",
                    )
                    right_compare_hops = st.slider(
                        "Right focus depth (hops)",
                        min_value=0,
                        max_value=10,
                        value=compare_hops,
                        step=1,
                        key=f"mermaid_compare_hops_right_{ctx.source_id}_{variant}",
                    )
            compare_include_external = st.checkbox(
                "Include external IRIs in comparison",
                value=False,
                key=f"mermaid_compare_include_external_{ctx.source_id}_{variant}",
            )

            # Single searchable focus selector shared by before/after graphs.
            shared_focus: dict[str, str] = {"All entities": ""}
            left_iri_set: set[str] = set()
            right_iri_set: set[str] = set()
            if before_exists:
                try:
                    left_opts = _mermaid_entity_options(left_ttl, compare_include_external)
                    left_iri_set = {iri for _, iri in left_opts}
                    for label, iri in left_opts:
                        shared_focus[label] = iri
                except Exception:
                    pass
            if after_exists:
                try:
                    right_opts = _mermaid_entity_options(right_effective_ttl, compare_include_external)
                    right_iri_set = {iri for _, iri in right_opts}
                    for label, iri in right_opts:
                        shared_focus[label] = iri
                except Exception:
                    pass
            shared_focus_labels = sorted(
                [label for label in shared_focus.keys() if label != "All entities"],
                key=lambda x: x.lower(),
            )
            shared_focus_labels.insert(0, "All entities")
            shared_focus_label = st.selectbox(
                "Focus entity (shared before/after)",
                options=shared_focus_labels,
                index=0,
                key=f"mermaid_compare_shared_focus_{ctx.source_id}_{variant}",
                help="Search by label or IRI. This single selection is applied to both graphs.",
            )
            shared_focus_iri = shared_focus.get(shared_focus_label, "")
            compare_state_key = f"mermaid_compare_result_{ctx.source_id}_{variant}"

            can_compare = before_exists and (right_effective_ttl.is_file())
            if st.button(
                "Generate before/after side-by-side",
                type="primary",
                key=f"gen_mermaid_compare_{ctx.source_id}_{variant}",
                disabled=not can_compare,
            ):
                left_focus_iri = shared_focus_iri
                right_focus_iri = shared_focus_iri
                mapped_notes: list[str] = []
                if shared_focus_iri:
                    left_has_focus = shared_focus_iri in left_iri_set or _ttl_contains_iri(left_ttl, shared_focus_iri)
                    right_has_focus = shared_focus_iri in right_iri_set or _ttl_contains_iri(right_ttl, shared_focus_iri)

                    if not left_has_focus:
                        mapped_left = _lookup_alignment_mapped_iri(
                            shared_focus_iri, target_side="left", source_slug=ctx.source_id
                        )
                        if mapped_left and _ttl_contains_iri(left_ttl, mapped_left):
                            left_focus_iri = mapped_left
                            mapped_notes.append(
                                f"Mapped left focus via approved alignment: `{shared_focus_iri}` -> `{mapped_left}`."
                            )
                    if not right_has_focus:
                        mapped_right, mapped_relation = _lookup_alignment_mapping_detail(
                            shared_focus_iri, target_side="right", source_slug=ctx.source_id
                        )
                        if mapped_right and _ttl_contains_iri(right_effective_ttl, mapped_right):
                            right_focus_iri = mapped_right
                            mapped_notes.append(
                                "Mapped right focus via approved alignment: "
                                f"`{shared_focus_iri}` -> `{mapped_right}`"
                                + (f" ({mapped_relation})." if mapped_relation else ".")
                            )
                if mapped_notes:
                    st.info(" ".join(mapped_notes))

                left_ok, left_text, left_msg = _build_mermaid(
                    input_ttl=left_ttl,
                    mode=compare_mode,
                    max_nodes=left_compare_max_nodes,
                    include_external=compare_include_external,
                    focus_entity_iri=left_focus_iri or None,
                    focus_max_hops=left_compare_hops,
                )
                right_ok, right_text, right_msg = _build_mermaid(
                    input_ttl=right_effective_ttl,
                    mode=compare_mode,
                    max_nodes=right_compare_max_nodes,
                    include_external=compare_include_external,
                    focus_entity_iri=right_focus_iri or None,
                    focus_max_hops=right_compare_hops,
                )
                if left_ok and right_ok:
                    st.session_state[compare_state_key] = {
                        "left_text": left_text,
                        "right_text": right_text,
                        "left_msg": left_msg,
                        "right_msg": right_msg,
                        "mapped_notes": mapped_notes,
                        "right_source_path": to_relpath(right_effective_ttl),
                    }
                else:
                    st.session_state.pop(compare_state_key, None)
                    if not left_ok:
                        st.error(f"Before graph failed: {left_msg}")
                    if not right_ok:
                        st.error(f"After graph failed: {right_msg}")

            compare_state = st.session_state.get(compare_state_key)
            if compare_state:
                st.success("Before/after comparison generated.")
                if compare_state.get("mapped_notes"):
                    st.info(" ".join(compare_state["mapped_notes"]))
                show_compare_mermaid_code = st.toggle(
                    "Show Mermaid syntax (copy/paste)",
                    value=True,
                    key=f"render_mermaid_compare_code_{ctx.source_id}_{variant}",
                )
                render_compare_mermaid = st.toggle(
                    "Render Mermaid compare in app",
                    value=True,
                    key=f"render_mermaid_compare_{ctx.source_id}_{variant}",
                )
                lc, rc = st.columns(2)
                with lc:
                    st.caption(f"Before: `{to_relpath(left_ttl)}`")
                    st.caption(str(compare_state.get("left_msg", "")))
                    if show_compare_mermaid_code:
                        st.code(str(compare_state.get("left_text", "")), language="mermaid")
                    if render_compare_mermaid:
                        components.html(_mermaid_embed_html(str(compare_state.get("left_text", ""))), height=700, scrolling=True)
                with rc:
                    right_source_path = str(compare_state.get("right_source_path", to_relpath(right_ttl)))
                    st.caption(f"After: `{right_source_path}`")
                    st.caption(str(compare_state.get("right_msg", "")))
                    if show_compare_mermaid_code:
                        st.code(str(compare_state.get("right_text", "")), language="mermaid")
                    if render_compare_mermaid:
                        components.html(_mermaid_embed_html(str(compare_state.get("right_text", ""))), height=700, scrolling=True)


    with tab_vega:
        st.caption("Hierarchical edge bundling with Vega grammar.")
        scenario = st.selectbox(
            "Scenario",
            options=[
                "Class hierarchy + domain/range",
                "Class hierarchy + all class-to-class links",
                "All entities + all links (full TTL)",
            ],
            index=0,
            key=f"vega_scenario_{ctx.source_id}_{variant}",
        )
        scenario_key = {
            "Class hierarchy + domain/range": "class_domain_range",
            "Class hierarchy + all class-to-class links": "class_all_predicates",
            "All entities + all links (full TTL)": "all_entities_all_links",
        }[scenario]
        vega_path = _vega_edge_output_path(ctx.source_id, variant, scenario_key)
        vcol1, vcol2, vcol3 = st.columns(3)
        with vcol1:
            vega_max_nodes = st.slider(
                "Max nodes",
                min_value=50,
                max_value=4000,
                value=800,
                step=50,
                key=f"vega_max_nodes_{ctx.source_id}_{variant}",
            )
        with vcol2:
            vega_max_links = st.slider(
                "Max links",
                min_value=500,
                max_value=50000,
                value=20000,
                step=500,
                key=f"vega_max_links_{ctx.source_id}_{variant}",
            )
        with vcol3:
            vega_include_external = st.checkbox(
                "Include external IRIs",
                value=False,
                key=f"vega_include_external_{ctx.source_id}_{variant}",
            )
        include_blank_nodes = st.checkbox(
            "Include blank nodes",
            value=False,
            key=f"vega_include_blank_nodes_{ctx.source_id}_{variant}",
            help="Blank node IDs look like n918...; disable for cleaner ontology-level views.",
        )

        prefix_map_for_vega = _collect_prefixes(effective_input_ttl)
        discovered_prefixes = _discover_uri_prefixes(effective_input_ttl)
        ns_options = sorted(set(prefix_map_for_vega.values()) | set(discovered_prefixes))
        exclude_state_key = f"vega_excluded_prefixes_{ctx.source_id}_{variant}"
        if exclude_state_key not in st.session_state:
            st.session_state[exclude_state_key] = []

        e1, e2 = st.columns([3, 1])
        with e1:
            new_excluded_prefix = st.text_input(
                "Exclude URI prefix",
                value="",
                placeholder="https://w3id.org/emi/npc",
                key=f"vega_exclude_input_{ctx.source_id}_{variant}",
                help="Exclude by URI prefix. You can enter with or without trailing # or /.",
            ).strip()
        with e2:
            if st.button("Add prefix", key=f"vega_add_excl_{ctx.source_id}_{variant}") and new_excluded_prefix:
                cur = list(st.session_state[exclude_state_key])
                if new_excluded_prefix not in cur:
                    cur.append(new_excluded_prefix)
                st.session_state[exclude_state_key] = sorted(set(cur))

        selected_excluded_prefixes = st.multiselect(
            "Excluded prefixes",
            options=sorted(set(ns_options + list(st.session_state[exclude_state_key]))),
            default=list(st.session_state[exclude_state_key]),
            key=f"vega_excluded_multiselect_{ctx.source_id}_{variant}",
            help="Selected prefixes are excluded from nodes and links.",
        )
        st.session_state[exclude_state_key] = selected_excluded_prefixes
        if selected_excluded_prefixes:
            st.caption("Excluding: " + " ".join([f"`{x}`" for x in selected_excluded_prefixes]))

        if st.button("Generate / refresh Vega bundling", type="primary", key=f"gen_vega_{ctx.source_id}_{variant}"):
            ok, spec, msg = _build_vega_edge_bundling_spec(
                input_ttl=effective_input_ttl,
                max_nodes=vega_max_nodes,
                include_external=vega_include_external,
                scenario=scenario_key,
                max_links=vega_max_links,
                excluded_prefixes=selected_excluded_prefixes,
                include_blank_nodes=include_blank_nodes,
            )
            if ok:
                vega_path.parent.mkdir(parents=True, exist_ok=True)
                vega_path.write_text(json.dumps(spec, indent=2), encoding="utf-8")
                st.success(msg)
            else:
                st.error(msg)

        if vega_path.is_file():
            st.caption(f"Vega spec: `{to_relpath(vega_path)}`")
            spec_text = vega_path.read_text(encoding="utf-8", errors="replace")
            try:
                spec_obj = json.loads(spec_text)
            except Exception:
                spec_obj = None
            if isinstance(spec_obj, dict):
                meta = spec_obj.get("usermeta", {})
                if isinstance(meta, dict):
                    mode = str(meta.get("endpoint_mode", "")).strip()
                    scenario_name = str(meta.get("scenario", "")).strip()
                    if scenario_name:
                        st.caption(f"Scenario: `{scenario_name}`")
                    if mode == "leaf_projected":
                        st.info(
                            "Class scenarios project links to leaf classes for bundling. "
                            "If a class has children, links may start from inside the circle via ancestor paths."
                        )
                    raw_pairs = meta.get("raw_pairs")
                    dropped_no_leaf = meta.get("dropped_no_leaf")
                    raw_edges = meta.get("raw_edges")
                    kept_edges = meta.get("kept_edges")
                    if raw_pairs is not None:
                        st.caption(
                            f"Link accounting: raw candidates={raw_pairs}, "
                            f"kept={kept_edges}, dropped_non_leaf={dropped_no_leaf}"
                        )
                    elif raw_edges is not None:
                        filtered = max(0, int(raw_edges) - int(kept_edges or 0))
                        st.caption(
                            f"Link accounting: raw edges={raw_edges}, kept={kept_edges}, filtered={filtered}"
                        )
            st.download_button(
                "Download Vega spec (.json)",
                data=spec_text.encode("utf-8"),
                file_name=vega_path.name,
                mime="application/json",
                key=f"download_vega_{ctx.source_id}_{variant}",
                use_container_width=True,
            )
            if st.toggle(
                "Render Vega in app",
                value=True,
                key=f"render_vega_{ctx.source_id}_{variant}",
            ):
                try:
                    spec_obj = json.loads(spec_text)
                    components.html(_vega_embed_html(spec_obj), height=980, scrolling=True)
                except Exception as exc:
                    st.error(f"Could not render Vega spec: {exc}")
            st.code(spec_text, language="json")
        else:
            st.info("No Vega spec yet. Click 'Generate / refresh Vega bundling'.")

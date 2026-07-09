"""JenaGraphRAG — main orchestrator for the 5 GraphRAG operations.

Framework-agnostic — this class doesn't know about SEMOSS reactors, the
Java bridge, or the Python thread-local. It just takes constructor args
and exposes clean Python methods. The Java engine constructs one of these
per open() and forwards calls into it.
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from typing import Any, Dict, List, Optional

from .guard_sparql import guard_sparql
from .jena_client import JenaClient, rows_from_select, vars_from_select
from .ontology import (
    DEFAULT_PREFIXES,
    load_prefixes_from_ttl,
    render_prefix_declarations,
)

logger = logging.getLogger(__name__)

# UUID5 seed for deterministic URIs — same source+fact yields the same URI,
# so re-ingest is idempotent.
_URI_NAMESPACE = uuid.UUID("6e8f4c1a-2b3d-5e7f-9a1c-4d2e6f8a0b1c")

# Default namespace for URIs we mint at ingest time.
_DEFAULT_ENTITY_NS = "https://semoss.org/graph_rag/entity/"
_DEFAULT_CHUNK_NS = "https://semoss.org/graph_rag/chunk/"


class JenaGraphRAG:
    """Orchestrates SPARQL queries + optional vector search over Fuseki."""

    def __init__(
        self,
        query_url: str,
        update_url: str,
        user: Optional[str] = None,
        password: Optional[str] = None,
        row_cap: int = 500,
        timeout_ms: str = "15000,30000",
        ontology_ttl: Optional[str] = None,
        paired_vector_search: Optional[Any] = None,
    ) -> None:
        """
        Args:
            query_url: full URL of the Fuseki SPARQL query endpoint,
                e.g. ``http://fuseki:3030/kb/sparql``
            update_url: SPARQL update endpoint,
                e.g. ``http://fuseki:3030/kb/update``
            user, password: optional basic auth
            row_cap: default LIMIT injected on SELECT/CONSTRUCT
            timeout_ms: "<first>,<overall>" ms — forwarded to the client
            ontology_ttl: optional TTL string, parsed for PREFIX declarations
                that get injected into every guarded query
            paired_vector_search: optional paired vector-store searcher.
                Accepts either the SEMOSS Python searcher object (with
                ``add_points`` / ``nearestNeighbor`` methods, e.g. a
                QdrantDatabase instance) or a legacy ``(query, k) -> list``
                callable. If None, hybrid_retrieve falls back to graph-only
                retrieval and chunks land only as smss:Chunk RDF nodes.
        """
        self.client = JenaClient(
            query_url=query_url,
            update_url=update_url,
            user=user,
            password=password,
            timeout_ms=timeout_ms,
        )
        self.row_cap = int(row_cap)
        self.prefixes: Dict[str, str] = dict(DEFAULT_PREFIXES)
        self.prefixes.update(load_prefixes_from_ttl(ontology_ttl))
        self.paired_vector_search = paired_vector_search

    # ------------------------------------------------------------------
    # Public GraphRAG API — mirrors AbstractGraphRAGEngine on the Java side
    # ------------------------------------------------------------------

    def guarded_query(
        self,
        query: str,
        row_cap: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Sanitize + execute a read-only SPARQL query."""
        safe = guard_sparql(
            query,
            prefixes=self.prefixes,
            row_cap=int(row_cap) if row_cap is not None else self.row_cap,
        )
        verb = self._detect_verb(safe)
        if verb in ("SELECT", "ASK"):
            result = self.client.select(safe)
            rows = rows_from_select(result)
            variables = vars_from_select(result)
        elif verb in ("CONSTRUCT", "DESCRIBE"):
            turtle = self.client.construct(safe)
            variables = ["turtle"]
            rows = [{"turtle": turtle}]
        else:
            raise ValueError(f"Unsupported query verb: {verb}")

        return {
            "query": safe,
            "vars": variables,
            "rows": rows,
            "rowCount": len(rows),
        }

    def entity_link(
        self,
        text: str,
        limit: int = 5,
        node_types: Optional[List[str]] = None,
        score_threshold: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """Match a natural-language phrase against entity labels in the graph.

        v1 strategy: SPARQL over ``rdfs:label`` / ``skos:prefLabel`` /
        ``skos:altLabel`` with case-insensitive CONTAINS. Optional node-type
        restriction. Optional vector-search boost if a paired retriever is
        configured.

        Returns a ranked list of matches (best first).
        """
        if not text or not text.strip():
            return []
        needle = text.strip().lower()
        limit = int(limit)

        type_filter = ""
        if node_types:
            type_uris = " ".join(
                f"<{t}>" if not t.startswith("_:") else t for t in node_types
            )
            type_filter = f"?uri a ?nodeType . FILTER(?nodeType IN ({type_uris}))"

        # Score = 3.0 exact match, 2.0 startswith, 1.0 substring.
        sparql = f"""
        SELECT DISTINCT ?uri ?label ?type WHERE {{
          ?uri ?labelProp ?label .
          FILTER(?labelProp IN (rdfs:label, skos:prefLabel, skos:altLabel))
          FILTER(CONTAINS(LCASE(STR(?label)), {json.dumps(needle)}))
          OPTIONAL {{ ?uri a ?type }}
          {type_filter}
        }}
        LIMIT {max(limit * 5, 20)}
        """
        result = self.guarded_query(sparql, row_cap=limit * 5)
        candidates: List[Dict[str, Any]] = []
        for row in result["rows"]:
            uri = row.get("uri")
            label = row.get("label") or ""
            node_type = row.get("type")
            if not uri:
                continue
            score = self._score_label_match(needle, label)
            candidates.append(
                {
                    "uri": uri,
                    "label": label,
                    "type": node_type,
                    "score": score,
                }
            )

        # Optional paired-vector-engine boost.
        if self.paired_vector_search is not None:
            try:
                vec_hits = self._invoke_paired_search(text, limit) or []
                seen_uris = {c["uri"] for c in candidates}
                for hit in vec_hits:
                    uri = hit.get("uri") or hit.get("id")
                    if uri and uri not in seen_uris:
                        candidates.append(
                            {
                                "uri": uri,
                                "label": hit.get("label", ""),
                                "type": hit.get("type"),
                                "score": float(hit.get("Score", 0.0)),
                            }
                        )
            except Exception as e:
                logger.warning("paired_vector_search failed in entity_link: %s", e)

        candidates.sort(key=lambda c: c["score"], reverse=True)
        if score_threshold is not None:
            candidates = [c for c in candidates if c["score"] >= score_threshold]
        return candidates[:limit]

    def graph_expand(
        self,
        node_uri: str,
        hops: int = 1,
        edge_types: Optional[List[str]] = None,
        max_edges: int = 200,
        include_labels: bool = True,
    ) -> List[Dict[str, Any]]:
        """Return triples in the N-hop neighborhood of ``node_uri``."""
        if not node_uri:
            return []
        hops = max(1, min(int(hops), 3))  # cap at 3 to prevent full-graph walks
        max_edges = int(max_edges)

        node_ref = f"<{node_uri}>"

        edge_filter = ""
        if edge_types:
            preds = " ".join(f"<{p}>" for p in edge_types)
            edge_filter = f"FILTER(?p IN ({preds}))"

        # UNION over hop distances 1..hops. Each hop uses a bounded path
        # length via the property-path {n} count.
        union_blocks = []
        for h in range(1, hops + 1):
            path = "/".join(["?p"] * h)  # not real SPARQL — use actual variable path
            # Actually we can't use ?p in a path directly like that. Use nested
            # BGPs for each hop distance.
            chain = self._build_hop_chain(h, node_ref)
            union_blocks.append("{ " + chain + " " + edge_filter + " }")

        union_body = " UNION ".join(union_blocks) if union_blocks else ""

        label_optionals = ""
        if include_labels:
            label_optionals = """
            OPTIONAL { ?s rdfs:label ?sLabel }
            OPTIONAL { ?o rdfs:label ?oLabel }
            """

        sparql = f"""
        SELECT DISTINCT ?s ?p ?o ?sLabel ?oLabel WHERE {{
          {union_body}
          {label_optionals}
        }}
        LIMIT {max_edges}
        """
        result = self.guarded_query(sparql, row_cap=max_edges)
        triples: List[Dict[str, Any]] = []
        for row in result["rows"]:
            s = row.get("s")
            p = row.get("p")
            o = row.get("o")
            if not (s and p and o):
                continue
            entry: Dict[str, Any] = {"s": s, "p": p, "o": o}
            if row.get("sLabel"):
                entry["sLabel"] = row["sLabel"]
            if row.get("oLabel"):
                entry["oLabel"] = row["oLabel"]
            triples.append(entry)
        return triples

    def hybrid_retrieve(
        self,
        question: str,
        limit: int = 6,
        hops: int = 1,
        section_filter: Optional[str] = None,
        min_score: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Full plan → link → expand → (optional vector) → merge pass.

        v1 extraction of candidate entity phrases: naive noun-ish token
        extraction. Downstream we'd swap this for an LLM extraction call.
        """
        if not question or not question.strip():
            return {
                "anchors": [],
                "subgraph": [],
                "passages": [],
                "sparql": None,
                "diagnostics": {"note": "empty question"},
            }
        limit = max(1, int(limit))
        hops = max(1, min(int(hops), 3))

        candidate_phrases = self._extract_candidate_phrases(question)
        anchors: List[Dict[str, Any]] = []
        seen_uris: set = set()
        for phrase in candidate_phrases:
            for match in self.entity_link(
                phrase, limit=3, score_threshold=min_score
            ):
                if match["uri"] in seen_uris:
                    continue
                seen_uris.add(match["uri"])
                match["phrase"] = phrase
                anchors.append(match)
                if len(anchors) >= limit * 2:
                    break
            if len(anchors) >= limit * 2:
                break

        # Expand each anchor 1..hops hops.
        subgraph: List[Dict[str, Any]] = []
        for anchor in anchors[:limit]:
            triples = self.graph_expand(
                anchor["uri"], hops=hops, max_edges=50
            )
            for triple in triples:
                triple.setdefault("anchor", anchor["uri"])
            subgraph.extend(triples)

        # Optional paired vector search over chunks.
        passages: List[Dict[str, Any]] = []
        if self.paired_vector_search is not None:
            try:
                vec_hits = self._invoke_paired_search(question, limit) or []
                for hit in vec_hits:
                    passages.append(
                        {
                            "text": hit.get("Content") or hit.get("text") or "",
                            "source": hit.get("Source") or hit.get("source"),
                            "score": float(hit.get("Score", hit.get("score", 0.0))),
                            "uri": hit.get("uri") or hit.get("id"),
                        }
                    )
            except Exception as e:
                logger.warning("paired_vector_search failed in hybrid_retrieve: %s", e)

        return {
            "anchors": anchors[:limit],
            "subgraph": subgraph,
            "passages": passages,
            "sparql": None,  # composed of multiple queries — not one shape
            "diagnostics": {
                "phrases_extracted": len(candidate_phrases),
                "anchors_matched": len(anchors),
                "triples_returned": len(subgraph),
                "passages_returned": len(passages),
            },
        }

    def ingest_doc(
        self,
        text: str,
        metadata: Optional[Dict[str, Any]] = None,
        extraction_mode: str = "structured",
    ) -> Dict[str, Any]:
        """Ingest a document into the graph.

        v1 accepts either:
        * ``extraction_mode="structured"`` — ``text`` is a JSON string with
          shape ``{"nodes": [...], "edges": [...], "chunks": [...]}``.
          Nodes/edges get RDFized into TTL. Chunks (optionally) get sent
          to the paired vector engine.
        * ``extraction_mode="triples"`` — ``text`` is a raw Turtle body
          POSTed directly to Fuseki.

        LLM-driven extraction (Microsoft-GraphRAG-style) is a v2 add-on.
        """
        metadata = metadata or {}
        source = str(metadata.get("source") or metadata.get("title") or "unknown")
        doc_uri = self._mint_entity_uri(f"doc|{source}")

        turtle_lines: List[str] = [
            render_prefix_declarations(self.prefixes),
            "",
            f"<{doc_uri}> a smss:Document ;",
        ]
        for key, val in metadata.items():
            prop = f"smss:{_slugify(key)}"
            turtle_lines.append(f'    {prop} {_ttl_literal(val)} ;')
        # replace trailing ";" on the last property line with "."
        if turtle_lines[-1].endswith(";"):
            turtle_lines[-1] = turtle_lines[-1][:-1] + "."

        nodes_added = 1  # the doc itself
        edges_added = 0
        chunks_indexed = 0

        if extraction_mode == "triples":
            self.client.post_turtle(text)
            return {
                "nodesAdded": None,
                "edgesAdded": None,
                "chunksIndexed": 0,
                "documentUri": None,
                "diagnostics": {"mode": "triples", "bytes": len(text)},
            }

        if extraction_mode != "structured":
            raise ValueError(
                f"unsupported extraction_mode {extraction_mode!r}; "
                "supported: 'structured', 'triples'"
            )

        try:
            payload = json.loads(text) if isinstance(text, str) else text
        except json.JSONDecodeError as e:
            raise ValueError(
                f"structured ingest requires JSON body: {e}"
            ) from e
        if not isinstance(payload, dict):
            raise ValueError("structured ingest payload must be a JSON object")

        # Nodes
        for node in payload.get("nodes") or []:
            n_uri = node.get("uri") or self._mint_entity_uri(
                f"node|{source}|{node.get('id', '')}"
            )
            n_type = node.get("type") or "smss:Entity"
            if not n_type.startswith("<") and ":" not in n_type:
                n_type = f'"{n_type}"'
            turtle_lines.append("")
            turtle_lines.append(f"<{n_uri}> a {n_type} ;")
            if node.get("label"):
                turtle_lines.append(f'    rdfs:label {_ttl_literal(node["label"])} ;')
            turtle_lines.append(f"    smss:belongsToDocument <{doc_uri}> ;")
            for prop_name, prop_val in (node.get("properties") or {}).items():
                turtle_lines.append(
                    f"    smss:{_slugify(prop_name)} {_ttl_literal(prop_val)} ;"
                )
            if turtle_lines[-1].endswith(";"):
                turtle_lines[-1] = turtle_lines[-1][:-1] + "."
            nodes_added += 1

        # Edges — (subject, predicate, object) triples
        for edge in payload.get("edges") or []:
            s = edge.get("s") or edge.get("subject")
            p = edge.get("p") or edge.get("predicate")
            o = edge.get("o") or edge.get("object")
            if not (s and p and o):
                continue
            turtle_lines.append(f"<{s}> <{p}> <{o}> .")
            edges_added += 1

        turtle_body = "\n".join(turtle_lines)
        self.client.post_turtle(turtle_body)

        # Chunks — pushed to paired vector engine if configured.
        chunks = payload.get("chunks") or []
        paired_items: List[Dict[str, Any]] = []
        for i, chunk in enumerate(chunks):
            chunk_text = chunk.get("text") if isinstance(chunk, dict) else str(chunk)
            if not chunk_text:
                continue
            chunk_uri = self._mint_entity_uri(f"chunk|{doc_uri}|{i}")
            chunk_ttl = "\n".join(
                [
                    "",
                    f"<{chunk_uri}> a smss:Chunk ;",
                    f'    smss:text {_ttl_literal(chunk_text)} ;',
                    f'    smss:chunkIndex "{i}"^^xsd:integer ;',
                    f"    smss:belongsToDocument <{doc_uri}> .",
                ]
            )
            self.client.post_turtle(chunk_ttl)
            chunks_indexed += 1

            chunk_source = chunk.get("source") if isinstance(chunk, dict) else None
            paired_items.append(
                {
                    "text": chunk_text,
                    "payload": {
                        "Source": chunk_source or source or "unknown",
                        "chunkUri": chunk_uri,
                        "documentUri": doc_uri,
                        "chunkIndex": i,
                    },
                }
            )

        # Push chunks to the paired vector store (e.g. Qdrant) if configured.
        paired_pushed = 0
        if paired_items and self.paired_vector_search is not None:
            try:
                self._invoke_paired_add(paired_items)
                paired_pushed = len(paired_items)
            except Exception as e:
                logger.warning("paired_vector_search push failed: %s", e)

        return {
            "nodesAdded": nodes_added,
            "edgesAdded": edges_added,
            "chunksIndexed": chunks_indexed,
            "pairedVectorPushed": paired_pushed,
            "documentUri": doc_uri,
            "diagnostics": {"mode": "structured", "source": source},
        }

    # ------------------------------------------------------------------
    # Paired-vector adapter — Qdrant/FAISS/etc. searcher object OR callable
    # ------------------------------------------------------------------

    def _invoke_paired_search(self, question: str, limit: int) -> List[Dict[str, Any]]:
        pvs = self.paired_vector_search
        if pvs is None:
            return []
        # Preferred: SEMOSS Python searcher object with nearestNeighbor method
        if hasattr(pvs, "nearestNeighbor"):
            return pvs.nearestNeighbor(question, limit=limit) or []
        # Legacy: plain callable (question, k=N) -> list
        if callable(pvs):
            return pvs(question, k=limit) or []
        logger.warning("paired_vector_search has no nearestNeighbor and is not callable")
        return []

    def _invoke_paired_add(self, items: List[Dict[str, Any]]) -> None:
        pvs = self.paired_vector_search
        if pvs is None or not items:
            return
        if hasattr(pvs, "add_points"):
            pvs.add_points(items=items)
            return
        if hasattr(pvs, "addPoints"):
            pvs.addPoints(items=items)
            return
        logger.warning("paired_vector_search has no add_points method; skipping push")

    def remove_document(
        self,
        sources: List[str],
    ) -> Dict[str, Any]:
        """Delete all triples belonging to documents matching any of ``sources``.

        Matches on ``smss:source`` on smss:Document nodes; cascades to
        smss:Chunk and smss:Entity nodes anchored via smss:belongsToDocument.
        """
        if not sources:
            return {"removed": 0, "diagnostics": {"reason": "no sources supplied"}}

        source_literals = ", ".join(_ttl_literal(s) for s in sources)
        update = (
            "PREFIX smss: <https://semoss.org/ontology/> "
            "DELETE { "
            "  ?child ?cp ?cv . "
            "  ?doc ?dp ?dv . "
            "} WHERE { "
            f"  ?doc a smss:Document ; smss:source ?src . FILTER (?src IN ({source_literals})) "
            "  OPTIONAL { ?child smss:belongsToDocument ?doc ; ?cp ?cv . } "
            "  ?doc ?dp ?dv . "
            "}"
        )
        self.client.update(update)
        return {
            "removed": len(sources),
            "diagnostics": {"sources": sources, "verb": "DELETE"},
        }

    def list_documents(
        self,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """List distinct smss:Document nodes with core metadata.

        Satisfies the IVectorDatabaseEngine.listDocuments contract for the
        graph-backed store. Each row is one document.
        """
        sparql = (
            "PREFIX smss: <https://semoss.org/ontology/> "
            "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
            "SELECT ?doc ?source ?title "
            "(COUNT(?chunk) AS ?chunkCount) "
            "WHERE { "
            "  ?doc a smss:Document . "
            "  OPTIONAL { ?doc smss:source ?source . } "
            "  OPTIONAL { ?doc smss:title ?title . } "
            "  OPTIONAL { ?chunk a smss:Chunk ; smss:belongsToDocument ?doc . } "
            "} GROUP BY ?doc ?source ?title "
            f"LIMIT {self.row_cap}"
        )
        result = self.client.select(sparql)
        return rows_from_select(result)

    def list_all_records(
        self,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """List all smss:Chunk records with text + parent document URI.

        Satisfies the IVectorDatabaseEngine.listAllRecords contract.
        """
        sparql = (
            "PREFIX smss: <https://semoss.org/ontology/> "
            "SELECT ?chunk ?text ?chunkIndex ?doc "
            "WHERE { "
            "  ?chunk a smss:Chunk . "
            "  OPTIONAL { ?chunk smss:text ?text . } "
            "  OPTIONAL { ?chunk smss:chunkIndex ?chunkIndex . } "
            "  OPTIONAL { ?chunk smss:belongsToDocument ?doc . } "
            "} "
            f"LIMIT {self.row_cap}"
        )
        result = self.client.select(sparql)
        return rows_from_select(result)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _detect_verb(sparql: str) -> str:
        m = re.search(r"\b(SELECT|CONSTRUCT|ASK|DESCRIBE)\b", sparql, re.IGNORECASE)
        return m.group(1).upper() if m else "SELECT"

    @staticmethod
    def _score_label_match(needle: str, label: str) -> float:
        low = str(label).lower()
        if low == needle:
            return 3.0
        if low.startswith(needle) or low.endswith(needle):
            return 2.0
        return 1.0

    @staticmethod
    def _extract_candidate_phrases(question: str) -> List[str]:
        """Naive phrase extraction — capitalized tokens + quoted spans.

        Good enough for v1. v2 swaps this for an LLM extraction call, or a
        proper NER model.
        """
        phrases: List[str] = []
        for m in re.finditer(r'"([^"]+)"', question):
            phrases.append(m.group(1))
        for m in re.finditer(r"\b([A-Z][a-zA-Z0-9\-]{2,}(?:\s+[A-Z][a-zA-Z0-9\-]+)*)\b", question):
            phrases.append(m.group(1))
        # Also include any long-ish lowercase noun-ish tokens as a fallback
        for m in re.finditer(r"\b([a-z][a-z\-]{5,})\b", question):
            phrases.append(m.group(1))
        # Dedupe while preserving order
        seen = set()
        out: List[str] = []
        for p in phrases:
            key = p.lower()
            if key not in seen:
                seen.add(key)
                out.append(p)
        return out[:8]

    @staticmethod
    def _mint_entity_uri(seed: str) -> str:
        return _DEFAULT_ENTITY_NS + str(uuid.uuid5(_URI_NAMESPACE, seed))

    @staticmethod
    def _build_hop_chain(hop_distance: int, anchor: str) -> str:
        """Return a triple-pattern BGP for exactly ``hop_distance`` steps.

        Uses direction-agnostic UNION so ``expand`` catches both outgoing
        and incoming edges. For hop_distance=1: {anchor ?p ?o} UNION {?s ?p anchor}.
        """
        if hop_distance == 1:
            return (
                f"{{ {anchor} ?p ?o . BIND({anchor} AS ?s) }} "
                f"UNION {{ ?s ?p {anchor} . BIND({anchor} AS ?o) }}"
            )
        # For hop > 1, chain via intermediate anonymous nodes.
        parts: List[str] = []
        current = anchor
        for i in range(hop_distance):
            var_p = f"?p{i}"
            var_next = f"?n{i}" if i < hop_distance - 1 else "?o"
            parts.append(f"{current} {var_p} {var_next} .")
            current = var_next
        # Reuse ?p as the final predicate for the API's expected column name
        parts.append(f"BIND({anchor} AS ?s)")
        parts.append(f"BIND({var_p} AS ?p)")
        return " ".join(parts)


def _slugify(s: Any) -> str:
    txt = re.sub(r"[^A-Za-z0-9]+", "_", str(s)).strip("_")
    return txt or "field"


def _ttl_literal(value: Any) -> str:
    """Best-effort TTL literal rendering for common Python types."""
    if value is None:
        return '""'
    if isinstance(value, bool):
        return f'"{str(value).lower()}"^^xsd:boolean'
    if isinstance(value, int):
        return f'"{value}"^^xsd:integer'
    if isinstance(value, float):
        return f'"{value}"^^xsd:double'
    if isinstance(value, (list, tuple)):
        # Fold into a single string — for a real graph you'd want repeat props
        return _ttl_literal(", ".join(str(v) for v in value))
    escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
    if "\n" in escaped:
        return f'"""{escaped}"""'
    return f'"{escaped}"'

"""SEMOSS Jena/Fuseki-backed GraphRAG module.

Exposes five operations the Java ``JenaGraphRAGEngine`` calls into via the
``pyTranslator`` bridge:

* ``guarded_query``   — LLM-authored SPARQL, allow-listed to reads, LIMIT-capped
* ``entity_link``     — text → matching graph entity URIs
* ``graph_expand``    — N-hop neighborhood around a node
* ``hybrid_retrieve`` — full plan → link → expand → return subgraph
* ``ingest_doc``      — RDFize + chunk + embed a document into the graph

Everything is framework-agnostic Python — the ``JenaGraphRAG`` class doesn't
know about SEMOSS reactors, it just talks HTTP SPARQL to a Fuseki endpoint
and (optionally) delegates chunk retrieval to a paired vector engine.
"""

from .graph_rag_jena import JenaGraphRAG
from .guard_sparql import GuardedQueryError, guard_sparql
from .ontology import DEFAULT_PREFIXES, load_prefixes_from_ttl

__all__ = [
    "JenaGraphRAG",
    "guard_sparql",
    "GuardedQueryError",
    "DEFAULT_PREFIXES",
    "load_prefixes_from_ttl",
]

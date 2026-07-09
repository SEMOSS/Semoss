"""Ontology helpers — extract PREFIX declarations from a TTL string."""

from __future__ import annotations

import re
from typing import Dict, Optional

DEFAULT_PREFIXES: Dict[str, str] = {
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "owl": "http://www.w3.org/2002/07/owl#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "dc": "http://purl.org/dc/elements/1.1/",
    "dct": "http://purl.org/dc/terms/",
    "foaf": "http://xmlns.com/foaf/0.1/",
    "prov": "http://www.w3.org/ns/prov#",
    "smss": "https://semoss.org/ontology/",
}

_PREFIX_RE = re.compile(
    r"@prefix\s+([A-Za-z][\w\-]*):\s*<([^>]+)>\s*\.",
    re.IGNORECASE,
)
_SPARQL_PREFIX_RE = re.compile(
    r"PREFIX\s+([A-Za-z][\w\-]*):\s*<([^>]+)>",
    re.IGNORECASE,
)


def load_prefixes_from_ttl(ttl: Optional[str]) -> Dict[str, str]:
    """Parse ``@prefix`` declarations from a TTL string; return a name→URI map.

    Merges with ``DEFAULT_PREFIXES`` so common vocabularies (rdf, rdfs, owl,
    xsd, skos, dc, foaf, prov) are always available even in a bare-bones
    workspace ontology.
    """
    prefixes: Dict[str, str] = dict(DEFAULT_PREFIXES)
    if not ttl:
        return prefixes
    for match in _PREFIX_RE.finditer(ttl):
        prefixes[match.group(1)] = match.group(2)
    for match in _SPARQL_PREFIX_RE.finditer(ttl):
        prefixes[match.group(1)] = match.group(2)
    return prefixes


def render_prefix_declarations(prefixes: Dict[str, str]) -> str:
    """Render a prefixes dict as SPARQL ``PREFIX`` declaration lines."""
    lines = [f"PREFIX {name}: <{uri}>" for name, uri in sorted(prefixes.items())]
    return "\n".join(lines)

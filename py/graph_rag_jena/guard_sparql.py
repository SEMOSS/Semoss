"""Guarded SPARQL — sanitize an LLM-authored query before it hits Fuseki.

Pattern is lifted from the rxgraph reference implementation:
* Reject write operations (INSERT/DELETE/DROP/LOAD/CLEAR/CREATE/COPY/MOVE/ADD).
* Require the query to start with a read verb (SELECT/CONSTRUCT/ASK/DESCRIBE).
* Auto-inject workspace PREFIX declarations that the LLM didn't include.
* Auto-append ``LIMIT n`` on unbounded SELECT/CONSTRUCT queries so a bad
  query can't drain the server.

Everything is a plain string transform — no rdflib parsing required, which
keeps the guard cheap and predictable. The trade-off is that pathologically
crafted queries with ``INSERT`` inside a comment slip through the regex —
Fuseki itself rejects them, so it's defense-in-depth rather than the sole
gate.
"""

from __future__ import annotations

import re
from typing import Dict, Optional

from .ontology import DEFAULT_PREFIXES, render_prefix_declarations


class GuardedQueryError(ValueError):
    """Raised when a query fails the read-only / shape guard."""


_BANNED_VERBS = (
    "INSERT",
    "DELETE",
    "DROP",
    "LOAD",
    "CLEAR",
    "CREATE",
    "COPY",
    "MOVE",
    "ADD",
)

# Match a banned verb as a whole word (not inside another token or URI).
_BANNED_RES = {
    verb: re.compile(rf"\b{verb}\b", re.IGNORECASE) for verb in _BANNED_VERBS
}

# Read-only verb the query is required to start with, after any PREFIX
# declarations and BASE.
_HEAD_RE = re.compile(
    r"^\s*(BASE\s+<[^>]+>\s*)?"
    r"(PREFIX\s+[A-Za-z][\w\-]*:\s*<[^>]+>\s*)*"
    r"\s*(SELECT|CONSTRUCT|ASK|DESCRIBE)\b",
    re.IGNORECASE | re.DOTALL,
)

_LIMIT_RE = re.compile(r"\bLIMIT\b\s+\d+", re.IGNORECASE)

_COMMENT_RE = re.compile(r"#[^\n\r]*", re.MULTILINE)


def guard_sparql(
    query: str,
    prefixes: Optional[Dict[str, str]] = None,
    row_cap: int = 500,
) -> str:
    """Return a safe version of ``query`` or raise :class:`GuardedQueryError`.

    Args:
        query: raw LLM-authored SPARQL.
        prefixes: name→URI map merged with :data:`DEFAULT_PREFIXES` and
            injected as ``PREFIX`` lines the LLM omitted.
        row_cap: default ``LIMIT`` appended to unbounded SELECT/CONSTRUCT
            queries.
    """
    if not query or not query.strip():
        raise GuardedQueryError("query is empty")

    all_prefixes: Dict[str, str] = dict(DEFAULT_PREFIXES)
    if prefixes:
        all_prefixes.update(prefixes)

    # Strip comments before scanning for banned verbs so `# DELETE the foo`
    # in a legitimate SELECT doesn't trigger a false positive.
    query_no_comments = _COMMENT_RE.sub("", query)
    for verb, pattern in _BANNED_RES.items():
        if pattern.search(query_no_comments):
            raise GuardedQueryError(
                f"'{verb}' is not allowed in guarded queries — "
                "GraphRAG queries are read-only."
            )

    if not _HEAD_RE.match(query):
        raise GuardedQueryError(
            "query must start with SELECT, CONSTRUCT, ASK, or DESCRIBE "
            "(optionally preceded by BASE and PREFIX declarations)."
        )

    # Inject any missing PREFIX declarations at the top.
    declared = _extract_declared_prefixes(query)
    missing = {
        name: uri for name, uri in all_prefixes.items() if name not in declared
    }
    injected = render_prefix_declarations(missing) if missing else ""

    # Auto-append LIMIT on SELECT/CONSTRUCT when none present.
    verb_match = re.search(r"\b(SELECT|CONSTRUCT|ASK|DESCRIBE)\b", query, re.IGNORECASE)
    needs_limit = (
        verb_match is not None
        and verb_match.group(1).upper() in {"SELECT", "CONSTRUCT"}
        and not _LIMIT_RE.search(query_no_comments)
    )
    tail = f"\nLIMIT {int(row_cap)}" if needs_limit else ""

    parts = [p for p in (injected, query.strip()) if p]
    return "\n".join(parts) + tail


def _extract_declared_prefixes(query: str) -> set:
    return {
        m.group(1)
        for m in re.finditer(
            r"PREFIX\s+([A-Za-z][\w\-]*):\s*<[^>]+>", query, re.IGNORECASE
        )
    }

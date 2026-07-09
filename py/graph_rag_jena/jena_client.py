"""Thin HTTP client for Apache Jena / Fuseki.

Uses ``requests`` because we already ship it (transitive dependency across
the SEMOSS Python surface), no need for SPARQLWrapper. Fuseki speaks the
SPARQL 1.1 Protocol — plain HTTP POST with a ``Content-Type`` header,
JSON results on the way back.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

_JSON_ACCEPT = "application/sparql-results+json"
_TURTLE_ACCEPT = "text/turtle"


class JenaClient:
    """Fuseki SPARQL 1.1 client (query + update + Graph Store Protocol)."""

    def __init__(
        self,
        query_url: str,
        update_url: str,
        user: Optional[str] = None,
        password: Optional[str] = None,
        timeout_ms: str = "15000,30000",
    ) -> None:
        self.query_url = query_url
        self.update_url = update_url
        self.data_url = query_url.replace("/sparql", "/data")
        self._auth: Optional[Tuple[str, str]] = (
            (user, password) if user and password else None
        )
        first, overall = self._parse_timeout(timeout_ms)
        # requests takes (connect_timeout, read_timeout) in seconds
        self._timeout = (first, overall)

    def _parse_timeout(self, timeout_ms: str) -> Tuple[float, float]:
        try:
            parts = [p.strip() for p in (timeout_ms or "").split(",") if p.strip()]
            if len(parts) == 2:
                return (int(parts[0]) / 1000.0, int(parts[1]) / 1000.0)
            if len(parts) == 1 and parts[0]:
                v = int(parts[0]) / 1000.0
                return (v, v)
        except Exception as e:
            logger.warning(
                "Could not parse timeout_ms=%r (%s); using 15/30", timeout_ms, e
            )
        return (15.0, 30.0)

    def select(self, sparql: str) -> Dict[str, Any]:
        """Run a SPARQL SELECT/ASK query, return the parsed JSON results.

        Response shape (SPARQL Results JSON):
            {
              "head": {"vars": [...]},
              "results": {"bindings": [{"var": {"type": "...", "value": "..."}, ...}, ...]}
            }
        """
        response = requests.post(
            self.query_url,
            data={"query": sparql},
            headers={"Accept": _JSON_ACCEPT},
            auth=self._auth,
            timeout=self._timeout,
        )
        response.raise_for_status()
        return response.json()

    def construct(self, sparql: str) -> str:
        """Run a SPARQL CONSTRUCT/DESCRIBE query, return Turtle body."""
        response = requests.post(
            self.query_url,
            data={"query": sparql},
            headers={"Accept": _TURTLE_ACCEPT},
            auth=self._auth,
            timeout=self._timeout,
        )
        response.raise_for_status()
        return response.text

    def update(self, sparql_update: str) -> None:
        """Run a SPARQL UPDATE (INSERT/DELETE). Bypasses the read-only guard."""
        response = requests.post(
            self.update_url,
            data={"update": sparql_update},
            auth=self._auth,
            timeout=self._timeout,
        )
        response.raise_for_status()

    def post_turtle(self, turtle_body: str, graph: Optional[str] = None) -> None:
        """Insert triples via the Graph Store Protocol (POST /data)."""
        params = {"graph": graph} if graph else None
        response = requests.post(
            self.data_url,
            data=turtle_body.encode("utf-8"),
            headers={"Content-Type": _TURTLE_ACCEPT},
            params=params,
            auth=self._auth,
            timeout=self._timeout,
        )
        response.raise_for_status()

    def ping(self) -> bool:
        """Cheap health check — return True if the server responds to a trivial ASK."""
        try:
            response = requests.post(
                self.query_url,
                data={"query": "ASK { }"},
                headers={"Accept": _JSON_ACCEPT},
                auth=self._auth,
                timeout=self._timeout,
            )
            return response.ok
        except Exception:
            return False


def rows_from_select(select_result: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Flatten a SPARQL Results JSON into a list of plain-value dicts."""
    if not isinstance(select_result, dict):
        return []
    bindings = select_result.get("results", {}).get("bindings", [])
    out: List[Dict[str, Any]] = []
    for row in bindings:
        flat: Dict[str, Any] = {}
        for var, binding in row.items():
            flat[var] = binding.get("value")
        out.append(flat)
    return out


def vars_from_select(select_result: Dict[str, Any]) -> List[str]:
    if not isinstance(select_result, dict):
        return []
    return list(select_result.get("head", {}).get("vars", []) or [])

/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *******************************************************************************/
package prerna.engine.impl.graphrag;

import java.util.List;
import java.util.Map;

import prerna.engine.impl.vector.AbstractVectorDatabaseEngine;
import prerna.om.Insight;

/**
 * Base class for GraphRAG engines — hybrid retrieval over a knowledge graph
 * plus paired vector search over chunked prose.
 *
 * <p>Extends {@link AbstractVectorDatabaseEngine} so callers get every
 * existing vector-engine primitive (addDocument, removeDocument,
 * listDocuments, nearestNeighbor, etc.) for free. Concrete subclasses add
 * five graph-aware operations designed to be exposed as MCP tools to LLM
 * agents:
 *
 * <ol>
 *   <li>{@link #guardedQuery} — LLM-authored graph query (SPARQL / Cypher)
 *       with a guardrail that rejects writes and injects safe defaults
 *       (LIMIT, PREFIXES) before hitting the store.</li>
 *   <li>{@link #entityLink} — natural-language phrase → matching graph
 *       entity URIs. The "linking" stage in the classic
 *       plan → link → retrieve → synthesize pipeline.</li>
 *   <li>{@link #graphExpand} — N-hop neighborhood around a node. Used
 *       to enrich anchor sets and drive visualizations of the agent's
 *       traversal path.</li>
 *   <li>{@link #hybridRetrieve} — the full pass: vector search for
 *       fuzzy chunk recall + graph traversal for structural relationships,
 *       fused into a single subgraph result with provenance.</li>
 *   <li>{@link #ingestDoc} — RDFize a document (or a batch) into nodes,
 *       edges, and chunk embeddings in one call. Bypasses the CSV path
 *       used by classic {@code addDocument}.</li>
 * </ol>
 *
 * <p>Concrete implementations decide the backing store (Apache Jena / Fuseki,
 * later possibly Neo4j / Kùzu-fork / AGE) and how the paired vector engine
 * is wired (bundled internal index vs. delegated to an existing SEMOSS
 * vector engine referenced by id).
 *
 * <p>All method contracts:
 * <ul>
 *   <li>{@code insight} must be non-null. Used for auth context, the
 *       Python bridge session id, and cross-engine calls.</li>
 *   <li>Return values are plain {@code Map} / {@code List<Map>} shapes so
 *       reactors can serialize them straight to MCP tool responses.</li>
 *   <li>Implementations should raise {@link IllegalArgumentException} for
 *       caller mistakes and let unexpected failures propagate — silent
 *       empty returns are a foot-gun (see the {@code _build_filter} bug
 *       hardening on the Qdrant engine for the reference pattern).</li>
 * </ul>
 */
public abstract class AbstractGraphRAGEngine extends AbstractVectorDatabaseEngine {

    /**
     * Execute a graph query authored by an LLM (SPARQL for Jena; Cypher /
     * openCypher for future property-graph backends), gated by a guard
     * layer that:
     * <ul>
     *   <li>Rejects any query that would mutate state (INSERT, DELETE,
     *       DROP, LOAD, CREATE, CLEAR, etc.).</li>
     *   <li>Injects the workspace's canonical namespace prefixes so the
     *       LLM doesn't have to remember them.</li>
     *   <li>Auto-appends a {@code LIMIT} on unbounded SELECT queries
     *       (default from {@code parameters} or a subclass-defined cap)
     *       to prevent runaway result sets from a poorly-authored query.</li>
     * </ul>
     *
     * @param insight     non-null insight for auth + session
     * @param query       raw LLM-authored query string
     * @param parameters  optional overrides — e.g. {@code rowCap},
     *                    {@code timeoutMs}, backend-specific tuning
     * @return map with keys: {@code "query"} (the sanitized query as
     *         actually executed), {@code "vars"} (ordered list of column
     *         names), {@code "rows"} (list of variable-binding maps),
     *         {@code "rowCount"} (int)
     * @throws IllegalArgumentException if the query fails the guard
     */
    public abstract Map<String, Object> guardedQuery(
            Insight insight,
            String query,
            Map<String, Object> parameters);

    /**
     * Match a natural-language phrase against entities in the graph and
     * return the top-k candidates ranked by hybrid score (vector similarity
     * against entity labels + descriptions, boosted by exact/substring
     * matches on identifier fields).
     *
     * <p>This is the "linking" stage: the agent takes a phrase like
     * "atorvastatin", gets back a small ranked list of candidate URIs and
     * decides which to anchor the rest of the query on.
     *
     * @param insight     non-null insight
     * @param text        the phrase to link (typically an entity name
     *                    extracted from the user's question)
     * @param limit       max candidates to return (subclasses may cap
     *                    lower for cost reasons)
     * @param parameters  optional: {@code nodeTypes} (restrict to certain
     *                    RDF classes / property-graph labels),
     *                    {@code scoreThreshold}
     * @return list of maps, each with keys: {@code "uri"}, {@code "label"},
     *         {@code "type"} (the graph class of the entity), {@code "score"}
     *         (float, higher = better), {@code "aliases"} (optional list of
     *         alternate labels)
     */
    public abstract List<Map<String, Object>> entityLink(
            Insight insight,
            String text,
            int limit,
            Map<String, Object> parameters);

    /**
     * Return the local subgraph around a given node — everything reachable
     * within {@code hops} typed edges, with optional edge-type filtering.
     *
     * <p>Used by agents to explore what a node is connected to, and by
     * the UI to render the "what the agent traversed" force-directed
     * graph.
     *
     * @param insight     non-null insight
     * @param nodeUri     the anchor node (a URI produced by
     *                    {@link #entityLink} or a prior graph query)
     * @param hops        max traversal depth (subclasses should cap this
     *                    to a safe default like 3 to prevent full-graph
     *                    walks)
     * @param parameters  optional: {@code edgeTypes} (list of predicate
     *                    URIs to follow — omit for all edges),
     *                    {@code maxEdges} (result cap),
     *                    {@code includeLabels} (attach rdfs:label to
     *                    s/o where available)
     * @return list of triples: each map has keys {@code "s"}, {@code "p"},
     *         {@code "o"}, and optionally {@code "sLabel"}, {@code "oLabel"}
     */
    public abstract List<Map<String, Object>> graphExpand(
            Insight insight,
            String nodeUri,
            int hops,
            Map<String, Object> parameters);

    /**
     * The full hybrid retrieval pass — this is the "just answer my
     * question" tool that composes all the other primitives:
     * <ol>
     *   <li>Extract candidate anchor entities from the question.</li>
     *   <li>Link each anchor to graph URIs via {@link #entityLink}.</li>
     *   <li>Expand the anchor set 1-2 hops via {@link #graphExpand} to
     *       collect the structural context.</li>
     *   <li>Run vector search over chunked prose passages for fuzzy
     *       semantic recall of narrative text (warnings, descriptions,
     *       relationships that aren't cleanly modeled as edges).</li>
     *   <li>Fuse both signals into a single subgraph + passage set with
     *       clear provenance.</li>
     * </ol>
     *
     * @param insight     non-null insight
     * @param question    the natural-language question
     * @param limit       max passages / max anchor entities to return
     * @param parameters  optional: {@code hops} (default 1),
     *                    {@code sectionFilter} (restrict vector search
     *                    to chunks tagged with a section name),
     *                    {@code minScore}, {@code pairedVectorEngineId}
     *                    (override the workspace's default paired
     *                    vector engine)
     * @return map with keys: {@code "anchors"} (list of matched entity
     *         URIs), {@code "subgraph"} (list of triples), {@code "passages"}
     *         (list of chunk maps: text, source, section, score, uri),
     *         {@code "sparql"} (the query that produced the subgraph, for
     *         transparency), {@code "diagnostics"} (timings + counts)
     */
    public abstract Map<String, Object> hybridRetrieve(
            Insight insight,
            String question,
            int limit,
            Map<String, Object> parameters);

    /**
     * Ingest a document (or a batch) into the graph — extract entities
     * and relationships, materialize them as nodes and edges, chunk the
     * prose and store the chunks with their embeddings in the paired
     * vector index, all in one call. Bypasses the CSV path used by the
     * classic {@code addDocument} flow.
     *
     * <p>The concrete strategy for entity/edge extraction is
     * implementation-defined:
     * <ul>
     *   <li>Ontology-driven — the caller provides a schema, the ingestor
     *       matches structured fields to classes and properties.</li>
     *   <li>LLM-driven — an LLM is invoked to propose an entity graph,
     *       Microsoft-GraphRAG-style. More expensive at ingest time,
     *       no upfront ontology cost.</li>
     *   <li>Hybrid — LLM for narrative extraction, ontology for
     *       structured fields.</li>
     * </ul>
     *
     * <p>URIs must be deterministic (typically via a slug or UUID5 seed
     * over stable source ids) so re-ingesting the same document is
     * idempotent — a hard requirement for incremental updates.
     *
     * @param insight     non-null insight
     * @param text        the document body (or a JSON array of docs for
     *                    batch mode)
     * @param metadata    per-document facts: {@code source}, {@code title},
     *                    {@code timestamp}, {@code tags}, etc. Attached to
     *                    the derived nodes as datatype properties.
     * @param parameters  optional: {@code chunking} (strategy + size),
     *                    {@code extractionMode} (ontology / llm / hybrid),
     *                    {@code ontologyRef} (workspace-scoped ontology
     *                    id), {@code embedderEngineId}
     * @return map with keys: {@code "nodesAdded"}, {@code "edgesAdded"},
     *         {@code "chunksIndexed"}, {@code "documentUri"} (the root
     *         node URI for the ingested doc), {@code "diagnostics"}
     */
    public abstract Map<String, Object> ingestDoc(
            Insight insight,
            String text,
            Map<String, Object> metadata,
            Map<String, Object> parameters);
}

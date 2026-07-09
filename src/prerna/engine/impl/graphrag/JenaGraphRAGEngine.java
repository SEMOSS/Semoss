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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.ds.py.PyUtils;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.om.Insight;
import prerna.util.Utility;

/**
 * Apache Jena / Fuseki-backed GraphRAG engine.
 *
 * <p>Stores the knowledge graph as RDF in an Apache Jena TDB2 dataset,
 * accessed over the SPARQL 1.1 HTTP protocol against a Fuseki endpoint.
 * Chunked prose lives in a paired vector engine (referenced by
 * {@link #FUSEKI_PAIRED_VECTOR_ENGINE_ID}) — hybrid retrieval fuses both.
 *
 * <p>SMSS properties consumed at {@link #open}:
 * <ul>
 *   <li>{@code FUSEKI_URL} — base URL of the Fuseki server
 *       (e.g. {@code http://fuseki:3030})</li>
 *   <li>{@code FUSEKI_DATASET} — dataset name (e.g. {@code kb})</li>
 *   <li>{@code FUSEKI_USER}, {@code FUSEKI_PASSWORD} — optional basic auth</li>
 *   <li>{@code FUSEKI_PAIRED_VECTOR_ENGINE_ID} — id of another SEMOSS
 *       vector engine to use for chunk retrieval. If unset, the engine
 *       uses its inherited vector-engine primitives against its own
 *       embedded store.</li>
 *   <li>{@code GRAPHRAG_ROW_CAP} — max SPARQL rows returned by
 *       {@link #guardedQuery} when no explicit LIMIT is provided
 *       (default 500)</li>
 *   <li>{@code GRAPHRAG_QUERY_TIMEOUT_MS} — first-result/overall timeout
 *       forwarded to Fuseki (default 15000/30000)</li>
 *   <li>{@code GRAPHRAG_ONTOLOGY_TTL} — optional workspace-scoped
 *       ontology to inject as PREFIX declarations and to guide the LLM
 *       in {@link #guardedQuery}</li>
 * </ul>
 *
 * <p>Phase-3a scaffold — the five GraphRAG operations throw
 * {@link UnsupportedOperationException}. Phase 3b wires them to
 * {@link prerna.engine.impl.rdf.RemoteJenaEngine} (for SPARQL) and the
 * SEMOSS Python bridge (for entity linking / vector search / ingest).
 */
public class JenaGraphRAGEngine extends AbstractGraphRAGEngine {

    private static final Logger classLogger = LogManager.getLogger(JenaGraphRAGEngine.class);
    private static final Gson GSON = new Gson();

    public static final String FUSEKI_URL = "FUSEKI_URL";
    public static final String FUSEKI_DATASET = "FUSEKI_DATASET";
    public static final String FUSEKI_USER = "FUSEKI_USER";
    public static final String FUSEKI_PASSWORD = "FUSEKI_PASSWORD";
    public static final String FUSEKI_PAIRED_VECTOR_ENGINE_ID = "FUSEKI_PAIRED_VECTOR_ENGINE_ID";

    public static final String GRAPHRAG_ROW_CAP = "GRAPHRAG_ROW_CAP";
    public static final String GRAPHRAG_QUERY_TIMEOUT_MS = "GRAPHRAG_QUERY_TIMEOUT_MS";
    public static final String GRAPHRAG_ONTOLOGY_TTL = "GRAPHRAG_ONTOLOGY_TTL";

    public static final int DEFAULT_ROW_CAP = 500;
    public static final String DEFAULT_QUERY_TIMEOUT_MS = "15000,30000";

    private String fusekiUrl = null;
    private String fusekiDataset = null;
    private String fusekiUser = null;
    private String fusekiPassword = null;
    private String pairedVectorEngineId = null;
    private int rowCap = DEFAULT_ROW_CAP;
    private String queryTimeoutMs = DEFAULT_QUERY_TIMEOUT_MS;
    private String ontologyTtl = null;

    private String vectorDatabaseSearcher = null;

    @Override
    public void open(Properties smssProp) throws Exception {
        super.open(smssProp);

        this.fusekiUrl = trimOrNull(this.smssProp.getProperty(FUSEKI_URL));
        if (this.fusekiUrl == null) {
            throw new IllegalArgumentException(
                    FUSEKI_URL + " is required — e.g. http://fuseki:3030");
        }
        // Normalize: strip trailing slashes so we can concat "/dataset/sparql" cleanly.
        while (this.fusekiUrl.endsWith("/")) {
            this.fusekiUrl = this.fusekiUrl.substring(0, this.fusekiUrl.length() - 1);
        }

        this.fusekiDataset = trimOrNull(this.smssProp.getProperty(FUSEKI_DATASET));
        if (this.fusekiDataset == null) {
            throw new IllegalArgumentException(
                    FUSEKI_DATASET + " is required — e.g. 'kb'");
        }
        // Strip a leading slash the user might have added.
        if (this.fusekiDataset.startsWith("/")) {
            this.fusekiDataset = this.fusekiDataset.substring(1);
        }

        this.fusekiUser = trimOrNull(this.smssProp.getProperty(FUSEKI_USER));
        this.fusekiPassword = trimOrNull(this.smssProp.getProperty(FUSEKI_PASSWORD));

        this.pairedVectorEngineId = trimOrNull(
                this.smssProp.getProperty(FUSEKI_PAIRED_VECTOR_ENGINE_ID));

        String rowCapStr = trimOrNull(this.smssProp.getProperty(GRAPHRAG_ROW_CAP));
        if (rowCapStr != null) {
            try {
                this.rowCap = Integer.parseInt(rowCapStr);
            } catch (NumberFormatException e) {
                classLogger.warn("Invalid {} value '{}', using default {}",
                        GRAPHRAG_ROW_CAP, rowCapStr, DEFAULT_ROW_CAP);
            }
        }

        String timeoutStr = trimOrNull(this.smssProp.getProperty(GRAPHRAG_QUERY_TIMEOUT_MS));
        if (timeoutStr != null) {
            this.queryTimeoutMs = timeoutStr;
        }

        this.ontologyTtl = trimOrNull(this.smssProp.getProperty(GRAPHRAG_ONTOLOGY_TTL));

        if (this.vectorDatabaseSearcher == null
                || (this.vectorDatabaseSearcher = this.vectorDatabaseSearcher.trim()).isEmpty()) {
            this.vectorDatabaseSearcher = Utility.getRandomString(6);
        }

        classLogger.info(
                "JenaGraphRAGEngine opened: fuseki={}, dataset={}, paired_vector_engine={}, row_cap={}",
                this.fusekiUrl, this.fusekiDataset,
                this.pairedVectorEngineId != null ? this.pairedVectorEngineId : "(bundled)",
                this.rowCap);
    }

    @Override
    public void close() throws IOException {
        this.vectorDatabaseSearcher = null;
        super.close();
    }

    @Override
    public VectorDatabaseTypeEnum getVectorDatabaseType() {
        return VectorDatabaseTypeEnum.JENA_GRAPH_RAG;
    }

    @Override
    protected String getDefaultDistanceMethod() {
        return "Cosine Similarity";
    }

    @Override
    protected String[] getServerStartCommands() {
        StringBuilder init = new StringBuilder();
        init.append("import graph_rag_jena;");
        init.append(this.vectorDatabaseSearcher).append(" = graph_rag_jena.JenaGraphRAG(")
                .append("query_url = ").append(PyUtils.determineStringType(getSparqlQueryUrl()))
                .append(", update_url = ").append(PyUtils.determineStringType(getSparqlUpdateUrl()))
                .append(", user = ").append(this.fusekiUser != null
                        ? PyUtils.determineStringType(this.fusekiUser) : "None")
                .append(", password = ").append(this.fusekiPassword != null
                        ? PyUtils.determineStringType(this.fusekiPassword) : "None")
                .append(", row_cap = ").append(this.rowCap)
                .append(", timeout_ms = ").append(PyUtils.determineStringType(this.queryTimeoutMs))
                .append(", ontology_ttl = ").append(this.ontologyTtl != null
                        ? PyUtils.determineStringType(this.ontologyTtl) : "None")
                .append(")");
        return init.toString().split(PyUtils.PY_COMMAND_SEPARATOR);
    }

    @Override
    protected void addIndexClass(String indexClass) {
        // Jena backs everything with named graphs inside the single dataset;
        // no per-class initialization needed. Vector-style index classes
        // map to smss:belongsToDocument groupings inside SPARQL.
        this.indexClasses.add(indexClass);
    }

    @Override
    protected void cleanUpAddDocument(java.io.File indexFilesFolder) {
        // noop — Jena doesn't stage intermediate CSVs
    }

    @SuppressWarnings("unchecked")
    @Override
    protected List<Map<String, Object>> nearestNeighborCall(Insight insight,
            String searchStatement, Number limit, Map<String, Object> parameters) {
        // v1: no bundled vector store — delegate to the paired vector engine
        // if one is configured, otherwise fall back to entity_link against
        // rdfs:label so a basic nearest-neighbor call still returns something.
        if (isPairedVectorMode()) {
            throw new UnsupportedOperationException(
                    "Paired vector engine delegation — v2 (needs cross-engine query orchestration). "
                            + "For now, use JenaEntityLinkReactor for text→URI matches or "
                            + "JenaHybridRetrieveReactor for the full graph+chunk pass.");
        }
        // Fallback: run entity_link and format results as vector-search rows.
        Map<String, Object> params = parameters != null ? parameters : new HashMap<>();
        int cap = limit != null ? limit.intValue() : 5;
        List<Map<String, Object>> matches = entityLink(insight, searchStatement, cap, params);
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> m : matches) {
            Map<String, Object> row = new HashMap<>();
            row.put("Score", m.get("score"));
            row.put("Content", m.get("label"));
            row.put("Source", m.get("type"));
            row.put("uri", m.get("uri"));
            out.add(row);
        }
        return out;
    }

    // ------------------------------------------------------------------
    // AbstractGraphRAGEngine surface — Phase 3b concrete implementations
    // ------------------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> guardedQuery(Insight insight, String query,
            Map<String, Object> parameters) {
        if (insight == null) {
            throw new IllegalArgumentException("insight is required");
        }
        if (query == null || query.trim().isEmpty()) {
            throw new IllegalArgumentException("query is required");
        }
        checkSocketStatus();

        StringBuilder script = new StringBuilder();
        script.append(this.vectorDatabaseSearcher).append(".guarded_query(")
                .append("query = ").append(PyUtils.determineStringType(query));
        if (parameters != null && parameters.containsKey("rowCap")) {
            script.append(", row_cap = ").append(parameters.get("rowCap"));
        }
        script.append(")");
        classLogger.info("Running >>> {}", script);
        return (Map<String, Object>) pyTranslator.runDirectPy(insight, script.toString());
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> entityLink(Insight insight, String text,
            int limit, Map<String, Object> parameters) {
        if (insight == null) {
            throw new IllegalArgumentException("insight is required");
        }
        if (text == null || text.trim().isEmpty()) {
            return Collections.emptyList();
        }
        checkSocketStatus();

        StringBuilder script = new StringBuilder();
        script.append(this.vectorDatabaseSearcher).append(".entity_link(")
                .append("text = ").append(PyUtils.determineStringType(text))
                .append(", limit = ").append(limit);
        if (parameters != null) {
            if (parameters.containsKey("nodeTypes")) {
                Object raw = parameters.get("nodeTypes");
                if (raw instanceof List) {
                    script.append(", node_types = ").append(GSON.toJson(raw));
                }
            }
            if (parameters.containsKey("scoreThreshold")) {
                script.append(", score_threshold = ").append(parameters.get("scoreThreshold"));
            }
        }
        script.append(")");
        classLogger.info("Running >>> {}", script);
        return (List<Map<String, Object>>) pyTranslator.runDirectPy(insight, script.toString());
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> graphExpand(Insight insight, String nodeUri,
            int hops, Map<String, Object> parameters) {
        if (insight == null) {
            throw new IllegalArgumentException("insight is required");
        }
        if (nodeUri == null || nodeUri.trim().isEmpty()) {
            throw new IllegalArgumentException("nodeUri is required");
        }
        checkSocketStatus();

        StringBuilder script = new StringBuilder();
        script.append(this.vectorDatabaseSearcher).append(".graph_expand(")
                .append("node_uri = ").append(PyUtils.determineStringType(nodeUri))
                .append(", hops = ").append(hops);
        if (parameters != null) {
            if (parameters.containsKey("edgeTypes")) {
                Object raw = parameters.get("edgeTypes");
                if (raw instanceof List) {
                    script.append(", edge_types = ").append(GSON.toJson(raw));
                }
            }
            if (parameters.containsKey("maxEdges")) {
                script.append(", max_edges = ").append(parameters.get("maxEdges"));
            }
            if (parameters.containsKey("includeLabels")) {
                boolean include = Boolean.TRUE.equals(parameters.get("includeLabels"))
                        || "true".equalsIgnoreCase(String.valueOf(parameters.get("includeLabels")));
                script.append(", include_labels = ").append(include ? "True" : "False");
            }
        }
        script.append(")");
        classLogger.info("Running >>> {}", script);
        return (List<Map<String, Object>>) pyTranslator.runDirectPy(insight, script.toString());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> hybridRetrieve(Insight insight, String question,
            int limit, Map<String, Object> parameters) {
        if (insight == null) {
            throw new IllegalArgumentException("insight is required");
        }
        if (question == null || question.trim().isEmpty()) {
            throw new IllegalArgumentException("question is required");
        }
        checkSocketStatus();

        StringBuilder script = new StringBuilder();
        script.append(this.vectorDatabaseSearcher).append(".hybrid_retrieve(")
                .append("question = ").append(PyUtils.determineStringType(question))
                .append(", limit = ").append(limit);
        if (parameters != null) {
            if (parameters.containsKey("hops")) {
                script.append(", hops = ").append(parameters.get("hops"));
            }
            if (parameters.containsKey("sectionFilter")) {
                script.append(", section_filter = ")
                        .append(PyUtils.determineStringType(parameters.get("sectionFilter")));
            }
            if (parameters.containsKey("minScore")) {
                script.append(", min_score = ").append(parameters.get("minScore"));
            }
        }
        script.append(")");
        classLogger.info("Running >>> {}", script);
        return (Map<String, Object>) pyTranslator.runDirectPy(insight, script.toString());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> ingestDoc(Insight insight, String text,
            Map<String, Object> metadata, Map<String, Object> parameters) {
        if (insight == null) {
            throw new IllegalArgumentException("insight is required");
        }
        if (text == null || text.trim().isEmpty()) {
            throw new IllegalArgumentException("text is required");
        }
        checkSocketStatus();

        String extractionMode = "structured";
        if (parameters != null && parameters.containsKey("extractionMode")) {
            extractionMode = String.valueOf(parameters.get("extractionMode"));
        }

        StringBuilder script = new StringBuilder();
        script.append(this.vectorDatabaseSearcher).append(".ingest_doc(")
                .append("text = ").append(PyUtils.determineStringType(text))
                .append(", metadata = ").append(metadata != null && !metadata.isEmpty()
                        ? GSON.toJson(metadata) : "None")
                .append(", extraction_mode = ").append(PyUtils.determineStringType(extractionMode))
                .append(")");
        classLogger.info("Running >>> {}", script);
        return (Map<String, Object>) pyTranslator.runDirectPy(insight, script.toString());
    }

    // ------------------------------------------------------------------
    // Accessors — used by reactors and by Phase 3b Python bridge builders
    // ------------------------------------------------------------------

    public String getFusekiUrl() {
        return this.fusekiUrl;
    }

    public String getFusekiDataset() {
        return this.fusekiDataset;
    }

    /** Full SPARQL query endpoint URL (e.g. http://fuseki:3030/kb/sparql). */
    public String getSparqlQueryUrl() {
        return this.fusekiUrl + "/" + this.fusekiDataset + "/sparql";
    }

    /** Full SPARQL update endpoint URL (e.g. http://fuseki:3030/kb/update). */
    public String getSparqlUpdateUrl() {
        return this.fusekiUrl + "/" + this.fusekiDataset + "/update";
    }

    public String getFusekiUser() {
        return this.fusekiUser;
    }

    public String getFusekiPassword() {
        return this.fusekiPassword;
    }

    public String getPairedVectorEngineId() {
        return this.pairedVectorEngineId;
    }

    public boolean isPairedVectorMode() {
        return this.pairedVectorEngineId != null;
    }

    public int getRowCap() {
        return this.rowCap;
    }

    public String getQueryTimeoutMs() {
        return this.queryTimeoutMs;
    }

    public String getOntologyTtl() {
        return this.ontologyTtl;
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static String trimOrNull(String s) {
        if (s == null) {
            return null;
        }
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }
}

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
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
        // Phase 3b: build the Python init command that primes the graph_rag_jena
        // module with fuseki URL + dataset + paired vector engine handle.
        return new String[] { "# JenaGraphRAGEngine Python init — filled in Phase 3b" };
    }

    @Override
    protected void addIndexClass(String indexClass) {
        // Phase 3b — Jena engines don't use vector-style index classes;
        // this becomes a named graph inside the dataset.
        throw new UnsupportedOperationException(
                "addIndexClass — Phase 3b (named-graph mapping)");
    }

    @Override
    protected void cleanUpAddDocument(java.io.File indexFilesFolder) {
        // noop — Jena doesn't stage intermediate CSVs
    }

    @Override
    protected List<Map<String, Object>> nearestNeighborCall(Insight insight,
            String searchStatement, Number limit, Map<String, Object> parameters) {
        // Phase 3b: delegate to the paired vector engine, or use the bundled
        // Python-side searcher against the local chunk store.
        throw new UnsupportedOperationException(
                "nearestNeighborCall — Phase 3b (delegate to paired vector engine)");
    }

    // ------------------------------------------------------------------
    // AbstractGraphRAGEngine surface — all Phase 3b implementations
    // ------------------------------------------------------------------

    @Override
    public Map<String, Object> guardedQuery(Insight insight, String query,
            Map<String, Object> parameters) {
        throw new UnsupportedOperationException(
                "guardedQuery — Phase 3b (SPARQL guard + Fuseki HTTP round-trip)");
    }

    @Override
    public List<Map<String, Object>> entityLink(Insight insight, String text,
            int limit, Map<String, Object> parameters) {
        throw new UnsupportedOperationException(
                "entityLink — Phase 3b (paired vector search over entity labels + boost)");
    }

    @Override
    public List<Map<String, Object>> graphExpand(Insight insight, String nodeUri,
            int hops, Map<String, Object> parameters) {
        throw new UnsupportedOperationException(
                "graphExpand — Phase 3b (N-hop CONSTRUCT via Fuseki)");
    }

    @Override
    public Map<String, Object> hybridRetrieve(Insight insight, String question,
            int limit, Map<String, Object> parameters) {
        throw new UnsupportedOperationException(
                "hybridRetrieve — Phase 3b (compose entityLink + graphExpand + vector)");
    }

    @Override
    public Map<String, Object> ingestDoc(Insight insight, String text,
            Map<String, Object> metadata, Map<String, Object> parameters) {
        throw new UnsupportedOperationException(
                "ingestDoc — Phase 3b (RDFize + chunk + embed + SPARQL UPDATE)");
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

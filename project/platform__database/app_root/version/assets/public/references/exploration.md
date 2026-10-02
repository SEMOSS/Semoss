---
name: database-exploration
description: Analyze data using the active database's supported SQL, RDF/SPARQL, or graph query route. Covers question framing, schema and data understanding, query design, Python analysis, evidence-driven troubleshooting, and communicating verified findings.
---

# Data analysis and exploration

Start from the user's question and the active database's capabilities. Choose the amount of exploration and the method that will produce a useful, defensible answer. The workflow below is guidance for deciding what to do next, not a mandatory report template or a requirement to profile every table.

## Understand what would answer the question

Identify the subject, the population of interest, the relevant time range, and the level of detail needed. Establish what one observation represents before combining or comparing records. Retain definitions and filters already established in the conversation. General explanations may require no database call, and a precise data question may need only one query.

Treat column names and sample values as evidence about meaning, not complete business definitions. Use available descriptions, documented relationships, and known definitions. Distinguish an inferred interpretation from one confirmed by the user or source documentation.

Separate ambiguity about meaning from optional refinements to scope. If competing definitions, incompatible units, an unclear source, or another missing detail would make the answer misleading, ask a focused clarification before choosing an interpretation. Describe the plausible choices from the evidence and why the distinction matters. Ask the question in your final chat reply, end the turn, and wait for the user's next prompt before continuing. Do not silently decide the unresolved meaning.

When the measure is clear and an unfiltered result is meaningful, an unspecified optional filter need not block progress. Return the useful broad answer, state its actual scope, and offer a relevant refinement if discovered dimensions would help. Do not silently narrow to a convenient sample, reset an agreed filter, or imply complete coverage without verifying it. A follow-up should help the user make a concrete choice, not require them to specify every possible dimension before seeing any results.

## Build enough understanding to act

Inspect the relevant schema and small previews when they answer an uncertainty. Reuse reliable schema, query, and help results already in the conversation; refresh when the data or schema may have changed. Expand exploration in response to what you learn instead of scanning unrelated tables by default.

Check types, keys, relationships, missing values, duplicates, units, and time coverage where they matter to the question. A matching column name does not prove a valid join. Confirm join cardinality before drawing conclusions from combined data.

`GetDatabaseTableStructure` returns schema rows ordered as:

```text
[logicalTable, logicalColumn, dataType, isVertex, physicalColumn, physicalTable]
```

The direct reactor returns rows; some client formatting wraps them in `output.data.values`. SQL uses physical names in positions 5 and 6. Structured Pixel selectors use logical names in positions 1 and 2. A graph vertex row is not a normal SQL primary-key column. If metadata is incomplete, use a supported bounded inspection or explain the missing information rather than inventing schema.

## Choose an analytical approach

Translate the question into operations the active engine supports. Queries can combine filtering, projection, relationships, grouping, calculations, and ordering; there does not need to be a separate platform tool for every analytical concept. Use the actual dialect and schema when constructing a query.

Do filtering and data reduction close to the source when appropriate. Retrieve only the data needed for the next step. Use managed Python for transformations, statistical methods, modeling, or visualization that are better expressed there; load the `python` skill for execution, runtime paths, libraries, and retained state. Do not assume variables from a separate Python execution context are available. Use `pagination` and `exports` when the task requires larger result sets or saved artifacts.

Choose simple methods that answer the question before adding complexity. More queries, a chart, or a model should resolve a relevant uncertainty or improve the explanation. Preserve the queries, parameters, transformations, and material assumptions needed to reproduce the result.

A small returned result can still require an expensive scan. A result limit controls returned rows, not necessarily database work. Decide whether full-data computation is needed; label bounded or sampled analysis accurately. Exploration is read-only unless the user's request authorizes a change. Query tools can support writes, and `commit=false` is not a read-only permission boundary.

## Use the engine's supported query route

Use the active workbench's engine ID, catalog subtype, and permissions. Generated schema/query tool descriptions identify the loaded implementation and supported route. The catalog subtype also identifies a SQL dialect such as POSTGRES or SQL_SERVER. The editor's language setting does not establish backend capabilities.

| Loaded engine | Query route | Query identifiers |
| --- | --- | --- |
| Relational (`IRDBMSEngine`) | `SqlQuery`; `SqlQueryBase64` for encoded SQL via `runPixel` | Physical tables/columns; dialect from the tool description |
| RDF (`IRDFDatabase`: Jena, Sesame, RDF4J, etc.) | `SparqlQuery` / `SparqlQueryBase64`, SELECT only | Actual RDF classes, predicates, and resource URIs |
| Tinker, JanusGraph, DataStax graph | `Database(...) \| Select(...) \| Collect(...)` via reactor-help `runPixel` | Logical concepts/properties; translated to Gremlin internally |
| Neo4j | Structured `Database \| Select \| Collect` via `runPixel` | Logical metamodel names; backend dialect is Cypher |
| Unknown or other implementation | Establish a supported route from its capability description | Do not assume SQL, SPARQL, or Gremlin |

`GetDatabaseCategory(engine=["<engine-id>"])` can supply missing category context, but SQL/RDF/NoSQL/Unknown is only a broad classification. NoSQL does not mean Gremlin, and Unknown does not imply SQL. Prefer the engine-specific tool descriptions.

Database rooms expose `GetDatabaseTableStructure` and the appropriate native query tool for relational or RDF engines. `SqlQuery` uses approval mode `ask` because it also supports mutations; honor the room's approval flow and engine permissions. SPARQL SELECT uses `auto`. Graph engines use the attached reactor-help execution tool for structured Pixel queries.

### SQL transport

Pass the database and complete query to the provided tool. This is a request-shape example; replace the illustrative table and columns with discovered physical names and choose a query appropriate to the user's question:

```json
{
  "database": "<active-engine-id>",
  "query": "SELECT category, value FROM measurements",
  "limit": 50
}
```

Use the actual dialect for expressions, identifier quoting, and date handling. SDK/Pixel transport and UTF-8 Base64 encoding are covered in the main database guide. Do not add a help lookup when the supplied schema already explains the call.

### RDF transport

A bounded inspection can preserve complete RDF URIs and literal representations:

```pixel
SparqlQuery(database=["<active-engine-id>"], query=["<encode>SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 25</encode>"], limit=[25], raw=[true]);
```

Construct graph patterns from the discovered classes and predicates. Preserve literal types and account for optional or repeated properties when interpreting results. The current reactor accepts SELECT, not ASK, CONSTRUCT, DESCRIBE, or UPDATE. Put PREFIX/BASE declarations on separate lines before SELECT, or use full URIs. `SparqlQueryBase64` provides encoded transport for dynamic text.

### Graph transport

For a confirmed logical concept `Person` with property `age`, a bounded structured query is:

```pixel
Database(database=["<active-engine-id>"]) | Select(Person__age) | Collect(50);
```

The engine builds a traversal from the query structure. There is no registered `GremlinQuery` reactor. Tinker and DataStax string `execQuery` methods are unimplemented; JanusGraph inherits the Tinker path. Wrapping `g.V()` in `Query(...)` does not provide raw Gremlin support. Neo4j has a structured query path but uses Cypher as its native dialect.

Graph missing properties, traversal multiplicity, and deduplication can differ from SQL row/null semantics. For Gremlin-backed engines, some analytical expressions are evaluated by the platform over traversal results rather than pushed down as native aggregates. Establish the input population and resource implications before interpreting them as whole-graph results. Consult the specific selector's contract when needed.

## Adapt to results and errors

Each tool call should answer an identified question, test an approach, or produce part of the requested result. Use `runPixelHelp` to resolve a concrete uncertainty about a supported operation, then apply what it tells you. There is no fixed lookup quota: legitimate unfamiliar work may need more documentation than a familiar query.

Notice when a sequence of calls is no longer changing your understanding or advancing the task. Reuse earlier answers; do not repeat unchanged failures or keep guessing alternative names for the same unknown operation. Diagnose whether the obstacle is syntax, data shape, permissions, missing metadata, or an unsupported capability. Revise the query or choose another supported analytical method based on that evidence. If none can answer the question, state the precise blocker and what would resolve it.

A successful documentation lookup is not successful query execution. Check Pixel errors and the actual result payload before proceeding. Direct MCP query tools return JSON text, normally with tabular `headers` and `values` under `output`. SDK `runPixel` uses the `pixelReturn` envelope described in the main guide; do not assume the two transports have identical nesting.

## Check and explain the conclusion

Verify that the result addresses the intended population, time period, and level of detail. Look for join expansion, missing values, duplicate handling, incompatible units, or incomplete samples that could change the conclusion. Cross-check surprising results with an independent query or a simpler view of the data.

Separate observed findings from hypotheses. Avoid causal claims from association alone, and distinguish a sampled estimate from a full-data result. Neither `numCollected` nor the number of preview rows is a total database count.

Lead with the answer or useful finding, then give the evidence and the query, table, chart, or saved artifact needed to understand it. Explain the population, filters, time coverage, units, and material assumptions when relevant. An aggregate over all qualifying records differs from a preview or sampled estimate; say which was computed, and disclose exclusions that could affect interpretation.

Use what the data reveals to offer a helpful next step. For a broad result, a discovered dimension may warrant asking whether the user wants a particular segment or a comparison across groups. Ground suggestions in actual available fields and known values; do not invent dimensions or force a follow-up question after every answer. When the user chooses a refinement, carry the established metric definition and other filters into the next analysis. A concise factual answer does not need a full analysis report; a complex investigation needs enough explanation to make the reasoning and limits clear.

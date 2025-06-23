# `DATABASE` Engines

Engines of the `DATABASE` catalog type provide connectivity to various database systems, enabling data querying, manipulation, and metadata discovery. They typically extend `prerna.engine.impl.AbstractDatabaseEngine` and implement `prerna.engine.api.IDatabaseEngine`.

These engines are fundamental for accessing and working with structured and semi-structured data from sources like relational databases (PostgreSQL, MySQL, Oracle, SQL Server, H2), RDF triple stores (Jena, Sesame), graph databases (Neo4j, JanusGraph), and more.

## Core Concepts for Database Engines

### `prerna.engine.api.IDatabaseEngine` Interface

This interface defines the specific contract for database engines in SEMOSS. Key methods include:

*   `execQuery(String query)`: Executes a native query string (e.g., SQL, SPARQL, Cypher) directly against the database. It typically returns an `IRawSelectWrapper` for iterating through the results.
*   `insertData(String query)` / `removeData(String query)`: Executes data modification language (DML) statements like INSERT, UPDATE, DELETE.
*   `commit()`: Commits the current transaction if the underlying database supports transactional operations and is not in auto-commit mode.
*   `getDatabaseType()`: Returns a `IDatabaseEngine.DATABASE_TYPE` enum (e.g., `RDBMS`, `JENA`, `NEO4J`, `TINKER`) which identifies the specific kind of database technology.
*   `getOWLEngineFactory()`: Provides access to the `OWLEngineFactory`. This factory manages the engine's semantic metadata, usually stored in an OWL (Web Ontology Language) file. This metadata layer allows for conceptual querying and data interpretation beyond the physical schema. This is crucial for engines that are "explorable" and have a semantic layer.
*   `query(SelectQueryStruct qs)` (inherited from `prerna.engine.api.IExplorable`): Executes a SEMOSS `SelectQueryStruct` object. This requires the engine (or its helpers) to translate the abstract `QueryStruct` into the native query language of the database.
*   `getTables()`: Returns a list of table/collection/concept names available in the database.
*   `getTableMetadata(String tableName)`: Returns metadata (column names, types) for a specific table/concept.

### `prerna.engine.impl.AbstractDatabaseEngine` Class

This abstract class provides a common base for many database engine implementations, especially those that utilize an OWL file for semantic metadata.

*   **OWL Metadata Management**: It handles the loading, initialization, and management of an associated OWL file. This is typically done by an embedded `RDFFileSesameEngine` instance (stored in `baseDataEngine`) which is managed by an `OWLEngineFactory` (`owlEnginefactory`). The OWL file stores the semantic model: concepts, properties (attributes), relationships, and their mappings to the physical database schema.
*   **SMSS Configuration**: It's responsible for loading database connection details and the OWL file location from the engine's `.smss` properties file. It also handles decryption of sensitive properties (like passwords) if they are encrypted in the SMSS file.
*   **Connection Management**: While `AbstractDatabaseEngine` sets up the framework, concrete implementations are responsible for managing the actual database connections (e.g., JDBC connection pools, graph database sessions).
*   **Query Interpretation Support**: It doesn't directly execute `SelectQueryStruct`s but provides the infrastructure. Concrete engines (like `RDBMSNativeEngine`) will host or use specific query interpreters (e.g., `prerna.query.interpreters.sql.SqlInterpreter`) to translate `QueryStruct` objects into native database queries.
*   **Basic Mode**: Supports a "basic" mode where an OWL file might not be strictly required, allowing for direct interaction with the database without a full semantic layer.

### Extending for a New Database

When developing a new `DATABASE` engine:

1.  **Implement `IDatabaseEngine`**: And likely `IExplorable`.
2.  **Extend `AbstractDatabaseEngine`**: This is recommended if your engine will use an OWL file for semantic metadata. If not, you might extend a more generic base or implement `IEngine` directly, but you'll need to handle metadata and `IExplorable` methods yourself.
3.  **Connection**: Implement logic to establish and manage connections to the target database using its specific drivers or client libraries.
4.  **Query Execution**:
    *   Implement `execQuery(String query)` to run native queries.
    *   Implement `query(SelectQueryStruct qs)` by developing or using a `prerna.query.interpreters.IQueryInterpreter` that can translate the `SelectQueryStruct` into the database's native query language. For RDBMS, you might extend `prerna.engine.impl.rdbms.RDBMSNativeEngine` and primarily provide a new `prerna.util.sql.AbstractSqlQueryUtil` for dialect-specific SQL generation.
5.  **Data Modification**: Implement `insertData()` and `removeData()`.
6.  **Schema Discovery**: Implement `getTables()` and `getTableMetadata()`.
7.  **Transaction Management**: Ensure proper handling of database transactions and connection pooling if applicable.
8.  **SMSS Properties**: Define all necessary connection parameters (URL, credentials, special flags) that will be stored in the `.smss` file for your engine.
9.  **OWL Integration (if using `AbstractDatabaseEngine`)**: Ensure your engine can work with the metadata provided by the OWL file, especially for mapping conceptual queries to physical table/column names.

### In-Depth: The `SelectQueryStruct` (SQL-focused)

The `prerna.query.querystruct.SelectQueryStruct` (often referred to as QS) is a pivotal Java object in SEMOSS used to programmatically define queries in an abstract, database-agnostic way. While it can be adapted for various query languages, its structure is particularly well-suited for generating SQL queries when interacting with `RDBMSNativeEngine` and other SQL-based database engines or frames (like `H2Frame`).

The `SelectQueryStruct` allows developers to specify what data to select, how to filter it, how to join different data sources (concepts/tables), and how to order or group the results, without writing raw SQL initially. An engine-specific interpreter (e.g., `prerna.query.interpreters.sql.SqlInterpreter`) then translates this `SelectQueryStruct` object into a native SQL query for execution.

**Key Components and Usage (Java Examples):**

*   **Instantiation**:
    ```java
    SelectQueryStruct qs = new SelectQueryStruct();
    ```

*   **Setting Context (Optional but common)**:
    *   Associate with an engine (for metadata resolution if using conceptual names):
        ```java
        // IEngine engine = Utility.getEngine("myRdbmsEngineId");
        // qs.setEngine(engine);
        ```
    *   Associate with a frame (if querying an in-memory frame):
        ```java
        // ITableDataFrame dataFrame = insight.getCurFrame();
        // qs.setFrame(dataFrame);
        ```
    *   Set Query Structure Type:
        ```java
        // qs.setQsType(AbstractQueryStruct.QUERY_STRUCT_TYPE.ENGINE); // For querying an external engine
        // qs.setQsType(AbstractQueryStruct.QUERY_STRUCT_TYPE.FRAME);  // For querying an in-memory frame
        ```

*   **Selectors (`IQuerySelector`)**: Define the columns or expressions to retrieve.
    *   **Column Selectors (`QueryColumnSelector`)**:
        *   Physical Naming (Table__Column):
            ```java
            qs.addSelector(new QueryColumnSelector("Products__ProductName"));
            qs.addSelector(new QueryColumnSelector("Sales__TransactionDate"));
            ```
        *   Using Alias (Conceptual Naming - requires OWL metadata for resolution):
            ```java
            qs.addSelector(new QueryColumnSelector("Product")); // Assumes 'Product' is a concept with a defined physical mapping
            qs.addSelector(new QueryColumnSelector("OrderDate"));
            ```
        *   If a concept has multiple properties and you want the "primary key" or main identifier:
            ```java
            // qs.addSelector(new QueryColumnSelector("ProductTable__" + SelectQueryStruct.PRIM_KEY_PLACEHOLDER));
            // More commonly, if 'ProductTable' is a known concept from OWL:
            // qs.addSelector(new QueryColumnSelector("ProductTable")); // This implies selecting the primary display value or identifier
            ```
    *   **Function Selectors (`QueryFunctionSelector`)**: For aggregations or functions.
        ```java
        // COUNT(Orders__OrderID) AS TotalOrders
        QueryFunctionSelector countSelector = new QueryFunctionSelector();
        countSelector.setFunction(QueryFunctionHelper.COUNT);
        countSelector.addInnerSelector(new QueryColumnSelector("Orders__OrderID"));
        countSelector.setAlias("TotalOrders");
        qs.addSelector(countSelector);

        // SUM(Sales__Amount) AS TotalSales
        QueryFunctionSelector sumSelector = new QueryFunctionSelector();
        sumSelector.setFunction(QueryFunctionHelper.SUM);
        sumSelector.addInnerSelector(new QueryColumnSelector("Sales__Amount"));
        sumSelector.setAlias("TotalSales");
        qs.addSelector(sumSelector);
        ```
    *   **Distinct**:
        ```java
        qs.setDistinct(true);
        ```

*   **Filters (`IQueryFilter`)**: Added to `explicitFilters`.
    *   **Simple Filters (`SimpleQueryFilter`)**:
        *   Column to Literal Value:
            ```java
            qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("Products__Category", "==", "Electronics"));
            qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("Sales__Quantity", ">", 10));
            qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("Customers__RegistrationDate", ">=", new SemossDate("2023-01-01", "yyyy-MM-dd")));
            ```
        *   Column to Column:
            ```java
            qs.addExplicitFilter(SimpleQueryFilter.makeColToColFilter("Orders__OrderDate", ">=", "Orders__ShipDate"));
            ```
        *   Using a list for `IN` or `NOT IN` (comparator `==` for IN, `!=` for NOT IN):
            ```java
            List<String> categories = Arrays.asList("Electronics", "Appliances");
            qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("Products__Category", "==", categories));
            ```
    *   **Compound Filters (`AndQueryFilter`, `OrQueryFilter`)**:
        ```java
        AndQueryFilter andFilter = new AndQueryFilter();
        andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("Products__Category", "==", "Electronics"));
        andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("Products__Price", "<", 500.00));
        qs.addExplicitFilter(andFilter);

        OrQueryFilter orFilter = new OrQueryFilter();
        orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("Region__Name", "==", "North"));
        orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("Region__Name", "==", "South"));
        // Example of nesting:
        // andFilter.addFilter(orFilter);
        // qs.addExplicitFilter(andFilter);
        qs.addExplicitFilter(orFilter); // Or add directly
        ```

*   **Joins / Relations (`IRelation`)**: Defines how different concepts or tables are connected.
    *   Using Conceptual Names (requires OWL metadata defining the relationship):
        ```java
        qs.addRelation("Orders", "Products", "INNER.JOIN"); // Assumes a defined relationship in OWL
        ```
    *   For more explicit control or when not using full OWL-driven joins, you might need to ensure underlying physical foreign key/primary key relationships are correctly defined in the OWL if you are using conceptual names for tables and columns. The actual ON clause is typically generated by the SQL interpreter based on OWL metadata linking concepts.
    *   The `RelationSet relationsSet` within `SelectQueryStruct` stores these.

*   **Group By (`IQuerySelector`)**:
    ```java
    qs.addGroupBy(new QueryColumnSelector("Products__Category"));
    // Typically used with aggregate function selectors like COUNT, SUM.
    ```

*   **Order By (`IQuerySort`)**:
    ```java
    qs.addOrderBy(new QueryColumnOrderBySelector("Sales__TransactionDate", "DESC"));
    qs.addOrderBy(new QueryColumnOrderBySelector("Products__ProductName", "ASC"));
    ```

*   **Limit and Offset**:
    ```java
    qs.setLimit(100);
    qs.setOffset(50);
    ```

*   **Translation to SQL**:
    Once the `SelectQueryStruct` is populated, it's passed to an SQL interpreter:
    ```java
    // Assuming 'qs' is your populated SelectQueryStruct
    // Assuming 'engine' is an instance of RDBMSNativeEngine or similar
    // prerna.util.sql.AbstractSqlQueryUtil queryUtil = engine.getQueryUtil();
    // prerna.query.interpreters.sql.SqlInterpreter sqlInterpreter = new prerna.query.interpreters.sql.SqlInterpreter(qs, queryUtil);
    // String generatedSql = sqlInterpreter.composeQuery();
    // System.out.println(generatedSql);
    ```
    The `SqlInterpreter` uses the engine's `AbstractSqlQueryUtil` to handle database-specific syntax for table and column quoting, function names, and other dialectical differences. If conceptual names were used in the QS, the interpreter relies on the engine's OWL metadata to resolve these to physical table and column names and to determine join conditions.

The `SelectQueryStruct` provides a structured and flexible way to define complex data retrieval operations that can then be translated and executed across different SQL-compliant database engines.

## Example Implementations

### `prerna.engine.impl.rdbms.RDBMSNativeEngine`
*   **Purpose**: Provides connectivity to a wide range of relational databases (e.g., PostgreSQL, MySQL, Oracle, SQL Server, H2) via JDBC.
*   **Implementation Highlights**:
    *   Uses a specific `prerna.util.sql.AbstractSqlQueryUtil` subclass (e.g., `H2QueryUtil`, `PostgresQueryUtil`) based on the database type to generate dialect-specific SQL from `SelectQueryStruct`s.
    *   Manages JDBC connections, often using connection pooling for efficiency.
    *   Relies heavily on the associated OWL metadata for mapping conceptual model elements (concepts, properties) to physical tables and columns when translating `SelectQueryStruct`s.
*   **SMSS Configuration**: Typically includes `DB_DRIVER_CLASS_NAME`, `DB_URL`, `DB_USERNAME`, `DB_PASSWORD`, `FETCH_SIZE`, `DATABASE_ZONE_ID`.

### `prerna.engine.impl.rdf.InMemoryJenaEngine` / `RDFFileJenaEngine`
*   **Purpose**: Manages RDF data using the Apache Jena framework, either entirely in-memory or backed by physical RDF files (e.g., TTL, RDF/XML).
*   **Implementation Highlights**:
    *   Uses the Jena API for executing SPARQL queries (both `SELECT` and `CONSTRUCT`) and for manipulating the RDF graph (adding/removing triples).
    *   For these engines, the OWL file itself often *is* the primary data store, not just metadata.
*   **SMSS Configuration**: For `RDFFileJenaEngine`, it includes the `OWL` property pointing to the RDF file path. Jena-specific settings might also be included.

### `prerna.engine.impl.neo4j.Neo4jEngine`
*   **Purpose**: Connects to Neo4j graph databases.
*   **Implementation Highlights**:
    *   Uses the Neo4j Java driver (Bolt protocol) to execute Cypher queries.
    *   Translates `SelectQueryStruct` elements (or parts of them, often focusing on graph traversal patterns) into Cypher.
    *   Manages sessions and transactions with the Neo4j server.
*   **SMSS Configuration**: Neo4j Bolt URI (e.g., `bolt://localhost:7687`), username, password.

### `prerna.engine.impl.tinker.JanusEngine`
*   **Purpose**: Interfaces with JanusGraph, a distributed graph database, which uses the TinkerPop graph computing framework.
*   **Implementation Highlights**:
    *   Translates `SelectQueryStruct` graph patterns into Gremlin queries.
    *   Interacts with JanusGraph using its TinkerPop API, typically by submitting Gremlin traversals.
*   **SMSS Configuration**: JanusGraph connection properties, which can be complex and specify the storage backend (e.g., Cassandra, HBase), indexing backend (e.g., Elasticsearch), and graph name. Often points to a JanusGraph properties file.
```

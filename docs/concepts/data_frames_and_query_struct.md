# DataFrames and QueryStructs in SEMOSS

SEMOSS uses internal data structures to represent and manipulate data within the Java backend, primarily through the `ITableDataFrame` interface and the `SelectQueryStruct` class for defining queries.

## The `ITableDataFrame` Interface and Implementations

The `prerna.algorithm.api.ITableDataFrame` interface is the primary abstraction for in-memory tabular and graph-like data structures. Concrete implementations handle data storage and querying for different backends.

*   **`prerna.ds.shared.AbstractTableDataFrame`**: A base class providing common functionalities:
    *   **Metadata (`OwlTemporalEngineMeta metaData`)**: Describes the frame's structure (headers, types, relationships).
    *   **Filtering (`GenRowFilters grf`)**: Manages filters applied to the frame.
    *   **Querying**: Defines a `query(SelectQueryStruct qs)` method for querying the frame's data.
    *   **Caching**: Caches metrics like column uniqueness.
    *   **Persistence**: Methods for saving/loading frame metadata and state.

*   **Key Implementations (found in `src/prerna/ds/`)**:
    *   **`TinkerFrame.java`**: For graph data, using Apache TinkerPop's `TinkerGraph`. Executes Gremlin queries translated from `SelectQueryStruct`.
    *   **`H2Frame.java`**: For tabular data backed by an in-memory H2 database. Executes SQL queries translated from `SelectQueryStruct`.
    *   **`NativeFrame.java`**: A pure Java in-memory table, suitable for smaller datasets.
    *   **`PandasFrame.java`**: Wraps a Pandas DataFrame in a Python environment, delegating operations.
    *   **`RDataTable.java`**: Wraps an R `data.table` or `data.frame`.
    *   **`SparkDataFrame.java`**: Represents a Spark DataFrame for distributed data.

## The `SelectQueryStruct` (Conceptual Overview)

The `prerna.query.querystruct.SelectQueryStruct` (QS) is a Java object that represents a query in an abstract, database-agnostic manner. It allows SEMOSS to define data retrieval and manipulation operations (select, filter, join, group by, order by) programmatically before they are translated into the native language of a target data store (which could be an external `IEngine` or an in-memory `ITableDataFrame`).

*   **Purpose**: To provide a common structure for defining queries that can be executed against various backends.
*   **Key Components**: Selectors, filters, joins, groupings, orderings, limit/offset.
*   **Interaction**:
    *   Reactors often build or modify `SelectQueryStruct` objects based on Pixel commands.
    *   These QS objects are then passed to an `IEngine` or an `ITableDataFrame`.
    *   Engine-specific interpreters (e.g., `SqlInterpreter`, `GremlinInterpreter`) translate the QS into executable queries (SQL, Gremlin, etc.).

(For a detailed guide on constructing and using `SelectQueryStruct` with Java examples, particularly for SQL databases, see `docs/engines/database_engines.md#in-depth-the-selectquerystruct-sql-focused)`).

## Interaction between Engines and DataFrames

1.  **Loading Data**: Data is typically fetched from an external `IEngine` using a `SelectQueryStruct`. The engine executes this (e.g., as SQL).
2.  **Frame Population**: The results are then loaded into an appropriate `ITableDataFrame` implementation (e.g., `H2Frame`).
3.  **In-Memory Operations**: Subsequent Pixel operations might query or modify this in-memory frame directly using its `query(SelectQueryStruct qs)` method or other `ITableDataFrame` APIs.

This layered approach allows SEMOSS to abstract data sources and provide a consistent way to work with data, whether it's remote or held in local memory structures.

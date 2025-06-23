# GAAS Database Interaction (`gaas_gpt_database.py`)

The `py/gaas_gpt_database.py` module provides the `DatabaseEngine` class, which serves as a Python proxy for interacting with SEMOSS database engines. This allows Python-based Generative AI Agent Services (GAAS) to execute queries and perform data operations on databases managed by the SEMOSS backend.

## `DatabaseEngine` Class

*   **Purpose**: The `DatabaseEngine` class enables Python code to interact with a specific SEMOSS database engine. It abstracts the communication details (via `ServerProxy`) needed to send commands to the Java backend where the actual database interaction occurs. It is primarily focused on *executing* existing queries rather than generating them using an LLM.
*   **Inheritance**: It extends `gaas_server_proxy.ServerProxy`, which handles the low-level communication (likely TCP-based) with the SEMOSS Java backend.

### Initialization

The constructor `__init__(self, engine_id=None, insight_id=None)`:
*   `engine_id` (str): **Required**. The ID of the target SEMOSS database engine to interact with.
*   `insight_id` (Optional[str]): The ID of the current insight. This can be used for context, such as resolving insight-specific variables or isolating operations within a temporal workspace if the backend supports it.
*   The constructor asserts that `engine_id` is provided and prints an initialization message.

### Key Methods and Functionality

*   **`execQuery(self, query=None, insight_id=None, return_pandas=True)`**:
    *   **Purpose**: Executes a read-only query (e.g., SQL SELECT, SPARQL SELECT) against the specified database engine.
    *   **Inputs**:
        *   `query` (str): **Required**. The native query string to execute.
        *   `insight_id` (Optional[str]): Overrides the instance's `insight_id` if provided.
        *   `return_pandas` (bool, default: `True`): If `True`, attempts to parse the JSON result file into a Pandas DataFrame. If `False`, returns the raw content of the result file (likely a JSON string).
    *   **Core Logic**:
        1.  Generates a unique `epoc` ID for the transaction.
        2.  Calls `super().callEngine(...)` to send a request to the Java backend. This method likely marshals the arguments and targets a Java method responsible for database engine query execution.
            *   `engine_type="database"`
            *   `engine_id=self.engine_id`
            *   `method_name="execQuery"`
            *   `method_args=[query]`
            *   `method_arg_types=["java.lang.String"]`
        3.  The Java backend executes the query and is expected to write the results to a temporary file, returning the path (`fileLoc`) to this file.
        4.  If `return_pandas` is true, it reads the JSON data from `fileLoc` into a Pandas DataFrame using `pd.read_json()`.
        5.  Otherwise, it reads and returns the raw content of the file.
        6.  **Crucially, it always attempts to delete the temporary result file (`fileLoc`) after processing.**
    *   **Outputs**: A Pandas DataFrame if `return_pandas` is `True` and successful, otherwise a string (JSON) or potentially raises an error if file operations fail.

*   **`runQuery(self, query=None, insight_id=None, commit: bool = True)`**:
    *   **Purpose**: Executes a query that might modify the database (e.g., INSERT, UPDATE, DELETE, DDL). This method uses a Pixel-based approach for execution.
    *   **Inputs**:
        *   `query` (str): **Required**. The native query string.
        *   `insight_id` (Optional[str]): Overrides the instance's `insight_id`.
        *   `commit` (bool, default: `True`): Controls whether the operation should be committed. This is translated into the `commit` parameter of the `ExecQuery` Pixel command.
    *   **Core Logic**:
        1.  Generates a unique `epoc` ID.
        2.  Constructs a Pixel script string: `Database("<engine_id>")|Query("<encode><query_string></encode>")|ExecQuery(commit=<commit_str>);`
            *   The `<query_string>` is XML-encoded.
        3.  Calls `super().callReactor(...)` to send this Pixel script to the Java backend for execution.
    *   **Outputs**: Returns the output from the Pixel execution, which is typically a boolean indicating success/failure or specific results from the `ExecQuery` reactor.

*   **`insertData(self, query=None, insight_id=None, commit: bool = True)`**:
    *   A convenience method that simply calls `self.runQuery(query, insight_id, commit)`. Intended for INSERT operations.
*   **`updateData(self, query=None, insight_id=None, commit: bool = True)`**:
    *   A convenience method that calls `self.runQuery(query, insight_id, commit)`. Intended for UPDATE operations.
*   **`removeData(self, query=None, insight_id=None, commit: bool = True)`**:
    *   A convenience method that calls `self.runQuery(query, insight_id, commit)`. Intended for DELETE operations.

*   **`to_langchain_database(self)`**:
    *   **Purpose**: Transforms the `DatabaseEngine` instance into a Langchain `BaseRetriever`-compatible object. This allows the SEMOSS database engine to be seamlessly integrated into Langchain workflows that expect a database retriever.
    *   **Core Logic**:
        1.  Defines an inner class `SemossLangchainDatabase` that inherits from `langchain_core.retrievers.BaseRetriever`.
        2.  The inner class constructor takes the `DatabaseEngine` instance.
        3.  It implements methods like `executeQuery`, `insertQuery`, `updateQuery`, `removeQuery` that directly call the corresponding methods of the outer `DatabaseEngine` instance.
        4.  The `_get_relevant_documents()` method (required by `BaseRetriever`) is stubbed to return "SQL Operations".
    *   **Outputs**: An instance of `SemossLangchainDatabase`.

### Interaction with SEMOSS Engines

The `DatabaseEngine` class acts as a Python client to a specific SEMOSS `IDatabaseEngine` (or a descendant like `RDBMSNativeEngine`) running on the Java backend. It does not directly connect to the database but rather delegates all operations to the SEMOSS backend via the `ServerProxy` communication mechanism.

### Query Generation

This specific module **does not perform query generation using LLMs**. Its primary role is the *execution* of queries that are already formulated. Query generation would typically be handled by a different tool or component that might utilize the `genai_client` package and then pass the generated query to an instance of this `DatabaseEngine` for execution.

### Error Handling

*   The methods include `assert` statements for required parameters like `engine_id` and `query`.
*   File operations in `execQuery` are within a `try...finally` block to ensure temporary file cleanup.
*   Other errors related to backend communication or database execution would likely be propagated from the `ServerProxy` or the SEMOSS backend.

### Example Usage (Conceptual)

```python
# Assuming gaas_server_proxy is configured and SEMOSS backend is running

# Initialize the database engine client for a specific SEMOSS engine
db_engine_id = "my_postgres_db" # ID of a configured database engine in SEMOSS
insight_id_context = "some_active_insight_id"
db_tool = DatabaseEngine(engine_id=db_engine_id, insight_id=insight_id_context)

# Execute a SELECT query and get results as a Pandas DataFrame
try:
    select_query = "SELECT ProductName, Price FROM Products WHERE Category = 'Electronics'"
    df_results = db_tool.execQuery(query=select_query)
    if df_results is not None:
        print(df_results.head())
except Exception as e:
    print(f"Error executing SELECT query: {e}")

# Run an INSERT query (which might modify data)
try:
    insert_query = "INSERT INTO Logs (Timestamp, Message) VALUES (NOW(), 'GAAS tool accessed database')"
    success = db_tool.insertData(query=insert_query, commit=True)
    if success:
        print("INSERT operation successful.")
    else:
        print("INSERT operation failed or returned no specific success status.")
except Exception as e:
    print(f"Error executing INSERT query: {e}")

# Use with Langchain (conceptual)
# lc_db = db_tool.to_langchain_database()
# documents = lc_db.get_relevant_documents(query="Find all products in Electronics category")
# (Note: _get_relevant_documents is a stub, so this specific Langchain usage might need more.)
# query_result_lc = lc_db.executeQuery("SELECT * FROM Products LIMIT 5")

```
This `DatabaseEngine` class provides the necessary Python interface for GAAS components to execute queries and data manipulation commands on SEMOSS-managed databases.

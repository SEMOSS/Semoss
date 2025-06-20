# Pixel: The SEMOSS Query Language and its Interaction with Reactors

"Pixel" is the custom query and scripting language of SEMOSS. It is a powerful, imperative, and dataflow-oriented language designed to allow users and developers to express a wide array of operations within the SEMOSS ecosystem. These operations range from data querying and transformation, to workflow automation, interaction with various engines (databases, models, functions), and influencing UI components.

The `src/prerna/sablecc2/` package and its sub-packages are responsible for parsing, translating, and executing these Pixel scripts. The name `sablecc2` suggests that the SableCC parser generator tool was used to create the underlying parsing infrastructure, which is evident from the presence of files like `lexer.dat`, `parser.dat`, and the typical structure of sub-packages (`lexer/`, `parser/`, `node/`, `analysis/`).

## Core Purposes of Pixel

*   **Data Access and Manipulation**: Querying data from various engines (databases, local frames, etc.), filtering, joining, aggregating, and transforming data.
*   **Workflow Orchestration**: Chaining multiple operations together to create complex data processing pipelines or analytical workflows.
*   **Engine Interaction**: Sending commands to and receiving data from different types of `IEngine` implementations (databases, AI models, storage, functions).
*   **Variable Management**: Creating and using variables to store intermediate results, parameters, or references to data frames.
*   **UI Interaction**: Triggering updates to UI components, such as panels and visualizations, by sending specific data or commands.
*   **Extensibility**: Providing a common language to invoke modular units of logic called "Reactors".

## Pixel Syntax Basics

Pixel scripts consist of one or more statements, typically separated by semicolons. Each statement usually involves a Reactor invocation or a variable assignment.

*   **Reactor Invocation**:
    *   The most common operation is calling a Reactor. Reactors are named functions that perform specific tasks.
    *   Syntax: `ReactorName(paramKey1=[value1], paramKey2=[value2], ...);`
    *   Example: `RunQuery(engine=["myPostgresDb"], query=["SELECT * FROM my_table"]);`
    *   Parameters are passed as key-value pairs, where the value is often a list (even for single values).

*   **Variable Assignment**:
    *   Results from Reactor calls or literal values can be assigned to variables.
    *   Syntax: `variableName = ... ;`
    *   Variables are stored in the current `Insight`'s `VarStore`.
    *   Examples:
        ```pixel
        myAge = 25;
        greeting = "Hello, SEMOSS!";
        myNumbers = [1, 2, 3, 4, 5];
        countryCapitals = {"USA":"Washington D.C.", "Canada":"Ottawa"};
        customerFrame = Database(id=["salesDb"]) | Select(columns=["CustomerID", "Name", "Region"]);
        ```

*   **Chaining Operations (Piping)**:
    *   The pipe operator `|` is used to chain operations, where the output of one operation becomes an implicit input to the next. This is fundamental to Pixel's dataflow nature.
    *   The output of the preceding operation is often implicitly passed as the primary input (or context) to the subsequent Reactor.
    *   Example:
        ```pixel
        myFrame = Database(id=["salesDb"]) | Select(columns=["OrderDate", "Product", "SalesAmount"]);
        filteredFrame = Frame("myFrame") | Filter(column=["Product"], comparator=["=="], value=["Laptop"]);
        groupedSales = Frame("filteredFrame") | GroupBy(columns=["Product"], aggregations=[{"SalesAmount":"SUM"}]);
        ```

*   **Referencing Variables**:
    *   Variables can be referenced in subsequent operations using the `$` prefix.
    *   Example:
        ```pixel
        productName = "Laptop";
        filteredFrame = Frame("myFrame") | Filter(column=["Product"], comparator=["=="], value=[$productName]);
        ```

*   **Comments**:
    *   Pixel supports comments using `//` for single-line comments or `/* ... */` for multi-line comments.
    *   Example:
        ```pixel
        // This is a single-line comment
        /*
         * This is a
         * multi-line comment.
         */
        myVar = 10; // Assign 10 to myVar
        ```

## Data Types in Pixel (Literals)

When providing literal values in Pixel scripts:

*   **Strings**: Enclosed in double quotes (e.g., `"Hello World"`).
*   **Numbers**: Integers (e.g., `123`) or decimals (e.g., `45.67`).
*   **Booleans**: `true` or `false`.
*   **Lists**: Comma-separated values enclosed in square brackets (e.g., `[1, "apple", true]`).
*   **Maps (Dictionaries)**: Key-value pairs enclosed in curly braces, with keys typically being strings (e.g., `{"name":"John Doe", "age":30}`).

These literals, when passed as parameters to Reactors, are wrapped into `NounMetadata` objects.

## Control Flow

Pixel itself doesn't have extensive built-in control flow structures like loops (`for`, `while`) or complex branching in its core syntax in the same way a general-purpose programming language might. However, control flow is primarily achieved through:

*   **Conditional Reactors**: Reactors like `IfReactor` allow for conditional execution of other Pixel commands or assignment of values.
    *   Example: `myValue = If(condition=[$someVar > 10], then=["High"], else=["Low"]);`
*   **Workflow Orchestration via Chaining**: The sequential nature of Pixel scripts and the piping mechanism inherently define a flow of execution.
*   **Java-Implemented Logic**: More complex looping or conditional logic is often handled within the Java code of a specific Reactor implementation.

## Execution Model (Simplified Overview)

1.  **Parsing**: The Pixel script string is parsed by `PixelRunner` using the SableCC-generated lexer and parser into an Abstract Syntax Tree (AST).
2.  **Translation & Planning**: `PixelRunner` uses `GreedyTranslation` which, in conjunction with `PixelPlanner`, traverses the AST.
3.  **Reactor Invocation**: Operations in the Pixel script (like `Database(...)`, `Select(...)`, assignments) are typically mapped to specific `IReactor` implementations. `ReactorFactory` might be involved in instantiating these reactors.
4.  **Execution Context (`Insight`)**: All Pixel execution happens within the context of an `Insight` object, which holds the `VarStore` for variables and the `PixelList` for tracking executed operations.
5.  **Result Handling**: Reactors return `NounMetadata`, which can be stored in variables or passed to other reactors.

This structure makes Pixel a flexible language for both interactive data exploration and for defining reusable, stored analytical recipes within SEMOSS.

## Pixel and Reactor Interaction

The Pixel language serves as a user-friendly and scriptable interface to the underlying Reactor framework, which provides the actual logic for most operations in SEMOSS. Understanding their relationship is key to understanding SEMOSS's execution model.

*   **Mapping Pixels to Reactors**:
    *   Most non-trivial operations expressed in Pixel (e.g., `Database(...)`, `Select(...)`, `Filter(...)`, `RunLlm(...)`) are designed to map directly or indirectly to a corresponding Java `IReactor` implementation.
    *   The name of the function used in Pixel (e.g., `Database`, `Select`) often corresponds to a specific Reactor class (e.g., `DatabaseReactor`, `SelectReactor`). The `prerna.reactor.ReactorFactory` class is typically responsible for resolving a Pixel function name to its Java Reactor class.
    *   For example, a Pixel command like `MyFrame = Database(id=["myEngine"]) | Select(columns=["Name", "Age"]);` involves at least two reactors: one for connecting to the database (`DatabaseReactor`) and one for selecting columns (`SelectReactor`).

*   **Role of `PixelRunner` and `PixelPlanner`**:
    *   When a Pixel script is executed via `PixelRunner.runPixel(...)`:
        1.  The script is parsed into an Abstract Syntax Tree (AST).
        2.  The `GreedyTranslation` visitor traverses this AST.
        3.  As `GreedyTranslation` encounters nodes in the AST that represent Reactor calls (like `Database(...)` or `Select(...)`), it instantiates the appropriate `IReactor` objects (often using `ReactorFactory`).
        4.  It populates the Reactor's `NounStore` with the parameters provided in the Pixel script.
        5.  The `PixelPlanner` (which is managed by `GreedyTranslation`) plays a crucial role in orchestrating the execution of these Reactors. It:
            *   Manages the overall execution plan, including the sequence of Reactor calls.
            *   Handles the data flow between chained Reactors (the `|` pipe operator). The output (`NounMetadata`) of one Reactor in a chain often becomes an implicit input (the "current row" or context, accessible via `getCurRow()` in `AbstractReactor`) for the next Reactor.
            *   Manages the `VarStore` (part of the `Insight`) for storing and retrieving variables (e.g., if a Reactor's output is assigned to a variable like `myFrame = ...`).
            *   May optimize the sequence of operations or combine steps where possible (e.g., merging multiple filter operations or pushing operations down to a database engine).

*   **Chaining and Data Flow**:
    *   The pipe operator `|` in Pixel is a powerful construct that `PixelPlanner` translates into a data flow pipeline between Reactors.
    *   Example: `Frame("inputFrame") | Filter(...) | GroupBy(...);`
        1.  `FrameReactor("inputFrame")` is executed, likely loading a frame into context (perhaps making it the `curRow` or a known variable in `VarStore`).
        2.  `FilterReactor` is executed next. It implicitly operates on the output/context of `FrameReactor`. Its parameters (filter conditions) are taken from the Pixel call. It produces a filtered frame or modifies the context.
        3.  `GroupByReactor` then operates on the output/context of `FilterReactor`.
    *   The `PixelPlanner` ensures that the output `NounMetadata` from one reactor is correctly made available as input to the next, either by setting it as the current data context or by resolving variable names specified as inputs in the Pixel parameters.

In essence, Pixel provides the syntax for defining workflows and operations, while Reactors provide the modular, executable logic. The `PixelRunner` and `PixelPlanner` act as the bridge, translating the user's Pixel script into a planned sequence of Reactor executions and managing the data flow between them within the context of an `Insight`.

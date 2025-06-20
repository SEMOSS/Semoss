# SEMOSS Java Backend Deep Dive

This document provides a more detailed look into the core Java components of the SEMOSS platform.

## 1. Pixel: The SEMOSS Query Language & Execution Engine (`src/prerna/sablecc2`)

"Pixel" is the custom query and scripting language of SEMOSS. It is a powerful, imperative, and dataflow-oriented language designed to allow users and developers to express a wide array of operations within the SEMOSS ecosystem. These operations range from data querying and transformation, to workflow automation, interaction with various engines (databases, models, functions), and influencing UI components.

The `src/prerna/sablecc2/` package and its sub-packages are responsible for parsing, translating, and executing these Pixel scripts. The name `sablecc2` suggests that the SableCC parser generator tool was used to create the underlying parsing infrastructure, which is evident from the presence of files like `lexer.dat`, `parser.dat`, and the typical structure of sub-packages (`lexer/`, `parser/`, `node/`, `analysis/`).

### 1.1. Core Purposes of Pixel

*   **Data Access and Manipulation**: Querying data from various engines (databases, local frames, etc.), filtering, joining, aggregating, and transforming data.
*   **Workflow Orchestration**: Chaining multiple operations together to create complex data processing pipelines or analytical workflows.
*   **Engine Interaction**: Sending commands to and receiving data from different types of `IEngine` implementations (databases, AI models, storage, functions).
*   **Variable Management**: Creating and using variables to store intermediate results, parameters, or references to data frames.
*   **UI Interaction**: Triggering updates to UI components, such as panels and visualizations, by sending specific data or commands.
*   **Extensibility**: Providing a common language to invoke modular units of logic called "Reactors".

### 1.2. Pixel Syntax Basics

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
        *   `myAge = 25;`
        *   `greeting = "Hello, SEMOSS!";`
        *   `myNumbers = [1, 2, 3, 4, 5];`
        *   `countryCapitals = {"USA":"Washington D.C.", "Canada":"Ottawa"};`
        *   `customerFrame = Database(id=["salesDb"]) | Select(columns=["CustomerID", "Name", "Region"]);` (Assigns the result of a data query to `customerFrame`)

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
    *   Pixel supports comments using `//` for single-line comments or `/* ... */` for multi-line comments (standard Java-like comments).
    *   Example:
        ```pixel
        // This is a single-line comment
        /*
         * This is a
         * multi-line comment.
         */
        myVar = 10; // Assign 10 to myVar
        ```

### 1.3. Data Types in Pixel (Literals)

When providing literal values in Pixel scripts:

*   **Strings**: Enclosed in double quotes (e.g., `"Hello World"`).
*   **Numbers**: Integers (e.g., `123`) or decimals (e.g., `45.67`).
*   **Booleans**: `true` or `false`.
*   **Lists**: Comma-separated values enclosed in square brackets (e.g., `[1, "apple", true]`).
*   **Maps (Dictionaries)**: Key-value pairs enclosed in curly braces, with keys typically being strings (e.g., `{"name":"John Doe", "age":30}`).

These literals, when passed as parameters to Reactors, are wrapped into `NounMetadata` objects.

### 1.4. Control Flow

Pixel itself doesn't have extensive built-in control flow structures like loops (`for`, `while`) or complex branching in its core syntax in the same way a general-purpose programming language might. However, control flow is primarily achieved through:

*   **Conditional Reactors**: Reactors like `IfReactor` allow for conditional execution of other Pixel commands or assignment of values.
    *   Example: `myValue = If(condition=[$someVar > 10], then=[“High”], else=[“Low”]);`
*   **Workflow Orchestration via Chaining**: The sequential nature of Pixel scripts and the piping mechanism inherently define a flow of execution.
*   **Java-Implemented Logic**: More complex looping or conditional logic is often handled within the Java code of a specific Reactor implementation.

### 1.5. Execution Model (Simplified Overview)

1.  **Parsing**: The Pixel script string is parsed by `PixelRunner` using the SableCC-generated lexer and parser into an Abstract Syntax Tree (AST).
2.  **Translation & Planning**: `PixelRunner` uses `GreedyTranslation` which, in conjunction with `PixelPlanner`, traverses the AST.
3.  **Reactor Invocation**: Operations in the Pixel script (like `Database(...)`, `Select(...)`, assignments) are typically mapped to specific `IReactor` implementations. `ReactorFactory` might be involved in instantiating these reactors.
4.  **Execution Context (`Insight`)**: All Pixel execution happens within the context of an `Insight` object, which holds the `VarStore` for variables and the `PixelList` for tracking executed operations.
5.  **Result Handling**: Reactors return `NounMetadata`, which can be stored in variables or passed to other reactors.

This structure makes Pixel a flexible language for both interactive data exploration and for defining reusable, stored analytical recipes within SEMOSS.

### 1.6. Pixel and Reactor Interaction

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

## 2. Reactor Framework (`src/prerna/reactor`)

The Reactor framework is a cornerstone of SEMOSS's Java backend, providing a flexible and extensible way to define and execute specific operations or commands, often as part of a Pixel script. Each "reactor" is a Java class that encapsulates a particular piece of logic.

### 2.1. Core Reactor Interfaces/Classes

*   **`IReactor.java`**: This is the primary interface that all reactors must implement (typically by extending `AbstractReactor`). It defines the contract for reactor behavior, including methods for:
    *   Execution: `NounMetadata execute()` is the main method where the reactor performs its logic.
    *   Input/Output Definition: `getInputs()` and `getOutputs()` (though often managed by `AbstractReactor` conventions).
    *   Lifecycle and Chaining: `setParentReactor()`, `getChildReactors()`, `mergeUp()` for integrating into a larger execution flow.
    *   Interaction with Pixel Execution: `setPixelPlanner()`, `setNounStore()`.
    *   Metadata: `getName()`, `getSignature()`, `getHelp()`.

*   **`AbstractReactor.java`**: This abstract class provides a robust base implementation of `IReactor`. Most concrete reactors in SEMOSS extend `AbstractReactor`. Key functionalities it provides include:
    *   **Noun Management**:
        *   `NounStore store`: Each reactor instance has a `NounStore` to hold its input parameters (nouns).
        *   `keysToGet`: Concrete reactors define an array of strings (`keysToGet`) specifying the names of the input nouns they expect (e.g., `"value"`, `"column"`, `"expression"`). These often correspond to keys from `ReactorKeysEnum.java`.
        *   `organizeKeys()`: A crucial method called typically at the start of `execute()`. It populates the `store` (and a convenience map `keyValue`) from the actual inputs provided in the Pixel script, matching them against `keysToGet`.
        *   `curRow`: A `GenRowStruct` representing the current data row or context, especially when reactors are chained or process input streams.
    *   **Planner and Insight**: Access to the `PixelPlanner` and the current `Insight` object.
    *   **Signature and Naming**: Storing the reactor's operation name and Pixel signature.
    *   **Error Handling and Logging**: Utility methods for standardized error reporting and logging.
    *   **Input Retrieval**: Helper methods like `getNounAsStringList(String key)` to easily access input values from the `NounStore`.

### 2.2. Key Reactor Examples and Roles

Reactors are used for a vast array of tasks in SEMOSS. The behavior and integration of a reactor can vary:

*   **General Purpose Reactors**: These perform a distinct operation and their results are typically used by subsequent reactors or returned to the user.
    *   **Example: `EchoReactor.java`**
        *   **Purpose**: A simple reactor that returns its primary input.
        *   **Inputs**: Expects a single noun, typically specified by the key `ReactorKeysEnum.VALUE.getKey()` (e.g., `Pixel: Echo(value=["Hello World"]);`).
        *   **Execution**: Calls `organizeKeys()` to load its inputs. Retrieves the specified noun from its `NounStore` and returns it as a `NounMetadata` object.
        *   **Role**: Useful for debugging, simple assignments, or as a basic building block.

*   **Data Structure Providers / Configuration Reactors**: Some reactors don't perform a final action themselves but rather construct an object or configure a state that is then used by a parent or subsequent reactor in the execution chain.
    *   **Example: `FilterReactor.java`**
        *   **Purpose**: To define a filter condition based on a left operand (column), a comparator, and a right operand (value or another column).
        *   **Inputs**: Expects nouns named "LCOL", "COMPARATOR", and "RCOL".
        *   **Execution**: Its `execute()` method creates a `prerna.sablecc2.om.Filter` object using these inputs.
        *   **Integration**:
            *   It overrides `mergeUp()` to add the created `Filter` object (as `NounMetadata`) directly into its parent reactor's current processing row (`parentReactor.getCurRow().add(filterNoun)`).
            *   Its `getInputs()` method returns `null`, indicating it's not treated as a standalone step in the `PixelPlanner` but rather contributes to a consuming reactor (e.g., a data query reactor like `SelectReactor` would consume this `Filter` object).
        *   **Role**: Defines a filter that will be applied by another reactor that actually performs data retrieval or manipulation.

*   **Control Flow Reactors**:
    *   **Example: `IfReactor.java`**
        *   **Purpose**: Implements conditional logic (if-then-else).
        *   **Inputs**: Typically takes a condition, a then-expression (reactor or value), and an optional else-expression.
        *   **Execution**: Evaluates the condition. Based on the result, it then executes either the "then" part or the "else" part.
        *   **Role**: Allows for branching logic within Pixel scripts.

*   **Data Operation Reactors (e.g., in `src/prerna/reactor/frame/` or `src/prerna/reactor/qs/`)**:
    *   Many reactors are dedicated to specific data operations like selecting columns, joining data, grouping, pivoting, etc. These often interact heavily with the `ITableDataFrame` (via `PixelPlanner` or directly) or build up components of a query structure (`QueryStruct`). For example, a hypothetical `SelectReactor` would take column names as input and modify the current frame or query to only include those columns.

The Reactor pattern, combined with the Pixel language, gives SEMOSS a highly modular and powerful way to define and execute a wide variety of operations. Developers can add new functionality by creating new reactor classes.

### 2.3. Reactor Inputs and `NounStore`

Reactors are designed to be configurable and reusable components. Their inputs are dynamically provided at runtime, typically defined in a Pixel script. Understanding how these inputs are passed and processed is crucial.

*   **Defining Inputs in Pixel Scripts**:
    *   When invoking a Reactor in Pixel, inputs are passed as key-value pairs within the parentheses. The key is the parameter name expected by the Reactor, and the value is the data to be passed.
    *   Pixel syntax generally requires values to be enclosed in square brackets `[]`, even if it's a single item. This allows for consistent passing of single values or lists of values.
    *   **Examples of Passing Different Data Types**:
        ```pixel
        // Passing literal strings, numbers, booleans
        CreateFile(fileName=["myDocument.txt"], content=["Hello SEMOSS!"], overwrite=[true]);
        Calculate(operation=["ADD"], values=[10, 20, 30]);

        // Passing a list of strings
        SelectColumns(columns=["ProductID", "ProductName", "Price", "Category"]);

        // Referencing a previously defined variable (e.g., a frame or a value)
        productFilter = "Electronics";
        FilterData(frame=[$currentFrame], column=["Category"], comparator=["=="], value=[$productFilter]);

        // Passing a map (less common as direct input, often constructed by a preceding reactor)
        // Conceptual example:
        // configMap = {"type":"bar", "xAxis":"Month", "yAxis":"Sales"};
        // UpdatePanelSettings(panelId=["panel1"], settings=[$configMap]);
        ```

*   **Reactor Input Handling via `AbstractReactor`**:
    *   **`keysToGet` (String Array)**: Each concrete Reactor (extending `AbstractReactor`) declares a `String[] keysToGet` array. This array lists the expected input parameter names (keys) that the Reactor can accept. For example, `EchoReactor` defines `this.keysToGet = new String[] {ReactorKeysEnum.VALUE.getKey()};`.
    *   **`NounStore`**: When `PixelRunner` (via `GreedyTranslation`) prepares to execute a Reactor, it populates the Reactor's `NounStore` (an instance of `prerna.sablecc2.om.NounStore`). The `NounStore` is essentially a map where keys are the parameter names (from the Pixel script, e.g., "fileName", "content") and values are `GenRowStruct` objects. A `GenRowStruct` can hold one or more `NounMetadata` objects, accommodating single values or lists passed from Pixel.
    *   **`organizeKeys()` Method**: Inside the Reactor's `execute()` method (or often in its constructor or an initialization block), `organizeKeys()` (a method from `AbstractReactor`) is typically called. This method:
        *   Iterates through the Reactor's declared `keysToGet`.
        *   For each key, it retrieves the corresponding `GenRowStruct` from the `NounStore`.
        *   It populates a convenience map `this.keyValue` (a `Hashtable<String, String>`) with the *first* value for each key, converted to a String. This is useful for quickly accessing single-value parameters.
        *   It also ensures that required parameters (as defined by `keyRequired` in the Reactor) are present, throwing an error if a required key is missing.
    *   **Accessing Full Input Data**:
        *   While `organizeKeys()` populates `keyValue` with string representations of the first item for each input, Reactors often need to access the full `NounMetadata` (to get the actual data type and full value, especially for lists or complex objects).
        *   This is done by directly accessing the `NounStore` using `this.store.getNoun(String key)` which returns the `GenRowStruct`.
        *   Example: `GenRowStruct columnsGrs = this.store.getNoun("columns");`
        *   The Reactor can then iterate through the `NounMetadata` objects in the `GenRowStruct` or get specific ones by index.

*   **Input Data Representation (`NounMetadata`)**:
    *   All inputs, whether literals or variables from Pixel, are wrapped as `NounMetadata` objects before being placed in the `NounStore`.
    *   A `NounMetadata` object contains:
        *   The actual value (e.g., a String, Integer, List, `ITableDataFrame` instance if a frame variable was passed).
        *   A `PixelDataType` enum indicating the type of the data (e.g., `CONST_STRING`, `CONST_INT`, `FRAME`).
        *   `PixelOperationType` (less critical for inputs, more for outputs).
    *   This consistent wrapping allows Reactors to inspect the type of input they have received and process it accordingly. For instance, a Reactor expecting a frame can check if `noun.getNounType() == PixelDataType.FRAME` and then cast `noun.getValue()` to `ITableDataFrame`.

*   **Conceptual Example of a Reactor Processing Inputs**:
    ```java
    // Inside a hypothetical "ProcessItemsReactor"
    // public class ProcessItemsReactor extends AbstractReactor {
    //     public ProcessItemsReactor() {
    //         this.keysToGet = new String[] {"items", "processingMode", "threshold"};
    //         this.keyRequired = new int[] {1, 0, 0}; // items is required
    //     }

    //     @Override
    //     public NounMetadata execute() {
    //         organizeKeys(); // Populates this.store and this.keyValue

    //         // Get the list of items
    //         List<Object> itemsList = new ArrayList<>();
    //         GenRowStruct itemsGrs = this.store.getNoun("items");
    //         if (itemsGrs != null) {
    //             for (NounMetadata itemNoun : itemsGrs.vector) {
    //                 itemsList.add(itemNoun.getValue());
    //             }
    //         }

    //         // Get optional processingMode (defaults if not present)
    //         String mode = this.keyValue.get("processingMode");
    //         if (mode == null) {
    //             mode = "default";
    //         }

    //         // Get optional threshold
    //         double threshold = 0.5;
    //         NounMetadata thresholdNoun = this.store.getNoun("threshold") != null ? this.store.getNoun("threshold").getNoun(0) : null;
    //         if (thresholdNoun != null && (thresholdNoun.getNounType() == PixelDataType.CONST_INT || thresholdNoun.getNounType() == PixelDataType.CONST_DECIMAL)) {
    //             threshold = ((Number) thresholdNoun.getValue()).doubleValue();
    //         }

    //         // ... perform processing with itemsList, mode, threshold ...
    //         // return new NounMetadata(...);
    //     }
    // }
    ```

This system allows for flexible parameter passing from Pixel to Java Reactors, supporting various data types and optional/required parameters.

### 2.4. Reactor Outputs and `NounMetadata`

Just as inputs are standardized, the way Reactors return data is also structured, primarily through the `prerna.sablecc2.om.nounmeta.NounMetadata` class. This class acts as a wrapper around the actual result, providing crucial context about the data's type and the nature of the operation that produced it.

*   **`NounMetadata` as the Standard Return**:
    *   The `execute()` method of an `IReactor` is declared to return a `NounMetadata` object.
    *   This object encapsulates the primary output of the Reactor.
    *   **Key fields of `NounMetadata`**:
        *   `value`: The actual data being returned (e.g., a String, Integer, Double, Boolean, List, Map, `ITableDataFrame` instance, or even a custom Java object).
        *   `nounType` (`PixelDataType` enum): Specifies the semantic type of the `value`. This helps SEMOSS and subsequent Reactors understand how to interpret the data.
        *   `opType` (List of `PixelOperationType` enums): A list of types that describe the operation performed or suggest how the result should be handled, especially by the UI.

*   **`PixelDataType` Enum (`prerna.sablecc2.om.PixelDataType`)**:
    *   This enum provides a classification for the data contained within the `NounMetadata`'s `value`.
    *   **Common `PixelDataType` values include**:
        *   `CONST_STRING`, `CONST_INT`, `CONST_DECIMAL`, `CONST_DATE`, `CONST_TIMESTAMP`, `BOOLEAN`: For literal values.
        *   `COLUMN`: Represents a column name or a reference to a column.
        *   `FRAME`: The value is an instance of `ITableDataFrame` (e.g., an H2Frame, TinkerFrame).
        *   `FILTER`: The value is a `prerna.sablecc2.om.Filter` object.
        *   `TASK_OPTIONS`: The value is a `prerna.sablecc2.om.task.options.TaskOptions` object, often used for configuring data retrieval for visualizations.
        *   `PANEL`: Represents an `InsightPanel` object or its ID.
        *   `SHEET`: Represents an `InsightSheet` object or its ID.
        *   `VARIABLE`: Represents the name of a variable stored in the `VarStore`.
        *   `LAMBDA`: The value is another `IReactor` instance (for nested or dynamically generated operations).
        *   `MAP`, `LIST`: For returning structured Java Maps or Lists.
        *   `ERROR_MESSAGE`, `WARNING_MESSAGE`, `SUCCESS_MESSAGE`: For specific feedback messages.
    *   **Example**: A reactor that calculates a sum might return:
        ```java
        // double sumResult = 105.5;
        // return new NounMetadata(sumResult, PixelDataType.CONST_DECIMAL);
        ```
        (Often, an appropriate `PixelOperationType` is also added).

*   **`PixelOperationType` Enum (`prerna.sablecc2.om.PixelOperationType`)**:
    *   This enum (or list of enums in `NounMetadata`) provides crucial information about what kind of operation was performed and/or how the result should be interpreted by the system, especially by the `PixelRunner` and potentially the UI.
    *   **Key `PixelOperationType` values and their significance**:
        *   `OPERATION`: A generic successful operation. The `value` in `NounMetadata` is the direct result.
        *   `FRAME_DATA_CHANGE`, `FRAME_HEADERS_CHANGE`, `FRAME_METADATA_CHANGE`: Indicate that an operation modified an existing data frame's data, headers, or metadata respectively. The UI would typically refresh the view of this frame.
        *   `NEW_FRAME`: Signals that a new `ITableDataFrame` has been created. The `value` is the new frame.
        *   `VIZ_DATA`: The `value` contains data specifically formatted or intended for a visualization on the UI. This often triggers a data update for a panel.
        *   `PANEL_ORNAMENT_CHANGE`: Signals that a panel's visual configuration (ornament) has changed. The `value` might be a Map containing the panel ID and the new ornament settings. The UI updates the panel's appearance.
        *   `PANEL_VIEW_CHANGE`: Indicates a change in the type of view for a panel (e.g., from a grid to a bar chart).
        *   `SHEET_ADD_PANEL`, `SHEET_REMOVE_PANEL`: Signals changes to the panels within an insight sheet.
        *   `ERROR`: An error occurred during the Reactor's execution. The `value` is typically an error message string, and `nounType` would be `PixelDataType.ERROR_MESSAGE`.
        *   `WARNING`: A warning message.
        *   `SUCCESS_MESSAGE`: An explicit success message to be shown to the user.
        *   `FILE_DOWNLOAD`: The result is a file to be downloaded by the client. The `value` might be a path or a `FileReference` object.
        *   `PARAM_SET`: Indicates a variable has been set in the `VarStore`.
    *   A single `NounMetadata` can have multiple `PixelOperationType`s to convey complex outcomes.

*   **How Pixel and the System Use Reactor Outputs**:
    *   **Variable Assignment**: If a Pixel command assigns the Reactor's output to a variable (e.g., `myResult = MyReactor();`), the entire `NounMetadata` object returned by `MyReactor` is stored in the `Insight`'s `VarStore` under the key "myResult". Subsequent Pixel commands can then access this variable (e.g., `UseResult(data=[$myResult]);`). The consuming Reactor would then typically access `$myResult.getValue()`.
    *   **Chaining Operations**: In a piped sequence (`R1() | R2();`), the `NounMetadata` returned by `R1().execute()` is often made available to `R2()` as its primary input context (e.g., accessible via `this.curRow` in `AbstractReactor`, or by `PixelPlanner` setting it as an implicit input). `R2` can then decide how to use this input based on its `PixelDataType` and `value`.
    *   **`PixelRunner` Interpretation**: The `PixelRunner` inspects the `PixelOperationType` list in the returned `NounMetadata`. Based on these types, it might perform additional actions:
        *   If `ERROR`, it might halt further execution or log the error.
        *   If `FRAME_DATA_CHANGE`, it might signal to the UI that a particular frame needs refreshing.
        *   If `VIZ_DATA` or `PANEL_ORNAMENT_CHANGE`, it packages this information to be sent back in the HTTP response so the frontend can update the UI.
    *   **Implicit Results**: If a Pixel line is just a Reactor call without assignment (e.g., `ExportFrame(type=["CSV"]);`), its returned `NounMetadata` might be added to a list of results for the overall Pixel execution, potentially for display or logging, depending on the `PixelOperationType`.

*   **Examples of Returning Data**:
    1.  **Reactor returning a simple success message**:
        ```java
        // return NounMetadata.getSuccessNounMessage("Operation completed successfully!");
        // Equivalent to:
        // return new NounMetadata("Operation completed successfully!", PixelDataType.SUCCESS_MESSAGE, PixelOperationType.SUCCESS_MESSAGE);
        ```
    2.  **Reactor creating and returning a new data frame**:
        ```java
        // ITableDataFrame newFrame = createMyFrame();
        // insight.getVarStore().put(frameName, new NounMetadata(newFrame, PixelDataType.FRAME, PixelOperationType.NEW_FRAME)); // Also store it
        // return new NounMetadata(newFrame, PixelDataType.FRAME, PixelOperationType.NEW_FRAME);
        ```
    3.  **Reactor returning data for a chart on a specific panel**:
        ```java
        // Map<String, Object> chartData = new HashMap<>();
        // chartData.put("panelId", "panel_1");
        // chartData.put("data", myChartDataList);
        // chartData.put("layout", "echarts"); // Or whatever charting library is used
        // return new NounMetadata(chartData, PixelDataType.MAP, PixelOperationType.VIZ_DATA);
        ```

By using `NounMetadata` with its `PixelDataType` and `PixelOperationType`, Reactors provide rich, contextual information about their results, enabling the SEMOSS backend to manage data flow, update state, and interact with the UI effectively.

### 2.5. Reactor Results and UI Interaction

A key aspect of SEMOSS is its interactive nature, where backend operations performed by Pixels and Reactors can dynamically update the user interface. This is achieved through conventions in how Reactors return data, particularly using `PixelOperationType` in `NounMetadata`, and how the `PixelRunner` processes these results.

*   **Signaling UI Updates via `PixelOperationType`**:
    *   As detailed in the "Reactor Outputs and `NounMetadata`" section, the `opType` (a list of `PixelOperationType` enums) in the returned `NounMetadata` is critical for signaling UI changes.
    *   When the Java backend (specifically `PixelRunner` or associated components handling the HTTP response) processes the `NounMetadata` from a top-level Pixel/Reactor execution, it inspects these `opType`s.
    *   Certain `opType`s are specifically designated to indicate that the UI needs to be updated.

*   **Common `PixelOperationType`s for UI Interaction**:
    *   **`VIZ_DATA`**: This is a primary signal for UI updates related to data in visualizations.
        *   When a Reactor returns `NounMetadata` with `VIZ_DATA` in its `opType` list, the `value` of the `NounMetadata` is expected to contain the data necessary for a specific UI panel (chart, grid, etc.).
        *   This data is often structured as a Map or List of Maps, ready for consumption by a frontend charting library (e.g., ECharts, D3) or data grid component.
        *   The `NounMetadata` might also include additional information, such as the `panelId` the data is for, or task options used to generate the data.
    *   **`PANEL_ORNAMENT_CHANGE`**: Signals that a panel's visual configuration or settings (its "ornament") have changed.
        *   The `value` of the `NounMetadata` typically contains a Map with the `panelId` and the new ornament data (e.g., chart title, axis labels, colors, layout options).
        *   The UI uses this to re-render or update the specified panel's settings without necessarily fetching new data.
    *   **`PANEL_VIEW_CHANGE`**: Indicates a change in the type of view for a panel (e.g., switching from a grid to a bar chart, or changing chart subtypes).
        *   The `value` would contain the `panelId` and the new view type identifier.
    *   **`SHEET_ADD_PANEL`, `SHEET_REMOVE_PANEL`, `SHEET_ORDER_CHANGE`**: These signal structural changes to an `InsightSheet`, such as adding new panels, removing existing ones, or reordering them. The UI updates the sheet layout accordingly.
    *   **`REFRESH_INSIGHT_VARIABLES`**: Signals that the insight's variables (in `VarStore`) have changed in a way that might be relevant to the UI (e.g., for displaying available variables to the user).
    *   **`SUCCESS_MESSAGE`, `WARNING_MESSAGE`, `ERROR_MESSAGE`**: While not direct UI component updates, these are often used to display toast notifications or alerts to the user via the UI.

*   **Role of `InsightPanel` and `InsightSheet`**:
    *   Reactors can directly interact with `InsightPanel` and `InsightSheet` objects stored within the current `Insight`.
    *   For example, a Reactor might:
        *   Retrieve an `InsightPanel` using `insight.getInsightPanel(panelId)`.
        *   Modify its properties (e.g., `panel.set cytologicOptions(...)`, `panel.setPanelView(viewType)`).
        *   Add or remove panels from an `InsightSheet` using `insightSheet.addPanel(newPanel)`.
    *   After making such modifications, the Reactor then returns a `NounMetadata` with the appropriate `PixelOperationType` (e.g., `PANEL_ORNAMENT_CHANGE`) to inform the frontend that these server-side changes need to be reflected in the UI.

*   **Message Flow to UI (High-Level)**:
    1.  A Pixel script is executed (e.g., triggered by a user action in the UI).
    2.  A Reactor performs its logic and returns `NounMetadata` containing data and relevant `PixelOperationType`s (e.g., `VIZ_DATA`, `PANEL_ORNAMENT_CHANGE`).
    3.  The `PixelRunner` collects these `NounMetadata` objects.
    4.  The Java servlet handling the HTTP request formats these results (often into a JSON structure) and sends them in the HTTP response. This JSON response will include the data and the operation types.
    5.  The frontend JavaScript code receives this JSON response.
    6.  It inspects the operation types and data. Based on this, it dispatches actions to update the corresponding UI elements:
        *   If `VIZ_DATA`, it updates the data for a specific chart/grid and triggers a re-render.
        *   If `PANEL_ORNAMENT_CHANGE`, it applies the new settings to the panel.
        *   If `SUCCESS_MESSAGE`, it displays a notification.

*   **Example Scenario: Updating a Chart**
    1.  **Pixel**: `Panel("panel_001") | SetChartFilter(column=["Category"], values=["Electronics"]);`
    2.  **`SetChartFilterReactor` (Conceptual)**:
        *   Receives `panelId="panel_001"`, filter details.
        *   Modifies the data query associated with `panel_001` (perhaps stored in its `TaskOptions`).
        *   Re-fetches data for `panel_001` using the new filter.
        *   Returns:
            ```java
            // Map<String, Object> vizDataPayload = new HashMap<>();
            // vizDataPayload.put("panelId", "panel_001");
            // vizDataPayload.put("view", "echarts"); // or other view type
            // vizDataPayload.put("options", newChartOptions); // updated chart options if any
            // vizDataPayload.put("data", fetchedDataForChart);
            // return new NounMetadata(vizDataPayload, PixelDataType.MAP, PixelOperationType.VIZ_DATA);
            ```
    3.  **Frontend**:
        *   Receives the JSON containing the `VIZ_DATA` operation type and the payload.
        *   Identifies `panel_001`.
        *   Updates the chart library instance for `panel_001` with `fetchedDataForChart` and `newChartOptions`.

This mechanism allows backend logic (Reactors) to drive dynamic updates and interactions in the SEMOSS user interface by sending structured messages and conventional operation types.

## 3. Engine Abstraction (`src/prerna/engine`)

## 3. Engine Abstraction (`src/prerna/engine`)

The "Engine" is a fundamental concept in SEMOSS, representing a connection to an external data source, a service, or a computational environment. The `src/prerna/engine/api/IEngine.java` interface is the root of the engine abstraction.

### 3.1. `IEngine.java`: The Core Engine Interface

`IEngine.java` defines the basic contract for all engine types within SEMOSS. It focuses on the lifecycle, configuration, and identification of an engine.

**Key Responsibilities and Methods:**

*   **Identity:**
    *   `getEngineId()` / `setEngineId(String engineId)`: Manages a unique identifier for the engine instance.
    *   `getEngineName()` / `setEngineName(String engineName)`: Manages a user-friendly name for the engine, which might be the same as its ID or a more descriptive alias.

*   **Lifecycle Management:**
    *   `open(String smssFilePath)` / `open(Properties smssProp)`: This is the core initialization method. Engines are configured via `.smss` files (which are Java Properties files). These files contain key-value pairs defining connection strings, credentials, paths, and other parameters specific to the engine type. The `open()` method reads this configuration and establishes the connection or prepares the engine for use.
    *   `close()`: Inherited from `java.io.Closeable`, this method is responsible for releasing any resources held by the engine (e.g., database connections, file handles).
    *   `delete()`: Allows for the removal of an engine, including its configuration files and any associated resources.

*   **Configuration Access:**
    *   `getSmssFilePath()` / `setSmssFilePath(String smssFilePath)`: Provides access to the path of the `.smss` configuration file.
    *   `getSmssProp()` / `setSmssProp(Properties smssProp)`: Allows direct access to the loaded `Properties` object containing the engine's configuration.
    *   `getOrigSmssProp()`: Returns the original properties from the SMSS file, before any potential runtime modifications.

*   **Engine Typing:**
    *   `getCatalogType()`: Returns an `IEngine.CATALOG_TYPE` enum value. This is a crucial piece of metadata that categorizes the engine's primary function. The defined types include:
        *   `DATABASE`: For connections to relational, graph, or other types of databases.
        *   `STORAGE`: For connections to file systems or object storage (e.g., S3, local disk).
        *   `MODEL`: For managing and interacting with machine learning models.
        *   `VECTOR`: For vector databases used in similarity search and AI.
        *   `FUNCTION`: For engines that provide access to custom functions or APIs.
        *   `GUARDRAIL`: For engines that enforce policies or rules.
        *   `PROJECT`: A special type representing a SEMOSS project itself.
        *   `VENV`: (Likely related to Python virtual environments, though noted as not heavily used anymore).
    *   `getCatalogSubType(Properties smssProp)`: Allows for a more granular classification of the engine within its main `CATALOG_TYPE`.

*   **Other Utilities:**
    *   `holdsFileLocks()`: Indicates if the engine's operation might result in file locks, which is important for coordinating operations like exports or backups.
    *   `buildOpenAIFunctionEngineToolMap()`: Suggests a mechanism for integrating engines with AI function-calling capabilities, particularly with OpenAI models, allowing AI agents to potentially interact with or query these engines.

The `IEngine` interface itself is quite generic. Specific data processing capabilities (like executing queries for a `DATABASE` engine, or reading/writing files for a `STORAGE` engine) are typically defined by more specialized interfaces (e.g., `IDatabaseEngine`, `IStorageEngine`) which concrete engine implementations will also implement in addition to `IEngine`.

### 3.2. Engine Implementations (`src/prerna/engine/impl/`)

SEMOSS provides a wide array of concrete engine implementations, catering to different data sources, services, and functionalities. These classes typically extend base abstract classes (like `prerna.engine.impl.AbstractEngine`, `prerna.engine.impl.AbstractDatabaseEngine`, etc.) and implement `IEngine` along with other necessary specialized interfaces (e.g., `IDatabaseEngine`, `IModelEngine`).

For detailed descriptions of specific engine implementations, their core abstract parent classes (e.g., `AbstractDatabaseEngine`, `AbstractModelEngine`, `AbstractVectorDatabaseEngine`, `AbstractFunctionEngine`, `AbstractStorageEngine`), key interface methods, and guidelines on how to extend them for new custom functionalities, please refer to the **[Engine Implementations Deep Dive](./06_engine_implementations.md)** document.

A brief overview of categories includes:
*   **`DATABASE` Engines**: For various SQL, NoSQL, and graph databases.
*   **`STORAGE` Engines**: For file systems and object stores like S3.
*   **`MODEL` Engines**: For interacting with AI/ML models (e.g., OpenAI, AWS Bedrock, local models).
*   **`VECTOR` Engines**: For vector databases (e.g., FAISS, ChromaDB).
*   **`FUNCTION` Engines**: For exposing scripts or APIs as callable functions.
*   **`PROJECT` Engines**: Representing SEMOSS projects themselves.

### 3.3. Engine Management

Managing the lifecycle, configuration, and accessibility of engines is a critical aspect of SEMOSS. This involves several components working together:

**1. Configuration (`.smss` files):**
*   Each engine instance is defined by a `.smss` file. This is a Java Properties file (`.properties` format) that contains all the necessary configuration for that specific engine.
*   Key properties typically include:
    *   `ENGINE_CLASS` (or `Constants.ENGINE_TYPE` in code): The fully qualified Java class name of the `IEngine` implementation (e.g., `prerna.engine.impl.rdbms.RDBMSNativeEngine`).
    *   `ENGINE_ALIAS` (or `Constants.ENGINE_ALIAS`): The user-friendly name of the engine.
    *   `ENGINE_ID` (or `Constants.ENGINE`): The unique ID for the engine.
    *   ...and other properties specific to the engine type (JDBC URLs, API keys, file paths, etc.).
*   These `.smss` files are typically stored in the `db/` directory or a configured asset location.

**2. Registration and Metadata Storage (`SecurityEngineUtils` and `securityDb`):**
*   The `prerna.auth.utils.SecurityEngineUtils.java` class plays a central role in managing engine metadata and permissions. It interacts with a dedicated security database (often an H2 database referred to as `securityDb`).
*   **Engine Creation/Registration**: When a new engine is added to SEMOSS (e.g., through the UI or an API call):
    *   Its `.smss` file is created/provided.
    *   `SecurityEngineUtils.addEngine(String engineId, boolean global, User user)` is called.
    *   This method reads the `.smss` file, determines the engine's catalog type and subtype (by temporarily instantiating the engine class to call its `getCatalogType()` methods).
    *   It then records the engine's ID, name, type, subtype, and other metadata into an `ENGINE` table within the `securityDb`.
    *   User permissions for this new engine (e.g., ownership for the creator) are also stored in tables like `ENGINEPERMISSION`.
*   **Metadata Access**: `SecurityEngineUtils` provides methods to retrieve engine details, list available engines for a user (respecting permissions), and get/set additional metadata (stored in `ENGINEMETA`).

**3. Instantiation and Runtime Access:**
*   **`DIHelper.java` (`prerna.util.DIHelper`)**: This singleton utility class acts as a runtime cache and provider for various global properties and object instances.
    *   When an engine is loaded and its `.smss` file path is known, this path can be registered with `DIHelper` (e.g., `DIHelper.getInstance().setEngineProperty(engineId + "_" + Constants.STORE, smssFilePath)`).
    *   Other parts of the system can then retrieve this path or other engine-related properties from `DIHelper`.
    *   `DIHelper` can also store actual instantiated engine objects via `setLocalProperty(String key, Object value)` if they are meant to be singletons or cached.
*   **Engine Instantiation**:
    *   When a Pixel script or another part of SEMOSS needs to use a specific engine, it typically starts with the engine's ID.
    *   The system retrieves the engine's class name and SMSS properties (either directly from the file system using the path from `DIHelper` or from a cache).
    *   The engine class is instantiated using `Class.forName(engineClassName).newInstance()`.
    *   The `engine.open(smssProperties)` method is called on the new instance to initialize it.
*   **Caching/Pooling**: For performance, frequently used engines, especially database engines, might be cached or managed in connection pools. The specifics would depend on the engine type.

**4. Engine Selection and Usage in Pixel:**
*   Pixel scripts typically refer to engines by their ID or alias (e.g., `Database("myRdbmsEngine") | Select(...)`).
*   The Pixel execution engine (`PixelRunner` and `PixelPlanner`):
    1.  Parses the Pixel script and identifies the target engine ID.
    2.  Likely interacts with `SecurityEngineUtils` to verify the current user's permission to access that engine.
    3.  Retrieves the engine's configuration (SMSS properties).
    4.  Ensures the engine is instantiated and opened (as described above).
    5.  Passes the subsequent operations (like `Select`, `Filter`) to the appropriate methods of the instantiated `IEngine` object (or a more specific interface it implements, like `IDatabaseEngine`).

**In summary:**
*   `.smss` files define individual engine configurations.
*   `SecurityEngineUtils` and its associated database track engine metadata and user permissions.
*   `DIHelper` helps manage runtime access to engine configurations and instances.
*   The Pixel execution framework uses this infrastructure to instantiate and interact with the correct engine based on user commands and permissions.

## 4. Data Source Layer (`src/prerna/ds`)

The `src/prerna/ds` package provides abstractions and concrete implementations for representing and manipulating datasets within SEMOSS's Java memory, once data has been fetched from an external `IEngine` or created through transformations. This layer is crucial for in-memory analytics, data manipulation, and serving data to visualization components.

**Core Concepts:**

*   **`ITableDataFrame` Interface (`prerna.algorithm.api.ITableDataFrame`)**:
    *   This is the primary interface for all in-memory frame-like data structures in SEMOSS.
    *   It defines common operations expected from a data frame, such as accessing metadata, iterating data, applying filters, and saving/loading.
*   **`AbstractTableDataFrame.java` (`prerna.ds.shared.AbstractTableDataFrame`)**:
    *   A key abstract class that provides a base implementation for most `ITableDataFrame`s.
    *   **Metadata (`OwlTemporalEngineMeta metaData`)**: Each frame instance holds an `OwlTemporalEngineMeta` object. This metadata object describes the frame's structure: column names (headers), data types, relationships between columns (if applicable, especially for graph frames), and potentially other semantic information.
    *   **Filtering (`GenRowFilters grf`)**: `AbstractTableDataFrame` includes a `GenRowFilters` object. This allows filters to be applied directly to the in-memory frame. Operations on the frame (like iteration or querying) will respect these filters.
    *   **Querying**: It defines a `query(SelectQueryStruct qs)` method (often abstract or overridden by concrete classes) that allows the frame to be queried using SEMOSS's internal `SelectQueryStruct` representation. This enables a consistent way to query data regardless of whether it's in an external engine or an in-memory frame.
    *   **Caching**: Implements mechanisms for caching metrics about the data (e.g., column uniqueness, min/max values) to improve performance.
    *   **Persistence**: Provides methods (`saveMeta`, `openCacheMeta`) for saving and loading the frame's metadata and filter state, often used in conjunction with caching the frame's actual data.

**Key Frame Implementations:**

The `src/prerna/ds/` package and its sub-packages contain various concrete implementations of `ITableDataFrame`:

*   **`TinkerFrame.java`**:
    *   **Purpose**: Represents graph data within SEMOSS, built upon Apache TinkerPop's `TinkerGraph` (an in-memory graph database).
    *   **Functionality**: Stores nodes and edges, allows graph traversals and queries using Gremlin (via `GremlinInterpreter` which translates `SelectQueryStruct` to Gremlin). It's used for graph-based analytics and visualizations.
    *   **DataFrameType**: `GRAPH`.

*   **`H2Frame.java` (`prerna.ds.rdbms.h2.H2Frame` or `prerna.ds.h2.H2Frame`)**:
    *   **Purpose**: Represents tabular data backed by an in-memory H2 database instance.
    *   **Functionality**: Allows SQL querying of the in-memory data. This is often used as a high-performance backend for tabular data that has been imported or transformed within SEMOSS.
    *   **DataFrameType**: `GRID` or `TABLE`.

*   **`NativeFrame.java` (`prerna.ds.nativeframe.NativeFrame`)**:
    *   **Purpose**: A purely Java-based in-memory representation of tabular data (e.g., using Lists of Lists or similar structures).
    *   **Functionality**: Suitable for smaller datasets or when direct Java manipulation is needed without the overhead of a database like H2.
    *   **DataFrameType**: `GRID` or `TABLE`.

*   **`PandasFrame.java` (`prerna.ds.py.PandasFrame`)**:
    *   **Purpose**: Acts as a wrapper or interface to a Pandas DataFrame in a Python environment managed by SEMOSS.
    *   **Functionality**: Allows SEMOSS to leverage Pandas' powerful data manipulation capabilities. Operations might be delegated to the Python environment.
    *   **DataFrameType**: `GRID` or `TABLE`.

*   **`RDataTable.java` (`prerna.ds.r.RDataTable`)**:
    *   **Purpose**: Similar to `PandasFrame`, but for R, likely wrapping R's `data.table` or `data.frame`.
    *   **Functionality**: Enables SEMOSS to utilize R for statistical analysis and data manipulation on data held in R.
    *   **DataFrameType**: `GRID` or `TABLE`.

*   **`SparkDataFrame.java` (`prerna.ds.spark.SparkDataFrame`)**:
    *   **Purpose**: Represents a Spark DataFrame, allowing SEMOSS to work with distributed datasets managed by Apache Spark.
    *   **Functionality**: Operations are typically translated into Spark actions and transformations.
    *   **DataFrameType**: `GRID` or `TABLE`.

**Interaction with `IEngine` and `QueryStruct`:**

*   **Loading Data into Frames**: Data is typically loaded into these `ITableDataFrame` implementations from an external `IEngine`.
    *   A query (often a `SelectQueryStruct`) is executed by an `IEngine` (e.g., `RDBMSNativeEngine` executing SQL).
    *   The results from the `IEngine` are then streamed or bulk-loaded into an appropriate frame type (e.g., data from an RDBMS might be loaded into an `H2Frame` for further in-memory work).
*   **Frames as Queryable Sources**: Once data is in an `ITableDataFrame`, the frame itself can often be queried using the same `SelectQueryStruct` mechanism that is used for external engines. This is achieved by each frame type providing its own implementation for `query(SelectQueryStruct qs)`, which translates the QS into operations on its specific backend (e.g., Gremlin for `TinkerFrame`, SQL for `H2Frame`, direct Java operations for `NativeFrame`).
*   **`QueryStruct.java` (`prerna.ds.QueryStruct`)**: This class is a pivotal data structure that represents a query in an abstract, engine-agnostic way. It includes selectors, filters, joins, group by clauses, order by clauses, etc.
    *   Reactors and other parts of SEMOSS often build or modify `QueryStruct` objects.
    *   These `QueryStruct`s are then passed to either an `IEngine` (to query external data) or an `ITableDataFrame` (to query in-memory data).
    *   Interpreters (like `GremlinInterpreter` for `TinkerFrame`, or SQL generators for RDBMS engines/frames) are responsible for translating the `QueryStruct` into the native query language of the target data store.

The data source layer (`src/prerna/ds`) thus provides a flexible way for SEMOSS to handle data both from external sources and within its own memory, using a common query abstraction (`QueryStruct`) and a set of versatile in-memory data frame representations.

## 5. Authentication and Authorization (`src/prerna/auth`)

The `src/prerna/auth/` package and its sub-packages are responsible for managing user identity, authentication, and access control throughout the SEMOSS platform.

**Core Security Objects:**

*   **`User.java`**: Represents an authenticated user within SEMOSS. It typically stores user identifiers (from various authentication providers), profile information, and potentially a collection of their permissions or roles.
*   **`AccessToken.java`**: Represents a security token (e.g., a JWT or an opaque token) issued to a user upon successful authentication. This token is then used to authenticate subsequent API requests and manage user sessions. `ReadOnlyAccessToken` might be a specialized version for read-only operations.
*   **`AuthProvider.java`**: An enumeration that defines the different methods by which a user can be authenticated (e.g., `NATIVE` for users stored in SEMOSS's own database, `LDAP`, `SAML`, `OIDC` for integration with external identity providers).
*   **`AccessPermissionEnum.java`**: Defines the various levels of access rights a user can have on a resource (e.g., `READ_ONLY`, `EDIT`, `OWNER`).

**Authentication Process (High-Level):**

1.  **Login Request**: A user initiates a login request, typically providing credentials or being redirected from an external Identity Provider (IdP).
2.  **Provider Determination**: SEMOSS identifies the `AuthProvider` being used for the login attempt.
3.  **Credential Validation**:
    *   For `NATIVE` users, `prerna.auth.utils.SecurityNativeUserUtils` likely handles the validation of credentials against a user store within SEMOSS's security database.
    *   For external providers (LDAP, SAML, OIDC), SEMOSS would interact with the respective IdP according to the protocol's specifications. This might involve validating assertions or tokens provided by the IdP.
4.  **User Object Creation**: Upon successful authentication, a `User` object is created or retrieved, populating it with identity information.
5.  **Token Issuance**: `prerna.auth.utils.SecurityTokenUtils` is responsible for generating an `AccessToken` for the authenticated `User`. This token encapsulates the user's authenticated state.
6.  **Session Management**: The `AccessToken` is used to manage the user's session, typically sent with each subsequent request to the backend.

**Authorization Process (High-Level):**

SEMOSS employs a role-based or permission-based access control model, primarily managed through its security database.

1.  **Resource Access Request**: A user, identified by their `AccessToken` and associated `User` object, attempts to access or modify a resource (e.g., an Engine, Project, Insight, or perform a specific action).
2.  **Permission Check**:
    *   Utility classes within `prerna.auth.utils/` are invoked to check permissions. For example:
        *   `SecurityEngineUtils.userCanViewEngine(User user, String engineId)`
        *   `SecurityProjectUtils.userCanEditProject(User user, String projectId)`
    *   These methods query the security database, which stores relationships between users (or groups they belong to) and resources, along with the `AccessPermissionEnum` level granted.
3.  **Decision**: Based on the permissions found in the security database, the system either grants or denies access to the resource or action. Both direct user permissions and permissions derived from group memberships are typically considered.
4.  **External Authorization**: The `prerna.auth.external.ExternalAuthorizationHelper` class suggests that SEMOSS can also integrate with external systems for making authorization decisions, potentially augmenting or overriding its internal permission model.

**Key Utility Packages/Classes:**

*   **`prerna.auth.utils`**: This package is central to security operations.
    *   `AbstractSecurityUtils`: Provides base functionality and access to the security database (often an H2 database instance).
    *   `SecurityEngineUtils`, `SecurityProjectUtils`, `SecurityInsightUtils`: Manage permissions and metadata for Engines, Projects, and Insights, respectively. They handle storing and retrieving these entities along with their associated user/group permissions from the security database.
    *   `SecurityAdminUtils`: Provides functions for administrative security tasks.
    *   `SecurityGroup*Utils`: A set of classes for managing security groups and their permissions on various resources.

The authentication and authorization mechanisms are designed to be comprehensive, supporting both native user management and integration with enterprise identity systems, while providing granular control over access to SEMOSS resources.

## 6. High-Level API Request Flow

Understanding how an API request flows through the SEMOSS Java backend helps tie together the various components discussed. While specifics can vary, a general request lifecycle often looks like this:

1.  **HTTP Request Reception:**
    *   An HTTP request (e.g., from the SEMOSS frontend UI, a custom application, or an API tool) arrives at the Java web server (typically Apache Tomcat) hosting the SEMOSS application.
    *   Standard Java EE components like Servlets (e.g., `prerna.servlets.ApiServlet`, `prerna.servlets.PixelServlet`) or Filters (e.g., `prerna.security.HttpFilter`) are the initial entry points.

2.  **Authentication and Authorization:**
    *   Security filters intercept the request.
    *   The user's `AccessToken` (usually in an HTTP header or cookie) is validated using `SecurityTokenUtils`.
    *   A `User` object is reconstituted.
    *   For protected resources, an authorization check is performed using utility classes like `SecurityEngineUtils`, `SecurityProjectUtils`, etc., to ensure the user has the necessary permissions for the requested resource or action. If authorization fails, an appropriate HTTP error (e.g., 401 Unauthorized, 403 Forbidden) is returned.

3.  **Request Parsing and Dispatching:**
    *   The servlet or controller extracts parameters from the HTTP request. This might include:
        *   A Pixel script to be executed.
        *   The name of a specific Reactor to invoke and its input parameters.
        *   Identifiers for resources like engine IDs, project IDs, or insight IDs.
    *   Based on the endpoint or request parameters, the request is dispatched to the appropriate handler.

4.  **Pixel Execution / Reactor Invocation:**
    *   **If the request contains a Pixel script (e.g., to `PixelServlet`):**
        *   An `Insight` object (representing the user's session or workspace) is retrieved or created.
        *   The `PixelRunner.runPixel(String expression, Insight insight)` method is called.
        *   The `PixelRunner` parses the script (using `sablecc2` components) and uses `GreedyTranslation` (which in turn uses `PixelPlanner`) to execute the script.
        *   This execution may involve a chain of Reactors, interactions with `ITableDataFrame`s (in-memory data), or queries to external `IEngine`s.
    *   **If the request is for a specific Reactor:**
        *   The `ReactorFactory` might be used to get an instance of the target `IReactor`.
        *   Input parameters (nouns) are loaded into the reactor's `NounStore`.
        *   The reactor's `execute()` method is called.
        *   The reactor performs its specific logic, which could involve calculations, data transformations, or calls to `IEngine`s or `ITableDataFrame`s.

5.  **Data Retrieval and Processing (Interaction with Engines/Frames):**
    *   If the operation requires data from an external source, the `PixelPlanner` or the invoked `IReactor` will:
        *   Identify the target `IEngine` ID.
        *   Ensure the user has permissions for this engine.
        *   Obtain an instance of the `IEngine` (potentially from a cache or by instantiating it using its SMSS configuration, often facilitated by `DIHelper` for configuration paths and `SecurityEngineUtils` for metadata).
        *   Formulate a `QueryStruct` representing the query.
        *   The `IEngine` executes the query (translating the `QueryStruct` to its native language like SQL, SPARQL, etc.) and returns the data.
        *   This data might then be loaded into an `ITableDataFrame` (e.g., `H2Frame`, `NativeFrame`) for further in-memory processing or transformation.
    *   If the operation acts on data already in an in-memory `ITableDataFrame` (like `TinkerFrame`, `H2Frame`), the frame's own `query(SelectQueryStruct qs)` method or other manipulation methods are called.

6.  **Result Aggregation and Formatting:**
    *   The `PixelRunner` (for Pixel scripts) or the top-level `IReactor` aggregates the results. Results are typically packaged as `NounMetadata` objects.
    *   For API responses, these results are often converted into a JSON format suitable for consumption by the client. Utility classes (e.g., within `prerna.util.gson`) might be used for this serialization.

7.  **HTTP Response Generation:**
    *   The servlet sends the formatted results (e.g., JSON data, success messages, or error messages) back to the client in the HTTP response, along with an appropriate HTTP status code.

This flow demonstrates how SEMOSS uses a combination of Pixel scripts, a modular Reactor framework, and a versatile Engine/DataFrame abstraction to handle a wide variety of requests, all while enforcing security and permissions.

## 7. The Insight Object (`prerna.om.Insight`)

The `prerna.om.Insight` class is a cornerstone object in SEMOSS, representing a user's session, a specific analysis workspace, or an individual unit of work (like a report or dashboard being built). It encapsulates the state, data, history of operations, and context for a user's interaction with the platform.

### 7.1. Purpose and Key Responsibilities

*   **Contextual Hub**: Acts as a central context for operations. When a user executes Pixel scripts or interacts with data, these actions occur within the scope of an `Insight`.
*   **State Management**: Maintains the state of an analysis, including variables, loaded data frames, applied filters, and UI configurations.
*   **Recipe of Operations**: Records the sequence of Pixel commands executed, forming a "recipe" that can potentially be replayed or saved.
*   **Data Management**: Holds references to in-memory data frames (`ITableDataFrame`) and manages their lifecycle within its scope.
*   **Session Association**: Is associated with a specific `User` and can be linked to a `Project`.

### 7.2. Key Information Held by an Insight

An `Insight` object holds various pieces of information critical to its function:

*   **`insightId` (String)**: A unique UUID automatically generated for each insight instance.
*   **`user` (User)**: The user object who owns or is interacting with this insight.
*   **`insightName` (String)**: A user-defined name for the insight, especially if it's saved.
*   **Project Association**:
    *   `projectId` (String): The ID of the project this insight belongs to (if saved).
    *   `projectName` (String): The name of the project.
    *   `rdbmsId` (String): An identifier for the saved insight definition within the project.
*   **`pixelList` (PixelList)**:
    *   A crucial transient field that stores an ordered list of `Pixel` objects. Each `Pixel` object represents a command executed (the Pixel script itself) and often the `NounMetadata` result of that command.
    *   This list effectively forms the "recipe" or history of operations performed within the insight.
*   **`varStore` (VarStore)**:
    *   A transient `prerna.sablecc2.om.VarStore` instance. This is a key-value store for variables created and used during Pixel script execution within the insight.
    *   It holds user-defined variables, intermediate results, and importantly, references to active data frames (`ITableDataFrame`) using keys (e.g., `Insight.CUR_FRAME_KEY` often points to the most recently used frame).
*   **`taskStore` (TaskStore)**: Manages ongoing tasks or iterators, such as those for streaming data from frames.
*   **UI State**:
    *   `insightSheets` (Map<String, InsightSheet>): Contains `InsightSheet` objects, allowing an insight to have multiple "sheets" or tabs, each with its own layout and content.
    *   `insightPanels` (Map<String, InsightPanel>): Stores `InsightPanel` objects, which represent individual components (like charts, tables, controls) within an insight sheet. Each panel can have its own configuration, data queries, and visualization settings.
    *   `insightOrnament` (Map<String, Object>): A general map for UI-related properties and settings.
*   **File System Context**:
    *   `insightFolder` (String): The path to a dedicated directory on the file system for this insight. Used for storing temporary files, scripts, cached data, etc. The location depends on whether the insight is saved and the overall SEMOSS configuration.
*   **External Process Integration**:
    *   `rJavaTranslator` (AbstractRJavaTranslator): Manages communication with an R environment for the insight.
    *   `pyt` (PyTranslator): Manages communication with a Python environment for the insight.
*   **Caching Configuration**: `cacheable`, `cacheMinutes`, `cacheCron`, `cacheEncrypt` for defining how and if the insight's state or results should be cached.
*   **`pragmap` (Map)**: Stores pragma directives (e.g., `#CACHE=TRUE`, `#RAW_DATA=TRUE`) encountered during Pixel execution, which can modify the behavior of subsequent operations.

### 7.3. How Insights are Used

*   **Pixel Execution (`PixelRunner`)**:
    *   All Pixel scripts are executed within the context of an `Insight` object.
    *   `PixelRunner` uses the insight's `varStore` to resolve variables and store new ones.
    *   Results of Pixel operations (`NounMetadata`) are often added to the insight's `pixelList`.
*   **Reactors (`IReactor`)**:
    *   Many reactors receive the current `Insight` object as part of their execution context.
    *   They can access the `varStore` to get input parameters or data frames, and they can update the `varStore` with their results.
    *   They might also interact with `insightPanels` or `insightSheets` to configure UI elements.
*   **Model Engines (`AbstractModelEngine`, `AbstractVectorDatabaseEngine`)**:
    *   Model and Vector engines often require an `Insight` object when their methods (e.g., `ask()`, `embeddings()`, `nearestNeighbor()`) are called.
    *   The insight provides user context for logging (`ModelInferenceLogsDatabase`), session context for conversation history (if the model engine supports it via `keepConversationHistory`), and access to data frames or variables from the `varStore` that might be needed as input to the models.
*   **Data Frames (`ITableDataFrame`)**:
    *   Data frames loaded or created within an insight are typically stored in its `varStore`.
    *   The insight effectively "owns" these in-memory data representations for the duration of the session or until they are explicitly cleared.
*   **UI Rendering**:
    *   The state of the `insightSheets` and `insightPanels` within an `Insight` object is used by the SEMOSS UI to render the user's workspace, including visualizations, data grids, and interactive controls.
*   **Saving and Loading**:
    *   When an insight is saved, its `pixelList` (the recipe), `varStore` (potentially some persistent variables or configurations), `insightSheets`, `insightPanels`, and other relevant metadata are serialized and stored, typically associated with a `Project`.
    *   Loading a saved insight involves recreating the `Insight` object and replaying its Pixel recipe or restoring its state to bring the user back to their previous analysis.

In summary, the `Insight` object is a dynamic and central component that encapsulates a user's analytical session, managing the flow of operations, data, and state, and serving as the bridge between backend processing and the user interface.
[end of docs/01_java_backend.md]

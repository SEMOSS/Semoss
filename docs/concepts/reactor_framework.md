# Reactor Framework

The Reactor framework is a cornerstone of SEMOSS's Java backend, providing a flexible and extensible way to define and execute specific operations or commands, often as part of a Pixel script. Each "reactor" is a Java class that encapsulates a particular piece of logic.

## Core Reactor Interfaces/Classes

*   **`prerna.reactor.IReactor.java`**: This is the primary interface that all reactors must implement (typically by extending `AbstractReactor`). It defines the contract for reactor behavior, including methods for:
    *   Execution: `NounMetadata execute()` is the main method where the reactor performs its logic.
    *   Input/Output Definition: `getInputs()` and `getOutputs()` (though often managed by `AbstractReactor` conventions).
    *   Lifecycle and Chaining: `setParentReactor()`, `getChildReactors()`, `mergeUp()` for integrating into a larger execution flow.
    *   Interaction with Pixel Execution: `setPixelPlanner()`, `setNounStore()`.
    *   Metadata: `getName()`, `getSignature()`, `getHelp()`.

*   **`prerna.reactor.AbstractReactor.java`**: This abstract class provides a robust base implementation of `IReactor`. Most concrete reactors in SEMOSS extend `AbstractReactor`. Key functionalities it provides include:
    *   **Noun Management**:
        *   `NounStore store`: Each reactor instance has a `NounStore` to hold its input parameters (nouns).
        *   `keysToGet`: Concrete reactors define an array of strings (`keysToGet`) specifying the names of the input nouns they expect (e.g., `"value"`, `"column"`, `"expression"`). These often correspond to keys from `ReactorKeysEnum.java`.
        *   `organizeKeys()`: A crucial method called typically at the start of `execute()`. It populates the `store` (and a convenience map `keyValue`) from the actual inputs provided in the Pixel script, matching them against `keysToGet`.
        *   `curRow`: A `GenRowStruct` representing the current data row or context, especially when reactors are chained or process input streams.
    *   **Planner and Insight**: Access to the `PixelPlanner` and the current `Insight` object.
    *   **Signature and Naming**: Storing the reactor's operation name and Pixel signature.
    *   **Error Handling and Logging**: Utility methods for standardized error reporting and logging.
    *   **Input Retrieval**: Helper methods like `getNounAsStringList(String key)` to easily access input values from the `NounStore`.

## Key Reactor Examples and Roles

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

## Reactor Inputs and `NounStore`

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

## Reactor Outputs and `NounMetadata`

A reactor's `execute()` method returns a [NounMetadata](../../src/prerna/sablecc2/om/nounmeta/NounMetadata.java) object. It wraps the result with type and operation information used by the execution pipeline and API consumers.

| Accessor | Purpose |
| --- | --- |
| `getValue()` | The returned value, such as a string, number, list, map, or `ITableDataFrame` |
| `getNounType()` | A `PixelDataType` describing the value |
| `getOpType()` | A list of `PixelOperationType` values describing the operation or outcome |

### Result types and operation types

[PixelDataType](../../src/prerna/sablecc2/om/PixelDataType.java) includes `CONST_STRING`, `CONST_INT`, `CONST_DECIMAL`, `BOOLEAN`, `MAP`, `VECTOR`, and `FRAME`. Use the type that matches the result; `VECTOR` represents list values and `FRAME` represents an `ITableDataFrame`.

[PixelOperationType](../../src/prerna/sablecc2/om/PixelOperationType.java) describes what the reactor did. Common examples are `OPERATION`, `FRAME`, `FRAME_DATA_CHANGE`, `FRAME_HEADERS_CHANGE`, `SUCCESS`, `WARNING`, and `ERROR`. A result can carry multiple operation types.

These snippets illustrate return values inside a reactor's `execute()` method:

```java
// Return a calculated scalar.
return new NounMetadata(sumResult, PixelDataType.CONST_DECIMAL);
```

```java
// Return a frame for subsequent Pixel operations.
return new NounMetadata(resultFrame, PixelDataType.FRAME, PixelOperationType.FRAME);
```

```java
// Return structured data.
return new NounMetadata(resultMap, PixelDataType.MAP, PixelOperationType.OPERATION);
```

For feedback, use `NounMetadata.getSuccessNounMessage(...)`, `getWarningNounMessage(...)`, or `getErrorNounMessage(...)`. These helpers set the appropriate data and operation types.

### Using reactor results

- **Variable assignment:** a result assigned in Pixel becomes available through the current Insight's `VarStore` for subsequent operations.
- **Pipelines:** typed results can become input to the next reactor in a piped expression. The receiving reactor interprets the value according to its input contract.
- **API responses:** the execution pipeline collects results for serialization and return to the caller, including their data and operation types.

See [the Insight object](insight_object.md) for execution state and [Monolith integration](../integrations/monolith_interaction.md) for the HTTP request and response path.

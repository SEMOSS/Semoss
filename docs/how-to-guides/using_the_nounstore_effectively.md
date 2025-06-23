# Using the NounStore Effectively in SEMOSS

## Introduction

The `NounStore` is a critical component in SEMOSS, acting as the primary mechanism for data exchange and state management during Pixel script execution and within Java Reactors. Understanding how to interact with the `NounStore` effectively is key to building robust and flexible SEMOSS components and scripts.

This guide covers the role of the `NounStore`, how to access and manipulate it from both Pixel and Java, and best practices for its usage.

## What is the NounStore?

-   **Central Data Hub**: The `NounStore` (an instance of `prerna.sablecc2.om.NounStore`) is essentially a key-value map associated with an `Insight`. It stores variables, data frames, intermediate results, and parameters that are passed between different parts of a Pixel script or between Pixel and Java Reactors.
-   **`NounMetadata`**: All data stored in the `NounStore` is wrapped in `NounMetadata` objects. A `NounMetadata` object contains:
    -   `value`: The actual data (e.g., a String, Integer, `ITableDataFrame`, List, Map).
    -   `nounType` (`PixelDataType` enum): Describes the semantic type of the data (e.g., `CONST_STRING`, `FRAME`, `LIST`).
    -   `opType` (List of `PixelOperationType` enums): Describes the operation that generated the noun or hints at its intended use (e.g., `NEW_FRAME`, `VIZ_DATA`).
-   **Scope**: The `NounStore` is scoped to an `Insight`. This means variables stored in it are generally accessible throughout the execution of a Pixel script within that insight and to any Reactors called by that script.

## Accessing and Interacting with the NounStore

### 1. In Pixel Scripts

Pixel interacts with the `NounStore` implicitly and explicitly.

**Implicit Interaction (Variable Assignment and Usage):**

-   When you assign a value to a variable in Pixel, it's stored in the `NounStore`.
-   When you use a variable (e.g., `$myVar`), Pixel retrieves its `NounMetadata` from the `NounStore`.

```pixel
// Implicitly stores "myMessage" in NounStore with value "Hello" and type CONST_STRING
myMessage = "Hello";

// Implicitly retrieves NounMetadata for "myMessage" from NounStore
LogInfo(message=[$myMessage]);

// Reactor outputs are also often stored in NounStore when assigned to variables
myFrame = Frame("MyDataTable"); // "myFrame" now stores NounMetadata of type FRAME
```

**Explicit Interaction (Using Specific Reactors):**

While direct `NounStore` manipulation is less common in typical Pixel scripts (as variable assignment handles most cases), certain reactors might expose more direct interaction if needed for advanced scenarios.

-   **`Var(variableName = [value])` or `SetNoun(variableName = [value])`**:
    These reactors explicitly create or update a variable in the `NounStore`. `Var()` is often preferred as it can also trigger UI updates if components are listening to that variable.
    ```pixel
    Var(myCounter = 10);
    SetNoun(anotherVar = [1, 2, 3]);
    ```

-   **`GetNoun(keys=["variableName"])`**:
    Retrieves the `NounMetadata` object associated with "variableName". This is rarely needed directly in Pixel as `$variableName` does this implicitly, but could be used for introspection.
    ```pixel
    // myVarNoun = GetNoun(keys=["myCounter"]);
    // Now myVarNoun holds the NounMetadata object for myCounter
    ```

-   **Checking if a Noun Exists**:
    You might use conditional logic based on whether a variable (noun) exists.
    ```pixel
    // Conceptual - actual check might be via a reactor like NounExists(key=["myOptionalVar"])
    // or by checking if GetNoun() returns a non-error/null type.
    // hasVar = NounExists(key=["myOptionalVar"]);
    // If ($hasVar) { ... }
    ```

### 2. In Java Reactors (Extending `AbstractReactor`)

Java Reactors have more direct access to the `NounStore` via the `Insight` object.

**Accessing the NounStore:**

-   `this.insight.getVarStore()`: Returns the `VarStore` instance, which is the `NounStore` associated with the current insight.
-   `this.qs`: In `AbstractReactor`, `this.qs` is often a reference to the `NounStore` that holds the *input* parameters for the current reactor execution. This is populated based on the Pixel call.

**Retrieving Input Parameters:**

-   **`organizeKeys()` and `this.keyValue`**: As covered in the "Writing Custom Reactors" guide, `organizeKeys()` (called on `this.keysToGet`) populates `this.keyValue` (a `Map<String, Object>`) with string representations of the input parameters. This is convenient for simple, single-value inputs.
    ```java
    // In constructor:
    // this.keysToGet = new String[]{"param1", "param2"};
    // In execute():
    // organizeKeys();
    // String param1Value = this.keyValue.get("param1");
    ```
-   **Accessing Full `NounMetadata` from `this.store` (or `this.qs`)**: For lists, complex objects, or to check data types, access the `NounStore` directly. `this.store` in `AbstractReactor` is the `NounStore` instance populated with the reactor's inputs.
    ```java
    // In execute():
    // organizeKeys(); // Populates this.store
    // NounMetadata paramNoun = this.store.getNoun("myListParam").getNoun(0); // Get first NounMetadata if it's a GenRowStruct
    // if (paramNoun.getNounType() == PixelDataType.LIST) {
    //     List<Object> myList = (List<Object>) paramNoun.getValue();
    //     // Process list
    // } else if (paramNoun.getNounType() == PixelDataType.FRAME) {
    //     ITableDataFrame inputFrame = (ITableDataFrame) paramNoun.getValue();
    //     // Process frame
    // }
    ```

**Storing/Returning Values (Output):**

-   Reactors return a single `NounMetadata` object from their `execute()` method. This object encapsulates the primary output of the reactor.
    ```java
    // In execute():
    // String result = "Operation successful";
    // return new NounMetadata(result, PixelDataType.CONST_STRING, PixelOperationType.SUCCESS_MESSAGE);

    // ITableDataFrame outputFrame = createMyFrame();
    // return new NounMetadata(outputFrame, PixelDataType.FRAME, PixelOperationType.NEW_FRAME);
    ```

**Storing Intermediate or Multiple Values in `Insight`'s `VarStore`:**

If a Reactor needs to make multiple distinct values or frames accessible to subsequent Pixel operations (not just as its direct return value), it can put them into the `Insight`'s main `VarStore`.

```java
// In execute():
// ITableDataFrame frame1 = ...;
// ITableDataFrame frame2 = ...;
// String statusMessage = "Processed two frames.";

// this.insight.getVarStore().put("outputFrameOne", new NounMetadata(frame1, PixelDataType.FRAME));
// this.insight.getVarStore().put("outputFrameTwo", new NounMetadata(frame2, PixelDataType.FRAME));

// The direct return value of the reactor could be a status or summary
// return new NounMetadata(statusMessage, PixelDataType.CONST_STRING);
// In Pixel, $outputFrameOne and $outputFrameTwo would then be available.
```

## Storing and Retrieving Various Data Types

-   **Primitives (String, Number, Boolean)**: Stored directly as the `value` in `NounMetadata`, with corresponding `PixelDataType` (e.g., `CONST_STRING`, `CONST_INT`, `CONST_DOUBLE`, `BOOLEAN`).
-   **Lists and Maps**: Can be stored as Java `List` or `Map` objects in `NounMetadata.value`, using `PixelDataType.LIST` or `PixelDataType.MAP`.
-   **DataFrames (`ITableDataFrame`)**: Stored as the `ITableDataFrame` instance itself in `NounMetadata.value`, with `PixelDataType.FRAME`.
-   **Custom Java Objects**: While possible, it's less common for general Pixel script interaction unless subsequent Java Reactors are designed to consume these specific custom objects. If returned to Pixel, they might be treated as opaque objects or their `toString()` representation.

## Practical Use Cases

1.  **Parameter Passing to Reactors**:
    -   Pixel: `MyReactor(paramA=["valueA"], paramB=[123]);`
    -   Java Reactor: `organizeKeys()` makes "valueA" and 123 available via `this.keyValue` or `this.store`.

2.  **Returning Single Values from Reactors**:
    -   Java Reactor: `return new NounMetadata("Success", PixelDataType.CONST_STRING);`
    -   Pixel: `resultStatus = MyReactor();` (`$resultStatus` holds "Success").

3.  **Returning DataFrames from Reactors**:
    -   Java Reactor: `return new NounMetadata(myDataFrame, PixelDataType.FRAME, PixelOperationType.NEW_FRAME);`
    -   Pixel: `newDataFrame = MyDataFrameCreatorReactor();`

4.  **Chaining Operations (Implicit NounStore Usage)**:
    The `PixelPlanner` heavily uses the `NounStore` (often via an internal context or by making the previous reactor's output the `curRow` for the next) to enable chaining.
    ```pixel
    resultFrame = Frame("MyTable") | Select(ColumnA) | Filter(ColumnA > 10);
    // Output of Frame("MyTable") is passed to Select, its output to Filter.
    ```
    In Java, if a Reactor is part of such a chain, `this.curRow` in `AbstractReactor` might hold the `NounMetadata` from the previous reactor in the chain.

5.  **Storing Intermediate Results for Later Use in a Script**:
    ```pixel
    intermediateFrame = Frame("SourceData") | SomeTransformation();
    // ... other operations ...
    finalResult = Frame($intermediateFrame) | AnotherTransformation();
    ```

6.  **Global or Session-Level Variables**:
    Variables set in the `NounStore` persist for the duration of the `Insight` session, allowing different Pixel scripts executed within the same insight (e.g., in different panels or subsequent executions) to potentially share data if designed to do so.

## Best Practices for NounStore Usage

-   **Clear Key Naming**: Use descriptive and consistent keys for nouns, especially for parameters passed to Reactors. Consider using `static final String` constants in your Java Reactors for these keys.
-   **Correct `PixelDataType`**: When creating `NounMetadata` in Java, always specify the accurate `PixelDataType`. This helps other Reactors and the SEMOSS system interpret the data correctly.
-   **Use `PixelOperationType`**: When returning `NounMetadata` from a Reactor, include relevant `PixelOperationType`s to signal UI updates or other system actions (e.g., `NEW_FRAME`, `VIZ_DATA`, `ERROR_MESSAGE`).
-   **Avoid Overwriting**: Be cautious about unintentionally overwriting existing nouns in the `NounStore` unless it's the desired behavior.
-   **Scope Awareness**: Remember that the `NounStore` is scoped to the `Insight`. Don't rely on it for persistent storage across different insights or user sessions without explicitly saving data to an engine or project asset.
-   **Clean Up (if necessary)**: For very large objects stored in the `NounStore` that are no longer needed within a long Pixel script or session, consider explicitly removing them if memory management becomes an issue (though SEMOSS often handles this as part of insight/session lifecycle).
    ```pixel
    // RemoveVar(name=["myTemporaryLargeFrame"]); // If such a reactor exists
    ```
-   **Input Validation in Reactors**: Reactors should validate the type and presence of expected nouns from the `NounStore` (via `organizeKeys()` or direct checks) to handle incorrect Pixel invocations gracefully.

The `NounStore` is a powerful mechanism that underpins data flow and state in SEMOSS. Using it effectively, both implicitly via Pixel variables and explicitly in Java Reactors, is essential for building complex and interactive SEMOSS solutions.
```

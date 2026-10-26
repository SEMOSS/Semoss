# Using the NounStore Effectively in SEMOSS

## Introduction

The `NounStore` is a critical component in SEMOSS, acting as the primary mechanism for data exchange and state management during Pixel script execution and within Java Reactors. Understanding how to interact with the `NounStore` effectively is key to building robust and flexible SEMOSS components and scripts.

This guide covers the role of the `NounStore`, how to access and manipulate it from both Pixel and Java, and best practices for its usage.

## What is the NounStore?

-   **Reactor Inputs**: The [NounStore](../../src/prerna/sablecc2/om/NounStore.java) groups named inputs into `GenRowStruct` rows containing `NounMetadata` values. A reactor accesses these inputs through `this.store`.
-   **`NounMetadata`**: Each typed input or result is wrapped in a `NounMetadata` object containing:
    -   `value`: The actual data (e.g., a String, Integer, `ITableDataFrame`, List, Map).
    -   `nounType` (`PixelDataType` enum): Describes the semantic type of the data (e.g., `CONST_STRING`, `FRAME`, `VECTOR`).
    -   `opType` (List of `PixelOperationType` enums): Describes the operation that generated the noun or hints at its intended use (e.g., `FRAME`, `OPERATION`, `ERROR`).
-   **Scope**: Reactor inputs live in the reactor's `NounStore`. Pixel variables and frame references shared across executions in an Insight live in its separate `VarStore`.

## Accessing and Interacting with the NounStore

### 1. In Pixel Scripts

Pixel arguments populate a reactor's `NounStore`; variable assignment and lookup use the Insight's `VarStore`.

**Implicit Interaction (Variable Assignment and Usage):**

-   When you assign a value to a variable in Pixel, it's stored in the Insight's `VarStore`.
-   When a variable supplies a reactor argument, its resolved value becomes part of that reactor's inputs.

```pixel
// Stores "myMessage" in the Insight's VarStore.
myMessage = "Hello";

// Passes the variable's value as the reactor's message input.
LogInfo(message=[$myMessage]);

// Assigned reactor outputs also become Insight variables.
myFrame = Frame("MyDataTable"); // "myFrame" now stores NounMetadata of type FRAME
```

### 2. In Java Reactors (Extending `AbstractReactor`)

Java reactors access their inputs and shared Insight variables separately.

**Accessing the NounStore:**

-   `this.insight.getVarStore()`: Returns the shared variable store for the current Insight.
-   `this.store`: Holds the reactor's named input parameters, populated from the Pixel call.

**Retrieving Input Parameters:**

-   **`organizeKeys()` and `this.keyValue`**: As covered in the "Writing Custom Reactors" guide, `organizeKeys()` uses `this.keysToGet` to populate `this.keyValue` (a `Map<String, String>`) with string representations of the input parameters. This is convenient for simple, single-value inputs.
    ```java
    // In constructor:
    // this.keysToGet = new String[]{"param1", "param2"};
    // In execute():
    // organizeKeys();
    // String param1Value = this.keyValue.get("param1");
    ```
-   **Accessing Full `NounMetadata` from `this.store`**: For lists, complex objects, or to check data types, access the `NounStore` directly. `this.store` in `AbstractReactor` is the `NounStore` instance populated with the reactor's inputs.
    ```java
    // In execute():
    // Check the input row exists and is nonempty before accessing its first noun.
    // NounMetadata paramNoun = this.store.getNoun("myListParam").getNoun(0); // Get first NounMetadata if it's a GenRowStruct
    // if (paramNoun.getNounType() == PixelDataType.VECTOR) {
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
    // return new NounMetadata(result, PixelDataType.CONST_STRING, PixelOperationType.SUCCESS);

    // ITableDataFrame outputFrame = createMyFrame();
    // return new NounMetadata(outputFrame, PixelDataType.FRAME, PixelOperationType.FRAME);
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

-   **Primitives (String, Number, Boolean)**: Stored directly as the `value` in `NounMetadata`, with corresponding `PixelDataType` (e.g., `CONST_STRING`, `CONST_INT`, `CONST_DECIMAL`, `BOOLEAN`).
-   **Lists and Maps**: Can be stored as Java `List` or `Map` objects in `NounMetadata.value`, using `PixelDataType.VECTOR` or `PixelDataType.MAP`.
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
    -   Java Reactor: `return new NounMetadata(myDataFrame, PixelDataType.FRAME, PixelOperationType.FRAME);`
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

6.  **Insight Variables**:
    Variables assigned in Pixel live in the Insight's `VarStore`. Subsequent Pixel executions within the same active Insight can reuse them. The reactor's `NounStore` holds inputs for that invocation.

## Best Practices for NounStore Usage

-   **Clear Key Naming**: Use descriptive and consistent keys for nouns, especially for parameters passed to Reactors. Consider using `static final String` constants in your Java Reactors for these keys.
-   **Correct `PixelDataType`**: When creating `NounMetadata` in Java, always specify the accurate `PixelDataType`. This helps other Reactors and the SEMOSS system interpret the data correctly.
-   **Use `PixelOperationType`**: Include operation types that describe the result, such as `FRAME`, `OPERATION`, `SUCCESS`, or `ERROR`.
-   **Avoid Overwriting**: Be cautious about unintentionally overwriting existing nouns in the `NounStore` unless it's the desired behavior.
-   **Scope Awareness**: Keep reactor inputs in the `NounStore` and shared execution variables in the Insight's `VarStore`. Persist data to an engine or project asset when it must survive the Insight or process.
-   **Lifecycle**: Release unused frames and temporary resources through their supported lifecycle operations. See [the Insight object](../concepts/insight_object.md) for execution state and Python namespace cleanup.
-   **Input Validation in Reactors**: Reactors should validate the type and presence of expected nouns from the `NounStore` (via `organizeKeys()` or direct checks) to handle incorrect Pixel invocations gracefully.

The `NounStore` is a powerful mechanism that underpins data flow and state in SEMOSS. Using it effectively, both implicitly via Pixel variables and explicitly in Java Reactors, is essential for building complex and interactive SEMOSS solutions.

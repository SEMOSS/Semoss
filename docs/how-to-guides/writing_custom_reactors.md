# How to Write a Custom Reactor in SEMOSS

## Introduction

Reactors are fundamental building blocks in SEMOSS that allow developers to extend the platform's capabilities by adding custom Java logic. They can be invoked via Pixel scripts and are essential for creating reusable functions, complex data transformations, interactions with external services, and much more. This guide provides a comprehensive overview of how to create, deploy, and use custom Reactors in your SEMOSS projects.

## What is a Reactor?

A Reactor is a Java class that implements the `prerna.reactor.IReactor` interface. More commonly, developers extend the `prerna.reactor.AbstractReactor` class, which provides default implementations for some methods in `IReactor`.

Reactors are designed to:
- Receive input parameters (nouns) from a Pixel script or another calling context.
- Perform some processing logic using these inputs.
- Return a result (output noun) back to the caller.
- Integrate seamlessly into the SEMOSS Pixel execution workflow.

## Core Concepts

### `IReactor` Interface and `AbstractReactor` Class

-   **`prerna.reactor.IReactor`**: The primary interface that all Reactors must implement. It defines the core methods for a Reactor's lifecycle and execution.
-   **`prerna.reactor.AbstractReactor`**: An abstract class that provides a convenient base for most custom Reactors. It handles common tasks like storing the `Insight` object and managing the `NounStore`. It's highly recommended to extend `AbstractReactor`.

### `NounStore`

-   The `NounStore` (accessible via `this.qs` or `this.insight.getNounStore()` if extending `AbstractReactor`) is a key-value store used to pass parameters into your Reactor and to retrieve results from other operations.
-   Nouns are identified by keys (strings).
-   Values can be various Java objects (strings, numbers, lists, maps, DataFrames, etc.).
-   When a Pixel script calls a Reactor with arguments, these arguments are typically placed into the `NounStore` using predefined or custom keys.

### `PixelPlanner` and `Insight`

-   **`PixelPlanner` (`this.planner` in `AbstractReactor`)**: Represents the execution plan for a series of Pixel operations. Reactors can add new operations (like other Reactors or utility functions) to the planner.
-   **`Insight` (`this.insight` in `AbstractReactor`)**: Represents the current user session and context, providing access to user information, project details, and the `NounStore`.

## Structure of a Custom Reactor

A typical custom Reactor Java class will have the following structure:

```java
package com.example.semoss.reactors; // Your package structure

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class MyCustomReactor extends AbstractReactor {

    // Optional: Define keys for your input nouns for clarity
    private static final String INPUT_PARAM_KEY = "inputParam";
    private static final String ANOTHER_PARAM_KEY = "anotherParam";

    public MyCustomReactor() {
        // Initialize any required instance variables
        // Define the input nouns (parameters) your reactor expects
        this.keysToGet = new String[]{INPUT_PARAM_KEY, ANOTHER_PARAM_KEY};
    }

    @Override
    public NounMetadata execute() {
        // 1. Organise and retrieve your inputs
        organizeKeys(); // Helper from AbstractReactor to process keysToGet
        String inputParamValue = this.keyValue.get(this.keysToGet[0]); // Example: get first param
        int anotherParamValue = 0;
        try {
            anotherParamValue = ((Number) this.keyValue.get(this.keysToGet[1])).intValue();
        } catch (Exception e) {
            // Handle potential casting errors or missing parameters
            throw new IllegalArgumentException("Second parameter ('" + ANOTHER_PARAM_KEY + "') must be a number.");
        }

        // 2. Perform your custom logic
        String resultString = "Processed: " + inputParamValue + " with number: " + anotherParamValue;
        // Example: Perform some calculation or transformation
        String processedResult = performComplexOperation(inputParamValue, anotherParamValue);

        // 3. Return the result
        // The NounMetadata constructor takes the result object and its data type
        return new NounMetadata(processedResult, PixelDataType.CONST_STRING);
        // Other PixelDataType options include:
        // PixelDataType.COLUMN, PixelDataType.FRAME, PixelDataType.BOOLEAN,
        // PixelDataType.INTEGER, PixelDataType.DOUBLE, PixelDataType.MAP, PixelDataType.LIST, etc.
    }

    // Optional: Helper method for your logic
    private String performComplexOperation(String text, int number) {
        // Replace with your actual complex logic
        return text.toUpperCase() + " - " + (number * 100);
    }

    @Override
    public String getDescription() {
        // Provide a human-readable description of what your reactor does.
        // This can be used for documentation or UI hints.
        return "This reactor demonstrates processing a string and a number input.";
    }

    // Starting from Semoss 2.9.0, this is the new method to define the reactor name.
    // For older versions, the reactor name was often inferred from the class name
    // or set via different mechanisms (e.g., within a project's properties or specific registration).
    // If this method is present, it will be used.
    // Ensure the returned name is how you want to call it in Pixel.
    @Override
    public String getReactorName() {
        return "MyCustomReactor"; // Or "MyReactor", "ProcessMyData", etc.
    }
}
```

### Key Methods to Implement/Override (when extending `AbstractReactor`)

1.  **Constructor (`public MyCustomReactor()`)**:
    *   Initialize any instance variables your Reactor might need.
    *   **Crucially, define `this.keysToGet`**: This string array lists the keys that your Reactor expects as input from the `NounStore`. The order matters if you retrieve them by index.

2.  **`execute()`**:
    *   This is the main method where your Reactor's logic resides.
    *   **`organizeKeys()`**: Call this helper method (from `AbstractReactor`) at the beginning. It populates `this.keyValue` (a `HashMap<String, Object>`) with the values for the keys specified in `this.keysToGet`. It also handles some default parameter processing.
    *   **Retrieve Inputs**: Access the input values from `this.keyValue.get(keyName)` or by using `this.curRow.get(index)` if you expect inputs in a specific order and `organizeKeys()` has populated `this.curRow`. Always perform necessary type checks and conversions.
    *   **Perform Logic**: Implement your custom data processing, calculations, external API calls, etc.
    *   **Return Result**: Create and return a `NounMetadata` object. The `NounMetadata` constructor takes two arguments:
        *   `value`: The actual result object (e.g., a `String`, `Integer`, `DataFrame`, `List`, `Map`).
        *   `type`: A `PixelDataType` enum value that accurately describes the type of the result (e.g., `PixelDataType.CONST_STRING`, `PixelDataType.FRAME`, `PixelDataType.BOOLEAN`).

3.  **`getDescription()`**:
    *   Return a `String` that describes what the Reactor does. This is useful for documentation and can be potentially used by UI components.

4.  **`getReactorName()`** (Recommended for modern SEMOSS versions):
    *   Return a `String` that defines the name by which this Reactor will be called in Pixel scripts. This provides explicit control over the Reactor's invocation name. If not overridden, the system might infer it from the class name, but this method is preferred for clarity.

### Example: Reactor that Adds Two Numbers

```java
package com.example.semoss.reactors;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AddNumbersReactor extends AbstractReactor {

    private static final String NUM1_KEY = "num1";
    private static final String NUM2_KEY = "num2";

    public AddNumbersReactor() {
        this.keysToGet = new String[]{NUM1_KEY, NUM2_KEY};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();

        Number num1Value = null;
        Number num2Value = null;

        // Retrieve and validate num1
        Object num1Obj = this.keyValue.get(this.keysToGet[0]);
        if (num1Obj instanceof Number) {
            num1Value = (Number) num1Obj;
        } else {
            try {
                num1Value = Double.parseDouble(num1Obj.toString());
            } catch (Exception e) {
                throw new IllegalArgumentException("Input '" + NUM1_KEY + "' must be a number. Provided: " + num1Obj);
            }
        }

        // Retrieve and validate num2
        Object num2Obj = this.keyValue.get(this.keysToGet[1]);
        if (num2Obj instanceof Number) {
            num2Value = (Number) num2Obj;
        } else {
            try {
                num2Value = Double.parseDouble(num2Obj.toString());
            } catch (Exception e) {
                throw new IllegalArgumentException("Input '" + NUM2_KEY + "' must be a number. Provided: " + num2Obj);
            }
        }

        // Perform calculation (handle potential double/integer arithmetic)
        double sum = num1Value.doubleValue() + num2Value.doubleValue();

        // If both inputs were integers and the sum is whole, return as integer
        if (num1Value instanceof Integer && num2Value instanceof Integer && sum == Math.floor(sum)) {
            return new NounMetadata((int) sum, PixelDataType.INTEGER);
        }
        return new NounMetadata(sum, PixelDataType.DOUBLE);
    }

    @Override
    public String getDescription() {
        return "This reactor adds two numbers provided as input.";
    }

    @Override
    public String getReactorName() {
        return "AddNumbers";
    }
}
```

## Deploying and Using Custom Reactors

As per the `SEMOSS/backend-training` repository's `README.md`:

1.  **Placement**:
    *   Compile your custom Reactor Java class(es).
    *   Place the resulting `.class` file(s) (or the `.java` file if your SEMOSS environment supports dynamic compilation, though `.class` is safer) into your project's directory structure. The typical location is:
        `PROJECT_ROOT/version/assets/java/<your_package_structure>/YourReactor.class`
        For example, if your project is `MyProject`, its ID is `00000000-0000-0000-0000-000000000000`, and your reactor is `com.example.semoss.reactors.MyCustomReactor`, you would place `MyCustomReactor.class` in:
        `/opt/semosshome/project/00000000-0000-0000-0000-000000000000/version/assets/java/com/example/semoss/reactors/MyCustomReactor.class`
        (The exact `PROJECT_ROOT` path depends on your SEMOSS installation's `BaseFolder`.)

2.  **Reloading Reactors**:
    *   After placing new or updated Reactor files, you need to tell SEMOSS to recognize them. This is done via the Pixel console (e.g., in an Insight or the Pixel console tool).
    *   Execute the following Pixel commands:
        ```pixel
        SetContext("<your-project-id>");
        ReloadInsightClasses("<your-project-id>");
        ```
        Replace `<your-project-id>` with the actual ID of your project. `SetContext` ensures operations are performed within the scope of your project, and `ReloadInsightClasses` clears any cached classloaders for that project and reloads classes from its `assets/java` directory.

3.  **Invoking in Pixel**:
    *   Once reloaded, you can call your custom Reactor in Pixel scripts using its defined name (from `getReactorName()` or inferred class name) and passing arguments.
    *   Arguments are typically passed as key-value pairs. The keys must match what the Reactor expects (defined in `this.keysToGet` in the Reactor's constructor).

    **Example Pixel Invocation for `AddNumbersReactor`**:
    ```pixel
    // Example 1: Direct invocation
    sumResult = AddNumbers(num1 = 10, num2 = 25);
    LogInfo(message = "The sum is: " + sumResult); // Output: The sum is: 35

    // Example 2: Using variables
    myFirstNum = 5.5;
    mySecondNum = 2.2;
    anotherSum = AddNumbers(num1 = myFirstNum, num2 = mySecondNum);
    LogInfo(message = "Another sum is: " + anotherSum); // Output: Another sum is: 7.7
    ```

    **Example Pixel Invocation for `MyCustomReactor`**:
    ```pixel
    processedText = MyCustomReactor(inputParam = "Hello SEMOSS", anotherParam = 7);
    LogInfo(message = "Processed text: " + processedText);
    // Output: Processed text: HELLO SEMOSS - 700
    ```

## Advanced Topics (Brief Overview)

-   **Returning DataFrames**: Reactors can create, manipulate, and return `IRawSelectWrapper` objects (which represent data frames or query results). Use `PixelDataType.FRAME` for the return type.
-   **Interacting with Engines**: Reactors can access database engines, storage engines, etc., via the `this.insight.getProject(<PROJECT_ID>).getEngine(<ENGINE_ID>)` or `Utility.getEngine(engineId)` methods to perform operations.
-   **Error Handling**: Use standard Java try-catch blocks. Throwing an `IllegalArgumentException` or `SemossPixelException` is common for input validation or operational errors.
-   **Pixel Operations within Reactors**: You can execute further Pixel scripts from within a Reactor using `this.planner.addPixelRecipe(String pixelScript)` and then retrieving results from the `NounStore`.
-   **Managing State**: Reactors are typically stateless. If state is needed across Reactor calls, store it in the `NounStore` or use SEMOSS's broader state management capabilities (e.g., project or user-level properties if appropriate).

## Best Practices

-   **Clear Naming**: Use descriptive names for your Reactor classes and their `getReactorName()` output.
-   **Explicit Input Keys**: Define constants for your input keys (`this.keysToGet`) for clarity and maintainability.
-   **Input Validation**: Always validate inputs within your `execute()` method (type, presence, format). Provide informative error messages.
-   **Precise Return Types**: Use the correct `PixelDataType` when creating your `NounMetadata` for the result.
-   **Modularity**: Keep Reactors focused on a specific task. For complex operations, consider breaking them into multiple, smaller Reactors.
-   **Documentation**: Write a clear `getDescription()` for your Reactor.
-   **Package Naming**: Use appropriate Java package names to organize your custom Reactors, especially if you have many.

This guide provides a foundational understanding of creating custom Reactors in SEMOSS. By leveraging this capability, developers can significantly extend and customize the SEMOSS platform to meet specific project requirements.

### Advanced Example: Reactors for Database CRUD Operations

Reactors can encapsulate complex database interactions, such as Create, Read, Update, and Delete (CRUD) operations, providing a clean API for your Pixel scripts. This approach promotes reusability and separates Java database logic from your Pixel workflows.

For instance, you might create reactors like `AddMovieReactor`, `UpdateGenreReactor`, or `DeleteMovieReactor`. These reactors would internally handle:
- Connecting to the appropriate database engine.
- Constructing and executing SQL `INSERT`, `UPDATE`, or `DELETE` statements (often using `PreparedStatement` for security and efficiency).
- Managing database transactions (e.g., `setAutoCommit(false)`, `commit()`, `rollback()`) if multiple operations need to be atomic.
- Performing necessary data validation before database operations.
- Returning meaningful success or error messages.

**Key Benefits:**
- **Abstraction**: Pixel scripts become simpler, calling high-level operations like `AddMovie(...)` instead of embedding complex SQL.
- **Maintainability**: Database logic is centralized in Java classes, making it easier to update and manage.
- **Testability**: Java reactors can be unit-tested more easily than complex Pixel scripts.
- **Security**: Using `PreparedStatement` within reactors helps prevent SQL injection vulnerabilities.

For detailed examples and explanations of how to implement such CRUD reactors, including Java code snippets and Pixel invocation examples for adding, updating, and deleting movie and genre data, please see the dedicated guide:
- [**Interacting with Databases - Performing CRUD with Reactors**](./interacting_with_databases.md#performing-crud-operations-with-custom-reactors)

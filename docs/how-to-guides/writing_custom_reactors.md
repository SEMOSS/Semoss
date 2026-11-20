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
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class MyCustomReactor extends AbstractReactor {

    // Optional: Define keys for your input nouns for clarity
    private static final String INPUT_PARAM_KEY = "inputParam";
    private static final String ANOTHER_PARAM_KEY = "anotherParam";

    public MyCustomReactor() {
        // Define the input nouns (parameters) your reactor expects
        this.keysToGet = new String[]{INPUT_PARAM_KEY, ANOTHER_PARAM_KEY};
        // 1 marks a key as required, 0 as optional
        this.keyRequired = new int[]{1, 1};
    }

    @Override
    public NounMetadata execute() {
        // 1. Organise and retrieve your inputs
        organizeKeys(); // Helper from AbstractReactor to process keysToGet
        String inputParamValue = this.keyValue.get(INPUT_PARAM_KEY);
        int anotherParamValue;
        try {
            // keyValue holds each input as text, so parse it into the type you need
            anotherParamValue = Integer.parseInt(this.keyValue.get(ANOTHER_PARAM_KEY));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Second parameter ('" + ANOTHER_PARAM_KEY + "') must be a whole number.");
        }

        // 2. Perform your custom logic
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

    // Describes the reactor for its MCP tool, Pixel help and the OpenAPI document
    @Override
    public ReactorDefinition getReactorDefinition() {
        return super.getReactorDefinition()
                .description("Converts text to upper case and appends a whole number multiplied by 100")
                .key(INPUT_PARAM_KEY, "The text to convert to upper case")
                .key(ANOTHER_PARAM_KEY, key -> key.type(Integer.class).description("The whole number to multiply by 100"))
                .output(String.class, "The upper case text, a dash, and the number times 100");
    }
}
```

### Key Methods to Implement/Override (when extending `AbstractReactor`)

1.  **Constructor (`public MyCustomReactor()`)**:
    *   Initialize any instance variables your Reactor might need.
    *   **Crucially, define `this.keysToGet`**: This string array lists the keys that your Reactor expects as input from the `NounStore`. The order matters if you retrieve them by index.
    *   **Define `this.keyRequired`**: An `int` array the same length as `keysToGet`, where `1` marks a key as required and `0` as optional. `organizeKeys()` throws an error naming every required key a call leaves out, and the reactor's MCP tool and OpenAPI contract mark the same keys required.

2.  **`execute()`**:
    *   This is the main method where your Reactor's logic resides.
    *   **`organizeKeys()`**: Call this helper method (from `AbstractReactor`) at the beginning. It populates `this.keyValue` (a `Map<String, String>`) with the value of each key specified in `this.keysToGet`, as text, and checks the keys `keyRequired` marks as required.
    *   **Retrieve Inputs**: Access the input values from `this.keyValue.get(keyName)` or by using `this.curRow.get(index)` if you expect inputs in a specific order and `organizeKeys()` has populated `this.curRow`. Parse each value into the type you need, such as `Integer.parseInt` or `Boolean.parseBoolean`, and validate it.
    *   **Perform Logic**: Implement your custom data processing, calculations, external API calls, etc.
    *   **Return Result**: Create and return a `NounMetadata` object. The `NounMetadata` constructor takes two arguments:
        *   `value`: The actual result object (e.g., a `String`, `Integer`, `DataFrame`, `List`, `Map`).
        *   `type`: A `PixelDataType` enum value that accurately describes the type of the result (e.g., `PixelDataType.CONST_STRING`, `PixelDataType.FRAME`, `PixelDataType.BOOLEAN`).

3.  **`getReactorDefinition()`**:
    *   Describe what the Reactor does, what each key means and its type, and what it returns. Agents read it through the reactor's MCP tool, Pixel help prints it, and the OpenAPI document publishes it. Keep it as the last method in the class. See [Describing a Reactor with `getReactorDefinition()`](#describing-a-reactor-with-getreactordefinition).

### Reactor Name in Pixel

A reactor's Pixel name is its class's simple name without the trailing `Reactor`. `AddNumbersReactor` is called as `AddNumbers`, and `MyCustomReactor` as `MyCustom`. Names match without regard to case, so `addnumbers` calls the same reactor. The same name is the reactor's MCP tool name.

### Example: Reactor that Adds Two Numbers

```java
package com.example.semoss.reactors;

import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AddNumbersReactor extends AbstractReactor {

    private static final String NUM1_KEY = "num1";
    private static final String NUM2_KEY = "num2";

    public AddNumbersReactor() {
        this.keysToGet = new String[]{NUM1_KEY, NUM2_KEY};
        this.keyRequired = new int[]{1, 1};
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
    public ReactorDefinition getReactorDefinition() {
        return super.getReactorDefinition()
                .description("Adds two numbers and returns their sum")
                .key(NUM1_KEY, key -> key.type(Double.class).description("The first number to add"))
                .key(NUM2_KEY, key -> key.type(Double.class).description("The second number to add"))
                .output(Double.class, "The sum of num1 and num2");
    }
}
```

## Describing a Reactor with `getReactorDefinition()`

`getReactorDefinition()` ([ReactorDefinition](../../src/prerna/reactor/ReactorDefinition.java)) is what a reactor tells everything that reads it about itself: the description and key text an AI agent sees in the reactor's MCP tool, the text Pixel help prints, and the contract the OpenAPI (swagger) document publishes for the SDKs. A reactor overrides it, calls `super.getReactorDefinition()`, and adds what it knows. `super` starts the definition from `keysToGet` and `keyRequired`, so it already lists every key in order.

| Call | What it sets |
| --- | --- |
| `.description(text)` | What the reactor does. Write it for an agent choosing between tools: what the reactor does, when to call it, and what it changes. |
| `.key(name, text)` | A key's description. `name` is the key from `keysToGet` or a `ReactorKeysEnum` constant. |
| `.key(name, key -> key.type(Integer.class).description(text))` | A key's type and description together. |
| `.key(name, key -> key.list(String.class).description(text))` | A key that takes a list, with the type of its items. |
| `.key(name, key -> key.required(true))` | Whether the key must be passed, for the MCP tool and the OpenAPI contract. |
| `.output(String.class, text)` | What the reactor returns. `.outputList(String.class, text)` declares a list. |
| `.noKeys()` | The reactor takes no keys on purpose. |

-   **Types**: A key or output takes `String`, `Integer`, `Long`, `Double`, `Boolean`, an enum, `Map`, `List`, a record, or `Object` for any value. In the MCP tool, `String` and enums become `string`, `Integer` and `Long` become `integer`, `Double` becomes `number`, `Boolean` becomes `boolean`, lists become `array`, and `Map` and records become `object`. A typed key lets an agent pass `25` rather than `"25"`.
-   **Metadata only**: `keysToGet` and `keyRequired` decide how `organizeKeys()` reads a call's inputs; the definition describes them. Every key the definition names is one in `keysToGet`.
-   **Placement**: `getReactorDefinition()` is the last method in the reactor class.

### Where the Definition Is Read

-   **MCP tools**: `MakePixelMCP` scans the app's compiled reactors and writes each one as a tool in the app's `assets/mcp/pixel_mcp.json`. The description becomes the tool's `description`, each key becomes an `inputSchema` property with its type and description, the required keys are listed in `required`, and the output becomes the tool's `outputSchema`. Agents and rooms that use the app read these tools to decide when to call the reactor and what to pass it. [MCP Tools and `_meta` Options](../agents/mcp_tools.md) covers the execution and display settings a reactor sets through `getMcpToolMetadata()`.
-   **Pixel help**: `AddNumbers --help;` returns the reactor's description, each input with its description, its MCP tool schema, and its response schema.
-   **OpenAPI (swagger)**: Every reactor in the SEMOSS `prerna` packages that declares a definition is published under `x-semoss-reactor` in the OpenAPI 3.1 document Monolith generates, and the TypeScript SDK in `semoss-ui/libs/sdk` is generated from that document. The description, key types and output become the reactor's input and output schemas there. `docs/openapi.md` in the Monolith repository covers generating the document and the SDK.

## Deploying and Using Custom Reactors

As per the `SEMOSS/backend-training` repository's `README.md`:

1.  **Placement**:
    *   Compile your custom Reactor Java class(es).
    *   Place the resulting `.class` file(s) (or the `.java` file if your SEMOSS environment supports dynamic compilation, though `.class` is safer) into your project's directory structure. The typical location is:
        `PROJECT_ROOT/version/assets/java/<your_package_structure>/YourReactor.class`
        For example, if your project is `MyProject`, its ID is `00000000-0000-0000-0000-000000000000`, and your reactor is `com.example.semoss.reactors.MyCustomReactor`, you would place `MyCustomReactor.class` in:
        `/opt/semosshome/project/00000000-0000-0000-0000-000000000000/version/assets/java/com/example/semoss/reactors/MyCustomReactor.class`
        (The exact `PROJECT_ROOT` path depends on your SEMOSS installation's `BaseFolder`.)

2.  **Loading the App and Compiling Reactors**:
    *   After placing new or updated Reactor files, tell SEMOSS to load your project and compile its reactors. This is done via the Pixel console (e.g., in an Insight or the Pixel console tool).
    *   Execute the following Pixel commands:
        ```pixel
        LoadApp(project = "<your-project-id>");
        CompileAppReactors(project = "<your-project-id>");
        ```
        Replace `<your-project-id>` with the actual ID of your project. `LoadApp` sets the insight's context to your project, so the insight finds the project's custom reactors, Python files, and MCP tools. `CompileAppReactors` clears the project's cached classes, compiles the reactors in its `assets/java` directory, and returns the compiler output. Add `release = true` to save the compiled reactors to cloud storage so every container picks them up; a release takes edit access to the project.

3.  **Publishing as MCP Tools**:
    *   To let agents call your reactors, generate the app's Pixel MCP tools after compiling:
        ```pixel
        MakePixelMCP(project = "<your-project-id>");
        ```
        `MakePixelMCP` writes each reactor's tool from its `getReactorDefinition()` into `assets/mcp/pixel_mcp.json` and commits the file to the app's git repository, so run it again after changing a definition. Pass `reactor = ["AddNumbers"]` to write only the reactors you list, or `package = "com.example.semoss.reactors"` to write the reactors in a package. It takes edit access to the project.

4.  **Invoking in Pixel**:
    *   Once compiled, you can call your custom Reactor in Pixel scripts using its Pixel name, the class name without `Reactor` (see [Reactor Name in Pixel](#reactor-name-in-pixel)), and passing arguments.
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
    processedText = MyCustom(inputParam = "Hello SEMOSS", anotherParam = 7);
    LogInfo(message = "Processed text: " + processedText);
    // Output: Processed text: HELLO SEMOSS - 700
    ```

## Advanced Topics (Brief Overview)

-   **Returning DataFrames**: Reactors can create, manipulate, and return `IRawSelectWrapper` objects (which represent data frames or query results). Use `PixelDataType.FRAME` for the return type.
-   **Interacting with Engines**: Reactors can access database engines, storage engines, etc., via the `this.insight.getProject(<PROJECT_ID>).getEngine(<ENGINE_ID>)` or `Utility.getEngine(engineId)` methods to perform operations.
-   **Error Handling**: Use standard Java try-catch blocks. Throwing an `IllegalArgumentException` or `SemossPixelException` is common for input validation or operational errors.
-   **Pixel Operations within Reactors**: You can execute further Pixel scripts from within a Reactor using `this.planner.addPixelRecipe(String pixelScript)` and then retrieving results from the `NounStore`.
-   **Managing State**: Reactors are typically stateless. If state is needed across Reactor calls, store it in the `NounStore` or use SEMOSS's broader state management capabilities (e.g., project or user-level properties if appropriate).

## Using Engines

A reactor gets an engine from `Utility`: `getDatabase`, `getModel`, `getStorage`, `getVectorDatabase`, `getFunctionEngine` and the other typed getters. Every call on the engine is checked: the reactor's call goes through when the signed in user holds the permission the method needs on that engine, or when the engine gave the app a permission that covers it. See [engine access](../platform_services/engine_access.md) for the level of each method.

| What the reactor does | What it needs on the engine |
| --- | --- |
| Reads: runs a query through the platform's query classes, asks a model, lists or downloads from a storage engine, searches a vector database | `READ_ONLY` |
| Writes, runs SQL as given, or holds the connection: `getConnection`, `getPreparedStatement`, `execQuery`, `insertData`, `copyToStorage`, `addDocument` | `EDIT` |

```java
IRDBMSEngine database = (IRDBMSEngine) Utility.getDatabase(databaseId);
try (Connection connection = database.getConnection();
        PreparedStatement insert = connection.prepareStatement("INSERT INTO ORDERS (ID, ITEM) VALUES (?, ?)")) {
    connection.setAutoCommit(false);
    insert.setString(1, orderId);
    insert.setString(2, item);
    insert.executeUpdate();
    connection.commit();
}
```

-   **An app that works for people with no access to the engine**: An app often inserts into a database or adds a file to a storage engine for people who cannot open that engine themselves. An owner of the engine gives the app a permission on it, on the engine's Access Control tab, in Apps, and the app's reactors then use the engine with that permission, whoever runs them. `READ_ONLY` covers the read methods, and `EDIT` adds the writes and the connection. The same section lists the apps an engine gives a permission to and takes one away; see [engine access](../platform_services/engine_access.md#giving-an-app-or-an-engine-a-permission-on-an-engine) for the endpoints behind it.
-   **A call the reactor has no permission for**: It throws with the engine id, the level the call needs and the method, such as `Engine <id> does not exist or neither the user nor the app has EDIT permission on it, which a custom reactor needs to call getConnection`.
-   **SQL for the engine's dialect**: `database.getQueryUtil()` builds SQL for the engine's type. The engine's user name, password and connection URL stay with the engine.
-   **The app's own project**: A reactor has `EDIT` on its own project, so it reads the project's insights database (`getInsightDatabase()`) and uses the project's Python process (`getProjectPyTranslator()`) for any user who can run it.

## Calling Web Services

Reactors call HTTP services through [HttpHelperUtility](../../src/prerna/security/HttpHelperUtility.java). Its `getRequest(url, headers, timeout)` and `postRequestStringBody(url, headers, body, contentType, timeout)` entry points take only JDK types and return the response body as text:

```java
Map<String, String> headers = Map.of("Accept", "application/json");
Duration timeout = Duration.ofSeconds(15);

String items = HttpHelperUtility.getRequest("https://api.example.com/items", headers, timeout);
String created = HttpHelperUtility.postRequestStringBody("https://api.example.com/items", headers,
        "{\"name\":\"example\"}", "application/json", timeout);
```

-   **Timeout**: The `Duration` limits how long the call waits to connect and for each read of the response. A null timeout waits for as long as the server stays silent.
-   **Content type**: The content type travels with the body, so it is passed as its own argument and stays out of the headers. A content type that names no charset is sent as UTF-8.
-   **Failed status**: A response outside 200 to 299 throws [HttpStatusException](../../src/prerna/security/HttpStatusException.java), an `IllegalArgumentException` that carries the status in `getStatusCode()`. A reactor can retry a 429 or a 503 and fail on the rest:

    ```java
    try {
        return HttpHelperUtility.getRequest(url, headers, timeout);
    } catch (HttpStatusException e) {
        if (e.getStatusCode() != 429 && e.getStatusCode() < 500) {
            throw e;
        }
    }
    ```

-   **No response**: A server that cannot be reached, or that stays silent past the timeout, throws an `IllegalArgumentException` whose cause is the `IOException`.
-   **Where a request can go**: Every request follows the admin's `BLACK_LIST_DOMAINS` and `WHITE_LIST_DOMAINS`, on the URL, on each redirect and on the addresses the host resolves to. A request a project's reactor sends to the cloud metadata service is refused. Localhost is open unless the admin blacklists it. A refused request throws an `IllegalArgumentException` that names the host. See the [project reactor sandbox](../platform_services/project_reactor_sandbox.md#http-requests).
-   **Error messages**: Both exceptions name the full URL in their message. When a key travels as a query parameter, build the message a reactor returns from the status code and the address without its query string.

## Running Work in Parallel

A reactor runs work on other threads, such as one model call per document, with [SignedInTasks](../../src/prerna/util/SignedInTasks.java). `SignedInTasks.newVirtualThreadExecutor()` returns an `ExecutorService` whose tasks run on virtual threads, and each task runs as the user signed in on the reactor's thread, so its engine calls go through with that user's permission or the app's, exactly as the reactor's own calls do:

```java
try (ExecutorService llmExecutor = SignedInTasks.newVirtualThreadExecutor()) {
    List<Future<ResponseMessage>> docCalls = new ArrayList<>();
    for (JsonObject doc : documents) {
        Room docRoom = RoomUtils.createRoomForStatelessAsk(docRoomKey, this.insight, modelEngine, "");
        InputMessage msg = InputMessage.builder(docRoom) /* prompt + the document */ .build();
        docCalls.add(llmExecutor.submit(() -> docRoom.ask(msg, modelEngine, null, false)));
    }
    for (Future<ResponseMessage> call : docCalls) {
        ResponseMessage response = call.get(timeout, TimeUnit.SECONDS);
        // merge the answers
    }
}
```

-   **Who a task runs as**: The user signed in on the thread that submits it, with the request's insight and job, its log context and, with `CHROOT_ENABLE` on, the user's folder for the files it opens. A thread from `Executors`, a parallel stream or `CompletableFuture` has no signed in user, so the engine refuses its calls.
-   **Closing**: Closing the executor, as the `try` does, waits for every task. A `get` that times out leaves its task running; to stop the rest when one answer fails, call `llmExecutor.shutdownNow()` before leaving the `try`. It interrupts the running tasks and cancels the waiting ones.
-   **Virtual or platform threads**: A virtual thread executor suits many tasks that spend their time waiting, such as model calls and web requests: it runs up to `SignedInTasks.VIRTUAL_PARALLEL` (256) of its tasks at once, and the server runs up to `SignedInTasks.VIRTUAL_THREADS` (4096) across every reactor. `SignedInTasks.newExecutor()` runs tasks on platform threads the server shares, for work that keeps a processor busy: up to `SignedInTasks.PLATFORM_PARALLEL` (16) at once, and up to `SignedInTasks.PLATFORM_THREADS` (64) across the server.
-   **How many wait**: Either executor holds the tasks past the ones it runs, up to `SignedInTasks.MOST_WAITING` (1024), and starts them in the order they were submitted. A task past these limits makes `submit` throw `RejectedExecutionException`.
-   **Failures**: A task's exception comes back from `get` as the cause of an `ExecutionException`.

## Best Practices

-   **Clear Naming**: Use descriptive class names. The class name without `Reactor` is the reactor's Pixel name and its MCP tool name.
-   **Explicit Input Keys**: Define constants for your input keys (`this.keysToGet`) for clarity and maintainability.
-   **Input Validation**: Always validate inputs within your `execute()` method (type, presence, format). Provide informative error messages.
-   **Precise Return Types**: Use the correct `PixelDataType` when creating your `NounMetadata` for the result.
-   **Modularity**: Keep Reactors focused on a specific task. For complex operations, consider breaking them into multiple, smaller Reactors.
-   **Documentation**: Describe the reactor, each of its keys, and its output in `getReactorDefinition()`. An agent picks and calls the reactor from that text alone.
-   **Package Naming**: Use appropriate Java package names to organize your custom Reactors, especially if you have many.

This guide provides a foundational understanding of creating custom Reactors in SEMOSS. By leveraging this capability, developers can significantly extend and customize the SEMOSS platform to meet specific project requirements.

### Advanced Example: Reactors for Database CRUD Operations

Reactors can encapsulate complex database interactions, such as Create, Read, Update, and Delete (CRUD) operations, providing a clean API for your Pixel scripts. This approach promotes reusability and separates Java database logic from your Pixel workflows.

For instance, you might create reactors like `AddMovieReactor`, `UpdateGenreReactor`, or `DeleteMovieReactor`. These reactors would internally handle:
- Connecting to the appropriate database engine, with the user's `EDIT` permission on it or one the engine gave the app (see [using engines](#using-engines)).
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

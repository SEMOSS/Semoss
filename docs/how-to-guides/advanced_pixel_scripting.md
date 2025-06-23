# Advanced Pixel Scripting Techniques

## Introduction

While basic Pixel commands are straightforward for simple data operations, SEMOSS's Pixel language offers more advanced capabilities that allow for complex logic, better script organization, and more robust error handling. This guide explores some of these advanced techniques to help you write more sophisticated and maintainable Pixel scripts.

Refer to the [Pixel Language Concepts](../concepts/pixel_language.md) for basic syntax and execution model.

## Effective Use of Variables and Scope

Variables in Pixel are dynamically typed and stored in the current `Insight`'s `VarStore`.

### Assigning and Using Variables

```pixel
// Basic assignment
myString = "Hello";
myNumber = 123;
myList = [1, 2, "apple"];
myFrame = Frame("MyDataTable");

// Using variables in subsequent commands
filteredFrame = Frame($myFrame) | Filter(Category == $myString);
result = MyReactor(inputParam=[$myNumber]);
```

### Variable Scope

-   **Insight Scope**: Variables created in a Pixel script are typically scoped to the current `Insight` session. They persist as long as the insight is active or until explicitly cleared.
-   **Reactor Scope**: When a Reactor is called, it receives its inputs from the `NounStore` (which is part of the `Insight`'s `VarStore`). Variables set within a Reactor's Java code directly into its local `NounStore` might not always automatically persist back to the main `Insight`'s `VarStore` unless the Reactor's output `NounMetadata` is assigned to a Pixel variable or the Reactor explicitly updates the shared `VarStore` (less common for simple value reactors).
-   **`Var()` Reactor**: To explicitly create or update a variable in the `Insight`'s `VarStore` that can be recognized by other parts of the system (like UI components that listen for variable changes), use the `Var()` reactor.
    ```pixel
    // Create or update a variable named 'dashboardFilterValue'
    Var(dashboardFilterValue = "Electronics");
    // Now $dashboardFilterValue can be used, and UI components might react to this change.
    ```

## Control Flow in Pixel

Pixel itself doesn't have traditional `for` or `while` loop syntax directly in the script. Control flow is primarily achieved through:

### 1. Conditional Execution (`If`, `Case`)

-   **`If()` Reactor**: Provides basic IF-THEN-ELSE logic.
    ```pixel
    // Simple If
    threshold = 50;
    value = 75;
    result = If(condition=[$value > $threshold], then=["High"], else=["Low"]);
    LogInfo(message="Result is: " + $result); // Result is: High

    // If without else (else implicitly returns null or previous context)
    // Note: The If reactor usually expects a 'then' and an 'else'.
    // For conditional execution of blocks, see "Conditional Pixel Execution" below.
    ```

-   **`Case()` Reactor** (or similar logic within `Map`): For multiple conditions.
    ```pixel
    // Using Map for CASE-like logic on a frame column
    myFrame = Frame("Orders") | Select(OrderID, OrderStatus) |
        Map(newColumn=["StatusDescription"],
            expression=["CASE WHEN OrderStatus == 'P' THEN 'Pending' WHEN OrderStatus == 'S' THEN 'Shipped' ELSE 'Unknown' END"]);
    Collect($myFrame);
    ```

### 2. Iteration and Looping Patterns

Direct looping in Pixel is less common. Iteration is often handled by:

-   **Reactors Operating on Lists**: Many reactors can take lists as input and process each item.
    ```pixel
    fileList = ["file1.csv", "file2.csv", "file3.csv"];
    // A hypothetical reactor that processes each file
    // ProcessFiles(filesToProcess=[$fileList]);
    ```

-   **Java Reactors**: Implementing looping logic within a custom Java Reactor is the standard way to perform complex iterations. The Reactor can then return a consolidated result.

-   **Pixel Sub-Scripts with `Repeat` (Conceptual / Advanced)**:
    While not a standard built-in loop, you could simulate loops for a fixed number of iterations using `RunPixel` or by chaining if a reactor supports iterative processing based on an input. Some specialized reactors might provide looping constructs.
    ```pixel
    // Conceptual example - check if a specific 'Repeat' reactor exists for your use case
    // Repeat(times=[5], pixel=["LogInfo(message='Iteration ' + GetIterationNumber());"]);
    ```
    *Actual looping constructs are rare in pure Pixel; Java reactors are preferred.*

-   **Processing Frame Rows**: Operations on frames (like `Map`, `Filter`) inherently iterate over rows. For row-by-row custom logic, you might use a Python/R script via `RunPy` or `RunR` if direct Pixel iteration is cumbersome.

### 3. Conditional Pixel Execution

For executing blocks of Pixel commands conditionally:

```pixel
// Use an If reactor to determine which block of subsequent pixels to run,
// often by setting a flag variable.
runPathA = If(condition=[$someCondition == true], then=[true], else=[false]);

// Path A
If($runPathA) {
    // Pixels to run if $runPathA is true
    LogInfo(message="Executing Path A");
    // ... more Path A pixels ...
}
// Path B (Else Path)
If(!$runPathA) { // Requires boolean negation support or alternative logic
    LogInfo(message="Executing Path B");
    // ... more Path B pixels ...
}
```
*Note: Direct block-level `If{...}` syntax as shown above is conceptual. Pixel typically achieves this by having the `If` reactor return a value that then determines subsequent reactor calls or by using multiple `IfReactor` calls to conditionally execute specific `RunPixel` commands for each block.*

A more robust way for conditional block execution is often to use `RunPixel` with conditional logic:
```pixel
myCondition = true; // Or derived from some operation
pixelToRun = If (
    condition = [$myCondition],
    then = ["LogInfo(message='Condition is true, running block A'); /* More pixels for A */"],
    else = ["LogInfo(message='Condition is false, running block B'); /* More pixels for B */"]
);
RunPixel(pixel=[$pixelToRun]);
```

## Error Handling Techniques

-   **Reactor-Level Error Handling**: Individual Reactors should ideally handle their own errors and can return `NounMetadata` with `PixelDataType.ERROR_MESSAGE`.
-   **Pixel Script Error Handling (Try-Catch like patterns)**:
    Pixel doesn't have a direct `try-catch` block in its syntax. However, you can achieve similar results using `IfError()` or by checking the type of returned Nouns.

    -   **`IfError()` Reactor**: (If available, behavior can vary by SEMOSS version)
        This reactor attempts to execute a primary Pixel expression. If it fails, it executes an alternative expression.
        ```pixel
        // result = IfError(
        // try = [ PixelCommandThatMightFail(param="value") ],
        // catch = [ FallbackPixelCommand(message="Failed, using fallback") ]
        // );
        ```
        *Consult specific SEMOSS documentation for `IfError` availability and exact syntax.*

    -   **Checking Noun Type**: After a reactor call, you can check the `PixelDataType` of the result.
        ```pixel
        operationResult = SomeReactorThatMightFail();
        // Assumes SomeReactorThatMightFail returns a Noun with specific type on success,
        // and ERROR_MESSAGE type on failure.
        // This requires more complex Pixel logic using IfReactor and GetNounType.
        // Example:
        // isError = If(condition=[GetNounType($operationResult) == "ERROR_MESSAGE"], then=[true], else=[false]);
        // If ($isError) {
        //    LogError(message="Operation failed: " + $operationResult);
        // } Else {
        //    ProcessResult($operationResult);
        // }
        ```
    -   **Pixel `try` Meta-Routine**: Some SEMOSS versions or extensions might offer a `try` meta-routine for error handling.
        ```pixel
        // result = try {
        //     PixelCommandThatMightFail();
        // } catch (Exception e) {
        //     LogError(message="Caught error: " + e);
        //     "DefaultErrorValue"; // Value to assign to result on error
        // };
        ```
        *This syntax is highly dependent on the SEMOSS version and specific parser capabilities.*

-   **Logging Errors**: Use `LogError(message=["My error message: " + $errorVariable]);` to record errors.

## Creating Reusable Pixel Scripts (`RunPixel`)

You can store Pixel scripts as strings (or in files/variables) and execute them using the `RunPixel` reactor. This is excellent for creating reusable functions or modules.

```pixel
// Define a reusable Pixel script as a string
myReusableScript = "
    inputVar = $input; // Assumes $input is set in the NounStore before calling RunPixel
    processedVar = $inputVar * 10;
    LogInfo(message='Processed var: ' + $processedVar);
    // The last expression's result is typically returned by RunPixel
    $processedVar;
";

// Set an input variable for the script
inputValue = 5;
Var(input = $inputValue); // Make 'input' available in the NounStore

// Execute the reusable script
scriptResult = RunPixel(pixel=[$myReusableScript]);
LogInfo(message="Result from reusable script: " + $scriptResult); // Output: Result from reusable script: 50
```
You can also store these scripts in project assets or database entries and retrieve them for execution.

## Working with Lists and Maps

Pixel supports list and map literals, and many reactors can consume or produce them.

```pixel
myList = [1, "apple", true, 3.14];
LogInfo(message="Third item: " + $myList[2]); // Accessing list items (may require specific list reactors)

myMap = {"name":"John Doe", "age":30, "city":"New York"};
LogInfo(message="Name: " + $myMap{"name"}); // Accessing map items (may require specific map reactors)

// Reactors for list/map manipulation might exist (e.g., ListSize, GetMapValue)
// listSize = ListSize(list=[$myList]);
// personName = GetMapValue(map=[$myMap], key=["name"]);
```
Direct indexing like `$myList[2]` or map access `$myMap{"name"}` in Pixel is highly dependent on the specific version and capabilities of the Pixel parser and `GreedyTranslation`. Often, dedicated reactors are used for list/map operations.

## Explicit NounStore Interaction

While most interactions are implicit (Reactors read from `NounStore`, results are put back), you can sometimes interact more directly, especially for debugging or complex state management.

-   **`GetNoun(keys=["myVar"])`**: Retrieves a noun from the `NounStore`.
-   **`SetNoun(myVar=["someValue"])`**: Sets a noun. (Similar to `Var()` but `Var()` often has more specific UI update implications).

## Best Practices for Advanced Pixel Scripting

1.  **Modularity**: Break down complex tasks into smaller, reusable Pixel scripts or custom Java Reactors.
2.  **Use Variables**: Store intermediate results in variables for clarity and reusability.
3.  **Parameterize Scripts**: When using `RunPixel` for reusable scripts, pass inputs via variables set in the `NounStore` rather than hardcoding values within the script string.
4.  **Logging**: Use `LogInfo()`, `LogWarning()`, and `LogError()` extensively for debugging and tracking script execution.
5.  **Error Handling**: Plan for potential errors, especially when dealing with external data or services. Use conditional logic or error-checking reactors if available.
6.  **Comments**: Liberally comment your Pixel scripts to explain logic, especially for complex sections.
7.  **Prefer Java for Complex Logic**: For very complex loops, data structures, or algorithms, implement them in a Java Reactor for better performance, testability, and maintainability.
8.  **Understand Reactor Signatures**: Know what inputs each Reactor expects (keys and data types) and what it returns.
9.  **Test Incrementally**: Build and test complex scripts piece by piece.
10. **Debug with `Collect()` and `Log` Reactors**: `Collect(frameOrVar)` is invaluable for inspecting the state of frames or variables at different points in your script.

By applying these advanced techniques, you can harness the full power of the Pixel language to build sophisticated data workflows and applications within SEMOSS.
```

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

## Reactor Outputs and `NounMetadata`

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

## Reactor Results and UI Interaction

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

# Working with DataFrames in SEMOSS (Pixel & Java)

## Introduction to DataFrames in SEMOSS

In SEMOSS, a "Frame" is a fundamental data structure that represents a table of data, similar to a DataFrame in Pandas or R, or a table in a relational database. Frames are central to nearly all data manipulation and analysis tasks performed within the platform. They provide a consistent way to work with data regardless of its original source.

SEMOSS supports various types of frames, including:
-   Frames directly representing tables in relational databases.
-   Frames holding data from NoSQL stores or flat files.
-   In-memory frames for intermediate calculations and transformations (e.g., `H2Frame`, `NativeFrame`, `TinkerFrame` for graph data).
-   Wrappers around external data structures (e.g., `PandasFrame`, `RDataTable`).

This guide focuses on how to create, manipulate, and utilize these frames using both Pixel scripts and Java Reactors.

## Creating DataFrames

DataFrames can be created in several ways using Pixel.

### 1. From Existing Database Engine Tables/Views

The most common way is to reference an existing table or view within a configured Database Engine.

```pixel
// First, set the context to your database engine
Database(database=["<YOUR_DATABASE_ENGINE_ID>"]);

// Create a frame variable pointing to a table
myDataTable = Frame("<TABLE_OR_VIEW_NAME>");

// You can immediately perform operations on it
activeCustomers = Frame("<TABLE_OR_VIEW_NAME>") | Filter(Status == "Active");
Collect(activeCustomers);
```
This creates a virtual frame. Data is typically not loaded into SEMOSS memory until an operation like `Collect()` is called or the frame is used in a visualization that requires data retrieval.

### 2. From Importing Files

SEMOSS can create frames by importing data from various file formats.

```pixel
// Import from a CSV file (path can be local to SEMOSS server or a URL)
csvFrame = Import(filePath=["/path/to/data.csv"]) | AsFrame(frameName=["MyCsvData"]);
Collect(csvFrame);

// Import from an Excel file
excelFrame = Import(filePath=["/path/to/data.xlsx"], sheetName=["Sheet1"]) | AsFrame(frameName=["MyExcelData"]);
Collect(excelFrame);

// Specify data types during import
typedFrame = Import(filePath=["/path/to/data.csv"],
                   dataTypes={"columnA":"TEXT", "columnB":"DOUBLE", "columnC":"DATE:MM/dd/yyyy"}) |
             AsFrame(frameName=["MyTypedData"]);
Collect(typedFrame);
```
The `AsFrame()` reactor converts the imported data into a usable frame structure.

### 3. As a Result of Queries

Any query that returns tabular data results in a frame.

```pixel
queryFrame = Query("SELECT Name, Age, City FROM Customers WHERE Country = 'USA'");
Collect(queryFrame);

// Chained operations also result in new frames
processedFrame = Frame("Customers") | Select(Name, City) | Filter(Age > 30);
Collect(processedFrame);
```

### 4. Creating an Empty Frame and Adding Data

You can define an empty frame and then populate it, often used for building frames programmatically.

```pixel
// Create an empty frame, optionally specifying its type (e.g., H2_DB for in-memory H2)
newFrame = CreateFrame(frameType=["H2_DB"], override=[true]) |
           AddColumn(columnName=["ID"], dataType=["INTEGER"]) |
           AddColumn(columnName=["ProductName"], dataType=["TEXT"]) |
           AddColumn(columnName=["Price"], dataType=["DOUBLE"]);

// Add data (can be from lists, other frames, etc.)
// Example: Adding literal data
dataToAdd = [
    [1, "Laptop", 1200.00],
    [2, "Mouse", 25.00],
    [3, "Keyboard", 75.00]
];
newFrame = newFrame | Append(newFrame, dataToAdd, headers=["ID", "ProductName", "Price"]);
Collect(newFrame);
```

### Creating DataFrames in Java (Conceptual)

While Pixel is the primary way users interact with frames, Java Reactors often create or manipulate frames internally.
-   They might instantiate concrete `ITableDataFrame` implementations (e.g., `H2Frame`, `NativeFrame`).
-   Populate them with data from external sources, calculations, or by transforming other frames.
-   Add them to the `NounStore` (usually via `this.insight.getVarStore().put(...)`) to make them accessible to subsequent Pixel commands.

```java
// Conceptual Java snippet within a Reactor's execute() method
// import prerna.ds.rdbms.h2.H2Frame;
// import prerna.om.HeadersDataRow;
// import java.util.List;
// import java.util.ArrayList;

// H2Frame myJavaFrame = new H2Frame("myNewJavaFrame");
// myJavaFrame.addHeaders(new String[]{"ColA", "ColB"});
// List<Object[]> frameData = new ArrayList<>();
// frameData.add(new Object[]{"val1", 100});
// frameData.add(new Object[]{"val2", 200});
// for(Object[] row : frameData) {
//    myJavaFrame.addRow(new HeadersDataRow(myJavaFrame.getHeaders(), row));
// }
// this.insight.getVarStore().put("myJavaFrameVar", new NounMetadata(myJavaFrame, PixelDataType.FRAME));
// return new NounMetadata(myJavaFrame, PixelDataType.FRAME, PixelOperationType.NEW_FRAME);
```

## Common DataFrame Operations in Pixel

Pixel provides a rich set of operations for manipulating DataFrames. Many of these are implemented as Reactors.

### 1. Selecting Columns (`Select`)

```pixel
// Select specific columns
selectedFrame = Frame("MyOriginalFrame") | Select(ColumnA, ColumnB, ColumnC);

// Select columns and rename them (aliasing)
aliasedFrame = Frame("MyOriginalFrame") | Select(Product_Name_From_Source AS ProductName, UnitPrice AS Price);
```

### 2. Filtering Rows (`Filter`)

```pixel
// Simple filter
filteredFrame = Frame("MyDataFrame") | Filter(Age > 30);

// Multiple conditions (AND implicit, or use &&)
filteredFrame = Frame("MyDataFrame") | Filter(Category == "Electronics" && Price < 500);

// OR condition
filteredFrame = Frame("MyDataFrame") | Filter(Status == "Active" || Status == "Pending");

// Using variables in filters
targetCategory = "Books";
filteredFrame = Frame("MyDataFrame") | Filter(Category == $targetCategory);
```

### 3. Adding or Modifying Columns (`Map`)

The `Map` reactor is versatile for adding new columns or transforming existing ones based on expressions.

```pixel
// Add a new column based on an existing one
frameWithNewCol = Frame("SalesData") | Map(newColumn=["DiscountedPrice"], existingColumn=["Price"], expression=["Price * 0.9"]);

// Modify an existing column (overwrite)
frameModifiedCol = Frame("SalesData") | Map(newColumn=["Price"], existingColumn=["Price"], expression=["Price * 1.1"]); // 10% price increase

// Conditional logic (CASE statement style)
frameWithCategoryGroup = Frame("Products") |
    Map(newColumn=["PriceCategory"], expression=["CASE WHEN Price > 1000 THEN 'High' WHEN Price > 100 THEN 'Medium' ELSE 'Low' END"]);
```

### 4. Joining DataFrames (`Join`)

```pixel
// Assuming Employees and Departments frames exist
// Join on DepartmentID from Employees and ID from Departments
joinedFrame = Join([Employees.DepartmentID], [Departments.ID]){left=Frame("Employees"), right=Frame("Departments")} |
              Select(Employees.Name, Departments.DepartmentName, Employees.Salary);

// Different join types can often be specified, though syntax might vary
// or be handled by more complex query structures if not directly in Join reactor.
// Example: Left Join (conceptual, specific Join reactor might have a 'joinType' param)
// joinedFrame = Join([Employees.DepartmentID], [Departments.ID], joinType=["leftouter"]){left=Frame("Employees"), right=Frame("Departments")};
```

### 5. Grouping and Aggregating Data (`GroupBy`)

```pixel
// Calculate total sales per category
salesSummary = Frame("SalesData") |
               Select(Category, Sum(SalesAmount)) |
               GroupBy(Category);

// Multiple aggregations
productStats = Frame("SalesData") |
               Select(Product, Count(OrderID) AS NumberOfOrders, Avg(SalesAmount) AS AverageSale) |
               GroupBy(Product);
```

### 6. Sorting Data (`OrderBy`)

```pixel
// Sort by SalesAmount in descending order
sortedSales = Frame("SalesData") | OrderBy(SalesAmount DESC);

// Sort by Category ascending, then Price descending
sortedProducts = Frame("ProductData") | OrderBy(Category ASC, Price DESC);
```

## Accessing DataFrame Metadata in Pixel

You can retrieve metadata about a frame's structure.

```pixel
// Get column names and types for a frame
frameMetadata = GetFrameMetamodel(frame=["MyDataFrame"]);
Collect(frameMetadata); // Displays as a frame with columns like 'col_name', 'data_type', etc.

// Get a list of all frames currently available (e.g., in the insight or database)
availableFrames = GetDatabaseFrames(); // If database context is set
Collect(availableFrames);
```

## Working with DataFrames in Java Reactors

Java Reactors frequently interact with DataFrames.

### How DataFrames are Passed to Reactors

-   If a Pixel script passes a frame variable to a Reactor (e.g., `MyReactor(inputFrame=[$myFrameVar]);`), the `NounMetadata` for that input in the Reactor's `NounStore` will have `PixelDataType.FRAME`.
-   The Reactor can retrieve it:
    ```java
    // In execute() method of a Reactor
    // organizeKeys(); // if "inputFrame" is in keysToGet
    // NounMetadata frameNoun = this.getNoun("inputFrame"); // Or from this.curRow if piped
    // if (frameNoun != null && frameNoun.getNounType() == PixelDataType.FRAME) {
    //     ITableDataFrame dataFrame = (ITableDataFrame) frameNoun.getValue();
    //     // Now you can work with dataFrame
    // }
    ```

### Iterating Over DataFrame Rows

-   Once you have an `ITableDataFrame` instance, you can iterate through its data. The most common way is using an `IRawSelectWrapper` obtained by querying the frame.

    ```java
    // ITableDataFrame dataFrame = ...;
    // SelectQueryStruct qs = new SelectQueryStruct();
    // qs.addSelector(new QueryColumnSelector("ColumnA")); // Select specific columns
    // qs.addSelector(new QueryColumnSelector("ColumnB"));
    // IRawSelectWrapper iterator = null;
    // try {
    //     iterator = dataFrame.query(qs); // Or dataFrame.iterator() for all data (less efficient for large frames)
    //     while (iterator.hasNext()) {
    //         Object[] row = iterator.next().getValues();
    //         String colAValue = row[0].toString();
    //         int colBValue = ((Number) row[1]).intValue();
    //         // Process row data
    //     }
    // } catch (Exception e) {
    //     // Handle exceptions
    // } finally {
    //     if (iterator != null) {
    //         try {
    //             iterator.close();
    //         } catch (IOException e) {
    //             // Log error
    //         }
    //     }
    // }
    ```

### Creating/Modifying DataFrames in Java

-   Reactors can construct new frames (e.g., `H2Frame`, `NativeFrame`), add columns, and populate rows.
-   They can also take an existing frame, transform it, and return a new frame or modify the existing one (if the frame type supports in-place modification and it's safe to do so).
-   See the conceptual Java example in "Creating DataFrames in Java" above.

### Returning DataFrames from Reactors

-   To return a DataFrame, wrap the `ITableDataFrame` instance in `NounMetadata` with `PixelDataType.FRAME`. It's also good practice to include a `PixelOperationType` like `NEW_FRAME` or `FRAME_DATA_CHANGE`.

    ```java
    // ITableDataFrame resultFrame = ...;
    // return new NounMetadata(resultFrame, PixelDataType.FRAME, PixelOperationType.NEW_FRAME);
    ```
-   This makes the returned frame available for assignment to a Pixel variable or for further piping in the Pixel script.

## Overview of Different Frame Types

SEMOSS uses various `ITableDataFrame` implementations, each suited for different scenarios:
-   **`H2Frame`**: An in-memory frame backed by an H2 database instance. Good for performance with SQL-like querying capabilities on already loaded data.
-   **`NativeFrame`**: A pure Java object-based frame. Simpler for smaller datasets, less overhead than H2Frame.
-   **`TinkerFrame`**: Represents graph data using Apache TinkerPop, allowing Gremlin queries.
-   **`PandasFrame` / `RDataTable`**: Wrappers around Python Pandas DataFrames or R data.tables, enabling operations in those respective environments.
-   **Engine-Specific Frames**: When you do `Frame("MyDatabaseTable")`, the object you get might be a specific frame type tied to that database engine (e.g., `PostgresFrame`), which translates Pixel operations into SQL for that database.

The choice of frame type can impact performance, memory usage, and the types of operations available. Often, SEMOSS manages frame type conversions implicitly (e.g., data from a query might be loaded into an `H2Frame` for further processing).

## Best Practices

-   **Minimize Data Transfer**: When working with large database tables, use filters and selectors in Pixel to retrieve only the data you need, rather than loading entire tables into memory unnecessarily.
-   **Use Appropriate Frame Types**: For extensive in-memory manipulation in Java, `H2Frame` can be efficient. For simpler tasks or smaller data, `NativeFrame` might suffice.
-   **Close Iterators**: Always close `IRawSelectWrapper` iterators in a `finally` block to release resources.
-   **Variable Scoping**: Understand that frames stored in Pixel variables are typically scoped to the current `Insight` or `PixelRunner` session.
-   **Saving Frames**: If you create or modify a frame and want to persist it, use `SaveFrame(frame=[myFrameVar], frameName=["NewOrExistingTableNameInDB"]);` to write it to the current database engine context.

Working with DataFrames is a core part of developing with SEMOSS. By mastering Pixel operations and understanding how to interact with frames in Java, you can build powerful and flexible data processing solutions.
```

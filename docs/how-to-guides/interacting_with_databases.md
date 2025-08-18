# Interacting with Databases in SEMOSS

## Introduction

SEMOSS provides powerful capabilities for connecting to, querying, and managing data across a wide variety of database systems and data sources. This is primarily achieved through its **Engine Abstraction** layer and the **Pixel** query language. This guide will walk you through common database interactions, focusing on CRUD (Create, Read, Update, Delete) operations using Pixel.

For detailed information on specific engine types (like relational databases, NoSQL stores, data warehouses, etc.) and their unique configurations, please refer to the main documentation on [Engine Abstraction](../concepts/engine_abstraction.md) and specific [Database Engine types](../engines/database_engines.md).

## Core Concepts

### Database Engines

-   In SEMOSS, a **Database Engine** (often referred to simply as an "Engine") is a configured connection to a specific data source. This could be a relational database (like PostgreSQL, MySQL, SQL Server), a data warehouse, a NoSQL database, a flat-file store, or even a specialized data API.
-   Engines are typically created and configured by administrators or users with appropriate permissions, either through the SEMOSS UI or via administrative Pixel commands.
-   Each engine has a unique ID (App ID) that you use to refer to it in Pixel scripts.

### Frames

-   A **Frame** in SEMOSS is an abstraction for a table-like data structure. It can represent a table in a relational database, a collection, a CSV file, or the result of a query.
-   Frames are central to data manipulation in SEMOSS. Pixel operations often create, transform, or consume frames.
-   Frames can be materialized (stored in an engine) or virtual (representing a query to be executed).

### Pixel Language

-   **Pixel** is SEMOSS's primary language for data access, manipulation, and orchestration. It provides a unified syntax for interacting with diverse data engines.
-   Pixel scripts are used to perform queries, transformations, create visualizations, and execute custom logic (like Reactors).

## Connecting to a Database (Setting Context)

Before you can interact with a specific database, you usually need to set the context to the Database Engine you want to work with.

```pixel
// Set the current database engine context
Database(database="<YOUR_DATABASE_ENGINE_ID>");
// Or, if it's a project-specific engine:
// Project(project="<YOUR_PROJECT_ID>");
// Database(database="<YOUR_PROJECT_DATABASE_ENGINE_ID>");
```
Replace `<YOUR_DATABASE_ENGINE_ID>` with the actual ID of your configured database engine. Once the context is set, subsequent Pixel commands will operate on this database by default unless another engine is specified.

## Reading Data (SELECT Queries)

Reading data is one ofthe most common database operations. Pixel uses a SQL-like syntax for querying.

### Basic Select

```pixel
// Select all columns from a table (frame)
frameData = Frame("<TABLE_OR_FRAME_NAME>") | SelectAll();
Collect(frameData);

// Select specific columns
frameData = Frame("<TABLE_OR_FRAME_NAME>") | Select(<COLUMN_1>, <COLUMN_2>);
Collect(frameData);
```

### Filtering Data (WHERE Clause)

```pixel
// Filter rows based on a condition
frameData = Frame("<TABLE_OR_FRAME_NAME>") | Filter(<COLUMN_NAME> == "<VALUE>");
Collect(frameData);

// Multiple conditions (AND)
frameData = Frame("<TABLE_OR_FRAME_NAME>") | Filter(<COLUMN_1> > 100 && <COLUMN_2> == "Active");
Collect(frameData);

// Multiple conditions (OR) - ensure parentheses for clarity if needed
frameData = Frame("<TABLE_OR_FRAME_NAME>") | Filter(<COLUMN_1> == "USA" || <COLUMN_1> == "Canada");
Collect(frameData);

// Using different operators: >, <, >=, <=, !=, LIKE, etc.
frameData = Frame("<TABLE_OR_FRAME_NAME>") | Filter(<COLUMN_NAME> LIKE "%pattern%");
Collect(frameData);
```

### Sorting Data (ORDER BY)

```pixel
// Sort by a single column (ascending by default)
frameData = Frame("<TABLE_OR_FRAME_NAME>") | Select(<COLUMN_1>, <COLUMN_2>) | OrderBy(<COLUMN_1>);
Collect(frameData);

// Sort descending
frameData = Frame("<TABLE_OR_FRAME_NAME>") | Select(<COLUMN_1>, <COLUMN_2>) | OrderBy(<COLUMN_1> DESC);
Collect(frameData);

// Sort by multiple columns
frameData = Frame("<TABLE_OR_FRAME_NAME>") | Select(<COLUMN_1>, <COLUMN_2>, <COLUMN_3>) | OrderBy(<COLUMN_1> ASC, <COLUMN_2> DESC);
Collect(frameData);
```

### Aggregating Data (GROUP BY)

```pixel
// Count rows by category
frameData = Frame("<TABLE_OR_FRAME_NAME>") | Select(<CATEGORY_COLUMN>, Count(<VALUE_COLUMN>)) | GroupBy(<CATEGORY_COLUMN>);
Collect(frameData);

// Sum, Average, Min, Max
frameData = Frame("<TABLE_OR_FRAME_NAME>") | Select(<GROUP_COLUMN>, Sum(<NUMERIC_COLUMN>), Avg(<NUMERIC_COLUMN>)) | GroupBy(<GROUP_COLUMN>);
Collect(frameData);
```

### Joining Data

Pixel supports various join types. The exact syntax can depend on the underlying engine's capabilities and how frames are structured. A common way is using `Join`.

```pixel
// Assuming frame1 and frame2 are already defined or are table names
// Join( [<FRAME1_COLUMN_1>, <FRAME1_COLUMN_2>], [<FRAME2_COLUMN_1>, <FRAME2_COLUMN_2>] ){
//      <FRAME_1_ALIAS> = <FRAME1_NAME_OR_VARIABLE>,
//      <FRAME_2_ALIAS> = <FRAME2_NAME_OR_VARIABLE>
// } | Select(...) | Filter(...);

// Example:
leftFrame = Frame("Employees");
rightFrame = Frame("Departments");

joinedData = Join([Employees.DepartmentID], [Departments.ID]){left=leftFrame, right=rightFrame} |
             Select(Employees.Name, Departments.DepartmentName);
Collect(joinedData);

// You can also specify join types (e.g., INNER, LEFT_OUTER, RIGHT_OUTER, FULL_OUTER)
// The exact syntax for specifying join type in the Join() function might vary
// or be inferred. Often, it's part of a more general query structure.
// A common pattern for explicit joins:
// Query("
//    SELECT E.Name, D.DepartmentName
//    FROM Employees E INNER JOIN Departments D ON E.DepartmentID = D.ID
// ") | Collect();
// (This uses raw query execution, see section below)
```
*Self-Service Joins*: In many SEMOSS interfaces, joins are also configured visually or through UI elements that generate underlying Pixel or SQL.

### Limiting Results

```pixel
// Get the top 10 rows
frameData = Frame("<TABLE_OR_FRAME_NAME>") | SelectAll() | Limit(10);
Collect(frameData);
```

## Creating Data / Inserting Rows

### Creating a New Frame and Inserting Data

You can create new frames (which might translate to new tables in some engines if materialized) and populate them.

```pixel
// Create an empty frame with a defined structure
newFrame = CreateFrame(frameType=[<ENGINE_TYPE>], override=[true]) |
           AddColumn(<COLUMN_1_NAME>, <COLUMN_1_TYPE>) |
           AddColumn(<COLUMN_2_NAME>, <COLUMN_2_TYPE>);

// Insert data (example with literal values)
// The exact syntax for bulk insert or row-by-row might vary.
// Often, data is inserted by importing from other sources or frames.
// A common pattern for adding data to an existing frame:
dataToInsert = [
    [<COL1_VAL_A1>, <COL2_VAL_A1>],
    [<COL1_VAL_A2>, <COL2_VAL_A2>]
];
// Assuming newFrame is a frame variable from CreateFrame or an existing frame
newFrame = newFrame | Append(newFrame, dataToInsert, headers=[<COLUMN_1_NAME>, <COLUMN_2_NAME>]);
// Or, more commonly through import:
// newFrame = Import(file="path/to/data.csv") | AsFrame(frameName="MyNewTable");
// SaveFrame(frame=newFrame, saveToEngine=["<YOUR_DATABASE_ENGINE_ID>"]); // To persist it
```

### Importing Data into a New or Existing Table

Importing data from files (CSV, Excel, etc.) or other frames is a very common way to create and populate tables.

```pixel
// Import a CSV file into a new frame, then save it as a table in the database
importedData = Import(filePath="path/to/your/file.csv") |
               AsFrame(frameName="NewTableFromCSV", override=[true]);
SaveFrame(frame=importedData); // Saves to the current context database

// If you need to specify data types during import:
// importedData = Import(filePath="path/to/your/file.csv",
//                      dataTypes={"ColumnA":"TEXT", "ColumnB":"NUMBER"}) | ...
```

## Updating Data

Updating existing records in a database can be done in a few ways, depending on the engine's capabilities and the nature of the frame.

### Direct Update (If Supported by Engine/Frame Type)

Some engines might support direct `UPDATE` statements via Pixel, especially for frames that directly map to database tables.

```pixel
// Conceptual example - actual syntax might vary or require specific frame types
// FrameUpdate(frame="<TABLE_NAME>") |
//    Set(column="<COLUMN_TO_UPDATE>", value="<NEW_VALUE>") |
//    Where(column="<CONDITION_COLUMN>", comparator="==", value="<CONDITION_VALUE>");

// More commonly, for SQL databases, you might use a raw query:
RunPixel(pixel="
    Database(database="<YOUR_DATABASE_ENGINE_ID>");
    Query("
        UPDATE <TABLE_NAME>
        SET <COLUMN_TO_UPDATE> = '<NEW_VALUE>'
        WHERE <CONDITION_COLUMN> = '<CONDITION_VALUE>';
    ");
");
```

### Update by Re-creation or Merge/Upsert

For many frame types or when complex transformations are involved, updates are often handled by:
1.  Reading the data.
2.  Performing transformations/updates in SEMOSS.
3.  Writing the modified data back, either by overwriting the old table/frame or by merging/upserting (if the target engine supports it).

```pixel
// Example: Read, modify a column, and overwrite
originalFrame = Frame("<TABLE_NAME>");
modifiedFrame = originalFrame | Map(newColumn="<COLUMN_TO_UPDATE>",
                                   existingColumn="<COLUMN_TO_UPDATE>",
                                   expression="CASE WHEN <CONDITION_COLUMN> == '<X>' THEN '<NEW_VALUE>' ELSE <COLUMN_TO_UPDATE> END");

// Overwrite the existing frame/table in the database
SaveFrame(frame=modifiedFrame, frameName="<TABLE_NAME>", override=[true]);
```

## Deleting Data

### Deleting Rows from a Table

```pixel
// Conceptual example - actual syntax might vary
// FrameDelete(frame="<TABLE_NAME>") |
//    Where(column="<CONDITION_COLUMN>", comparator="==", value="<CONDITION_VALUE>");

// Using a raw query for SQL databases:
RunPixel(pixel="
    Database(database="<YOUR_DATABASE_ENGINE_ID>");
    Query("
        DELETE FROM <TABLE_NAME>
        WHERE <CONDITION_COLUMN> = '<CONDITION_VALUE>';
    ");
");
```

### Deleting an Entire Table/Frame

```pixel
// Drop a frame (which may delete the underlying table if it's materialized in the engine)
DropFrame(frameName="<TABLE_NAME_TO_DELETE>");

// To be certain a table is dropped in the database, ensure the frame is associated with the engine
// and the engine user has drop permissions.
```

## Executing Raw SQL/Native Queries

For operations not directly covered by a high-level Pixel command, or for complex engine-specific queries, you can execute raw queries.

```pixel
// Execute a raw SQL query (assuming current context is a SQL database)
queryResult = Query("SELECT * FROM <NATIVE_TABLE_NAME> WHERE <NATIVE_COLUMN> = 'some_value'");
Collect(queryResult);

// For DDL or DML statements that don't return results (like CREATE TABLE, INSERT, UPDATE, DELETE):
Query("CREATE TABLE MyNewNativeTable (ID INT, Name VARCHAR(255))");
// Note: Some engines might require these to be wrapped differently, e.g., via a specific "Execute" reactor.
```

## Querying Database Metadata

SEMOSS allows you to inspect the structure of your databases.

```pixel
// List all tables/frames in the current database engine
tablesList = GetDatabaseFrames();
Collect(tablesList);

// Get metadata (columns, types) for a specific table/frame
tableMetadata = GetFrameMetamodel(frame="<TABLE_NAME>");
Collect(tableMetadata);

// For some engines, you might use specific metadata queries via the Query() command
// E.g., for a SQL database:
// tables = Query("SHOW TABLES;"); // Syntax varies by SQL dialect
// Collect(tables);
```

## Performing CRUD Operations with Custom Reactors

While Pixel provides direct ways to query and sometimes manipulate data, encapsulating Create, Read, Update, and Delete (CRUD) logic within custom Java Reactors offers better abstraction, reusability, and control, especially for complex operations or when direct Java database interaction is preferred.

The following examples are inspired by the `SEMOSS/backend-training` repository and demonstrate how to build Reactors for managing a sample movie database.

**(Assume the image will be placed at `docs/how-to-guides/images/movie-metamodel.png`)**
The database schema (metamodel) we'll be referring to is:
![Movie Database Metamodel](./images/movie-metamodel.png)
*(This schema includes tables like TITLE, GENRE, and NOMINATED.)*

**General Pattern in CRUD Reactors:**
1.  Define input keys (e.g., database ID, data for new records, IDs for updates/deletes).
2.  Retrieve and validate inputs in the `execute()` method.
3.  Get the `RDBMSNativeEngine` instance using `Utility.getEngine(databaseId)`.
4.  Construct SQL queries (often using `PreparedStatement` to prevent SQL injection).
5.  Set parameters for the `PreparedStatement`.
6.  Execute the statement.
7.  Manage transactions (`database.setAutoCommit(false);` and `database.commit();`) if multiple operations need to be atomic.
8.  Close `PreparedStatement` and other resources in a `finally` block.
9.  Return a `NounMetadata` object indicating success or failure.

### Creating a New Genre (`AddGenreReactor`)

This reactor adds a new genre for a specific movie title. It also checks if the genre already exists for that title to prevent duplicates.

**Java Reactor Code (`AddGenreReactor.java` - Snippet):**
```java
package prerna.sablecc2.reactor.project.template.movie;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.rdbms.RDBMSNativeEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ConnectionUtils;
import prerna.util.Utility;

public class AddGenreReactor extends AbstractReactor {

    private static final Logger logger = LogManager.getLogger(AddGenreReactor.class.getName());
    private static String selectQuery = "SELECT * FROM GENRE WHERE GENRE = ? AND TITLE_FK = ?";
    private static String insertQuery = "INSERT INTO GENRE (GENRE, TITLE_FK) VALUES (?, ?)";

    public AddGenreReactor() {
        this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.GENRE.getKey(), ReactorKeysEnum.TITLE.getKey()};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        User user = this.insight.getUser();
        if (user == null) {
            throw new IllegalArgumentException("User must be signed in to add a genre.");
        }
        String databaseId = this.keyValue.get(this.keysToGet[0]);
        if (databaseId == null || databaseId.isEmpty()) {
            throw new IllegalArgumentException("Need to define " + ReactorKeysEnum.DATABASE.getKey());
        }
        RDBMSNativeEngine database = (RDBMSNativeEngine) Utility.getEngine(databaseId);

        String genre = this.keyValue.get(this.keysToGet[1]);
        if (genre == null || genre.isEmpty()) {
            throw new IllegalArgumentException("Need to define " + ReactorKeysEnum.GENRE.getKey());
        }
        String title = this.keyValue.get(this.keysToGet[2]);
        if (title == null || title.isEmpty()) {
            throw new IllegalArgumentException("Need to define " + ReactorKeysEnum.TITLE.getKey());
        }

        checkExistingGenre(database, genre, title);
        PreparedStatement ps = null;
        try {
            ps = database.getPreparedStatement(AddGenreReactor.insertQuery);
            int i = 1;
            ps.setString(i++, genre);
            ps.setString(i++, title);
            ps.execute();
            if(!database.isCloud()) {
                database.commit();
            }
        } catch (SQLException e) {
            logger.error("Error adding genre: " + e.getMessage());
            throw new IllegalArgumentException("Error adding genre: " + e.getMessage());
        } finally {
            ConnectionUtils.closeStatement(ps);
        }
        return new NounMetadata("Successfully added new Genre: " + genre, PixelDataType.CONST_STRING, PixelOperationType.SUCCESS);
    }

    private void checkExistingGenre(RDBMSNativeEngine database, String genre, String title) {
        List<Map<String, Object>> existing = null;
        try {
            String specificQuery = selectQuery.replaceFirst("\\?", "'" + genre + "'").replaceFirst("\\?", "'" + title + "'");
            existing = database.getQueryUtil().selectQuery(specificQuery);
        } catch (Exception e) {
            logger.error("Error checking existing genre: " + e.getMessage());
            throw new IllegalArgumentException("Error validating genre: " + e.getMessage());
        }
        if (existing != null && !existing.isEmpty()) {
            throw new IllegalArgumentException("Genre '" + genre + "' already exists for title '" + title + "'.");
        }
    }
}
```

**Pixel Invocation:**
```pixel
AddGenre(database=["YOUR_DB_ENGINE_ID"], title=["NameOfMovie"], genre=["Action"]);
```
*(Replace `YOUR_DB_ENGINE_ID`, `NameOfMovie`, and `Action` with actual values.)*

### Creating a New Movie (`AddMovieReactor`)

This reactor adds a new movie to the `TITLE` table and, if applicable, an entry to the `NOMINATED` table within a single transaction.

**Java Reactor Code (`AddMovieReactor.java` - Snippet):**
```java
package prerna.sablecc2.reactor.project.template.movie;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.rdbms.RDBMSNativeEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ConnectionUtils;
import prerna.util.Utility;

public class AddMovieReactor extends AbstractReactor {

    private static final Logger logger = LogManager.getLogger(AddMovieReactor.class.getName());
    private static final String TITLE_KEY = ReactorKeysEnum.TITLE.getKey();
    private static final String YEAR_KEY = ReactorKeysEnum.YEAR.getKey();
    private static final String NOMINATED_KEY = ReactorKeysEnum.NOMINATED.getKey();

    public AddMovieReactor() {
        this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), TITLE_KEY, YEAR_KEY, NOMINATED_KEY};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        User user = this.insight.getUser();
        if (user == null) {
            throw new IllegalArgumentException("User must be signed in to add a movie.");
        }
        String databaseId = this.keyValue.get(this.keysToGet[0]);
        RDBMSNativeEngine database = (RDBMSNativeEngine) Utility.getEngine(databaseId);

        String title = this.keyValue.get(this.keysToGet[1]);
        int year = ((Number) this.keyValue.get(this.keysToGet[2])).intValue();
        String nominated = this.keyValue.get(this.keysToGet[3]);

        Connection conn = null;
        PreparedStatement psTitle = null;
        PreparedStatement psNominated = null;
        String insertTitleQuery = "INSERT INTO TITLE (TITLE, YEAR) VALUES (?, ?)";
        String insertNominatedQuery = "INSERT INTO NOMINATED (NOMINATED, TITLE_FK, YEAR_FK) VALUES (?, ?, ?)";

        try {
            conn = database.getConnection();
            conn.setAutoCommit(false);

            psTitle = conn.prepareStatement(insertTitleQuery);
            psTitle.setString(1, title);
            psTitle.setInt(2, year);
            psTitle.execute();

            if (nominated != null && (nominated.equalsIgnoreCase("Y") || nominated.equalsIgnoreCase("Yes"))) {
                psNominated = conn.prepareStatement(insertNominatedQuery);
                psNominated.setString(1, "Y");
                psNominated.setString(2, title);
                psNominated.setInt(3, year);
                psNominated.execute();
            }
            conn.commit();
        } catch (SQLException e) {
            logger.error("Error adding movie: " + e.getMessage());
            ConnectionUtils.rollback(conn);
            throw new IllegalArgumentException("Error adding movie: " + e.getMessage());
        } finally {
            ConnectionUtils.closeStatement(psTitle);
            ConnectionUtils.closeStatement(psNominated);
            if (conn != null) {
                try {
                    conn.setAutoCommit(true);
                } catch (SQLException e) {
                    logger.error("Error resetting auto-commit: " + e.getMessage());
                }
                ConnectionUtils.closeConnection(conn);
            }
        }
        return new NounMetadata("Successfully added new Movie: " + title, PixelDataType.CONST_STRING, PixelOperationType.SUCCESS);
    }
}
```

**Pixel Invocation:**
```pixel
AddMovie(database=["YOUR_DB_ENGINE_ID"], title=["New Blockbuster"], year=[2023], nominated=["Y"]);
```

### Updating a Genre (`UpdateGenreReactor`)

This reactor updates the genre of an existing movie title.

**Java Reactor Code (`UpdateGenreReactor.java` - Snippet):**
```java
package prerna.sablecc2.reactor.project.template.movie;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.rdbms.RDBMSNativeEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ConnectionUtils;
import prerna.util.Utility;

public class UpdateGenreReactor extends AbstractReactor {

    private static final Logger logger = LogManager.getLogger(UpdateGenreReactor.class.getName());
    private static String updateQuery = "UPDATE GENRE SET GENRE = ? WHERE TITLE_FK = ? AND GENRE = ?";

    public UpdateGenreReactor() {
        this.keysToGet = new String[]{
                ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.NEW_GENRE.getKey(),
                ReactorKeysEnum.OLD_GENRE.getKey(), ReactorKeysEnum.TITLE.getKey()
        };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        User user = this.insight.getUser();
        // ... (null/empty checks for user and inputs) ...
        String databaseId = this.keyValue.get(this.keysToGet[0]);
        RDBMSNativeEngine database = (RDBMSNativeEngine) Utility.getEngine(databaseId);
        String newGenre = this.keyValue.get(this.keysToGet[1]);
        String oldGenre = this.keyValue.get(this.keysToGet[2]);
        String title = this.keyValue.get(this.keysToGet[3]);

        PreparedStatement ps = null;
        try {
            ps = database.getPreparedStatement(updateQuery);
            ps.setString(1, newGenre);
            ps.setString(2, title);
            ps.setString(3, oldGenre);
            ps.executeUpdate();
            if(!database.isCloud()) {
                database.commit();
            }
        } catch (SQLException e) {
            logger.error("Error updating genre: " + e.getMessage());
            throw new IllegalArgumentException("Error updating genre: " + e.getMessage());
        } finally {
            ConnectionUtils.closeStatement(ps);
        }
        return new NounMetadata("Successfully updated genre for " + title, PixelDataType.CONST_STRING, PixelOperationType.SUCCESS);
    }
}
```

**Pixel Invocation:**
```pixel
UpdateGenre(database=["YOUR_DB_ENGINE_ID"], title=["NameOfMovie"], oldGenre=["Action"], newGenre=["Sci-Fi Action"]);
```

### Updating a Movie Title (`UpdateMovieReactor`)

This reactor updates a movie's title and its year. It demonstrates updating related records in `GENRE` and `NOMINATED` tables transactionally.

**Java Reactor Code (`UpdateMovieReactor.java` - Snippet):**
```java
package prerna.sablecc2.reactor.project.template.movie;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.rdbms.RDBMSNativeEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ConnectionUtils;
import prerna.util.Utility;

public class UpdateMovieReactor extends AbstractReactor {

    private static final Logger logger = LogManager.getLogger(UpdateMovieReactor.class.getName());
    // ... (keys for OLD_TITLE, NEW_TITLE, NEW_YEAR) ...

    public UpdateMovieReactor() {
        this.keysToGet = new String[]{
                ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.OLD_TITLE.getKey(),
                ReactorKeysEnum.NEW_TITLE.getKey(), ReactorKeysEnum.NEW_YEAR.getKey()
        };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        // ... (retrieve inputs: databaseId, oldTitle, newTitle, newYear) ...
        RDBMSNativeEngine database = (RDBMSNativeEngine) Utility.getEngine(databaseId);
        Connection conn = null;
        PreparedStatement psUpdateTitle = null;
        PreparedStatement psUpdateGenre = null;
        PreparedStatement psUpdateNominated = null;

        String updateTitleQuery = "UPDATE TITLE SET TITLE = ?, YEAR = ? WHERE TITLE = ?";
        String updateGenreQuery = "UPDATE GENRE SET TITLE_FK = ? WHERE TITLE_FK = ?";
        String updateNominatedQuery = "UPDATE NOMINATED SET TITLE_FK = ?, YEAR_FK = ? WHERE TITLE_FK = ?";

        try {
            conn = database.getConnection();
            conn.setAutoCommit(false);

            // Update GENRE table
            psUpdateGenre = conn.prepareStatement(updateGenreQuery);
            psUpdateGenre.setString(1, newTitle);
            psUpdateGenre.setString(2, oldTitle);
            psUpdateGenre.executeUpdate();

            // Update NOMINATED table
            psUpdateNominated = conn.prepareStatement(updateNominatedQuery);
            psUpdateNominated.setString(1, newTitle);
            psUpdateNominated.setInt(2, newYear);
            psUpdateNominated.setString(3, oldTitle);
            psUpdateNominated.executeUpdate();

            // Update TITLE table (last, due to FK constraints)
            psUpdateTitle = conn.prepareStatement(updateTitleQuery);
            psUpdateTitle.setString(1, newTitle);
            psUpdateTitle.setInt(2, newYear);
            psUpdateTitle.setString(3, oldTitle);
            psUpdateTitle.executeUpdate();

            conn.commit();
        } catch (SQLException e) {
            logger.error("Error updating movie: " + e.getMessage());
            ConnectionUtils.rollback(conn);
            throw new IllegalArgumentException("Error updating movie: " + e.getMessage());
        } finally {
            ConnectionUtils.closeAllStatements(psUpdateTitle, psUpdateGenre, psUpdateNominated);
            if (conn != null) {
                try { conn.setAutoCommit(true); } catch (SQLException e) { /* ignore */ }
                ConnectionUtils.closeConnection(conn);
            }
        }
        return new NounMetadata("Successfully updated Movie: " + newTitle, PixelDataType.CONST_STRING, PixelOperationType.SUCCESS);
    }
}
```

**Pixel Invocation:**
```pixel
UpdateMovie(database=["YOUR_DB_ENGINE_ID"], oldTitle=["Old Movie Name"], newTitle=["New Movie Name"], newYear=[2024]);
```

### Deleting a Movie (`DeleteMovieReactor`)

This reactor deletes a movie from the `TITLE` table and its associated records from the `NOMINATED` and `GENRE` tables within a single transaction.

**Java Reactor Code (`DeleteMovieReactor.java` - Snippet):**
```java
package prerna.sablecc2.reactor.project.template.movie;

// ... (imports) ...
public class DeleteMovieReactor extends AbstractReactor {
    // ... (logger, keys) ...
    public DeleteMovieReactor() {
        this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.TITLE.getKey()};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        // ... (retrieve inputs: databaseId, title) ...
        RDBMSNativeEngine database = (RDBMSNativeEngine) Utility.getEngine(databaseId);
        Connection conn = null;
        PreparedStatement psDeleteNominated = null;
        PreparedStatement psDeleteGenre = null;
        PreparedStatement psDeleteTitle = null;

        String deleteNominatedQuery = "DELETE FROM NOMINATED WHERE TITLE_FK = ?";
        String deleteGenreQuery = "DELETE FROM GENRE WHERE TITLE_FK = ?";
        String deleteTitleQuery = "DELETE FROM TITLE WHERE TITLE = ?";

        try {
            conn = database.getConnection();
            conn.setAutoCommit(false);

            // Delete from child tables first
            psDeleteNominated = conn.prepareStatement(deleteNominatedQuery);
            psDeleteNominated.setString(1, title);
            psDeleteNominated.executeUpdate();

            psDeleteGenre = conn.prepareStatement(deleteGenreQuery);
            psDeleteGenre.setString(1, title);
            psDeleteGenre.executeUpdate();

            // Delete from parent table
            psDeleteTitle = conn.prepareStatement(deleteTitleQuery);
            psDeleteTitle.setString(1, title);
            psDeleteTitle.executeUpdate();

            conn.commit();
        } catch (SQLException e) { /* ... rollback, error handling ... */ }
        finally { /* ... close statements, setAutoCommit(true), close connection ... */ }
        return NounMetadata.getSuccessNounMessage("Successfully deleted Movie: " + title);
    }
}
```

**Pixel Invocation:**
```pixel
DeleteMovie(database=["YOUR_DB_ENGINE_ID"], title=["MovieToDelete"]);
```

### Deleting a Genre (`DeleteGenreReactor`)

This reactor deletes a specific genre associated with a movie title.

**Java Reactor Code (`DeleteGenreReactor.java` - Snippet):**
```java
package prerna.sablecc2.reactor.project.template.movie;

// ... (imports) ...
public class DeleteGenreReactor extends AbstractReactor {
    // ... (logger, keys) ...
    public DeleteGenreReactor() {
        this.keysToGet = new String[]{
            ReactorKeysEnum.DATABASE.getKey(),
            ReactorKeysEnum.GENRE.getKey(),
            ReactorKeysEnum.TITLE.getKey()
        };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        // ... (retrieve inputs: databaseId, genre, title) ...
        RDBMSNativeEngine database = (RDBMSNativeEngine) Utility.getEngine(databaseId);
        PreparedStatement ps = null;
        String deleteQuery = "DELETE FROM GENRE WHERE GENRE = ? AND TITLE_FK = ?";
        try {
            ps = database.getPreparedStatement(deleteQuery);
            ps.setString(1, genre);
            ps.setString(2, title);
            ps.executeUpdate();
            if(!database.isCloud()) {
                database.commit();
            }
        } catch (SQLException e) { /* ... error handling ... */ }
        finally { ConnectionUtils.closeStatement(ps); }
        return NounMetadata.getSuccessNounMessage("Successfully deleted Genre " + genre + " for movie " + title);
    }
}
```

**Pixel Invocation:**
```pixel
DeleteGenre(database=["YOUR_DB_ENGINE_ID"], title=["NameOfMovie"], genre=["ObsoleteGenre"]);
```

**Summary of Benefits for CRUD Reactors:**
- **Abstraction & Simplicity**: Pixel scripts remain clean and readable.
- **Reusability**: Reactors can be called from multiple places.
- **Maintainability**: Database logic is centralized in Java, making updates easier.
- **Testability**: Java code is more easily unit-tested.
- **Security**: Use of `PreparedStatement` helps prevent SQL injection.
- **Transactional Integrity**: Complex multi-step operations can be managed atomically.


## Best Practices

-   **Use Parameterized Queries (if available/applicable)**: When constructing raw SQL queries that include user inputs, be mindful of SQL injection. If the `Query()` function or underlying Java utilities support parameterized queries, use them. Otherwise, ensure inputs are properly sanitized if they are partof raw query strings.
-   **Error Handling**: Wrap database operations in try-catch blocks if writing more complex Java-based interactions (e.g., in Reactors that call Pixel). Pixel itself has error handling mechanisms.
-   **Permissions**: Ensure the database user configured in the SEMOSS Engine has the necessary permissions (SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, etc.) for the operations you intend to perform.
-   **Efficiency**: Be mindful of query performance. Select only necessary columns, use filters effectively, and understand how joins are processed by the underlying engine.
-   **Transaction Management**: For operations requiring multiple steps to be atomic (all succeed or all fail), understand how the underlying database engine and SEMOSS handle transactions. Complex transactional logic might require custom Java code or specific database procedures.
-   **Engine-Specific Syntax**: While Pixel aims to provide a unified interface, raw queries passed via `Query()` must use the native syntax of the target database engine.

This guide provides a starting point for interacting with databases in SEMOSS. The flexibility of Pixel combined with the Engine Abstraction allows for powerful data management across diverse systems. For more advanced scenarios or engine-specific details, always refer to the detailed documentation for Pixel and the specific SEMOSS data engines.

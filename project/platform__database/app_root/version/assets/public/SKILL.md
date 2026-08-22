---
name: database
description: Use when writing code in an app that queries a relational or graph database on the platform, running SELECTs, inserts, updates, deletes, or fetching schema/table structure. Covers the SqlQuery(), SqlQueryBase64(), and GetDatabaseTableStructure() pixel commands via @semoss/sdk's runPixel, plus listing databases with MyEngines(engineTypes=["DATABASE"]). Do not use for LLM calls (see model-engine) or vector database queries.
---

# Database Engine

Query a database on the platform using `runPixel` from `@semoss/sdk`. Use `SqlQuery` only for simple, static SQL that is safe to embed in a Pixel string. Use `SqlQueryBase64` for dynamic SQL, especially inserts, updates, and deletes containing quotes, Unicode, or newlines.

## Usage

```typescript
import { runPixel } from "@semoss/sdk";

const DATABASE_ID = "e188c7d8-076f-4847-967a-fff45f4ca355";
const sql =
  "SELECT CITY FROM SALES_DATA_SAMPLE WHERE SALES_DATA_SAMPLE.DEALSIZE = 'Small'";

const result = await runPixel(
  `SqlQuery(database="${DATABASE_ID}", query="${sql}", limit=500);`,
);

assertPixelSuccess(result);

const { headers, values } = result.pixelReturn[0].output.data;
// headers: ["CITY"]
// values:  [["NYC"], ["Reims"], ["Lille"], ...]

function assertPixelSuccess(result: {
  errors?: unknown[];
  pixelReturn: Array<{ operationType?: string[]; output?: unknown }>;
}) {
  const applicationErrors = result.pixelReturn
    .filter((item) => item.operationType?.includes("ERROR"))
    .map((item) => item.output);
  const failures = [...(result.errors ?? []), ...applicationErrors];
  if (failures.length) {
    throw new Error(
      failures
        .map((value) =>
          typeof value === "string" ? value : JSON.stringify(value),
        )
        .join("\n"),
    );
  }
}
```

Do not use `encodeURIComponent`; it produces literal percent escapes in the SQL. Plain interpolation is suitable only for controlled static SQL. Dynamic SQL can break the surrounding Pixel string even when it is valid SQL, so Base64-encode the complete UTF-8 SQL string instead.

The variations below show only the Pixel string. Apply `assertPixelSuccess` to every `runPixel` result.

### Insert / update / delete with SqlQueryBase64

Use a UTF-8-safe encoder and pass `commit=true`. Escape SQL string literals before building the SQL; Base64 protects the Pixel transport but does not prevent SQL injection.

```typescript
function encodeUtf8Base64(value: string): string {
  const bytes = new TextEncoder().encode(value);
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
}

function sqlString(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

const customerName = "O'Brien – 東京";
const notes = "First line\nSecond \"quoted\" line";
const mutationSql = `
  INSERT INTO CUSTOMER_NOTES (CUSTOMER_NAME, NOTES)
  VALUES (${sqlString(customerName)}, ${sqlString(notes)});
`;

const mutation = await runPixel(
  `SqlQueryBase64(database="${DATABASE_ID}", query="${encodeUtf8Base64(mutationSql)}", commit=true);`,
);
assertPixelSuccess(mutation);

// Verify the persisted state before claiming success.
const verificationSql = `
  SELECT CUSTOMER_NAME, NOTES
  FROM CUSTOMER_NOTES
  WHERE CUSTOMER_NAME = ${sqlString(customerName)};
`;
const verification = await runPixel(
  `SqlQueryBase64(database="${DATABASE_ID}", query="${encodeUtf8Base64(verificationSql)}", limit=50);`,
);
assertPixelSuccess(verification);

const verifiedRows = verification.pixelReturn[0].output.data.values;
if (verifiedRows.length !== 1) {
  throw new Error("Mutation was not verified; inspect database state before retrying.");
}
```

When an error is returned after a write may have started, do not retry blindly. Read the authoritative database state first; a retry can duplicate a partially completed mutation.

### Base64-encoded queries

`SqlQueryBase64` has the same wrapper behavior as `SqlQuery`; only the query input format changes. The `query` value is the Base64 representation of the complete UTF-8 SQL string.

```
SqlQueryBase64(database="${DATABASE_ID}", query="U0VMRUNUICogRlJPTSB0YWJsZV9uYW1lOw==", limit=500);
```

`U0VMRUNUICogRlJPTSB0YWJsZV9uYW1lOw==` decodes to `SELECT * FROM table_name;`.

### Database structure

Fetch logical + physical metadata for every table/column (or vertex/property, for graph databases).

```
GetDatabaseTableStructure(database="${DATABASE_ID}");
```

Each row in `output.data.values` is a 6-tuple:

1. Logical table name (RDBMS) or vertex name (graph)
2. Logical column name (RDBMS) or property name (graph)
3. Data type of the column/property
4. Whether this row represents a graph vertex itself rather than a property on it (only meaningful for RDF/graph databases)
5. Physical column/property name as stored in the database
6. Physical table/vertex name as stored in the database

## Response shape

`pixelReturn[0].output` contains:

- `data.values` — 2D array of rows; each row is a tuple whose cells align with `data.headers`
- `data.headers` — display column names (aliased where the query aliased them)
- `data.rawHeaders` — raw underlying column names
- `headerInfo[]` — per-column metadata `{ dataType, alias, header, type, derived }`
- `sources[]` — engines that served the query: `{ name, type }`
- `numCollected` — number of rows actually returned (bounded by `limit`)

For the full response schema, see `references/response-schema.md`.

## Listing available databases

This listing pattern is for app-runtime features where the app's end user picks a database. When *you* are deciding which database the app should use, do not enumerate accessible databases: use the project's selected database engine (see the Selected Engines section of your system prompt), and only ask the user to choose or attach one when none is selected.

For the app-runtime case, use the `MyEngines` pixel with `engineTypes=["DATABASE"]` to list databases the current user has access to.

```typescript
import { runPixel } from "@semoss/sdk";

const { errors, pixelReturn } = await runPixel(
  `MyEngines(engineTypes=["DATABASE"], limit=[50], offset=[0]);`,
);

if (errors.length) throw new Error(errors[0]);

const databases = pixelReturn[0].output as Array<{
  engine_id: string;
  engine_name: string;
  engine_display_name: string;
  engine_subtype: string; // e.g. "H2_DB", "POSTGRES", "MYSQL", "TINKER"
  engine_cost: string;
  engine_favorite: 0 | 1;
}>;
```

### Filtering and paging

`MyEngines` accepts several optional arguments. All are arrays, even when passing a single value:

- `filterWord=["sales"]` — substring match against engine name.
- `limit=[50]`, `offset=[0]` — paging. Omit both to return all results.
- `onlyFavorites=[true]` — restrict to the user's favorited engines.
- `sort={"ENGINENAME": "ASC"}` — sort by `ENGINENAME` or `DATECREATED`, direction `ASC` or `DESC`.

```
MyEngines(engineTypes=["DATABASE"], filterWord=["sales"], sort={"ENGINENAME": "ASC"}, limit=[20], offset=[0]);
```

### Response field conventions

Use `engine_*` fields (`engine_id`, `engine_name`, `engine_display_name`, `engine_subtype`, etc.). The response also contains `app_*` and `database_*` fields with the same values — these are legacy aliases and should not be used in new code.

Common pattern — render a picker and use the selected `engine_id` as `DATABASE_ID` in the `SqlQuery()` call above:

```typescript
const [databases, setDatabases] = useState<Database[]>([]);
const [selectedId, setSelectedId] = useState<string>("");

useEffect(() => {
  runPixel(`MyEngines(engineTypes=["DATABASE"], limit=[50], offset=[0]);`).then(
    ({ pixelReturn }) => setDatabases(pixelReturn[0].output),
  );
}, []);
```

# Database query response schema

Full response shape returned from a `runPixel` call that wraps a `SqlQuery()` or `SqlQueryBase64()` command. The top-level response is an envelope; the tabular result lives at `pixelReturn[0].output.data`.

## Example response

```json
{
  "insightID": "019dba7a-fe47-7832-a745-ffc5af0971d7",
  "pixelReturn": [
    {
      "pixelId": "0",
      "pixelExpression": "SqlQuery ( database = [ \"e188c7d8-076f-4847-967a-fff45f4ca355\" ] , query = [ \"SELECT CITY FROM SALES_DATA_SAMPLE WHERE SALES_DATA_SAMPLE.DEALSIZE = 'Small';\" ] , commit = [ true ] ) ;",
      "isMeta": false,
      "timeToRun": 30,
      "output": {
        "data": {
          "values": [["NYC"], ["Reims"], ["Lille"], ["San_Francisco"]],
          "headers": ["CITY"],
          "rawHeaders": ["CITY"]
        },
        "headerInfo": [
          {
            "dataType": "STRING",
            "alias": "CITY",
            "header": "CITY",
            "type": "STRING",
            "derived": false
          }
        ],
        "sources": [
          {
            "name": "e188c7d8-076f-4847-967a-fff45f4ca355",
            "type": "RAW_ENGINE_QUERY"
          }
        ],
        "numCollected": 50,
        "taskId": "null"
      },
      "operationType": ["OPERATION"]
    }
  ]
}
```

## Envelope fields

- `insightID` — The insight ID used for the pixel execution.
- `pixelReturn[]` — array of results, one per pixel command in the call. For a single query pixel, always index `[0]`.

## pixelReturn[0] fields

- `pixelId` — sequence ID of the command within the call.
- `pixelExpression` — the parsed pixel string the platform actually executed. Useful for debugging encoding issues.
- `isMeta` — internal flag; ignore for query responses.
- `timeToRun` — execution time in milliseconds.
- `operationType` — categorization of the pixel; `["OPERATION"]` for database queries.

## pixelReturn[0].output fields — the query response

- `data.values` _(array of arrays)_ — rows returned by the query. Each row is a tuple whose cells align positionally with `data.headers`. **Use this as the primary payload.**
- `data.headers` _(string[])_ — display column names. Aliased where the query aliased them.
- `data.rawHeaders` _(string[])_ — raw underlying column names as reported by the engine (before any aliasing).
- `headerInfo[]` — per-column metadata, one entry per column, each `{ dataType, alias, header, type, derived }`. `dataType` / `type` values include `"STRING"`, `"NUMBER"`, `"DATE"`, etc. `derived` is `true` for columns produced by a platform transform rather than the underlying SQL.
- `sources[]` — `{ name, type }` identifying the engine(s) queried. `name` is the database engine ID; `type` is typically `"RAW_ENGINE_QUERY"`.
- `numCollected` _(number)_ — number of rows actually returned, bounded by the `limit` argument.
- `taskId` _(string | "null")_ — background-task ID when the query streamed; the literal string `"null"` for synchronous returns.

## Variant: `GetDatabaseTableStructure`

The envelope and `output.data.values` / `output.data.headers` shape is identical, but each row is a schema-metadata tuple rather than application data:

```
[logicalTable, logicalColumn, dataType, isVertex, physicalColumn, physicalTable]
```

See the `### Database structure` section of `SKILL.md` for how to interpret each column.

## Variant: modification queries (`commit=true`)

INSERT / UPDATE / DELETE queries return the same envelope, but the `output` body typically carries a status / affected-row payload rather than tabular `data.values`. Check both the SDK `errors` array and every `pixelReturn[].operationType` for `ERROR`; then issue a SELECT readback before claiming the write succeeded.

## Common access patterns

```typescript
// Raw rows as array-of-arrays
const rows = pixelReturn[0].output.data.values;

// Map rows to objects keyed by header
const { headers, values } = pixelReturn[0].output.data;
const records = values.map((row) =>
  Object.fromEntries(headers.map((h, i) => [h, row[i]])),
);

// Inspect a column's type
const cityType = pixelReturn[0].output.headerInfo.find(
  (h) => h.header === "CITY",
)?.dataType;

// Row count (bounded by limit)
const { numCollected } = pixelReturn[0].output;
```

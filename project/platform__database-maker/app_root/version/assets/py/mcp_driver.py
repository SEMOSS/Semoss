"""Canonical MCP tools for creating and querying SEMOSS databases."""

import base64
import json
from pathlib import Path


BOOTSTRAP_ASSET_PATH = "version/assets/csv/database_bootstrap.csv"
BOOTSTRAP_TABLE = "SMSS_DATABASE_BOOTSTRAP"


def extract_pixel_output(response):
    """Extract first pixel output from SEMOSS response.

    Args:
        response (list): Raw response from Insight.run_pixel

    Returns:
        object: The first output payload, or None when unavailable
    """
    if not response:
        return None
    pixel_return = response[0].get("pixelReturn", [])
    if not pixel_return:
        return None
    return pixel_return[0].get("output")


def load_local_semoss_config():
    """Load project configuration.

    Returns:
        dict: Parsed configuration when available, otherwise an empty dict
    """
    config_path = Path(__file__).resolve().parents[1] / "semoss_config" / "config.json"
    if not config_path.exists():
        return {}
    with config_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def resolve_project_id(project_id=None):
    """Resolve current SEMOSS project id.

    Args:
        project_id (str, optional): Optional explicit project id

    Returns:
        str: The resolved project id
    """
    if project_id:
        return str(project_id)
    config = load_local_semoss_config()
    for key in ("project_id", "app_id", "projectId", "appId"):
        value = config.get(key)
        if value:
            return str(value)
    raise ValueError("Project id could not be resolved.")


def list_databases_output():
    """Get database list from SEMOSS.

    Returns:
        list: Available database entries
    """
    from semoss import Insight

    insight = Insight()
    # Use MyEngines() Pixel command to get database engines
    output = extract_pixel_output(insight.run_pixel("MyEngines();"))

    if not output or not isinstance(output, list):
        return []

    # Filter for database engines only
    databases = []
    for engine in output:
        if isinstance(engine, dict) and engine.get('engine_type') == 'DATABASE':
            databases.append({
                'database_id': engine.get('engine_id'),
                'database_name': engine.get('engine_name'),
                'database_display_name': engine.get('engine_name')
            })

    return databases


def encode_to_string(data):
    """Encode data to Base64 string.

    Args:
        data (any): The data to encode (dict, list, or string)

    Returns:
        str: Base64 encoded string representation of the input data
    """
    if isinstance(data, (dict, list)):
        data = json.dumps(data, separators=(',', ':'))
    elif not isinstance(data, str):
        data = str(data)
    data_bytes = data.encode("utf-8")
    output = base64.b64encode(data_bytes)
    return str(output)[2:-1]


def _error_message(value):
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, default=str)


def _run_pixel_checked(pixel):
    """Run Pixel and fail when SEMOSS reports an application-level error."""
    from semoss import Insight

    response = Insight().run_pixel(pixel)
    if not isinstance(response, list) or not response:
        raise RuntimeError("SEMOSS returned an empty or malformed Pixel response.")

    outputs = []
    for envelope in response:
        if not isinstance(envelope, dict):
            raise RuntimeError("SEMOSS returned a malformed Pixel response envelope.")
        pixel_returns = envelope.get("pixelReturn")
        if not isinstance(pixel_returns, list) or not pixel_returns:
            raise RuntimeError("SEMOSS returned no pixelReturn entries.")
        for pixel_return in pixel_returns:
            if not isinstance(pixel_return, dict):
                raise RuntimeError("SEMOSS returned a malformed pixelReturn entry.")
            operation_types = pixel_return.get("operationType") or []
            if isinstance(operation_types, str):
                operation_types = [operation_types]
            if any(str(value).upper() == "ERROR" for value in operation_types):
                raise RuntimeError(_error_message(pixel_return.get("output")))
            outputs.append(pixel_return.get("output"))
    return outputs


def _load_local_semoss_config():
    config_path = Path(__file__).resolve().parents[1] / "semoss_config" / "config.json"
    if not config_path.exists():
        return {}
    with config_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _resolve_project_id():
    config = _load_local_semoss_config()
    for key in ("project_id", "app_id", "projectId", "appId"):
        value = config.get(key)
        if value:
            return str(value)
    raise RuntimeError("The canonical database-maker project id is not configured.")


def _normalize_database(database):
    return {
        "database_id": database.get("engine_id") or database.get("database_id") or database.get("app_id"),
        "database_name": database.get("engine_name") or database.get("database_name") or database.get("app_name"),
        "database_display_name": database.get("engine_display_name")
        or database.get("database_display_name")
        or database.get("app_name"),
        "database_subtype": database.get("engine_subtype") or database.get("database_subtype"),
        "date_created": database.get("date_created") or database.get("DATECREATED"),
    }


def _list_databases():
    outputs = _run_pixel_checked('MyEngines(engineTypes=["DATABASE"]);')
    databases = outputs[0] if outputs else None
    if not isinstance(databases, list):
        raise RuntimeError("MyEngines returned a malformed database list.")
    return [_normalize_database(database) for database in databases if isinstance(database, dict)]


def _exact_database_matches(database_name, databases=None):
    normalized_name = database_name.strip().casefold()
    matches = []
    for database in databases if databases is not None else _list_databases():
        names = (database.get("database_name"), database.get("database_display_name"))
        if any(str(name).strip().casefold() == normalized_name for name in names if name is not None):
            matches.append(database)
    return matches


def _split_sql_script(script):
    statements = []
    buffer = []
    in_single_quote = False
    in_double_quote = False
    in_line_comment = False
    in_block_comment = False
    index = 0

    while index < len(script):
        char = script[index]
        next_char = script[index + 1] if index + 1 < len(script) else ""
        if in_line_comment:
            if char == "\n":
                in_line_comment = False
                buffer.append(char)
            index += 1
            continue
        if in_block_comment:
            if char == "*" and next_char == "/":
                in_block_comment = False
                index += 2
            else:
                index += 1
            continue
        if not in_single_quote and not in_double_quote:
            if char == "-" and next_char == "-":
                in_line_comment = True
                index += 2
                continue
            if char == "/" and next_char == "*":
                in_block_comment = True
                index += 2
                continue
        if char == "'" and not in_double_quote:
            buffer.append(char)
            if in_single_quote and next_char == "'":
                buffer.append(next_char)
                index += 2
                continue
            in_single_quote = not in_single_quote
            index += 1
            continue
        if char == '"' and not in_single_quote:
            buffer.append(char)
            if in_double_quote and next_char == '"':
                buffer.append(next_char)
                index += 2
                continue
            in_double_quote = not in_double_quote
            index += 1
            continue
        if char == ";" and not in_single_quote and not in_double_quote:
            statement = "".join(buffer).strip()
            if statement:
                statements.append(statement)
            buffer = []
            index += 1
            continue
        buffer.append(char)
        index += 1

    if in_single_quote or in_double_quote or in_block_comment:
        raise ValueError("SQL script contains an unterminated string or block comment.")
    trailing = "".join(buffer).strip()
    if trailing:
        statements.append(trailing)
    return statements


def _row_count(result):
    if not isinstance(result, dict):
        return 0
    data = result.get("data") if isinstance(result.get("data"), dict) else result
    values = data.get("values") if isinstance(data, dict) else None
    return len(values) if isinstance(values, list) else 0


def run_query(database_id, query):
    """Execute SQL and return a JSON-safe tabular result."""
    from ai_server import DatabaseEngine

    if not str(database_id or "").strip():
        raise ValueError("database_id is required.")
    if not str(query or "").strip():
        raise ValueError("query is required.")

    result = DatabaseEngine(engine_id=str(database_id)).execQuery(query=str(query))
    if hasattr(result, "to_json") and hasattr(result, "columns"):
        split = json.loads(result.to_json(orient="split", date_format="iso"))
        return {
            "headers": [str(column) for column in split.get("columns", [])],
            "rows": split.get("data", []),
            "row_count": len(split.get("data", [])),
        }
    if isinstance(result, list):
        if result and isinstance(result[0], dict):
            headers = list(result[0].keys())
            return {
                "headers": headers,
                "rows": [[row.get(header) for header in headers] for row in result],
                "row_count": len(result),
            }
        return {"headers": [], "rows": result, "row_count": len(result)}
    if isinstance(result, dict):
        data = result.get("data") if isinstance(result.get("data"), dict) else result
        headers = data.get("headers") or data.get("rawHeaders") or []
        rows = data.get("values") or []
        return {"headers": headers, "rows": rows, "row_count": len(rows)}
    return {"headers": [], "rows": [[str(result)]], "row_count": 1}


def split_sql_script(script):
    """Split a SQL script into statements while ignoring semicolons inside strings or comments.

    Args:
        script (str): Raw SQL script content

    Returns:
        list: Ordered executable SQL statements
    """
    statements = []
    buffer = []
    in_single_quote = False
    in_double_quote = False
    in_line_comment = False
    in_block_comment = False
    index = 0

    while index < len(script):
        char = script[index]
        next_char = script[index + 1] if index + 1 < len(script) else ""

        if in_line_comment:
            if char == "\n":
                in_line_comment = False
                buffer.append(char)
            index += 1
            continue

        if in_block_comment:
            if char == "*" and next_char == "/":
                in_block_comment = False
                index += 2
            else:
                index += 1
            continue

        if not in_single_quote and not in_double_quote:
            if char == "-" and next_char == "-":
                in_line_comment = True
                index += 2
                continue
            if char == "/" and next_char == "*":
                in_block_comment = True
                index += 2
                continue

        if char == "'" and not in_double_quote:
            if in_single_quote and next_char == "'":
                buffer.append(char)
                buffer.append(next_char)
                index += 2
                continue
            in_single_quote = not in_single_quote
            buffer.append(char)
            index += 1
            continue

        if char == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
            buffer.append(char)
            index += 1
            continue

        if char == ";" and not in_single_quote and not in_double_quote:
            statement = "".join(buffer).strip()
            if statement:
                statements.append(statement)
            buffer = []
            index += 1
            continue

        buffer.append(char)
        index += 1

    trailing_statement = "".join(buffer).strip()
    if trailing_statement:
        statements.append(trailing_statement)

    return statements


def run_script(database_id, script):
    """Execute a SQL script statement-by-statement against a target database.

    Automatically refreshes database ontology after any DDL operations (CREATE/DROP/ALTER TABLE).

    Args:
        database_id (str): The unique identifier of the target database
        script (str): The SQL script to run

    Returns:
        str: JSON string with execution results and ontology refresh status
    """
    from semoss import Insight

    try:
        # Convert literal \n to actual newlines in case the SQL comes with escaped characters
        normalized_script = (script or "").strip()
        if '\\n' in normalized_script:
            normalized_script = normalized_script.replace('\\n', '\n').replace('\\t', '\t')

        if not normalized_script:
            return json.dumps({
                "status": "error",
                "error": "Script is empty",
                "statement_count": 0,
                "results": []
            })

        statements = split_sql_script(normalized_script)
        if not statements:
            return json.dumps({
                "status": "error",
                "error": "No executable SQL statements found",
                "statement_count": 0,
                "results": []
            })

        insight = Insight()
        results = []

        for index, statement in enumerate(statements, start=1):
            statement_type = statement.split(None, 1)[0].upper() if statement.split() else "UNKNOWN"
            try:
                if statement_type in {"SELECT", "WITH", "SHOW", "DESCRIBE", "EXPLAIN"}:
                    pixel = f'Database(database=["{database_id}"]) | Query("<encode>{statement}</encode>") | Collect(100);'
                else:
                    pixel = f'Database(database=["{database_id}"]) | Query("<encode>{statement}</encode>") | ExecQuery();'

                response = insight.run_pixel(pixel)
                result = extract_pixel_output(response) or {}
                row_count = 0
                if isinstance(result, dict):
                    row_count = len(result.get("values") or [])

                results.append({
                    "index": index,
                    "statement": statement,
                    "statement_type": statement_type,
                    "row_count": row_count,
                    "result": result,
                })
            except Exception as exc:
                return json.dumps({
                    "status": "error",
                    "error": str(exc),
                    "statement_count": len(results),
                    "results": results,
                    "failed_statement": statement,
                })

        response_data = {
            "status": "success",
            "statement_count": len(results),
            "results": results,
        }

        # Check if any statements were table DDL operations
        ddl_keywords = ['CREATE TABLE', 'DROP TABLE', 'ALTER TABLE', 'CREATE INDEX', 'DROP INDEX']
        has_table_ddl = False

        for result in results:
            statement = result.get("statement", "").upper()
            if any(keyword in statement for keyword in ddl_keywords):
                has_table_ddl = True
                break

        # If we had table DDL operations, auto-refresh ontology
        if has_table_ddl:
            try:
                ontology_result = refresh_database_ontology(database_id)
                ontology_data = json.loads(base64.b64decode(ontology_result).decode('utf-8'))

                response_data["ontology_refresh"] = {
                    "status": ontology_data.get("status"),
                    "synchronized_tables": ontology_data.get("synchronized_tables", []),
                    "table_count": ontology_data.get("table_count", 0)
                }
                response_data["auto_refreshed"] = True

            except Exception as refresh_error:
                response_data["ontology_refresh"] = {
                    "status": "error",
                    "error": str(refresh_error)
                }
                response_data["auto_refreshed"] = False
        else:
            response_data["auto_refreshed"] = False
            response_data["refresh_reason"] = "No table DDL operations found"

        return json.dumps(response_data)

    except Exception as e:
        return json.dumps({
            "status": "error",
            "error": str(e),
            "statement_count": 0,
            "results": []
        })


def _save_schema_asset(database_id, schema):
    asset_path = f"schema/schema_{database_id}.json"
    warnings = []
    for pixel in (
        'NewInsightAssetsDirectory(filePath=["schema"]);',
        f'NewInsightAssetsFile(filePath=[{json.dumps(asset_path)}]);',
    ):
        try:
            _run_pixel_checked(pixel)
        except RuntimeError as exc:
            warnings.append(str(exc))
    content = json.dumps(schema, indent=2, ensure_ascii=False)
    _run_pixel_checked(
        f'SaveInsightAssets(filePath=[{json.dumps(asset_path)}], content=["<encode>{content}</encode>"]);'
    )
    return asset_path, warnings


def get_schema(database_id, playground=True):
    """Retrieves and simplifies the schema for a given database ID.

    Args:
        database_id (str): The unique identifier of the database to inspect
        playground (bool): Whether to persist a copy under the current project's assets

    Returns:
        str: A Base64 encoded JSON string representing the simplified schema
    """
    from semoss import Insight

    i = Insight()

    # Get the raw metamodel using Pixel
    pixel_cmd = f'GetDatabaseMetamodel(database=["{database_id}"], options=["dataTypes", "descriptions"]);'
    resp = i.run_pixel(pixel_cmd)

    # Extract the output from the pixel response
    metamodel = {}
    if resp and len(resp) > 0:
        pixel_return = resp[0].get('pixelReturn', [])
        if pixel_return and len(pixel_return) > 0:
             metamodel = pixel_return[0].get('output', {})

    # Transform the complex metamodel into a clean schema structure
    schema_output = []

    nodes = metamodel.get("nodes", [])
    data_types = metamodel.get("dataTypes", {})
    descriptions = metamodel.get("descriptions", {})

    for node in nodes:
        table_name = node.get("conceptualName")
        if not table_name:
            continue

        table_info = {
            "table_name": table_name,
            "columns": []
        }

        # Iterate through columns (properties)
        for prop in node.get("propSet", []):
            # The key for types/descriptions is typically Table__Column
            key = f"{table_name}__{prop}"

            column_info = {
                "name": prop,
                "type": data_types.get(key, "UNKNOWN"),
                "description": descriptions.get(key, "")
            }
            table_info["columns"].append(column_info)

        schema_output.append(table_info)

    # Serialize schema for return and optional file saving
    schema_json = json.dumps(schema_output, indent=2)

    # Save to project assets if playground mode is enabled
    if playground:
        try:
            project_id = resolve_project_id()
            filename = f"schema/schema_{database_id}.json"

            # Use SaveEngineAssets reactor
            save_pixel = f'SaveEngineAssets(project=["{project_id}"], filePath=["{filename}"], content=["<encode>{schema_json}</encode>"], comment=["Database schema for {database_id}"]);'
            i.run_pixel(save_pixel)

            return encode_to_string({
                "schema": schema_output,
                "file_saved": filename,
                "status": "success"
            })
        except Exception as e:
            # If file saving fails, still return the schema data
            return encode_to_string({
                "schema": schema_output,
                "file_save_error": str(e),
                "status": "partial_success"
            })
    else:
        # Just return the schema data without saving to file
        return encode_to_string(schema_json)


def execute_sql(database_id, sql_statement):
    """Execute a single SQL statement against a database.

    Automatically refreshes database ontology after DDL operations (CREATE/DROP/ALTER TABLE).

    Args:
        database_id (str): The unique identifier of the target database
        sql_statement (str): The SQL statement to execute

    Returns:
        str: JSON string with execution results and ontology refresh status
    """
    from semoss import Insight

    try:
        if not database_id or not sql_statement:
            return json.dumps({
                "status": "error",
                "error": "database_id and sql_statement are required"
            })

        insight = Insight()

        # Clean up the SQL
        clean_sql = sql_statement.strip()
        if '\\n' in clean_sql:
            clean_sql = clean_sql.replace('\\n', '\n').replace('\\t', '\t')

        # Determine if it's a SELECT query or DDL/DML
        sql_upper = clean_sql.upper().strip()
        is_select = sql_upper.startswith(('SELECT', 'WITH', 'SHOW', 'DESCRIBE', 'EXPLAIN'))

        if is_select:
            # Use Collect for SELECT queries
            pixel = f'Database(database=["{database_id}"]) | Query("<encode>{clean_sql}</encode>") | Collect(100);'
        else:
            # Use ExecQuery for DDL/DML
            pixel = f'Database(database=["{database_id}"]) | Query("<encode>{clean_sql}</encode>") | ExecQuery();'

        response = insight.run_pixel(pixel)
        result = extract_pixel_output(response)

        response_data = {
            "status": "success",
            "sql_statement": clean_sql,
            "result": result,
            "is_select": is_select
        }

        # If this is a DDL operation (not SELECT), auto-refresh ontology
        if not is_select:
            try:
                # Check if this is a table-related DDL operation
                ddl_keywords = ['CREATE TABLE', 'DROP TABLE', 'ALTER TABLE', 'CREATE INDEX', 'DROP INDEX']
                sql_upper_clean = ' '.join(sql_upper.split())  # Normalize whitespace

                is_table_ddl = any(keyword in sql_upper_clean for keyword in ddl_keywords)

                if is_table_ddl:
                    # Auto-refresh the ontology
                    ontology_result = refresh_database_ontology(database_id)
                    ontology_data = json.loads(base64.b64decode(ontology_result).decode('utf-8'))

                    response_data["ontology_refresh"] = {
                        "status": ontology_data.get("status"),
                        "synchronized_tables": ontology_data.get("synchronized_tables", []),
                        "table_count": ontology_data.get("table_count", 0)
                    }
                    response_data["auto_refreshed"] = True
                else:
                    response_data["auto_refreshed"] = False
                    response_data["refresh_reason"] = "Not a table DDL operation"

            except Exception as refresh_error:
                response_data["ontology_refresh"] = {
                    "status": "error",
                    "error": str(refresh_error)
                }
                response_data["auto_refreshed"] = False
                # Don't fail the main operation if ontology refresh fails

        return json.dumps(response_data)

    except Exception as e:
        return json.dumps({
            "status": "error",
            "error": str(e),
            "sql_statement": sql_statement
        })


def debug_database_tables(database_id):
    """Cross-check SQL-visible tables against ontology (metamodel) nodes for a database.

    Read-only diagnostic used by the portal's "Debug Tables" action to help identify
    drift between the raw JDBC tables and the tables SEMOSS has registered in its
    ontology. Never mutates the database.

    Args:
        database_id (str): The unique identifier of the database to inspect

    Returns:
        str: Base64 encoded JSON string with:
            - sql_tables: {"headers": [...], "values": [[table_name, table_schema], ...]}
              as returned by an INFORMATION_SCHEMA.TABLES query
            - metamodel_nodes: list of conceptual table names known to the ontology
            - status: "success" if at least one of the two lookups succeeded,
              otherwise "error" with an explanatory message
    """
    from semoss import Insight

    database_id = str(database_id or "").strip()
    if not database_id:
        return encode_to_string({"status": "error", "error": "database_id is required."})

    insight = Insight()
    result = {"status": "success", "database_id": database_id}

    try:
        sql_pixel = (
            f'Database(database=[{json.dumps(database_id)}]) | '
            'Query("<encode>SELECT TABLE_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES '
            'WHERE TABLE_TYPE = \'BASE TABLE\';</encode>") | Collect(500);'
        )
        sql_output = extract_pixel_output(insight.run_pixel(sql_pixel)) or {}
        result["sql_tables"] = {
            "headers": sql_output.get("headers", []) if isinstance(sql_output, dict) else [],
            "values": sql_output.get("values", []) if isinstance(sql_output, dict) else [],
        }
    except Exception as exc:
        result["sql_tables_error"] = str(exc)
        result["sql_tables"] = {"headers": [], "values": []}

    try:
        metamodel_pixel = f'GetDatabaseMetamodel(database=[{json.dumps(database_id)}], options=["dataTypes"]);'
        metamodel = extract_pixel_output(insight.run_pixel(metamodel_pixel)) or {}
        nodes = metamodel.get("nodes", []) if isinstance(metamodel, dict) else []
        result["metamodel_nodes"] = [
            node.get("conceptualName") for node in nodes
            if isinstance(node, dict) and node.get("conceptualName")
        ]
    except Exception as exc:
        result["metamodel_error"] = str(exc)
        result["metamodel_nodes"] = []

    if result.get("sql_tables_error") and result.get("metamodel_error"):
        result["status"] = "error"
        result["error"] = "Failed to retrieve both SQL tables and ontology metamodel."

    return encode_to_string(result)


def search_database(search_term):
    """Return normalized databases whose names contain the search term."""
    normalized_term = str(search_term or "").strip().casefold()
    databases = _list_databases()
    if not normalized_term:
        return databases
    return [
        database
        for database in databases
        if normalized_term
        in str(database.get("database_display_name") or database.get("database_name") or "").casefold()
    ]


def create_new_db_script(database_name, initial_sql=None):
    """Create a new H2 database.

    Args:
        database_name (str): Name of the database to create
        initial_sql (str, optional): SQL script to execute after database creation

    Returns:
        str: Base64 encoded JSON payload describing the create result
    """
    from semoss import Insight

    debug_info = []
    debug_info.append(f"create_new_db_script called with: '{database_name}'")

    normalized_name = (database_name or "").strip()
    if not normalized_name:
        debug_info.append("Error: database_name is required")
        return encode_to_string({
            "status": "error",
            "error": "database_name is required.",
            "debug": debug_info
        })

    debug_info.append(f"Normalized name: '{normalized_name}'")

    # Check if database already exists
    debug_info.append("Checking for existing databases...")
    try:
        existing_databases = list_databases_output()
        debug_info.append(f"Found {len(existing_databases)} existing databases")

        existing_database = next(
            (db for db in existing_databases if str(db.get("database_name", "")).lower() == normalized_name.lower()),
            None,
        )
        if existing_database is not None:
            debug_info.append(f"Database already exists with ID: {existing_database.get('database_id')}")
            return encode_to_string({
                "status": "exists",
                "database_name": existing_database.get("database_name"),
                "database_id": existing_database.get("database_id"),
                "debug": debug_info
            })
    except Exception as e:
        debug_info.append(f"Error checking existing databases: {str(e)}")
        # Continue with creation anyway

    # Create new database
    debug_info.append("Creating new database...")
    insight = Insight()
    create_pixel = f'CreateEmptyRdbmsDatabase(database=[{json.dumps(normalized_name)}], rdbmsType=["H2_DB"], username=["sa"], password=[""]);'
    debug_info.append(f"Pixel command: {create_pixel}")

    try:
        response = insight.run_pixel(create_pixel)
        debug_info.append(f"Pixel response length: {len(response) if response else 0}")

        database_id = None
        database_name = None

        # Extract database info from the creation response
        for item in response or []:
            if "ERROR" in (item.get("operationType", []) or []):
                raise RuntimeError(str(item.get("output") or "Database creation failed."))

            # Look for the database info in pixelReturn
            pixel_returns = item.get("pixelReturn", [])
            for pixel_return in pixel_returns:
                output = pixel_return.get("output", {})
                if isinstance(output, dict) and output.get("database_id"):
                    database_id = output.get("database_id")
                    database_name = output.get("database_name", normalized_name)
                    debug_info.append(f"Found database_id in response: {database_id}")
                    break

            if database_id:
                break

        # If we got the database info from the response, return it directly
        if database_id:
            debug_info.append("Successfully extracted database info from creation response")
            return encode_to_string({
                "status": "success",
                "database_name": database_name,
                "database_id": database_id,
                "debug": debug_info
            })
        else:
            debug_info.append("No database_id found in creation response")
    except Exception as exc:
        debug_info.append(f"Exception during creation: {str(exc)}")
        return encode_to_string({
            "status": "error",
            "database_name": normalized_name,
            "error": str(exc),
            "debug": debug_info
        })

    # Verify database was created
    debug_info.append("Verifying database was created...")
    all_databases_after = list_databases_output()
    debug_info.append(f"Found {len(all_databases_after)} total databases after creation")

    created_database = next(
        (db for db in all_databases_after if str(db.get("database_name", "")).lower() == normalized_name.lower()),
        None,
    )

    if created_database is None:
        debug_info.append("Database not found in list after creation")
        database_names = [db.get("database_name", "UNKNOWN") for db in all_databases_after]
        debug_info.append(f"Available databases: {database_names}")
        return encode_to_string({
            "status": "error",
            "database_name": normalized_name,
            "error": f"Database '{normalized_name}' was not found after creation.",
            "debug": debug_info
        })

    debug_info.append(f"Successfully found created database with ID: {created_database.get('database_id')}")
    return encode_to_string({
        "status": "success",
        "database_name": created_database.get("database_name"),
        "database_id": created_database.get("database_id"),
        "debug": debug_info
    })


def create_new_db(database_name=None, user_intent=None):
    """Create a new database.

    Args:
        database_name (str): Name of the database to be created
        user_intent (str, optional): The intent of the user or requirement for the database

    Returns:
        str: JSON string with status, database_name, and database_id
    """
    debug_log = []

    try:
        debug_log.append(f"create_new_db called with database_name='{database_name}', user_intent='{user_intent}'")

        # Call the internal function
        result = create_new_db_script(database_name=database_name, initial_sql=None)
        debug_log.append(f"create_new_db_script returned: {result}")

        # For MCP, decode the base64 result and return plain JSON
        try:
            decoded_bytes = base64.b64decode(result)
            decoded_str = decoded_bytes.decode('utf-8')
            debug_log.append("Successfully decoded result")

            # Parse the result to add debug info
            try:
                result_obj = json.loads(decoded_str)
                result_obj["debug_log"] = debug_log
                return json.dumps(result_obj)
            except Exception:
                return decoded_str

        except Exception as decode_error:
            debug_log.append(f"Failed to decode base64 result: {str(decode_error)}")
            debug_log.append(f"Raw result was: {result}")
            return json.dumps({
                "status": "error",
                "error": f"Failed to decode result: {str(decode_error)}",
                "debug_log": debug_log
            })

    except Exception as main_error:
        debug_log.append(f"Main function failed: {str(main_error)}")
        return json.dumps({
            "status": "error",
            "error": str(main_error),
            "debug_log": debug_log
        })


def create_table_from_schema(database_id, table_name, columns):
    """Create a table from a schema definition.

    Automatically refreshes database ontology after table creation.

    Args:
        database_id (str): The unique identifier of the target database
        table_name (str): Name of the table to create
        columns (list): List of column definitions, each containing name, type, and optional constraints

    Returns:
        str: JSON string with creation results and ontology refresh status
    """
    try:
        if not database_id or not table_name or not columns:
            return json.dumps({
                "status": "error",
                "error": "database_id, table_name, and columns are required"
            })

        # Build CREATE TABLE statement
        column_defs = []
        for col in columns:
            if isinstance(col, dict):
                name = col.get('name', '')
                col_type = col.get('type', 'VARCHAR(255)')
                constraints = col.get('constraints', '')

                if name:
                    col_def = f"{name} {col_type}"
                    if constraints:
                        col_def += f" {constraints}"
                    column_defs.append(col_def)
            elif isinstance(col, str):
                column_defs.append(col)

        if not column_defs:
            return json.dumps({
                "status": "error",
                "error": "No valid column definitions provided"
            })

        # Generate CREATE TABLE SQL
        # NOTE: backslashes are not permitted inside f-string expressions before
        # Python 3.12, so the column list is joined before interpolation to keep
        # this compatible with the Python 3.10 runtime that hosts this driver.
        columns_sql = ",\n  ".join(column_defs)
        create_sql = f"CREATE TABLE {table_name} (\n  {columns_sql}\n);"

        # Execute the SQL
        return execute_sql(database_id, create_sql)

    except Exception as e:
        return json.dumps({
            "status": "error",
            "error": str(e)
        })


def refresh_database_ontology(database_id):
    """Refresh JDBC metadata and synchronize the local master ontology.

    Args:
        database_id (str): The unique identifier of the database to refresh

    Returns:
        str: JSON string with status and synchronized table information
    """
    from semoss import Insight

    try:
        database_id = str(database_id)
        insight = Insight()

        # Get initial metamodel
        initial_pixel = f'GetDatabaseMetamodel(database=[{json.dumps(database_id)}], options=["dataTypes", "physicalTypes", "additionalDataTypes", "logicalNames", "descriptions", "positions"]);'
        initial_result = insight.run_pixel(initial_pixel)
        initial = extract_pixel_output(initial_result) or {}

        # Discover new tables and views from JDBC
        discover_pixel = f'ExternalUpdateJdbcTablesAndViews(database=[{json.dumps(database_id)}]);'
        discover_result = insight.run_pixel(discover_pixel)
        discovered = extract_pixel_output(discover_result) or {}

        # Get the list of tables that were discovered
        tables = [table for table in (discovered.get("tables", []) if isinstance(discovered, dict) else []) if table]

        # Update schema for each discovered table
        for table_name in tables:
            schema_pixel = f'ExternalUpdateJdbcSchema(database=[{json.dumps(database_id)}], filters=[{json.dumps(table_name)}]);'
            insight.run_pixel(schema_pixel)

        # Get the refreshed metamodel
        refresh_pixel = f'GetDatabaseMetamodel(database=[{json.dumps(database_id)}], options=["dataTypes", "physicalTypes", "additionalDataTypes", "logicalNames", "descriptions", "positions"]);'
        refresh_result = insight.run_pixel(refresh_pixel)
        refreshed = extract_pixel_output(refresh_result) or {}

        # Prepare metamodel for upload
        metamodel = refreshed or initial or {}
        selected = {str(table) for table in tables}
        metamodel_tables = {}
        position_map = {}

        for index, node in enumerate(metamodel.get("nodes", []) if isinstance(metamodel, dict) else []):
            if not isinstance(node, dict):
                continue
            table_name = node.get("conceptualName") or node.get("name")
            if not table_name or (selected and str(table_name) not in selected):
                continue
            columns = [column for column in (node.get("propSet", []) or []) if column]
            if not columns:
                continue
            row_identifier = node.get("primaryKey") or node.get("primaryKeyColumn") or columns[0]
            metamodel_tables[f"{table_name}.{row_identifier}"] = columns
            position_map[str(table_name)] = {
                "top": 3.9127503633499146 + (index // 3) * 3,
                "left": (index % 3) * 3,
            }

        # Upload the metamodel
        upload_metamodel = {"relationships": [], "tables": metamodel_tables}
        upload_pixel = f'RdbmsExternalUpload(database=[{json.dumps(database_id)}], metamodel=[{json.dumps(upload_metamodel)}], existing=[true]);'
        insight.run_pixel(upload_pixel)

        # Save OWL positions
        positions_pixel = f'META|SaveOwlPositions(database=[{json.dumps(database_id)}], positionMap=[{json.dumps(position_map)}]);'
        insight.run_pixel(positions_pixel)

        # Sync with local master
        sync_pixel = f'SyncDatabaseWithLocalMaster(database=[{json.dumps(database_id)}]);'
        insight.run_pixel(sync_pixel)

        return encode_to_string({
            "status": "success",
            "database_id": database_id,
            "synchronized_tables": tables,
            "table_count": len(tables)
        })

    except Exception as exc:
        return encode_to_string({
            "status": "error",
            "database_id": database_id,
            "error": str(exc)
        })


def create_ontology_metadata(database_id):
    """Create and save ontology metadata files for the database.

    Args:
        database_id (str): The unique identifier of the database to document

    Returns:
        str: JSON string with status and file information
    """
    from semoss import Insight

    try:
        insight = Insight()

        # Get the current metamodel
        metamodel_pixel = f'GetDatabaseMetamodel(database=["{database_id}"], options=["dataTypes", "descriptions"]);'
        metamodel_response = insight.run_pixel(metamodel_pixel)
        metamodel = extract_pixel_output(metamodel_response) or {}

        # Generate OWL content
        owl_content = '''<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#"
         xmlns:owl="http://www.w3.org/2002/07/owl#"
         xml:base="http://semoss.org/ontologies/database">

<owl:Ontology rdf:about="http://semoss.org/ontologies/database"/>

'''

        # Add classes and properties for each table
        if metamodel.get("nodes"):
            for node in metamodel["nodes"]:
                table_name = node.get("conceptualName")
                if not table_name:
                    continue

                # Add class for table
                owl_content += f'  <owl:Class rdf:about="#{table_name}">\n'
                owl_content += f'    <rdfs:label>{table_name}</rdfs:label>\n'
                owl_content += f'  </owl:Class>\n\n'

                # Add properties for columns
                for prop in node.get("propSet", []):
                    physical_name = f"{table_name}__{prop}"
                    description = metamodel.get("descriptions", {}).get(physical_name, "")

                    owl_content += f'  <owl:DatatypeProperty rdf:about="#{table_name}__{prop}">\n'
                    owl_content += f'    <rdfs:domain rdf:resource="#{table_name}"/>\n'
                    owl_content += f'    <rdfs:label>{prop}</rdfs:label>\n'
                    if description:
                        owl_content += f'    <rdfs:comment>{description}</rdfs:comment>\n'
                    owl_content += f'  </owl:DatatypeProperty>\n\n'

        owl_content += '</rdf:RDF>'

        # Save the OWL file using SaveEngineAssets
        project_id = resolve_project_id()
        owl_filename = f"ontology/{database_id}_metadata.owl"

        save_pixel = f'SaveEngineAssets(project=["{project_id}"], filePath=["{owl_filename}"], content=["<encode>{owl_content}</encode>"], comment=["Database ontology metadata for {database_id}"]);'
        insight.run_pixel(save_pixel)

        # Also create a JSON summary
        summary = {
            "database_id": database_id,
            "created_at": "timestamp_placeholder",
            "tables": []
        }

        if metamodel.get("nodes"):
            for node in metamodel["nodes"]:
                table_name = node.get("conceptualName")
                if table_name:
                    table_info = {
                        "name": table_name,
                        "columns": node.get("propSet", []),
                        "column_count": len(node.get("propSet", []))
                    }
                    summary["tables"].append(table_info)

        summary_json = json.dumps(summary, indent=2)
        summary_filename = f"ontology/{database_id}_summary.json"

        summary_pixel = f'SaveEngineAssets(project=["{project_id}"], filePath=["{summary_filename}"], content=["<encode>{summary_json}</encode>"], comment=["Database summary metadata for {database_id}"]);'
        insight.run_pixel(summary_pixel)

        return encode_to_string({
            "status": "success",
            "database_id": database_id,
            "owl_file": owl_filename,
            "summary_file": summary_filename,
            "table_count": len(summary["tables"])
        })

    except Exception as exc:
        return encode_to_string({
            "status": "error",
            "database_id": database_id,
            "error": str(exc)
        })


def test():
    """Test function to verify module is loading correctly.

    Returns:
        str: Success message confirming module loaded
    """
    return "Module loaded successfully"

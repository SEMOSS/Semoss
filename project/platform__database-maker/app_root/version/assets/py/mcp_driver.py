"""
MCP Driver Module for SEMOSS Database Management.

This module provides the core functionality regarding the Model Context Protocol (MCP)
integration for the database management application. It handles:
1.  Schema Extraction: Retrieving and simplifying database metamodels for LLM consumption.
2.  Asset Persistence: Saving generated schemas and temporary files to the Insight's asset store.
3.  Data Encoding: Managing Base64 encoding for safe transport of JSON data back to the frontend.
4.  Script Execution: Running prepared SQL scripts statement-by-statement against a target database.
5.  Database Creation: Provisioning a database by mirroring the portal's bootstrap upload flow.

Dependencies:
    - semoss.Insight: For executing Pixel commands against the backend platform.
    - ai_server.DatabaseEngine: For direct query execution.
    - json, base64: For data serialization and encoding.
"""

import base64
import json
from pathlib import Path


def extract_pixel_output(response):
    """
    Extracts the first pixel output payload from a SEMOSS response.

    Args:
        response (list[dict] | None): Raw response from Insight.run_pixel.

    Returns:
        object: The first output payload, or None when unavailable.
    """
    if not response:
        return None

    pixel_return = response[0].get("pixelReturn", [])
    if not pixel_return:
        return None

    return pixel_return[0].get("output")


def load_local_semoss_config():
    """
    Loads the project configuration from the sibling semoss_config asset folder.

    Returns:
        dict: Parsed configuration when available, otherwise an empty dict.
    """
    config_path = Path(__file__).resolve().parents[1] / "semoss_config" / "config.json"
    if not config_path.exists():
        return {}

    with config_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def resolve_project_id(project_id=None):
    """
    Resolves the current SEMOSS project/app id for asset-scoped Pixel commands.

    Args:
        project_id (str | None): Optional explicit project id.

    Returns:
        str: The resolved project id.
    """
    if project_id:
        return str(project_id)

    config = load_local_semoss_config()
    for key in ("project_id", "app_id", "projectId", "appId"):
        value = config.get(key)
        if value:
            return str(value)

    raise ValueError("Project id could not be resolved. Pass project_id explicitly or add it to semoss_config/config.json.")


def list_databases_output():
    """
    Returns the raw database list from SEMOSS.

    Returns:
        list[dict]: Available database entries.
    """
    from semoss import Insight

    insight = Insight()
    output = extract_pixel_output(insight.run_pixel("GetDatabases();"))
    return output if isinstance(output, list) else []

def add_to_context(context):
    """
    Pass-through function to prepare context strings for the orchestrator.
    
    Args:
        context (str): The context string (usually a JSON schema representation).
        
    Returns:
        str: The same context string, ready to be acted upon by the MCP tool.
    """
    return context

def run_query(database_id, query):
    """
    Executes a SQL query against a specified database engine.

    Args:
        database_id (str): The unique identifier (GUID) of the target database.
        query (str): The SQL query string to execute.

    Returns:
        dict: The result set of the query execution.
    """
    from ai_server import DatabaseEngine
    db = DatabaseEngine(engine_id=database_id)
    return db.execQuery(query=query)


def split_sql_script(script):
    """
    Splits a SQL script into statements while ignoring semicolons inside strings or comments.

    Args:
        script (str): Raw SQL script content.

    Returns:
        list[str]: Ordered executable SQL statements.
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
    """
    Executes a prepared SQL script statement-by-statement against a target database.

    Args:
        database_id (str): The unique identifier (GUID) of the target database.
        script (str): The SQL script to run.

    Returns:
        str: Base64 encoded JSON payload summarizing execution results.
    """
    from semoss import Insight

    normalized_script = (script or "").strip()
    if not normalized_script:
        return encode_to_string({
            "status": "error",
            "error": "Script is empty.",
            "statement_count": 0,
            "results": [],
        })

    statements = split_sql_script(normalized_script)
    if not statements:
        return encode_to_string({
            "status": "error",
            "error": "No executable SQL statements were found.",
            "statement_count": 0,
            "results": [],
        })

    insight = Insight()
    results = []
    last_result = None

    for index, statement in enumerate(statements, start=1):
        statement_type = statement.split(None, 1)[0].upper() if statement.split() else "UNKNOWN"
        try:
            if statement_type in {"SELECT", "WITH", "SHOW", "DESCRIBE", "EXPLAIN"}:
                pixel = (
                    f'Database(database=[{json.dumps(str(database_id))}]) '
                    f'| Query("<encode>{statement}</encode>") '
                    '| Collect(100);'
                )
            else:
                pixel = (
                    f'Database(database=[{json.dumps(str(database_id))}]) '
                    f'| Query("<encode>{statement}</encode>") '
                    '| ExecQuery();'
                )

            response = insight.run_pixel(pixel)
            result = extract_pixel_output(response)
            last_result = result
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
            return encode_to_string({
                "status": "error",
                "error": str(exc),
                "statement_count": len(results),
                "results": results,
                "failed_statement": statement,
            })

    return encode_to_string({
        "status": "success",
        "statement_count": len(results),
        "results": results,
        "last_result": last_result,
    })


def create_new_db_script(database_name, project_id=None, sample_asset_path="version/assets/csv/sample.csv", temp_table_name="temp_init_table"):
    """
    Creates a database by mirroring the portal bootstrap flow that uploads a sample CSV,
    discovers the created engine id, and removes the temporary bootstrap table.

    Args:
        database_name (str): Name of the database to create.
        project_id (str, optional): Project/app id used for the asset-space upload.
        sample_asset_path (str, optional): Asset path to the bootstrap CSV file.
        temp_table_name (str, optional): Temporary table name to create and then remove.

    Returns:
        str: Base64 encoded JSON payload describing the create result.
    """
    from semoss import Insight

    normalized_name = (database_name or "").strip()
    if not normalized_name:
        return encode_to_string({
            "status": "error",
            "error": "database_name is required.",
        })

    existing_database = next(
        (db for db in list_databases_output() if str(db.get("database_name", "")).lower() == normalized_name.lower()),
        None,
    )
    if existing_database is not None:
        return encode_to_string({
            "status": "exists",
            "database_name": existing_database.get("database_name"),
            "database_id": existing_database.get("database_id"),
        })

    insight = Insight()
    app_space = resolve_project_id(project_id)
    data_type_map = {"col1": "STRING", "col2": "STRING"}
    create_pixel = (
        f'RdbmsUploadTableData(database=[{json.dumps(normalized_name)}], '
        f'space=[{json.dumps(app_space)}], '
        f'filePath=[{json.dumps(sample_asset_path)}], '
        f'delimiter=[","], '
        f'dataTypeMap=[{json.dumps(data_type_map)}], '
        'newHeaders=[{}], '
        'additionalDataTypes=[{}], '
        'descriptionMap=[{}], '
        'logicalNamesMap=[{}], '
        'existing=[false], '
        f'table=[{json.dumps(temp_table_name)}]);'
    )

    try:
        insight.run_pixel(create_pixel)
    except Exception as exc:
        return encode_to_string({
            "status": "error",
            "database_name": normalized_name,
            "error": str(exc),
        })

    created_database = next(
        (db for db in list_databases_output() if str(db.get("database_name", "")).lower() == normalized_name.lower()),
        None,
    )
    if created_database is None:
        return encode_to_string({
            "status": "error",
            "database_name": normalized_name,
            "error": f"Database '{normalized_name}' was not found after creation.",
        })

    cleanup_error = None
    database_id = created_database.get("database_id")
    if database_id:
        try:
            drop_pixel = (
                f'Database(database=[{json.dumps(str(database_id))}]) '
                f'| Query("<encode>DROP TABLE {temp_table_name};</encode>") '
                '| ExecQuery();'
            )
            insight.run_pixel(drop_pixel)
        except Exception as exc:
            cleanup_error = str(exc)

    return encode_to_string({
        "status": "success",
        "database_name": created_database.get("database_name"),
        "database_id": database_id,
        "bootstrap_asset": sample_asset_path,
        "temporary_table": temp_table_name,
        "cleanup_status": "warning" if cleanup_error else "success",
        "cleanup_error": cleanup_error,
    })

def get_schema(database_id, playground=True):
    """
    Retrieves and simplifies the schema for a given database ID.

    This function fetches the complex metamodel from the SEMOSS engine,
    simplifies it into a flat list of tables and columns (optimized for LLM context),
    and persists this simplified schema to 'schema/schema.json' in the app assets.

    Args:
        database_id (str): The unique identifier of the database to inspect.

    Returns:
        str: A Base64 encoded JSON string representing the simplified schema.
    """
    from semoss import Insight
    import json
    
    i = Insight()
    
    # Get the raw metamodel using Pixel
    pixel_cmd = f'GetDatabaseMetamodel(database=["{database_id}"], options=["dataTypes", "descriptions"]);'
    resp = i.run_pixel(pixel_cmd)

    #print("response is.. ", resp)
    
    # Extract the output from the pixel response
    metamodel = {}
    if resp and len(resp) > 0:
        pixel_return = resp[0].get('pixelReturn', [])
        if pixel_return and len(pixel_return) > 0:
             metamodel = pixel_return[0].get('output', {})
    
    # Transform the complex metamodel into a clean schema structure
    schema_output = []
    #print("metamodel.. ", metamodel)
    
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
        
    # Serialize schema for file saving
    schema_json = json.dumps(schema_output, indent=2)

    #print("Final schema is ", schema_json)
    
    # Save to Insight Assets using Pixel via Insight class
    i = Insight()
    
    # 1. Create Directory: NewInsightDir(dir=["schema"]);
    i.run_pixel(f'NewInsightAssetsDirectory(filePath=["schema"]);')
    
    # 2. Create File: NewInsightFile(filePath=["schema/schema.json"]);
    i.run_pixel(f'NewInsightAssetsFile(filePath=["schema/schema_{database_id}.json"]);')
    
    # 3. Write Content: SaveFile(filePath=["schema/schema.json"], content=["..."]);
    # We need to construct the pixel string carefully for the encoded content
    save_pixel = f'SaveInsightAssets(filePath=["schema/schema_{database_id}.json"], content=["<encode>{schema_json}</encode>"]);'
    i.run_pixel(save_pixel)

    schema_output = f"Schema saved to schema/schema__{database_id}.json"
    
    if playground:
        return encode_to_string(schema_output)

    return encode_to_string(schema_json)



def delete_database(database_id):
    """
    Deletes a specified database.

    Args:
        database_id (str): The unique identifier of the database to delete.

    Returns:
        str: Base64 encoded JSON message indicating success.
    """
    from semoss import Insight
    i = Insight()
    pixel_cmd = f'DeleteDatabase(database=["{database_id}"]);'
    i.run_pixel(pixel_cmd)
    return encode_to_string({"status": "success", "database_id": database_id})

def search_database(search_term):
    """
    Searches for databases matching the search term by name.

    Args:
        search_term (str): The name or partial name to search for.

    Returns:
        str: Base64 encoded JSON list of matching databases.
    """
    matches = []
    for db in list_databases_output():
        name = db.get('database_name', '')
        if search_term and search_term.lower() in name.lower():
            matches.append(db)

    return encode_to_string(matches)

def create_new_db(database_name=None, user_intent=None):
    """
    Creates a new database given this database name.

    The main work is done by the user this cannot be a non-ui or auto complete. 

    Args:
        database_name (str): Name of the databse to be created.
        user_intent (str): The intent of the user or the requirement from the user based on which the database needs to be created

    Returns:
        str: Success message as string .
    """

    del user_intent
    return create_new_db_script(database_name=database_name)

def encode_to_string(data):
    """
    Helper function to serialize and Base64 encode data for transport.

    Args:
        data (any): The data to encode (dict, list, or string).

    Returns:
        str: Base64 encoded string representation of the input data.
    """
    if isinstance(data, (dict, list)):
        data = json.dumps(data, separators=(',', ':'))
    elif not isinstance(data, str):
        data = str(data)
        
    data_bytes = data.encode("utf-8")
    output = base64.b64encode(data_bytes)
    return str(output)[2:-1]

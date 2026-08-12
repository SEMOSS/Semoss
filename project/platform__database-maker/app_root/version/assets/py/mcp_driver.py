"""Canonical MCP tools for creating and querying SEMOSS databases."""

import json
from pathlib import Path


BOOTSTRAP_ASSET_PATH = "version/assets/csv/database_bootstrap.csv"
BOOTSTRAP_TABLE = "SMSS_DATABASE_BOOTSTRAP"


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


def run_script(database_id, script):
    """Execute a SQL script statement-by-statement with checked Pixel results."""
    database_id = str(database_id or "").strip()
    if not database_id:
        raise ValueError("database_id is required.")
    statements = _split_sql_script(str(script or "").strip())
    if not statements:
        raise ValueError("No executable SQL statements were found.")

    results = []
    for index, statement in enumerate(statements, start=1):
        statement_type = statement.split(None, 1)[0].upper()
        if statement_type in {"SELECT", "WITH", "SHOW", "DESCRIBE", "EXPLAIN"}:
            pixel = (
                f'Database(database=[{json.dumps(database_id)}]) '
                f'| Query("<encode>{statement}</encode>") | Collect(100);'
            )
        else:
            pixel = (
                f'Database(database=[{json.dumps(database_id)}]) '
                f'| Query("<encode>{statement}</encode>") | ExecQuery();'
            )
        try:
            outputs = _run_pixel_checked(pixel)
        except Exception as exc:
            details = {
                "error": str(exc),
                "failed_statement_index": index,
                "failed_statement": statement[:500],
                "completed_statement_count": len(results),
            }
            raise RuntimeError(json.dumps(details, ensure_ascii=False)) from exc
        result = outputs[-1] if outputs else None
        results.append(
            {
                "index": index,
                "statement_type": statement_type,
                "row_count": _row_count(result),
                "result": result,
            }
        )
    return {"status": "success", "statement_count": len(results), "results": results}


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


def get_schema(database_id, playground=False):
    """Return a simplified database schema and optionally save it to insight assets."""
    database_id = str(database_id or "").strip()
    if not database_id:
        raise ValueError("database_id is required.")
    outputs = _run_pixel_checked(
        f'GetDatabaseMetamodel(database=[{json.dumps(database_id)}], options=["dataTypes", "descriptions"]);'
    )
    metamodel = outputs[0] if outputs and isinstance(outputs[0], dict) else {}
    schema = []
    data_types = metamodel.get("dataTypes", {})
    descriptions = metamodel.get("descriptions", {})
    for node in metamodel.get("nodes", []):
        table_name = node.get("conceptualName")
        if not table_name:
            continue
        columns = []
        for prop in node.get("propSet", []):
            key = f"{table_name}__{prop}"
            columns.append(
                {
                    "name": prop,
                    "type": data_types.get(key, "UNKNOWN"),
                    "description": descriptions.get(key, ""),
                }
            )
        schema.append({"table_name": table_name, "columns": columns})

    asset_path = None
    warnings = []
    if bool(playground):
        asset_path, warnings = _save_schema_asset(database_id, schema)
    return {
        "database_id": database_id,
        "tables": schema,
        "table_count": len(schema),
        "asset_path": asset_path,
        "warnings": warnings,
    }


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


def create_new_db(database_name):
    """Create one H2 database, or return the single existing exact match."""
    normalized_name = str(database_name or "").strip()
    if not normalized_name:
        raise ValueError("database_name is required.")

    before = _list_databases()
    matches = _exact_database_matches(normalized_name, before)
    if len(matches) == 1:
        database = matches[0]
        return {
            "status": "exists",
            "created": False,
            "database_id": database.get("database_id"),
            "database_name": database.get("database_name"),
            "warnings": [],
        }
    if len(matches) > 1:
        conflict = {
            "error": "Multiple databases already have this exact name; creation was not attempted.",
            "database_name": normalized_name,
            "database_ids": [database.get("database_id") for database in matches],
            "do_not_retry": True,
        }
        raise RuntimeError(json.dumps(conflict, ensure_ascii=False))

    before_ids = {database.get("database_id") for database in before if database.get("database_id")}
    data_type_map = {"col1": "STRING", "col2": "STRING"}
    create_pixel = (
        f'RdbmsUploadTableData(database=[{json.dumps(normalized_name)}], '
        f'space=[{json.dumps(_resolve_project_id())}], '
        f'filePath=[{json.dumps(BOOTSTRAP_ASSET_PATH)}], delimiter=[","], '
        f'dataTypeMap=[{json.dumps(data_type_map)}], newHeaders=[{{}}], '
        'additionalDataTypes=[{}], descriptionMap=[{}], logicalNamesMap=[{}], '
        f'existing=[false], table=[{json.dumps(BOOTSTRAP_TABLE)}]);'
    )
    _run_pixel_checked(create_pixel)

    after = _list_databases()
    after_matches = _exact_database_matches(normalized_name, after)
    new_matches = [
        database for database in after_matches if database.get("database_id") not in before_ids
    ]
    if len(new_matches) != 1:
        verification_error = {
            "error": "Database creation could not be verified unambiguously.",
            "database_name": normalized_name,
            "matching_database_ids": [database.get("database_id") for database in after_matches],
            "new_database_ids": [database.get("database_id") for database in new_matches],
            "may_have_side_effect": True,
            "do_not_retry": True,
        }
        raise RuntimeError(json.dumps(verification_error, ensure_ascii=False))

    created = new_matches[0]
    cleanup_error = None
    try:
        _run_pixel_checked(
            f'Database(database=[{json.dumps(str(created.get("database_id")))}]) '
            f'| Query("<encode>DROP TABLE {BOOTSTRAP_TABLE}</encode>") | ExecQuery();'
        )
    except Exception as exc:
        cleanup_error = str(exc)

    warnings = []
    if cleanup_error:
        warnings.append(f"Database was created, but bootstrap table cleanup failed: {cleanup_error}")
    return {
        "status": "created",
        "created": True,
        "database_id": created.get("database_id"),
        "database_name": created.get("database_name") or normalized_name,
        "cleanup_status": "warning" if cleanup_error else "success",
        "warnings": warnings,
    }

/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.automation;

public final class AutomationConstants {

	private AutomationConstants() {}

	// -- File names ----------------------------------------------------------------

	public static final String AUTOMATION_FILE_NAME = "automation.json";
	public static final String AUTOMATION_CONFIG_FILE_NAME = "automation-config.json";

	public static final String SENSITIVE_MASK = "***";

	// -- DB Table names ------------------------------------------------------------

	public static final String TABLE_AUTOMATION_RUNS = "AUTOMATION_RUNS";
	public static final String TABLE_AUTOMATION_NODE_OUTPUTS = "AUTOMATION_NODE_OUTPUTS";
	public static final String TABLE_AUTOMATION_ACTIVE_RUN = "AUTOMATION_ACTIVE_RUN";

	// -- AUTOMATION_RUNS columns ---------------------------------------------------

	public static final String RUN_ID = "RUN_ID";
	public static final String PROJECT_ID = "PROJECT_ID";
	public static final String AUTOMATION_ID = "AUTOMATION_ID";
	public static final String STATUS = "STATUS";
	public static final String TRIGGER_TYPE = "TRIGGER_TYPE";
	public static final String STARTED_AT = "STARTED_AT";
	public static final String COMPLETED_AT = "COMPLETED_AT";
	public static final String FAILED_NODE_ID = "FAILED_NODE_ID";
	public static final String ERROR_MESSAGE = "ERROR_MESSAGE";
	public static final String LAST_HEARTBEAT = "LAST_HEARTBEAT";
	public static final String TOTAL_NODES = "TOTAL_NODES";
	public static final String COMPLETED_NODES = "COMPLETED_NODES";
	public static final String CREATED_BY = "CREATED_BY";
	public static final String CANCEL_REQUESTED = "CANCEL_REQUESTED";

	// -- AUTOMATION_ACTIVE_RUN columns ---------------------------------------------

	public static final String CLAIMED_AT = "CLAIMED_AT";

	// -- AUTOMATION_NODE_OUTPUTS columns ------------------------------------------

	public static final String NODE_ID = "NODE_ID";
	public static final String NODE_LABEL = "NODE_LABEL";
	public static final String EXECUTION_ORDER = "EXECUTION_ORDER";
	public static final String DURATION_MS = "DURATION_MS";
	public static final String OUTPUT_VAR = "OUTPUT_VAR";
	public static final String OUTPUT_VALUE = "OUTPUT_VALUE";
	public static final String OUTPUT_PREVIEW = "OUTPUT_PREVIEW";

	// -- Run statuses --------------------------------------------------------------

	public static final String STATUS_RUNNING = "RUNNING";
	public static final String STATUS_SUCCESS = "SUCCESS";
	public static final String STATUS_FAILED = "FAILED";
	public static final String STATUS_INTERRUPTED = "INTERRUPTED";
	public static final String STATUS_CANCELLED = "CANCELLED";

	// -- Node statuses -------------------------------------------------------------

	public static final String NODE_STATUS_PENDING = "PENDING";
	public static final String NODE_STATUS_RUNNING = "RUNNING";
	public static final String NODE_STATUS_SUCCESS = "SUCCESS";
	public static final String NODE_STATUS_FAILED = "FAILED";
	public static final String NODE_STATUS_SKIPPED = "SKIPPED";

	// -- Trigger types -------------------------------------------------------------

	public static final String TRIGGER_MANUAL = "MANUAL";

	// -- Node types (Phase 1) ------------------------------------------------------

	public static final String NODE_TRIGGER = "trigger";
	public static final String NODE_DATABASE_ENGINE = "database-engine";
	public static final String NODE_STORAGE_ENGINE = "storage-engine";
	public static final String NODE_VECTOR_ENGINE = "vector-engine";
	public static final String NODE_MODEL_ENGINE = "model-engine";
	public static final String NODE_FUNCTION_ENGINE = "function-engine";
	public static final String NODE_WAIT = "wait";
	public static final String NODE_APP = "app";

	// -- Node config keys (node.config map fields, shared across executors) --------

	public static final String CONFIG_ENGINE_ID = "engineId";
	public static final String CONFIG_OPERATION = "operation";
	public static final String CONFIG_EXPRESSION = "expression";
	public static final String CONFIG_LIMIT = "limit";
	public static final String CONFIG_VALUES = "values";
	public static final String CONFIG_COMMAND = "command";
	public static final String CONFIG_CONTEXT = "context";
	public static final String CONFIG_PARAM_VALUES = "paramValues";
	public static final String CONFIG_PARAMS = "params";
	public static final String CONFIG_STORAGE_PATH = "storagePath";
	public static final String CONFIG_FILE_PATH = "filePath";
	public static final String CONFIG_FILE_NAMES = "fileNames";
	public static final String CONFIG_SECONDS = "seconds";
	public static final String CONFIG_PIXEL = "pixel";
	public static final String CONFIG_APP_ID = "appId";
	public static final String CONFIG_TIMEOUT_SECONDS = "timeoutSeconds";
	public static final String DEFAULT_STORAGE_PATH = "/";
	public static final String EMPTY_JSON_OBJECT = "{}";
	public static final String EMPTY_JSON_ARRAY = "[]";

	// -- Node operation values -------------------------------------------------------

	public static final String OP_READ = "read";
	public static final String OP_WRITE = "write";
	public static final String OP_LLM = "llm";
	public static final String OP_EMBEDDINGS = "embeddings";
	public static final String OP_VISION = "vision";
	public static final String OP_NER = "ner";
	public static final String OP_SEARCH = "search";
	public static final String OP_ADD_FILE = "add-file";
	public static final String OP_ADD_CSV = "add-csv";
	public static final String OP_LIST = "list";
	public static final String OP_DELETE = "delete";
	public static final String OP_DOWNLOAD = "download";
	public static final String OP_UPLOAD = "upload";
	public static final String OP_READ_BASE64 = "read-base64";

	// -- Node execution defaults / bounds --------------------------------------------

	public static final int DEFAULT_DB_QUERY_LIMIT = 50;
	public static final int DEFAULT_VECTOR_SEARCH_LIMIT = 5;
	public static final int DEFAULT_LIST_RUNS_LIMIT = 25;
	public static final int WAIT_MIN_SECONDS = 0;
	public static final int WAIT_MAX_SECONDS = 3600;
	public static final int WAIT_DEFAULT_SECONDS = 1;
	public static final int WAIT_CANCEL_CHECK_INTERVAL_SECONDS = 5;

	// -- automation.json document field names ----------------------------------------

	public static final String DOC_VERSION = "version";
	public static final String DOC_GRAPH = "graph";
	public static final String DOC_NODES = "nodes";
	public static final String DOC_EDGES = "edges";
	public static final int DOC_CURRENT_VERSION = 1;
	/**
	 * Optional {@code ${var}}/{@code ${config.KEY}} template resolved against the final run
	 * scope once all nodes complete, producing a workflow-specific human-readable summary
	 * (e.g. "Indexed 20 files") instead of a raw JSON blob for MCP/agent consumers.
	 */
	public static final String DOC_RESULT_MESSAGE_TEMPLATE = "resultMessageTemplate";

	// -- Node/edge field names --------------------------------------------------------

	public static final String NODE_FIELD_ID = "id";
	public static final String NODE_FIELD_TYPE = "type";
	public static final String NODE_FIELD_LABEL = "label";
	public static final String NODE_FIELD_CONFIG = "config";
	public static final String NODE_FIELD_OUTPUT_TRANSFORM = "outputTransform";
	public static final String NODE_FIELD_OUTPUT_VAR = "outputVar";
	public static final String EDGE_FIELD_SOURCE = "source";
	public static final String EDGE_FIELD_TARGET = "target";
	public static final String UNNAMED_NODE_LABEL = "unnamed";

	// -- automation-config.json entry field names ------------------------------------

	public static final String CONFIG_ENTRY_KEY = "key";
	public static final String CONFIG_ENTRY_VALUE = "value";
	public static final String CONFIG_ENTRY_SENSITIVE = "sensitive";

	// -- Output transform field names / modes ----------------------------------------

	public static final String TRANSFORM_MODE = "mode";
	public static final String TRANSFORM_COLUMN = "column";
	public static final String TRANSFORM_PATH = "path";
	public static final String TRANSFORM_MODE_RAW = "raw";
	public static final String TRANSFORM_MODE_ROWS_AS_OBJECTS = "rows-as-objects";
	public static final String TRANSFORM_MODE_FIRST_ROW = "first-row";
	public static final String TRANSFORM_MODE_COLUMN = "column";
	public static final String TRANSFORM_MODE_JSONPATH = "jsonpath";
	public static final String DATASET_HEADERS = "headers";
	public static final String DATASET_VALUES = "values";
	public static final String DATASET_DATA = "data";

	// -- Scope variable names ---------------------------------------------------------

	public static final String SCOPE_DATE = "date";
	public static final String SCOPE_TRIGGERED_AT = "triggered_at";
	public static final String SCOPE_RUN_ID = "run_id";
	public static final String TEST_RUN_ID = "test";
	public static final String SYSTEM_USER_ID = "system";

	// -- Result map keys ---------------------------------------------------------------

	public static final String RESULT_NODE_RESULTS = "nodeResults";
	public static final String RESULT_CANCEL_REQUESTED = "cancelRequested";
	public static final String RESULT_SIGNALLED_LOCALLY = "signalledLocally";
	public static final String RESULT_OUTPUT_VALUE = "outputValue";
	/** Human-readable, per-workflow summary surfaced to MCP/agent consumers (see {@link #DOC_RESULT_MESSAGE_TEMPLATE}). */
	public static final String RESULT_SUMMARY = "summary";
	/** Enriched summary including per-step output previews, sent as the MCP tool response so the LLM can describe what happened. */
	public static final String RESULT_LLM_CONTEXT = "llmContext";

	// -- Pixel execution defaults ----------------------------------------------------

	public static final String AUTOMATION_INPUTS_KEY = "inputs";
	public static final int DEFAULT_TIMEOUT_SECONDS = 300;

	// -- Data type constants (for table creation) ----------------------------------

	public static final String VARCHAR_50 = "VARCHAR(50)";
	public static final String VARCHAR_255 = "VARCHAR(255)";
	public static final String VARCHAR_500 = "VARCHAR(500)";
	public static final String VARCHAR_2000 = "VARCHAR(2000)";
	public static final String INTEGER = "INTEGER";
	public static final String BIGINT = "BIGINT";
	public static final String NOT_NULL = "NOT NULL";

	// -- DDL object names (indexes / primary keys) ----------------------------------

	public static final String PK_AUTOMATION_RUNS = "PK_AUTOMATION_RUNS";
	public static final String PK_AUTO_NODE_OUT = "PK_AUTO_NODE_OUT";
	public static final String PK_AUTO_ACTIVE_RUN = "PK_AUTO_ACTIVE_RUN";
	public static final String IDX_AR_PROJECT = "IDX_AR_PROJECT";
	public static final String IDX_AR_STATUS = "IDX_AR_STATUS";
	public static final String IDX_AR_STARTED = "IDX_AR_STARTED";
	public static final String IDX_ANO_RUN = "IDX_ANO_RUN";

	// -- Defaults ------------------------------------------------------------------

	public static final String DEFAULT_AUTOMATION_ID = "default";
	public static final int HEARTBEAT_INTERVAL_SECONDS = 30;
	public static final int STALE_HEARTBEAT_THRESHOLD_MINUTES = 5;
	public static final int OUTPUT_PREVIEW_MAX_LENGTH = 2000;
}

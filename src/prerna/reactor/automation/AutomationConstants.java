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

/**
 * Shared constants for the Automation Engine subsystem.
 * Covers table/column names, status values, and node types.
 */
public class AutomationConstants {

	private AutomationConstants() {}

	// -- File names ----------------------------------------------------------------

	public static final String AUTOMATION_FILE_NAME = "automation.json";
	public static final String AUTOMATION_CONFIG_FILE_NAME = "automation-config.json";

	/** Placeholder returned by GetAutomationConfig in place of a sensitive value; never persisted back. */
	public static final String SENSITIVE_MASK = "***";

	// -- DB Table names ------------------------------------------------------------

	public static final String TABLE_AUTOMATION_RUNS = "AUTOMATION_RUNS";
	public static final String TABLE_AUTOMATION_NODE_OUTPUTS = "AUTOMATION_NODE_OUTPUTS";
	public static final String TABLE_AUTOMATION_FOREACH_ROWS = "AUTOMATION_FOREACH_ROWS";
	/**
	 * Single-row-per-project marker table enforcing "at most one active run per project"
	 * cluster-wide, via a primary key on PROJECT_ID. Claiming a row is an atomic INSERT
	 * (fails with a constraint violation if another run already holds it); the row is
	 * released on any terminal run status.
	 */
	public static final String TABLE_AUTOMATION_ACTIVE_RUN = "AUTOMATION_ACTIVE_RUN";

	// -- AUTOMATION_RUNS columns ---------------------------------------------------

	public static final String RUN_ID = "RUN_ID";
	public static final String PROJECT_ID = "PROJECT_ID";
	public static final String AUTOMATION_ID = "AUTOMATION_ID";
	public static final String STATUS = "STATUS";
	public static final String TRIGGER_TYPE = "TRIGGER_TYPE";
	public static final String RESUMED_FROM_RUN = "RESUMED_FROM_RUN";
	public static final String STARTED_AT = "STARTED_AT";
	public static final String COMPLETED_AT = "COMPLETED_AT";
	public static final String FAILED_NODE_ID = "FAILED_NODE_ID";
	public static final String ERROR_MESSAGE = "ERROR_MESSAGE";
	public static final String LAST_HEARTBEAT = "LAST_HEARTBEAT";
	public static final String TOTAL_NODES = "TOTAL_NODES";
	public static final String COMPLETED_NODES = "COMPLETED_NODES";
	public static final String CREATED_BY = "CREATED_BY";
	public static final String PARENT_RUN_ID = "PARENT_RUN_ID";
	public static final String PARENT_NODE_ID = "PARENT_NODE_ID";
	/**
	 * Cluster-safe cancellation flag. Set by CancelAutomationRunReactor regardless of which
	 * pod receives the cancel request; polled by the executing pod's between-node check
	 * alongside the in-memory (same-pod fast path) AtomicBoolean.
	 */
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
	public static final String ROW_COUNT = "ROW_COUNT";

	// -- AUTOMATION_FOREACH_ROWS columns ------------------------------------------

	public static final String ROW_INDEX = "ROW_INDEX";
	public static final String ROW_KEY = "ROW_KEY";

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
	public static final String TRIGGER_SCHEDULED = "SCHEDULED";
	public static final String TRIGGER_RESUME = "RESUME";
	public static final String TRIGGER_SUB_AUTOMATION = "SUB_AUTOMATION";
	public static final String TRIGGER_WEBHOOK = "WEBHOOK";
	public static final String TRIGGER_STORAGE_POLL = "STORAGE_POLL";
	public static final String TRIGGER_DB_POLL = "DB_POLL";

	// -- Node types ----------------------------------------------------------------

	public static final String NODE_TRIGGER = "trigger";
	public static final String NODE_DATABASE_ENGINE = "database-engine";
	public static final String NODE_STORAGE_ENGINE = "storage-engine";
	public static final String NODE_VECTOR_ENGINE = "vector-engine";
	public static final String NODE_MODEL_ENGINE = "model-engine";
	public static final String NODE_FUNCTION_ENGINE = "function-engine";
	public static final String NODE_APP = "app";
	public static final String NODE_CUSTOM_PIXEL = "custom-pixel";
	public static final String NODE_FOR_EACH = "for-each";
	public static final String NODE_TRANSFORM = "transform";
	public static final String NODE_SUB_AUTOMATION = "sub-automation";
	public static final String NODE_CONDITIONAL = "conditional";
	public static final String NODE_WHILE_LOOP = "while-loop";
	public static final String NODE_TRY_CATCH = "try-catch";
	public static final String NODE_WAIT = "wait";
	public static final String NODE_SET_VARIABLE = "set-variable";
	public static final String NODE_EMAIL = "email";
	public static final String NODE_HTTP_REQUEST = "http-request";
	public static final String NODE_NOTIFICATION = "notification";
	public static final String NODE_SWITCH = "switch";
	public static final String NODE_RETRY = "retry";
	public static final String NODE_PARALLEL = "parallel";

	// -- Sub-automation node config keys ------------------------------------------

	public static final String SUB_AUTOMATION_TARGET_PROJECT = "targetProjectId";
	public static final String SUB_AUTOMATION_INPUT_MAPPING = "inputMapping";
	public static final int MAX_SUB_AUTOMATION_DEPTH = 10;

	// -- Data type constants (for table creation) ----------------------------------

	public static final String VARCHAR_255 = "VARCHAR(255)";
	public static final String VARCHAR_500 = "VARCHAR(500)";
	public static final String VARCHAR_1000 = "VARCHAR(1000)";
	public static final String VARCHAR_2000 = "VARCHAR(2000)";
	public static final String INTEGER = "INTEGER";
	public static final String BIGINT = "BIGINT";
	public static final String NOT_NULL = "NOT NULL";

	// -- Defaults ------------------------------------------------------------------

	public static final String DEFAULT_AUTOMATION_ID = "default";
	public static final int DEFAULT_TIMEOUT_SECONDS = 300;
	public static final int HEARTBEAT_INTERVAL_SECONDS = 30;
	public static final int STALE_HEARTBEAT_THRESHOLD_MINUTES = 5;
	public static final int FOREACH_BATCH_SIZE = 100;
	public static final int OUTPUT_PREVIEW_MAX_LENGTH = 2000;
}

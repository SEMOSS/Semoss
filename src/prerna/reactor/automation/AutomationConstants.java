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

public class AutomationConstants {

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
	public static final int HEARTBEAT_INTERVAL_SECONDS = 30;
	public static final int STALE_HEARTBEAT_THRESHOLD_MINUTES = 5;
	public static final int OUTPUT_PREVIEW_MAX_LENGTH = 2000;
}

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
package prerna.engine.impl.model.inferencetracking;

import java.util.ArrayList;
import java.util.Arrays;

import org.javatuples.Pair;

import prerna.engine.impl.owl.AbstractOwlCreator;
import prerna.util.sql.AbstractSqlQueryUtil;

public class ModelInferenceLogsOwlCreator extends AbstractOwlCreator {

	public ModelInferenceLogsOwlCreator(AbstractSqlQueryUtil queryUtil) {
		createColumnsAndTypes(queryUtil);
	}

	public void createColumnsAndTypes(AbstractSqlQueryUtil queryUtil) {
		final String BLOB_DATATYPE_NAME = queryUtil.getBlobDataTypeName();
		final String CLOB_DATATYPE_NAME = queryUtil.getClobDataTypeName();
		final String BOOLEAN_DATATYPE_NAME = queryUtil.getBooleanDataTypeName();
		final String TIMESTAMP_DATATYPE_NAME = queryUtil.getDateWithTimeDataType();
		final String INTEGER_DATATYPE_NAME = queryUtil.getIntegerDataTypeName();
		final String DOUBLE_DATATYPE_NAME = queryUtil.getDoubleDataTypeName();
		final String VARCHAR_50 = "VARCHAR(50)";
		final String VARCHAR_255 = "VARCHAR(255)";

		this.allSchemas = new ArrayList<>();

		// @formatter:off
		addTable("AGENT", Arrays.asList(
				Pair.with("AGENT_ID", VARCHAR_50),
				Pair.with("AGENT_NAME", VARCHAR_255),
				Pair.with("DESCRIPTION", VARCHAR_255),
				Pair.with("AGENT_TYPE", VARCHAR_50),
				Pair.with("AUTHOR", VARCHAR_255),
				Pair.with("DATE_CREATED", TIMESTAMP_DATATYPE_NAME)));

		addTable("ROOM", Arrays.asList(
				Pair.with("INSIGHT_ID", VARCHAR_50),
				Pair.with("ROOM_ID", VARCHAR_50),
				Pair.with("ROOM_NAME", VARCHAR_255),
				Pair.with("ROOM_CONTEXT", CLOB_DATATYPE_NAME),
				Pair.with("USER_ID", VARCHAR_255),
				Pair.with("USER_NAME", VARCHAR_255),
				Pair.with("USER_EMAIL_ID", VARCHAR_50),
				Pair.with("AGENT_TYPE", VARCHAR_50),
				Pair.with("AGENT_ID", VARCHAR_50),
				Pair.with("IS_ACTIVE", BOOLEAN_DATATYPE_NAME),
				Pair.with("DATE_CREATED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("UPDATED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("PROJECT_ID", VARCHAR_50),
				Pair.with("PROJECT_NAME", VARCHAR_255),
				Pair.with("MODEL_ID", VARCHAR_255),
				Pair.with("MESSAGES", CLOB_DATATYPE_NAME),
				Pair.with("PINNED", BOOLEAN_DATATYPE_NAME),
				Pair.with("OPTIONS", CLOB_DATATYPE_NAME),
				Pair.with("SHARE_ID", VARCHAR_255),
				Pair.with("WORKSPACE_ID", VARCHAR_255),
				Pair.with("PARENT_ROOM_ID", VARCHAR_50)));

		addTable("MESSAGE", Arrays.asList(
				Pair.with("MESSAGE_ID", VARCHAR_50),
				Pair.with("TRANSACTION_ID", VARCHAR_50),
				Pair.with("MESSAGE_TYPE", VARCHAR_50),
				Pair.with("MESSAGE_DATA", BLOB_DATATYPE_NAME),
				Pair.with("MESSAGE_TOKENS", INTEGER_DATATYPE_NAME),
				Pair.with("INPUT_TOKENS", INTEGER_DATATYPE_NAME),
				Pair.with("OUTPUT_TOKENS", INTEGER_DATATYPE_NAME),
				Pair.with("THINKING_TOKENS", INTEGER_DATATYPE_NAME),
				Pair.with("CACHE_READ_TOKENS", INTEGER_DATATYPE_NAME),
				Pair.with("CACHE_CREATION_TOKENS", INTEGER_DATATYPE_NAME),
				Pair.with("MESSAGE_METHOD", VARCHAR_50),
				//Pair.with("MESSAGE_SEPARATOR", VARCHAR_50),
				Pair.with("RESPONSE_TIME", DOUBLE_DATATYPE_NAME),
				Pair.with("DATE_CREATED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("AGENT_ID", VARCHAR_50),
				Pair.with("MODEL_ID", VARCHAR_50),
				Pair.with("INSIGHT_ID", VARCHAR_50),
				Pair.with("ROOM_ID", VARCHAR_50),
				Pair.with("SESSIONID", VARCHAR_255),
				Pair.with("USER_ID", VARCHAR_255),
				Pair.with("USER_NAME", VARCHAR_255),
				Pair.with("USER_EMAIL_ID", VARCHAR_50)));

		addTable("FEEDBACK", Arrays.asList(
				Pair.with("MESSAGE_ID", VARCHAR_50),
				Pair.with("MESSAGE_TYPE", VARCHAR_50),
				Pair.with("FEEDBACK_TEXT", CLOB_DATATYPE_NAME),
				Pair.with("FEEDBACK_DATE", TIMESTAMP_DATATYPE_NAME),
				Pair.with("RATING", BOOLEAN_DATATYPE_NAME)));

		// CONFIG_JSON holds the full AgentConfig serialization for this workspace -
		// system_prompt mirror, mcps, budgets, hooks. See AgentConfigLoader for the
		// read order (CONFIG_JSON-first with legacy column/WORKSPACE_RESOURCE fallback)
		// and the workspace setter reactors (SetAgentHooks etc.) for the write path.
		addTable("WORKSPACE", Arrays.asList(
				Pair.with("WORKSPACE_ID", VARCHAR_255),
				Pair.with("OWNER", VARCHAR_255),
				Pair.with("NAME", VARCHAR_255),
				Pair.with("DESCRIPTION", CLOB_DATATYPE_NAME),
				Pair.with("SYSTEM_PROMPT", CLOB_DATATYPE_NAME),
				Pair.with("CONFIG_JSON", CLOB_DATATYPE_NAME),
				Pair.with("IS_ACTIVE", BOOLEAN_DATATYPE_NAME),
				Pair.with("DATE_CREATED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("DATE_UPDATED", TIMESTAMP_DATATYPE_NAME)));

		addTable("WORKSPACE_RESOURCE", Arrays.asList(
				Pair.with("WORKSPACE_RESOURCE_ID", VARCHAR_255),
				Pair.with("WORKSPACE_ID", VARCHAR_255),
				Pair.with("RESOURCE_ID", VARCHAR_255),
				Pair.with("RESOURCE_TYPE", VARCHAR_255),
				Pair.with("RESOURCE_SUBTYPE", VARCHAR_255)));

		addTable("AGENT_RUN", Arrays.asList(
				Pair.with("RUN_ID", VARCHAR_50),
				Pair.with("PARENT_RUN_ID", VARCHAR_50),
				Pair.with("ROOM_ID", VARCHAR_50),
				Pair.with("WORKSPACE_ID", VARCHAR_255),
				Pair.with("MODEL_ID", VARCHAR_255),
				Pair.with("HARNESS_TYPE", VARCHAR_50),
				Pair.with("JOB_ID", VARCHAR_50),
				Pair.with("STATUS", VARCHAR_50),
				Pair.with("INPUT", CLOB_DATATYPE_NAME),
				Pair.with("REQUEST_JSON", CLOB_DATATYPE_NAME),
				Pair.with("INPUT_MESSAGE_ID", VARCHAR_50),
				Pair.with("FINAL_OUTPUT", CLOB_DATATYPE_NAME),
				Pair.with("FINAL_OUTPUT_MESSAGE_ID", VARCHAR_50),
				Pair.with("ERROR_MESSAGE", CLOB_DATATYPE_NAME),
				Pair.with("DATE_CREATED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("STARTED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("COMPLETED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("USER_ID", VARCHAR_255)));

		addTable("AGENT_RUN_ACTION", Arrays.asList(
				Pair.with("ACTION_ID", VARCHAR_50),
				Pair.with("RUN_ID", VARCHAR_50),
				Pair.with("ROOM_ID", VARCHAR_50),
				Pair.with("PARENT_MESSAGE_ID", VARCHAR_50),
				Pair.with("TOOL_CALL_ID", VARCHAR_255),
				Pair.with("TOOL_NAME", VARCHAR_255),
				Pair.with("TOOL_ARGS", CLOB_DATATYPE_NAME),
				Pair.with("EDITED_ARGS", CLOB_DATATYPE_NAME),
				Pair.with("TOOL_META", CLOB_DATATYPE_NAME),
				Pair.with("HAS_UI", VARCHAR_50),
				Pair.with("UI_URL", CLOB_DATATYPE_NAME),
				Pair.with("STATUS", VARCHAR_50),
				Pair.with("RESULT", CLOB_DATATYPE_NAME),
				Pair.with("TOOL_STATUS", VARCHAR_50),
				Pair.with("DATE_CREATED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("DECIDED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("USER_ID", VARCHAR_255)));
		// @formatter:on
	}

}

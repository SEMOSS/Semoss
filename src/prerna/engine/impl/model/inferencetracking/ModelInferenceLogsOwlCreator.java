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
		super(queryUtil);
	}

	@Override
	public void createColumnsAndTypes(AbstractSqlQueryUtil queryUtil) {
		final String BLOB_DATATYPE_NAME = queryUtil.getBlobDataTypeName();
		final String CLOB_DATATYPE_NAME = queryUtil.getClobDataTypeName();
		final String BOOLEAN_DATATYPE_NAME = queryUtil.getBooleanDataTypeName();
		final String TIMESTAMP_DATATYPE_NAME = queryUtil.getDateWithTimeDataType();
		final String INTEGER_DATATYPE_NAME = queryUtil.getIntegerDataTypeName();
		final String DOUBLE_DATATYPE_NAME = queryUtil.getDoubleDataTypeName();
		final String VARCHAR_20 = "VARCHAR(20)";
		final String VARCHAR_50 = "VARCHAR(50)";
		final String VARCHAR_255 = "VARCHAR(255)";
		final String BIGINT_DATATYPE_NAME = "BIGINT";

		this.allSchemas = new ArrayList<>();

		// @formatter:off
		// an engine whose calls the inference logs record, a model or a vector database, added on
		// its first logged call (ModelEngineInferenceLogsWorker): AGENT_ID is the engine id,
		// AGENT_TYPE its catalog subtype and AUTHOR the user whose call added it. MESSAGE.AGENT_ID
		// and ROOM.AGENT_ID point here. A reusable agent is a WORKSPACE row, not an AGENT row
		addTable("AGENT", Arrays.asList(
				Pair.with("AGENT_ID", VARCHAR_50),
				Pair.with("AGENT_NAME", VARCHAR_255),
				Pair.with("DESCRIPTION", VARCHAR_255),
				Pair.with("AGENT_TYPE", VARCHAR_50),
				Pair.with("AUTHOR", VARCHAR_255),
				Pair.with("DATE_CREATED", TIMESTAMP_DATATYPE_NAME)));

		// a conversation room, one row per ROOM_ID and USER_ID, the user who owns it. It names the
		// insight and project it was opened in, the engine it started with (AGENT_ID, AGENT_TYPE)
		// and the model it last used (MODEL_ID), and holds the conversation itself in MESSAGES, the
		// durable copy RoomMessageStore reads and writes, with its OPTIONS and ROOM_CONTEXT.
		// IS_ACTIVE false closes the room, PINNED marks a room its user pinned, WORKSPACE_ID is the
		// agent it runs as and PARENT_ROOM_ID the room that spawned it for a subagent. Its model
		// calls are logged in MESSAGE and its agent runs in AGENT_RUN
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

		// the log of model and vector engine calls, two rows per call: an INPUT row (MESSAGE_TYPE
		// INPUT) with the prompt side token counts, and a RESPONSE row with the output and thinking
		// tokens, the RESPONSE_TIME and COST_MICROS, so a SUM of any column counts each call once.
		// Both rows share TRANSACTION_ID, the RESPONSE row's MESSAGE_ID, and name the engine
		// (AGENT_ID), room, insight, session, user and the usage limit source billed. MESSAGE_DATA
		// holds the prompt or the response only when the engine keeps its input and output. Usage
		// reports and the usage limit counters read these rows (docs/model_usage_limits)
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
				Pair.with("USER_EMAIL_ID", VARCHAR_50),
				// the primary login's type, beside USER_ID
				Pair.with("USER_TYPE", VARCHAR_255),
				// the usage limit source the call was billed to: USER, TEAM, PUBLIC,
				// ALL_MODELS or PLATFORM, or null when no limit applied
				Pair.with("BILLED_SOURCE_TYPE", VARCHAR_20),
				Pair.with("BILLED_SUBJECT_ID", VARCHAR_255),
				Pair.with("BILLED_SUBJECT_TYPE", VARCHAR_255),
				// the call's cost in millionths of a dollar, on the RESPONSE row, or null
				// when the model has no known prices
				Pair.with("COST_MICROS", BIGINT_DATATYPE_NAME)));

		// a user's feedback on one model response in their room: MESSAGE_ID is the response, RATING
		// the user's true or false rating and FEEDBACK_TEXT their comment. SubmitLlmFeedbackReactor
		// writes it, and the admin feedback report joins it to MESSAGE by MESSAGE_ID
		addTable("FEEDBACK", Arrays.asList(
				Pair.with("MESSAGE_ID", VARCHAR_50),
				Pair.with("MESSAGE_TYPE", VARCHAR_50),
				Pair.with("FEEDBACK_TEXT", CLOB_DATATYPE_NAME),
				Pair.with("FEEDBACK_DATE", TIMESTAMP_DATATYPE_NAME),
				Pair.with("RATING", BOOLEAN_DATATYPE_NAME)));

		// an agent, one row per WORKSPACE project, keyed by its project id: its NAME, DESCRIPTION,
		// OWNER (the id of the user who made it) and SYSTEM_PROMPT, and IS_ACTIVE, false when the
		// agent is turned off. CONFIG_JSON holds the agent's full configuration (system_prompt,
		// model_id, mcps, budgets, hooks, subagents and more), written by the workspace reactors,
		// such as AddWorkspaceReactor, EditWorkspaceReactor and AttachSkillToWorkspaceReactor.
		// AgentConfigLoader builds each run's configuration from it: the prompt is
		// CONFIG_JSON.system_prompt, or SYSTEM_PROMPT when that is empty, and the MCPs combine the
		// WORKSPACE_RESOURCE rows, CONFIG_JSON.mcps and the room's options. A room runs as an agent
		// through ROOM.WORKSPACE_ID
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

		// a resource attached to an agent, one row per WORKSPACE_ID and RESOURCE_ID: an engine
		// (RESOURCE_TYPE its engine type, RESOURCE_SUBTYPE its subtype), a project (PROJECT, with
		// the project type), a saved prompt (PROMPT) or a skill (SKILL, with any pinned version in
		// RESOURCE_SUBTYPE). AgentConfigLoader reads the engine and project rows as the agent's
		// MCPs
		addTable("WORKSPACE_RESOURCE", Arrays.asList(
				Pair.with("WORKSPACE_RESOURCE_ID", VARCHAR_255),
				Pair.with("WORKSPACE_ID", VARCHAR_255),
				Pair.with("RESOURCE_ID", VARCHAR_255),
				Pair.with("RESOURCE_TYPE", VARCHAR_255),
				Pair.with("RESOURCE_SUBTYPE", VARCHAR_255)));

		// one row per agent run, the durable record AgentRunStore keeps: the room, agent
		// (WORKSPACE_ID), model and HARNESS_TYPE it runs with, PARENT_RUN_ID when it is a subagent
		// run, the INPUT and REQUEST_JSON it was submitted with, the job that runs it,
		// PROGRESS_JSON, its FINAL_OUTPUT or ERROR_MESSAGE, the ids of the room messages holding
		// its input and final output, and its owner's id and login type. STATUS is SUBMITTED,
		// RUNNING or INPUT_REQUIRED, then COMPLETED, FAILED or CANCELLED; a worker claims a
		// SUBMITTED run, taking one room's runs in the order they came. The tool calls it pauses on
		// are in AGENT_RUN_ACTION
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
				Pair.with("PROGRESS_JSON", CLOB_DATATYPE_NAME),
				Pair.with("INPUT_MESSAGE_ID", VARCHAR_50),
				Pair.with("FINAL_OUTPUT", CLOB_DATATYPE_NAME),
				Pair.with("FINAL_OUTPUT_MESSAGE_ID", VARCHAR_50),
				Pair.with("ERROR_MESSAGE", CLOB_DATATYPE_NAME),
				Pair.with("DATE_CREATED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("STARTED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("COMPLETED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("USER_ID", VARCHAR_255),
				Pair.with("USER_TYPE", VARCHAR_255)));

		// a tool call an agent run paused on for its user (AgentRunActionStore): one row per call
		// to a tool whose MCP execution setting is ask, made as the run moves to INPUT_REQUIRED. It
		// keeps the tool, its TOOL_ARGS and TOOL_META, any UI to show (HAS_UI, UI_URL), and STATUS,
		// which starts PENDING and records what the user chose, APPROVED, EDITED, REJECTED or
		// RESPONDED, with DECIDED_AT, the user's EDITED_ARGS and the tool's RESULT and TOOL_STATUS.
		// Deciding every pending action returns the run to SUBMITTED
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

		// a guardrail check hit that an engine's hit limits count: one row each time a check named
		// in one of the engine's hitLimits flags a call (ACTION BLOCK, MASK or RESPOND), with the
		// check, the guardrail engine that ran it, the method and PHASE (INPUT or OUTPUT), and the
		// room, insight and user. GuardrailHitLimits counts a room limit over the room's hits and a
		// user limit over the user's hits in its window
		addTable("GUARDRAIL_HIT", Arrays.asList(
				Pair.with("HIT_ID", VARCHAR_50),
				Pair.with("ENGINE_ID", VARCHAR_50),
				Pair.with("CHECK_ID", VARCHAR_255),
				Pair.with("GUARDRAIL_ENGINE_ID", VARCHAR_50),
				Pair.with("METHOD_NAME", VARCHAR_255),
				Pair.with("PHASE", VARCHAR_50),
				Pair.with("ACTION", VARCHAR_50),
				Pair.with("ROOM_ID", VARCHAR_50),
				Pair.with("INSIGHT_ID", VARCHAR_50),
				Pair.with("USER_ID", VARCHAR_255),
				Pair.with("USER_TYPE", VARCHAR_255),
				Pair.with("DATE_CREATED", TIMESTAMP_DATATYPE_NAME)));

		// a user locked out of an engine after their hits reached one of its limits: LIMIT_ID is
		// the user limit in the engine's pipeline file, and the lock is in force from LOCKED_AT
		// until LOCKED_UNTIL while CLEARED_AT is empty. A user who can edit the engine can clear it
		// early (ClearEngineGuardrailLockReactor), and a save that removes the limit ends it;
		// CLEARED_BY_ID and CLEARED_BY_TYPE name who did
		addTable("GUARDRAIL_LOCK", Arrays.asList(
				Pair.with("LOCK_ID", VARCHAR_50),
				Pair.with("ENGINE_ID", VARCHAR_50),
				Pair.with("LIMIT_ID", VARCHAR_255),
				Pair.with("USER_ID", VARCHAR_255),
				Pair.with("USER_TYPE", VARCHAR_255),
				Pair.with("LOCKED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("LOCKED_UNTIL", TIMESTAMP_DATATYPE_NAME),
				Pair.with("CLEARED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("CLEARED_BY_ID", VARCHAR_255),
				Pair.with("CLEARED_BY_TYPE", VARCHAR_255)));
		// @formatter:on
	}

}

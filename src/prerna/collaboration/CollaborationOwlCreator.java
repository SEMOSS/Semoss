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
package prerna.collaboration;

import java.util.ArrayList;
import java.util.Arrays;

import org.javatuples.Pair;

import prerna.engine.impl.owl.AbstractOwlCreator;
import prerna.util.sql.AbstractSqlQueryUtil;

// Brain and Work tables; every row is owner-scoped and no table holds a message body
public class CollaborationOwlCreator extends AbstractOwlCreator {

	public CollaborationOwlCreator(AbstractSqlQueryUtil queryUtil) {
		super(queryUtil);
	}

	@Override
	public void createColumnsAndTypes(AbstractSqlQueryUtil queryUtil) {
		final String CLOB_DATATYPE_NAME = queryUtil.getClobDataTypeName();
		final String BOOLEAN_DATATYPE_NAME = queryUtil.getBooleanDataTypeName();
		final String TIMESTAMP_DATATYPE_NAME = queryUtil.getDateWithTimeDataType();
		final String INTEGER_DATATYPE_NAME = queryUtil.getIntegerDataTypeName();
		final String VARCHAR_20 = "VARCHAR(20)";
		final String VARCHAR_50 = "VARCHAR(50)";
		final String VARCHAR_255 = "VARCHAR(255)";
		// owner boundary columns on every table
		final Pair<String, String> OWNER_ID = Pair.with("OWNER_ID", VARCHAR_255);
		final Pair<String, String> OWNER_TYPE = Pair.with("OWNER_TYPE", VARCHAR_50);

		this.allSchemas = new ArrayList<>();

		// @formatter:off

		// --- Owner and sources ---
		addTable("COLLAB_OWNER", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("MS_USER_ID", VARCHAR_255),
				Pair.with("MS_UPN", VARCHAR_255),
				Pair.with("STATUS", VARCHAR_20),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("UPDATED_AT", TIMESTAMP_DATATYPE_NAME)));
		addTable("SOURCE_CONNECTION", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("SOURCE", VARCHAR_50),
				Pair.with("ENABLED", BOOLEAN_DATATYPE_NAME),
				Pair.with("LAST_EVENT_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("LAST_ERROR", CLOB_DATATYPE_NAME),
				Pair.with("LAST_ERROR_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("REAUTH_NEEDED", BOOLEAN_DATATYPE_NAME),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("UPDATED_AT", TIMESTAMP_DATATYPE_NAME)));
		addTable("SOURCE_SYNC_STATE", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("SOURCE", VARCHAR_50),
				Pair.with("FOLDER", VARCHAR_255),
				Pair.with("DELTA_LINK", CLOB_DATATYPE_NAME),
				Pair.with("LAST_SYNC_AT", TIMESTAMP_DATATYPE_NAME)));
		addTable("INBOUND_EVENT", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("EVENT_ID", VARCHAR_50),
				Pair.with("PROVIDER", VARCHAR_50),
				Pair.with("EVENT_KEY", VARCHAR_255),
				Pair.with("SOURCE", VARCHAR_50),
				Pair.with("RESOURCE_LOCATOR", CLOB_DATATYPE_NAME),
				Pair.with("CHANGE_TYPE", VARCHAR_50),
				Pair.with("STATUS", VARCHAR_20),
				Pair.with("ATTEMPT_COUNT", INTEGER_DATATYPE_NAME),
				Pair.with("RUN_ID", VARCHAR_50),
				Pair.with("ERROR_SUMMARY", CLOB_DATATYPE_NAME),
				Pair.with("RECEIVED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("PROCESSED_AT", TIMESTAMP_DATATYPE_NAME)));

		// --- Brain: you ---
		addTable("BRAIN_PROFILE", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("DISPLAY_NAME", VARCHAR_255),
				Pair.with("EMAIL", VARCHAR_255),
				Pair.with("ROLE", VARCHAR_255),
				Pair.with("ROLE_STATE", VARCHAR_20),
				Pair.with("ROLE_NOTE", VARCHAR_255),
				Pair.with("ORG", VARCHAR_255),
				Pair.with("TIMEZONE", VARCHAR_50),
				Pair.with("WORKING_HOURS_JSON", CLOB_DATATYPE_NAME),
				Pair.with("STYLE_SUMMARY", CLOB_DATATYPE_NAME),
				Pair.with("STYLE_STATE", VARCHAR_20),
				Pair.with("STYLE_EXAMPLES_JSON", CLOB_DATATYPE_NAME),
				Pair.with("UPDATED_AT", TIMESTAMP_DATATYPE_NAME)));
		// no topic cap; FILE_AT 85 and ASK_AT 40 defaults are set by the service
		addTable("BRAIN_SETTINGS", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("CLASSIFIER_ENGINE_ID", VARCHAR_50),
				Pair.with("FILE_AT", INTEGER_DATATYPE_NAME),
				Pair.with("ASK_AT", INTEGER_DATATYPE_NAME),
				Pair.with("PRIORITY_THRESHOLDS_JSON", CLOB_DATATYPE_NAME),
				Pair.with("WEIGHTS_JSON", CLOB_DATATYPE_NAME),
				Pair.with("SOURCES_JSON", CLOB_DATATYPE_NAME),
				Pair.with("OPEN_ROOMS_JSON", CLOB_DATATYPE_NAME),
				Pair.with("VERSION", INTEGER_DATATYPE_NAME),
				Pair.with("UPDATED_AT", TIMESTAMP_DATATYPE_NAME)));

		// --- Brain: people ---
		addTable("BRAIN_ACCOUNT", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("ACCOUNT_ID", VARCHAR_50),
				Pair.with("NAME", VARCHAR_255),
				Pair.with("KIND", VARCHAR_20),
				Pair.with("DOMAINS_JSON", CLOB_DATATYPE_NAME),
				Pair.with("COLOR", VARCHAR_20),
				Pair.with("STATUS", VARCHAR_20),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("UPDATED_AT", TIMESTAMP_DATATYPE_NAME)));
		addTable("BRAIN_PERSON", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("PERSON_ID", VARCHAR_50),
				Pair.with("EMAIL_NORM", VARCHAR_255),
				Pair.with("DIRECTORY_ID", VARCHAR_255),
				Pair.with("DISPLAY_NAME", VARCHAR_255),
				Pair.with("GIVEN_NAME", VARCHAR_255),
				Pair.with("SURNAME", VARCHAR_255),
				Pair.with("JOB_TITLE", VARCHAR_255),
				Pair.with("DEPARTMENT", VARCHAR_255),
				Pair.with("COMPANY", VARCHAR_255),
				Pair.with("DIRECTORY_CHECKED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("ACCOUNT_ID", VARCHAR_50),
				Pair.with("RELATIONSHIP", VARCHAR_50),
				Pair.with("RELATIONSHIP_STATE", VARCHAR_20),
				Pair.with("IS_VIP", BOOLEAN_DATATYPE_NAME),
				Pair.with("NEVER_INGEST", BOOLEAN_DATATYPE_NAME),
				Pair.with("CHANNELS_JSON", CLOB_DATATYPE_NAME),
				Pair.with("STRENGTH", INTEGER_DATATYPE_NAME),
				Pair.with("LAST_CONTACT_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("WIKI_PATH", VARCHAR_255),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("UPDATED_AT", TIMESTAMP_DATATYPE_NAME)));
		addTable("BRAIN_PERSON_ADDRESS", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("PERSON_ID", VARCHAR_50),
				Pair.with("KIND", VARCHAR_20),
				Pair.with("VALUE_NORM", VARCHAR_255),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME)));

		// --- Brain: topics ---
		// flat topics grouped by ACCOUNT_ID
		addTable("BRAIN_TOPIC", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("TOPIC_ID", VARCHAR_50),
				Pair.with("NAME", VARCHAR_255),
				Pair.with("SHORT_NAME", VARCHAR_50),
				Pair.with("KIND", VARCHAR_20),
				Pair.with("ACCOUNT_ID", VARCHAR_50),
				Pair.with("DESCRIPTION", CLOB_DATATYPE_NAME),
				Pair.with("KEYWORDS_JSON", CLOB_DATATYPE_NAME),
				Pair.with("CALENDAR_SERIES_JSON", CLOB_DATATYPE_NAME),
				Pair.with("COLOR", VARCHAR_20),
				Pair.with("STATUS", VARCHAR_20),
				Pair.with("ORIGIN", VARCHAR_20),
				Pair.with("SUGGEST_REASON", CLOB_DATATYPE_NAME),
				Pair.with("MERGE_CANDIDATE_ID", VARCHAR_50),
				Pair.with("LAST_ACTIVITY_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("UPDATED_AT", TIMESTAMP_DATATYPE_NAME)));
		addTable("BRAIN_TOPIC_NOTE", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("NOTE_ID", VARCHAR_50),
				Pair.with("TOPIC_ID", VARCHAR_50),
				Pair.with("KIND", VARCHAR_20),
				Pair.with("TEXT", CLOB_DATATYPE_NAME),
				Pair.with("STATE", VARCHAR_20),
				Pair.with("ORIGIN", VARCHAR_20),
				Pair.with("SOURCE_REF", VARCHAR_255),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("UPDATED_AT", TIMESTAMP_DATATYPE_NAME)));
		addTable("BRAIN_TOPIC_PERSON", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("TOPIC_ID", VARCHAR_50),
				Pair.with("PERSON_ID", VARCHAR_50),
				Pair.with("STATE", VARCHAR_20),
				Pair.with("ORIGIN", VARCHAR_20),
				Pair.with("ROLE_LABEL", VARCHAR_50),
				Pair.with("ENGAGEMENT", INTEGER_DATATYPE_NAME),
				Pair.with("REASON", CLOB_DATATYPE_NAME),
				Pair.with("CHANGED_BY", VARCHAR_255),
				Pair.with("CHANGED_AT", TIMESTAMP_DATATYPE_NAME)));

		// --- Brain: threads ---
		// topics and participants live in the link tables; one IS_PRIMARY topic per thread
		addTable("BRAIN_THREAD", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("THREAD_ID", VARCHAR_50),
				Pair.with("THREAD_KEY", VARCHAR_255),
				Pair.with("SOURCE", VARCHAR_20),
				Pair.with("SUBJECT", VARCHAR_255),
				Pair.with("MUTED", BOOLEAN_DATATYPE_NAME),
				Pair.with("AUTOMATED", BOOLEAN_DATATYPE_NAME),
				Pair.with("ROOM_ID", VARCHAR_50),
				Pair.with("GOAL", CLOB_DATATYPE_NAME),
				Pair.with("GOAL_STATE", VARCHAR_20),
				Pair.with("SUMMARY", CLOB_DATATYPE_NAME),
				Pair.with("SUMMARY_BUILT_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("MESSAGE_COUNT", INTEGER_DATATYPE_NAME),
				Pair.with("LAST_MESSAGE_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME)));
		addTable("BRAIN_THREAD_TOPIC", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("THREAD_ID", VARCHAR_50),
				Pair.with("TOPIC_ID", VARCHAR_50),
				Pair.with("SOURCE", VARCHAR_20),
				Pair.with("CONFIDENCE", INTEGER_DATATYPE_NAME),
				Pair.with("IS_PRIMARY", BOOLEAN_DATATYPE_NAME),
				Pair.with("SIGNALS_JSON", CLOB_DATATYPE_NAME),
				Pair.with("CLASSIFIER_VERSION", VARCHAR_50),
				Pair.with("CHANGED_BY", VARCHAR_255),
				Pair.with("CHANGED_AT", TIMESTAMP_DATATYPE_NAME)));
		addTable("BRAIN_THREAD_PARTICIPANT", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("THREAD_ID", VARCHAR_50),
				Pair.with("PERSON_ID", VARCHAR_50),
				Pair.with("ROLES_JSON", CLOB_DATATYPE_NAME),
				Pair.with("INCLUDED", BOOLEAN_DATATYPE_NAME),
				Pair.with("EXCLUDED_BY", VARCHAR_20),
				Pair.with("EXCLUDED_RULE_ID", VARCHAR_50),
				Pair.with("EXCLUDED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("HIDDEN_COUNT", INTEGER_DATATYPE_NAME),
				Pair.with("FIRST_SEEN_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("LAST_SEEN_AT", TIMESTAMP_DATATYPE_NAME)));
		addTable("BRAIN_THREAD_LINK", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("LINK_ID", VARCHAR_50),
				Pair.with("FROM_THREAD_KEY", VARCHAR_255),
				Pair.with("TO_THREAD_KEY", VARCHAR_255),
				Pair.with("VIA", VARCHAR_20),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME)));
		// metadata and gate decision only, never a message body
		addTable("BRAIN_MESSAGE", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("MESSAGE_KEY", VARCHAR_50),
				Pair.with("THREAD_ID", VARCHAR_50),
				Pair.with("GRAPH_ID", VARCHAR_255),
				Pair.with("SENDER_PERSON_ID", VARCHAR_50),
				Pair.with("FOLDER", VARCHAR_255),
				Pair.with("RECEIVED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("DECISION", VARCHAR_20),
				Pair.with("RULE_ID", VARCHAR_50),
				Pair.with("CLASSIFIED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("CLASSIFIER_VERSION", VARCHAR_50)));

		// --- Brain: control ---
		addTable("BRAIN_RULE", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("RULE_ID", VARCHAR_50),
				Pair.with("KIND", VARCHAR_50),
				Pair.with("VALUE", VARCHAR_255),
				Pair.with("TOPIC_ID", VARCHAR_50),
				Pair.with("PERSON_ID", VARCHAR_50),
				Pair.with("CHANNEL", VARCHAR_20),
				Pair.with("NOTE", CLOB_DATATYPE_NAME),
				Pair.with("CREATED_BY", VARCHAR_255),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("DISABLED_AT", TIMESTAMP_DATATYPE_NAME)));
		addTable("BRAIN_REVIEW", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("REVIEW_ID", VARCHAR_50),
				Pair.with("KIND", VARCHAR_50),
				Pair.with("REF_TYPE", VARCHAR_50),
				Pair.with("REF_ID", VARCHAR_50),
				Pair.with("DETAIL_JSON", CLOB_DATATYPE_NAME),
				Pair.with("STATUS", VARCHAR_20),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("RESOLVED_AT", TIMESTAMP_DATATYPE_NAME)));
		addTable("BRAIN_CHANGE", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("CHANGE_ID", VARCHAR_50),
				Pair.with("ENTITY_TYPE", VARCHAR_50),
				Pair.with("ENTITY_ID", VARCHAR_50),
				Pair.with("FIELD", VARCHAR_50),
				Pair.with("OLD_VALUE", VARCHAR_255),
				Pair.with("NEW_VALUE", VARCHAR_255),
				Pair.with("ACTOR", VARCHAR_20),
				Pair.with("AT", TIMESTAMP_DATATYPE_NAME)));
		// quoted-text redaction for excluded people; hashes only, dropped when the
		// exclusion is removed
		addTable("BRAIN_EXCLUSION_FINGERPRINT", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("THREAD_KEY", VARCHAR_255),
				Pair.with("PERSON_ID", VARCHAR_50),
				Pair.with("HASH", VARCHAR_255),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME)));

		// --- Work ---
		addTable("WORK_ITEM", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("ITEM_ID", VARCHAR_50),
				Pair.with("THREAD_ID", VARCHAR_50),
				Pair.with("SOURCE", VARCHAR_20),
				Pair.with("SOURCE_REF", VARCHAR_255),
				Pair.with("ACTOR_TYPE", VARCHAR_20),
				Pair.with("ACTOR_ID", VARCHAR_255),
				Pair.with("ACTOR_NAME", VARCHAR_255),
				Pair.with("TITLE", VARCHAR_255),
				Pair.with("KIND", VARCHAR_20),
				Pair.with("ASK_TYPE", VARCHAR_20),
				Pair.with("ORIGIN", VARCHAR_20),
				Pair.with("PRIORITY", VARCHAR_20),
				Pair.with("SCORE", INTEGER_DATATYPE_NAME),
				Pair.with("REASONS_JSON", CLOB_DATATYPE_NAME),
				Pair.with("DUE_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("RECEIVED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("PROCESSED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("STATUS", VARCHAR_20),
				Pair.with("CLOSED_REASON", VARCHAR_20),
				Pair.with("SNOOZE_UNTIL", TIMESTAMP_DATATYPE_NAME),
				Pair.with("ROOM_ID", VARCHAR_50),
				Pair.with("ASSIGNEE_PERSON_ID", VARCHAR_50),
				Pair.with("LINK_TOPIC_ID", VARCHAR_50),
				Pair.with("CLASSIFIER_VERSION", VARCHAR_50),
				Pair.with("DEDUPE_KEY", VARCHAR_255),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME)));
		addTable("WORK_ITEM_HISTORY", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("HISTORY_ID", VARCHAR_50),
				Pair.with("ITEM_ID", VARCHAR_50),
				Pair.with("CHANGED_BY", VARCHAR_255),
				Pair.with("CHANGED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("FIELD", VARCHAR_50),
				Pair.with("OLD_VALUE", VARCHAR_255),
				Pair.with("NEW_VALUE", VARCHAR_255),
				Pair.with("REASON", CLOB_DATATYPE_NAME)));
		// one row per open workspace tab (assumption: not detailed in the wiki beyond
		// WorkListOpenRooms/WorkCloseRoom; minimal set to list and close tabs)
		addTable("WORK_OPEN_ROOM", Arrays.asList(
				OWNER_ID, OWNER_TYPE,
				Pair.with("THREAD_ID", VARCHAR_50),
				Pair.with("ROOM_ID", VARCHAR_50),
				Pair.with("OPENED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("LAST_ACTIVE_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("PINNED", BOOLEAN_DATATYPE_NAME)));
		// @formatter:on
	}
}

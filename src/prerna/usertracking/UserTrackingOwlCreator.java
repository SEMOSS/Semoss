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
package prerna.usertracking;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.javatuples.Pair;

import prerna.engine.impl.owl.AbstractOwlCreator;
import prerna.util.sql.AbstractSqlQueryUtil;

public class UserTrackingOwlCreator extends AbstractOwlCreator {

	public UserTrackingOwlCreator(AbstractSqlQueryUtil queryUtil) {
		super(queryUtil);
	}

	@Override
	public void createColumnsAndTypes(AbstractSqlQueryUtil queryUtil) {
		final String CLOB_DATATYPE_NAME = queryUtil.getClobDataTypeName();
		final String BOOLEAN_DATATYPE_NAME = queryUtil.getBooleanDataTypeName();
		final String TIMESTAMP_DATATYPE_NAME = queryUtil.getDateWithTimeDataType();
		final String INTEGER_DATATYPE_NAME = queryUtil.getIntegerDataTypeName();
		final String VARCHAR_255 = "VARCHAR(255)";

		this.allSchemas = new ArrayList<>();

		// @formatter:off
		// the user tracking database: activity records, written only when user tracking is turned
		// on (Utility.isUserTrackingEnabled), and the security audit trail

		// one row per login session: the HTTP SESSIONID, the login's USERID and TYPE (its
		// provider), CREATED_ON at login and ENDED_ON at logout, and the client's IP address with
		// its location (latitude, longitude, country, state and city). UserActivityReactor counts
		// logins per day from it
		addTable("USER_TRACKING", Arrays.asList(
				Pair.with("SESSIONID", VARCHAR_255),
				Pair.with("USERID", VARCHAR_255),
				Pair.with("TYPE", VARCHAR_255),
				Pair.with("CREATED_ON", TIMESTAMP_DATATYPE_NAME),
				Pair.with("ENDED_ON", TIMESTAMP_DATATYPE_NAME),
				Pair.with("IP_ADDR", VARCHAR_255),
				Pair.with("IP_LAT", VARCHAR_255),
				Pair.with("IP_LONG", VARCHAR_255),
				Pair.with("IP_COUNTRY", VARCHAR_255),
				Pair.with("IP_STATE", VARCHAR_255),
				Pair.with("IP_CITY", VARCHAR_255))); 

		// the views of an engine per day: one row per ENGINEID and DATE with that day's VIEWS.
		// EngineViewsUtils adds to the day's count and sums the counts, and an engine's rows go
		// when the engine is deleted
		addTable("ENGINE_VIEWS", Arrays.asList(
				Pair.with("ENGINEID", VARCHAR_255),
				Pair.with("DATE", "DATE"),
				Pair.with("VIEWS", INTEGER_DATATYPE_NAME)));

		// the engines a saved insight queries: one row per ENGINEID, INSIGHTID and PROJECTID, with
		// the DATE it was last saved. SaveInsightReactor adds the rows and UpdateInsightReactor
		// reconciles them, and they go with the engine, the project or the insight
		addTable("ENGINE_USES", Arrays.asList(
				Pair.with("ENGINEID", VARCHAR_255),
				Pair.with("INSIGHTID", VARCHAR_255),
				Pair.with("PROJECTID", VARCHAR_255),
				Pair.with("DATE", "DATE")));

		// a user's vote on an engine in the catalog: one row per USERID, TYPE (login type) and
		// ENGINEID, with VOTE 1 for up or -1 for down and when it last changed. VoteEngineReactor
		// and UnvoteEngineReactor change it, and the engine lists show each engine's total
		addTable("USER_CATALOG_VOTES", Arrays.asList(
				Pair.with("USERID", VARCHAR_255),
				Pair.with("TYPE", VARCHAR_255),
				Pair.with("ENGINEID", VARCHAR_255),
				Pair.with("VOTE", INTEGER_DATATYPE_NAME),
				Pair.with("LAST_MODIFIED", TIMESTAMP_DATATYPE_NAME)));

		// one row per email the platform sends through EmailUtility: when, whether the send
		// succeeded, the sender, the to, cc and bcc recipients, the subject, the body, the
		// attachments and whether the body is HTML
		addTable("EMAIL_TRACKING", Arrays.asList(
				Pair.with("ID", VARCHAR_255),
				Pair.with("SENT_TIME", TIMESTAMP_DATATYPE_NAME),
				Pair.with("SUCCESSFUL", BOOLEAN_DATATYPE_NAME),
				Pair.with("E_FROM", VARCHAR_255),
				Pair.with("E_TO", CLOB_DATATYPE_NAME),
				Pair.with("E_CC", CLOB_DATATYPE_NAME),
				Pair.with("E_BCC", CLOB_DATATYPE_NAME),
				Pair.with("E_SUBJECT", "VARCHAR(1000)"),
				Pair.with("BODY", CLOB_DATATYPE_NAME),
				Pair.with("ATTACHMENTS", CLOB_DATATYPE_NAME),
				Pair.with("IS_HTML", BOOLEAN_DATATYPE_NAME)));

		// one row each time a user opens an insight (OpenInsightReactor): the insight, the user,
		// when and the ORIGIN the open came from
		addTable("INSIGHT_OPENS", Arrays.asList(
				Pair.with("INSIGHTID", VARCHAR_255),
				Pair.with("USERID", VARCHAR_255),
				Pair.with("OPENED_ON", TIMESTAMP_DATATYPE_NAME),
				Pair.with("ORIGIN", "VARCHAR(2000)")));

		// one row per query run on a database through the raw select wrappers
		// (RawRDBMSSelectWrapper, RawSesameSelectWrapper): the user's id and login type, the
		// database, the query text, its start and end, TOTAL_EXECUTION_TIME in milliseconds and
		// whether it failed. UserQueryTrackingThread writes it off the query's thread
		addTable("QUERY_TRACKING", Arrays.asList(
				Pair.with("ID", VARCHAR_255),
				Pair.with("USERID", VARCHAR_255),
				Pair.with("USERTYPE", VARCHAR_255),
				Pair.with("DATABASEID", VARCHAR_255),
				Pair.with("QUERY_EXECUTED", CLOB_DATATYPE_NAME),
				Pair.with("START_TIME", TIMESTAMP_DATATYPE_NAME),
				Pair.with("END_TIME", TIMESTAMP_DATATYPE_NAME),
				Pair.with("TOTAL_EXECUTION_TIME", "BIGINT"),
				Pair.with("FAILED_EXECUTION", BOOLEAN_DATATYPE_NAME)));

		// the security audit trail, USER_AUDIT_EVENTS: one row per audited event
		// (UserAuditTrailUtils), such as a login, a permission change, a scheduled job change or an
		// Automation run. It records the event and its outcome, the actor (a user's id and login
		// type, an agent or the system) and the user affected, the request, with a hash of the
		// session id and never the id itself, the target and the project, engine, insight or room
		// it concerns, the values before and after, any error, and the code that recorded it.
		// AUDIT_SEQUENCE orders the rows; each row's HASH_CURRENT is the hash of the row, and its
		// HASH_PREVIOUS the HASH_CURRENT of the row before it, which chains the rows together, and
		// AuditChainStore keeps the chain's head in USER_AUDIT_CHAIN_STATE. AdminAuditEventsReactor
		// and the resource audit reactors read it
		addTable(UserAuditTrailUtils.TABLE, Arrays.asList(
				// event
				Pair.with("EVENT_ID", VARCHAR_255),
				Pair.with("AUDIT_SEQUENCE", "BIGINT"),
				Pair.with("EVENT_TIME", TIMESTAMP_DATATYPE_NAME),
				Pair.with("EVENT_OCCURRED_TIME", TIMESTAMP_DATATYPE_NAME),
				Pair.with("EVENT_TYPE", VARCHAR_255),
				Pair.with("ACTION", VARCHAR_255),
				Pair.with("STATUS", "VARCHAR(50)"),
				Pair.with("CATEGORY", "VARCHAR(100)"),
				Pair.with("SEVERITY", "VARCHAR(50)"),
				// actor
				Pair.with("ACTOR_USER_ID", VARCHAR_255),
				Pair.with("ACTOR_USER_TYPE", VARCHAR_255),
				Pair.with("ACTOR_USER_NAME", VARCHAR_255),
				Pair.with("ACTOR_IS_ADMIN", BOOLEAN_DATATYPE_NAME),
				// affected user
				Pair.with("SUBJECT_USER_ID", VARCHAR_255),
				Pair.with("SUBJECT_USER_TYPE", VARCHAR_255),
				Pair.with("SUBJECT_USER_NAME", VARCHAR_255),
				// request
				Pair.with("SESSION_ID_HASH", VARCHAR_255),
				Pair.with("REQUEST_ID", VARCHAR_255),
				Pair.with("IP_ADDR", VARCHAR_255),
				Pair.with("USER_AGENT", "VARCHAR(1000)"),
				Pair.with("HTTP_METHOD", "VARCHAR(20)"),
				Pair.with("REQUEST_PATH", "VARCHAR(2000)"),
				Pair.with("HTTP_STATUS", INTEGER_DATATYPE_NAME),
				// target
				Pair.with("TARGET_TYPE", VARCHAR_255),
				Pair.with("TARGET_ID", VARCHAR_255),
				Pair.with("TARGET_NAME", "VARCHAR(1000)"),
				// resource context
				Pair.with("PROJECT_ID", VARCHAR_255),
				Pair.with("ENGINE_ID", VARCHAR_255),
				Pair.with("INSIGHT_ID", VARCHAR_255),
				Pair.with("ROOM_ID", VARCHAR_255),
				// change
				Pair.with("OLD_VALUE", CLOB_DATATYPE_NAME),
				Pair.with("NEW_VALUE", CLOB_DATATYPE_NAME),
				Pair.with("DETAILS", CLOB_DATATYPE_NAME),
				// failure
				Pair.with("ERROR_CODE", VARCHAR_255),
				Pair.with("ERROR_MESSAGE", CLOB_DATATYPE_NAME),
				// source
				Pair.with("SOURCE_APP", "VARCHAR(100)"),
				Pair.with("SOURCE_MODULE", VARCHAR_255),
				Pair.with("SOURCE_CLASS", VARCHAR_255),
				// integrity: the hash chain
				Pair.with("HASH_PREVIOUS", "VARCHAR(100)"),
				Pair.with("HASH_CURRENT", "VARCHAR(100)")));
		// @formatter:on
	}

	/**
	 * Indexes that keep the admin audit queries (newest first, filtered by actor,
	 * target, or resource) from scanning the whole table.
	 *
	 * @return audit table indexes
	 */
	public static List<OwlIndex> getIndexes() {
		String table = UserAuditTrailUtils.TABLE;
		return List.of(OwlIndex.of("USER_AUDIT_EVENTS_SEQUENCE_INDEX", table, "AUDIT_SEQUENCE"),
				OwlIndex.of("USER_AUDIT_EVENTS_TIME_INDEX", table, "EVENT_TIME", "EVENT_ID"),
				OwlIndex.of("USER_AUDIT_EVENTS_TYPE_INDEX", table, "EVENT_TYPE"),
				OwlIndex.of("USER_AUDIT_EVENTS_ACTOR_INDEX", table, "ACTOR_USER_ID"),
				OwlIndex.of("USER_AUDIT_EVENTS_SUBJECT_INDEX", table, "SUBJECT_USER_ID"),
				OwlIndex.of("USER_AUDIT_EVENTS_TARGET_INDEX", table, "TARGET_ID"),
				OwlIndex.of("USER_AUDIT_EVENTS_PROJECT_INDEX", table, "PROJECT_ID"),
				OwlIndex.of("USER_AUDIT_EVENTS_ENGINE_INDEX", table, "ENGINE_ID"));
	}
}

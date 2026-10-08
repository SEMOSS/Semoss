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

		addTable("ENGINE_VIEWS", Arrays.asList(
				Pair.with("ENGINEID", VARCHAR_255),
				Pair.with("DATE", "DATE"),
				Pair.with("VIEWS", INTEGER_DATATYPE_NAME)));

		addTable("ENGINE_USES", Arrays.asList(
				Pair.with("ENGINEID", VARCHAR_255),
				Pair.with("INSIGHTID", VARCHAR_255),
				Pair.with("PROJECTID", VARCHAR_255),
				Pair.with("DATE", "DATE")));

		addTable("USER_CATALOG_VOTES", Arrays.asList(
				Pair.with("USERID", VARCHAR_255),
				Pair.with("TYPE", VARCHAR_255),
				Pair.with("ENGINEID", VARCHAR_255),
				Pair.with("VOTE", INTEGER_DATATYPE_NAME),
				Pair.with("LAST_MODIFIED", TIMESTAMP_DATATYPE_NAME)));

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

		addTable("INSIGHT_OPENS", Arrays.asList(
				Pair.with("INSIGHTID", VARCHAR_255),
				Pair.with("USERID", VARCHAR_255),
				Pair.with("OPENED_ON", TIMESTAMP_DATATYPE_NAME),
				Pair.with("ORIGIN", "VARCHAR(2000)")));

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

		addTable(UserAuditTrailUtils.TABLE, Arrays.asList(
				// event
				Pair.with("EVENT_ID", VARCHAR_255),
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
				// optional integrity
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
		return List.of(OwlIndex.of("USER_AUDIT_EVENTS_TIME_INDEX", table, "EVENT_TIME"),
				OwlIndex.of("USER_AUDIT_EVENTS_TYPE_INDEX", table, "EVENT_TYPE"),
				OwlIndex.of("USER_AUDIT_EVENTS_ACTOR_INDEX", table, "ACTOR_USER_ID"),
				OwlIndex.of("USER_AUDIT_EVENTS_SUBJECT_INDEX", table, "SUBJECT_USER_ID"),
				OwlIndex.of("USER_AUDIT_EVENTS_TARGET_INDEX", table, "TARGET_ID"),
				OwlIndex.of("USER_AUDIT_EVENTS_PROJECT_INDEX", table, "PROJECT_ID"),
				OwlIndex.of("USER_AUDIT_EVENTS_ENGINE_INDEX", table, "ENGINE_ID"));
	}
}

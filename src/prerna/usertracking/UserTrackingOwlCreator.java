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

import org.javatuples.Pair;

import prerna.engine.impl.owl.AbstractOwlCreator;
import prerna.util.sql.AbstractSqlQueryUtil;

public class UserTrackingOwlCreator extends AbstractOwlCreator {

	public UserTrackingOwlCreator(AbstractSqlQueryUtil queryUtil) {
		createColumnsAndTypes(queryUtil);
	}

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

		addTable("USER_AUDIT_EVENTS", Arrays.asList(
				Pair.with("EVENT_ID", VARCHAR_255),
				Pair.with("EVENT_TIME", TIMESTAMP_DATATYPE_NAME),
				Pair.with("EVENT_TYPE", VARCHAR_255),
				Pair.with("ACTION", VARCHAR_255),
				Pair.with("STATUS", "VARCHAR(50)"),
				Pair.with("ACTOR_USER_ID", VARCHAR_255),
				Pair.with("ACTOR_USER_TYPE", VARCHAR_255),
				Pair.with("ACTOR_USER_NAME", VARCHAR_255),
				Pair.with("SESSION_ID", VARCHAR_255),
				Pair.with("REQUEST_ID", VARCHAR_255),
				Pair.with("IP_ADDR", VARCHAR_255),
				Pair.with("TARGET_TYPE", VARCHAR_255),
				Pair.with("TARGET_ID", VARCHAR_255),
				Pair.with("TARGET_NAME", "VARCHAR(1000)"),
				Pair.with("PROJECT_ID", VARCHAR_255),
				Pair.with("ENGINE_ID", VARCHAR_255),
				Pair.with("INSIGHT_ID", VARCHAR_255),
				Pair.with("ROOM_ID", VARCHAR_255),
				Pair.with("OLD_VALUE", CLOB_DATATYPE_NAME),
				Pair.with("NEW_VALUE", CLOB_DATATYPE_NAME),
				Pair.with("DETAILS", CLOB_DATATYPE_NAME),
				Pair.with("ERROR_MESSAGE", CLOB_DATATYPE_NAME)));
		// @formatter:on
	}
}

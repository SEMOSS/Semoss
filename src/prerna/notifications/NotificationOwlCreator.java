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
package prerna.notifications;

import java.util.ArrayList;
import java.util.Arrays;

import org.javatuples.Pair;

import prerna.engine.impl.owl.AbstractOwlCreator;
import prerna.util.sql.AbstractSqlQueryUtil;

public class NotificationOwlCreator extends AbstractOwlCreator {

	public NotificationOwlCreator(AbstractSqlQueryUtil queryUtil) {
		createColumnsAndTypes(queryUtil);
	}

	public void createColumnsAndTypes(AbstractSqlQueryUtil queryUtil) {
		final String CLOB_DATATYPE_NAME = queryUtil.getClobDataTypeName();
		final String BOOLEAN_DATATYPE_NAME = queryUtil.getBooleanDataTypeName();
		final String TIMESTAMP_DATATYPE_NAME = queryUtil.getDateWithTimeDataType();
		final String VARCHAR_255 = "VARCHAR(255)";

		this.allSchemas = new ArrayList<>();

		// @formatter:off
		addTable("NOTIFICATION_EVENT", Arrays.asList(
				Pair.with("NOTIFICATION_ID", "VARCHAR(50)"),
				Pair.with("KIND", "VARCHAR(20)"),
				Pair.with("TYPE", "VARCHAR(50)"),
				Pair.with("SCOPE_TYPE", "VARCHAR(20)"),
				Pair.with("SCOPE_ID", "VARCHAR(50)"),
				Pair.with("AUDIENCE_TYPE", "VARCHAR(20)"),
				Pair.with("AUDIENCE_ID", VARCHAR_255),
				Pair.with("AUDIENCE_USER_TYPE", "VARCHAR(50)"),
				Pair.with("TITLE", VARCHAR_255),
				Pair.with("MESSAGE", CLOB_DATATYPE_NAME),
				Pair.with("PRIORITY", "VARCHAR(20)"),
				Pair.with("DISPLAY_SURFACE", "VARCHAR(20)"),
				Pair.with("SOURCE_TYPE", "VARCHAR(20)"),
				Pair.with("SOURCE_ID", "VARCHAR(50)"),
				Pair.with("TARGET_TYPE", "VARCHAR(30)"),
				Pair.with("TARGET_ID", "VARCHAR(50)"),
				Pair.with("TARGET_URL", CLOB_DATATYPE_NAME),
				Pair.with("ACTION_LABEL", "VARCHAR(50)"),
				Pair.with("STATUS", "VARCHAR(20)"),
				Pair.with("GROUP_ID", "VARCHAR(50)"),
				Pair.with("METADATA_JSON", CLOB_DATATYPE_NAME),
				Pair.with("CREATED_BY", VARCHAR_255),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("RESOLVED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("EXPIRES_AT", TIMESTAMP_DATATYPE_NAME)));
		addTable("NOTIFICATION_USER_STATE", Arrays.asList(
				Pair.with("NOTIFICATION_ID", "VARCHAR(50)"),
				Pair.with("USER_ID", VARCHAR_255),
				Pair.with("USER_TYPE", "VARCHAR(50)"),
				Pair.with("IS_READ", BOOLEAN_DATATYPE_NAME),
				Pair.with("READ_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("IS_DISMISSED", BOOLEAN_DATATYPE_NAME),
				Pair.with("DISMISSED_AT", TIMESTAMP_DATATYPE_NAME)));
		addTable("NOTIFICATION_DELIVERY", Arrays.asList(
				Pair.with("DELIVERY_ID", "VARCHAR(50)"),
				Pair.with("NOTIFICATION_ID", "VARCHAR(50)"),
				Pair.with("USER_ID", VARCHAR_255),
				Pair.with("USER_TYPE", "VARCHAR(50)"),
				Pair.with("CHANNEL", "VARCHAR(20)"),
				Pair.with("STATUS", "VARCHAR(20)"),
				Pair.with("ATTEMPTS", "INT"),
				Pair.with("LAST_ERROR", CLOB_DATATYPE_NAME),
				Pair.with("CREATED_AT", TIMESTAMP_DATATYPE_NAME),
				Pair.with("SENT_AT", TIMESTAMP_DATATYPE_NAME)));
		// @formatter:on
	}
}

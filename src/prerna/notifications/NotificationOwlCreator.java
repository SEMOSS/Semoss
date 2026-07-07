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
		addTable("NOTIFICATION", Arrays.asList(
				Pair.with("NOTIFICATIONID", VARCHAR_255),
				Pair.with("RECIPIENTID", VARCHAR_255),
				Pair.with("RECIPIENTTYPE", VARCHAR_255), 
				Pair.with("NOTIFICATIONTITLE", VARCHAR_255),
				Pair.with("MESSAGE", CLOB_DATATYPE_NAME),
				Pair.with("ACTIONTYPE", "VARCHAR(50)"),
				Pair.with("ACTIONTARGET", VARCHAR_255),
				Pair.with("ISREAD", BOOLEAN_DATATYPE_NAME),
				Pair.with("PRIORITY", "VARCHAR(20)"),
				Pair.with("NOTIFICATIONTYPE", VARCHAR_255),
				Pair.with("CATALOGID", VARCHAR_255),
				Pair.with("CREATEDBY", VARCHAR_255),
				Pair.with("CREATEDDATE", TIMESTAMP_DATATYPE_NAME),
				Pair.with("READDATE", TIMESTAMP_DATATYPE_NAME),
				Pair.with("NOTIFICATIONSOURCE", VARCHAR_255),
				Pair.with("USERID", VARCHAR_255),
				Pair.with("USERTYPE", VARCHAR_255),
				Pair.with("USEREXISTINGROLE", VARCHAR_255),
				Pair.with("USERNEWROLE", VARCHAR_255)));
		// @formatter:on
	}
}

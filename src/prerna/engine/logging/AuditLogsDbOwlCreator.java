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
package prerna.engine.logging;

import java.util.ArrayList;
import java.util.Arrays;

import org.javatuples.Pair;

import prerna.engine.impl.owl.AbstractOwlCreator;
import prerna.util.sql.AbstractSqlQueryUtil;

public class AuditLogsDbOwlCreator extends AbstractOwlCreator {

	public AuditLogsDbOwlCreator(AbstractSqlQueryUtil queryUtil) {
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
		addTable("AUDIT_LOGS", Arrays.asList( 
				Pair.with("LOG_ID", VARCHAR_255),
				Pair.with("REQUEST_ID", VARCHAR_255),
				Pair.with("IS_SUCCESS", BOOLEAN_DATATYPE_NAME),
				Pair.with("SESSION_ID", VARCHAR_255),
				Pair.with("USER_ID", VARCHAR_255),
				Pair.with("USER_NAME", VARCHAR_255),
				Pair.with("USER_TYPE", VARCHAR_255),
				Pair.with("SPAN_ID", VARCHAR_255),
				Pair.with("INSIGHT_ID", VARCHAR_255),
				Pair.with("PROJECT_ID", VARCHAR_255),
				Pair.with("PROJECT_NAME", VARCHAR_255),
				Pair.with("ROOM_ID", VARCHAR_255),
				Pair.with("ENGINE_ID", VARCHAR_255),
				Pair.with("ENGINE_NAME", VARCHAR_255),
				Pair.with("ENGINE_TYPE", VARCHAR_255),
				Pair.with("METHOD_NAME", VARCHAR_255),
				Pair.with("ENGINE_SUBTYPE", VARCHAR_255),
				Pair.with("INPUT_REACTOR_NAME", VARCHAR_255),
				Pair.with("OUTPUT_REACTOR_NAME", VARCHAR_255),
				Pair.with("MESSAGE", CLOB_DATATYPE_NAME),
				Pair.with("REQUEST", CLOB_DATATYPE_NAME),
				Pair.with("RESPONSE", CLOB_DATATYPE_NAME),
				Pair.with("NUMBER_OF_TOKENS_IN_PROMPT", INTEGER_DATATYPE_NAME),
				Pair.with("NUMBER_OF_TOKENS_IN_RESPONSE", INTEGER_DATATYPE_NAME),
				Pair.with("REQUEST_START_TIME", TIMESTAMP_DATATYPE_NAME),
				Pair.with("RESPONSE_END_TIME", TIMESTAMP_DATATYPE_NAME),
				Pair.with("LOG_LEVEL", VARCHAR_255),
				Pair.with("LOG_TIMESTAMP", TIMESTAMP_DATATYPE_NAME),
				Pair.with("LOGGER_NAME", VARCHAR_255),
				Pair.with("LOGGER_LOCATION", VARCHAR_255)));

		addTable("SERVER_LOGS", Arrays.asList(
				Pair.with("LOG_ID", VARCHAR_255),
				Pair.with("SESSION_ID", VARCHAR_255),
				Pair.with("REQUEST_ID", VARCHAR_255),
				Pair.with("USER_ID", VARCHAR_255),
				Pair.with("USER_TYPE", VARCHAR_255),
				Pair.with("LEVEL", "VARCHAR(50)"),
				Pair.with("LOGGER_NAME", VARCHAR_255),
				Pair.with("LOGGER_LOCATION", VARCHAR_255),
				Pair.with("THREAD_NAME", VARCHAR_255),
				Pair.with("LOG_TIMESTAMP", TIMESTAMP_DATATYPE_NAME),
				Pair.with("MESSAGE", CLOB_DATATYPE_NAME)));
		// @formatter:on
	}
}

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
package prerna.theme;

import java.util.ArrayList;
import java.util.Arrays;

import org.javatuples.Pair;

import prerna.engine.impl.owl.AbstractOwlCreator;
import prerna.util.sql.AbstractSqlQueryUtil;

public class ThemeOwlCreator extends AbstractOwlCreator {

	public ThemeOwlCreator(AbstractSqlQueryUtil queryUtil) {
		createColumnsAndTypes(queryUtil);
	}

	public void createColumnsAndTypes(AbstractSqlQueryUtil queryUtil) {
		final String CLOB_DATATYPE_NAME = queryUtil.getClobDataTypeName();
		final String BOOLEAN_DATATYPE_NAME = queryUtil.getBooleanDataTypeName();
		final String TIMESTAMP_DATATYPE_NAME = queryUtil.getDateWithTimeDataType();
		final String VARCHAR_255 = "VARCHAR(255)";

		this.allSchemas = new ArrayList<>();

		// @formatter:off
		addTable("ADMIN_THEME", Arrays.asList(
				Pair.with("ID", VARCHAR_255),
				Pair.with("THEME_NAME", VARCHAR_255),
				Pair.with("THEME_MAP", CLOB_DATATYPE_NAME),
				Pair.with("IS_ACTIVE", BOOLEAN_DATATYPE_NAME)));

		addTable("BLOCKS_TABLE", Arrays.asList(
				Pair.with("ID", VARCHAR_255),
				Pair.with("NAME", VARCHAR_255),
				Pair.with("SECTION", VARCHAR_255),
				Pair.with("HOVER_TEXT", "VARCHAR(500)"),
				Pair.with("BLOCK_JSON", CLOB_DATATYPE_NAME),
				Pair.with("DATE_ADDED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("IS_LATEST", BOOLEAN_DATATYPE_NAME),
				Pair.with("CREATED_BY", VARCHAR_255)));
		// @formatter:on
	}
}

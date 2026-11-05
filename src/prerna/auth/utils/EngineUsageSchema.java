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
package prerna.auth.utils;

import java.sql.Connection;
import java.util.List;

import prerna.engine.api.IRDBMSEngine;

/** Additive schema only: model tables and data are deliberately untouched. */
final class EngineUsageSchema {
	private EngineUsageSchema() {
	}

	static void initialize(IRDBMSEngine db, Connection conn) throws Exception {
		var q = db.getQueryUtil();
		create(db, conn, "ENGINEUSAGELIMIT",
				new String[] { "ID", "CATALOGTYPE", "ENGINEID", "LIMITSOURCE", "SUBJECTID", "SUBJECTTYPE", "LIMITJSON",
						"MODIFIEDAT", "MODIFIEDBY", "MODIFIEDBYTYPE" },
				new String[] { "VARCHAR(64)", "VARCHAR(32)", "VARCHAR(255)", "VARCHAR(32)", "VARCHAR(255)",
						"VARCHAR(255)", q.getClobDataTypeName(), q.getDateWithTimeDataType(), "VARCHAR(255)",
						"VARCHAR(255)" });
		create(db, conn, "ENGINEUSAGEHISTORY", new String[] { "ID", "COUNTERID", "AMOUNT", "EXPIRESAT" },
				new String[] { "VARCHAR(64)", "VARCHAR(128)", "BIGINT", "BIGINT" });
		index(db, "ENGINEUSAGELIMIT_ENGINE_INDEX", "ENGINEUSAGELIMIT", List.of("CATALOGTYPE", "ENGINEID"));
		index(db, "ENGINEUSAGEHISTORY_COUNTER_INDEX", "ENGINEUSAGEHISTORY", List.of("COUNTERID"));
	}

	private static void create(IRDBMSEngine db, Connection conn, String table, String[] columns, String[] types)
			throws Exception {
		var q = db.getQueryUtil();
		Object[] constraints = new Object[columns.length];
		constraints[0] = "PRIMARY KEY";
		if (q.allowsIfExistsTableSyntax())
			db.insertData(q.createTableIfNotExistsWithCustomConstraints(table, columns, types, constraints));
		else if (!q.tableExists(conn, table, db.getDatabase(), db.getSchema()))
			db.insertData(q.createTableWithCustomConstraints(table, columns, types, constraints));
	}

	private static void index(IRDBMSEngine db, String name, String table, List<String> columns) throws Exception {
		var q = db.getQueryUtil();
		if (q.allowIfExistsIndexSyntax())
			db.insertData(q.createIndexIfNotExists(name, table, columns));
		else if (!q.indexExists(db, name, table, db.getDatabase(), db.getSchema()))
			db.insertData(q.createIndex(name, table, columns));
	}
}

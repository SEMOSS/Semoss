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
package prerna.ds.util;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.Utility;

@Deprecated
public class RdbmsQueryBuilder {

	private static final Logger classLogger = LogManager.getLogger(RdbmsQueryBuilder.class);

	@Deprecated
	public static String escapeForSQLStatement(String s) {
		if (s == null) {
			return s;
		}
		return s.replace("'", "''");
	}

	/**
	 * Column names must match between the 2 tables!
	 * 
	 * @param leftTableName
	 * @param mergeTable
	 * @param columnNames
	 * @return
	 */
	@Deprecated
	public static String makeMergeIntoQuery(String leftTableName, String mergeTable, String[] keyColumns,
			String[] columnNames) {
		StringBuilder sql = new StringBuilder("MERGE INTO ");
		sql.append(leftTableName).append(" KEY(").append(keyColumns[0]);
		for (int i = 1; i < keyColumns.length; i++) {
			sql.append(",").append(keyColumns[i]);
		}
		sql.append(") (SELECT ").append(columnNames[0]);
		for (int i = 1; i < columnNames.length; i++) {
			sql.append(",").append(columnNames[i]);
		}
		sql.append(" FROM ").append(mergeTable).append(")");
		return sql.toString();
	}

	@Deprecated
	public static String createTableFromFile(String fileName, Map<String, String> conceptTypes) {
		// if the fileName db exists delete it
		// I also need to think about multi-user ?
		// may be not, not until they move to the new version ok
		String normalizedFileName = Utility.normalizePath(fileName);
		String dbName = normalizedFileName;
		dbName = normalizedFileName.replace(".csv", "");
		dbName = normalizedFileName.replace(".tsv", "");

		try {
			File file = new File(dbName + ".mv.db");
			if (file.exists()) {
				FileUtils.forceDelete(file);
			}
			file = new File(dbName + ".trace.db");
			if (file.exists()) {
				FileUtils.forceDelete(file);
			}
		} catch (IOException e) {
			classLogger.error("Failed to delete existing H2 database files for '{}'", dbName, e);
		}

		StringBuffer dropTable = new StringBuffer("DROP TABLE IF EXISTS ");
		StringBuffer createString = new StringBuffer("CREATE TABLE ");
		StringBuffer selectString = new StringBuffer("SELECT ");

		Iterator<String> keys = conceptTypes.keySet().iterator();
		int count = 0;
		while (keys.hasNext()) {
			String name = keys.next();
			String tableName = Utility.getInstanceName(name);
			String type = conceptTypes.get(name);
			name = Utility.getClassName(name);

			if (count == 0) {
				createString.append(tableName + " (");
				dropTable.append(tableName + "; ");
			}
			type = type.replace("TYPE:", "");

			StringBuffer tempSelect = new StringBuffer("");

			if (name.contains("UNIQUE_ROW_ID")) {
				tempSelect.append("ROWNUM()");
			} else {
				if (type.equalsIgnoreCase("DOUBLE") || type.equalsIgnoreCase("FLOAT")
						|| type.equalsIgnoreCase("NUMBER")) {
					tempSelect.append("CONVERT(" + name + ", " + "Double)");
				} else if (type.equalsIgnoreCase("Integer")) {
					tempSelect.append("CONVERT(" + name + ", " + "Int)");
				} else if (type.equalsIgnoreCase("Date")) {
					tempSelect.append("CONVERT(" + name + ", " + "Date)");
				} else if (type.equalsIgnoreCase("Bigint") || type.equalsIgnoreCase("Long")) {
					tempSelect.append("CONVERT(" + name + ", " + "Bigint)");
				} else if (type.equalsIgnoreCase("boolean")) {
					tempSelect.append("CONVERT(" + name + ", " + "boolean)");
				} else { // if(type.contains("varchar"))
					tempSelect.append(name);
				}
			}
			if (count == 0) {
				createString.append(name + " " + type);
				selectString.append(tempSelect);
			} else {
				createString.append(", " + name + " " + type);
				selectString.append(", " + tempSelect);
			}
			count++;
		}

		createString.append(") AS ").append(selectString).append(" from CSVREAD('" + fileName + "');");
		dropTable.append(createString);
		return dropTable.toString();
	}

}

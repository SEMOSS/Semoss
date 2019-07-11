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
package prerna.util.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class H2QueryUtil extends AnsiSqlQueryUtil {
	
	H2QueryUtil() {
		super();
	}
	
	H2QueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
	}
	
	H2QueryUtil(RdbmsTypeEnum dbType, String hostname, String port, String schema, String username, String password) {
		super(dbType, hostname, port, schema, username, password);
	}
	
	@Override
	public void enhanceConnection(Connection con) {
		try {
			Statement stmt = con.createStatement();
			stmt.execute("DROP AGGREGATE IF EXISTS MEDIAN");
			stmt.close();
			stmt = con.createStatement();
			stmt.execute("CREATE AGGREGATE MEDIAN FOR \"prerna.ds.h2.H2MedianAggregation\";");
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public String dropIndex(String indexName, String tableName) {
		return "DROP INDEX " + indexName;
	}

	@Override
	public String dropIndexIfExists(String indexName, String tableName) {
		return "DROP INDEX IF EXISTS " + indexName;
	}

	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Query database scripts
	 */
	
	@Override
	public String tableExistsQuery(String tableName, String schema) {
		// do not need to use the schema
		return "SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '" + tableName + "'";
	}
	
	@Override
	public String columnDetailsQuery(String tableName, String columnName, String schema) {
		// do not need to use the schema
		return "SELECT COLUMN_NAME, TYPE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '" + tableName + "' AND COLUMN_NAME='" + columnName + "';";
	}
	
	@Override
	public String getIndexList(String schema) {
		// do not need to use the schema
		return "SELECT DISTINCT INDEX_NAME, TABLE_NAME FROM INFORMATION_SCHEMA.INDEXES;";
	}
	
	@Override
	public String getIndexDetails(String indexName, String tableName, String schema) {
		// do not use the schema
		return "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.INDEXES WHERE INDEX_NAME='" + indexName + "' AND TABLE_NAME='" + tableName + "';";
	}
	
	@Override
	public String allIndexForTableQuery(String tableName, String schema) {
		// do not need to use the schema
		return "SELECT INDEX_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_NAME='" + tableName + "';";
	}
	
}
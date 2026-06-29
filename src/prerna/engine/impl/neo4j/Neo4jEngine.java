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
package prerna.engine.impl.neo4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.AbstractDatabaseEngine;
import prerna.query.interpreters.CypherInterpreter;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * This is the connection to a remote neo4j graph database using the jdbc
 * connection
 */
public class Neo4jEngine extends AbstractDatabaseEngine {

	private static final Logger classLogger = LogManager.getLogger(Neo4jEngine.class);

	protected Map<String, String> typeMap = new HashMap<String, String>();
	protected Map<String, String> nameMap = new HashMap<String, String>();
	protected boolean useLabel = false;
	private Connection conn;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		// get type map
		String typeMapStr = this.smssProp.getProperty(Constants.TYPE_MAP);
		if (typeMapStr != null && !typeMapStr.trim().isEmpty()) {
			try {
				this.typeMap = new ObjectMapper().readValue(typeMapStr, Map.class);
			} catch (IOException e) {
				classLogger.error("Failed to parse neo4j type map", e);
			}
		}
		// get the name map
		String nameMapStr = this.smssProp.getProperty(Constants.NAME_MAP);
		if (nameMapStr != null && !nameMapStr.trim().isEmpty()) {
			try {
				this.nameMap = new ObjectMapper().readValue(nameMapStr, Map.class);
			} catch (IOException e) {
				classLogger.error("Failed to parse neo4j name map", e);
			}
		}
		if (smssProp.containsKey(Constants.TINKER_USE_LABEL)) {
			String booleanStr = smssProp.get(Constants.TINKER_USE_LABEL).toString();
			useLabel = Boolean.parseBoolean(booleanStr);
		}
		this.conn = getGraphDatabaseConnection();
	}

	@Override
	public DATABASE_TYPE getDatabaseType() {
		return IDatabaseEngine.DATABASE_TYPE.NEO4J;
	}

	@Override
	public Object execQuery(String query) {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = getGraphDatabaseConnection().prepareStatement(query);
			Map<String, Object> map = new HashMap();
			rs = stmt.executeQuery();
			map.put(IRDBMSEngine.RESULTSET_OBJECT, rs);
			if (isConnected()) {
				map.put(IRDBMSEngine.CONNECTION_OBJECT, null);
				map.put(IRDBMSEngine.ENGINE_CONNECTION_OBJECT, conn);
			} else {
				map.put(IRDBMSEngine.CONNECTION_OBJECT, conn);
				map.put(IRDBMSEngine.ENGINE_CONNECTION_OBJECT, null);
			}
			map.put(IRDBMSEngine.STATEMENT_OBJECT, stmt);
			return map;
		} catch (Exception e) {
			classLogger.error("Error executing cypher query = {}", Utility.cleanLogString(query), e);
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					classLogger.error("Failed to close neo4j statement", e);
				}
			}
		}
		return null;
	}

	@Override
	public IQueryInterpreter getQueryInterpreter() {
		CypherInterpreter interp = new CypherInterpreter(this.typeMap, this.nameMap);
		interp.setUseLabel(this.useLabel);
		return interp;
	}

	public Connection getGraphDatabaseConnection() {
		try {
			if (this.conn == null || this.conn.isClosed()) {
				classLogger.info("Opening neo4j graph: ");
				// org.neo4j.jdbc.Neo4jDriver auto-registers via ServiceLoader; load explicitly
				// so the webapp classloader registers it with DriverManager regardless of
				// container behavior.
				Class.forName("org.neo4j.jdbc.Neo4jDriver");
				String connectionURL = smssProp.getProperty(Constants.CONNECTION_URL);
				// The Neo4j JDBC 6.x driver uses the scheme jdbc:neo4j:// (with +s / +ssc for
				// TLS).
				// Legacy connections persisted by the old neo4j-jdbc-bolt driver use
				// jdbc:neo4j:bolt://; normalize so existing .smss files keep working without a
				// data migration.
				// jdbc:neo4j:bolt:// -> jdbc:neo4j://, jdbc:neo4j:bolt+s:// -> jdbc:neo4j+s://,
				// etc.
				if (connectionURL != null && connectionURL.startsWith("jdbc:neo4j:bolt")) {
					connectionURL = connectionURL.replace("jdbc:neo4j:bolt", "jdbc:neo4j");
					classLogger.info("Normalized legacy neo4j bolt connection URL scheme to jdbc:neo4j");
				}
				String username = smssProp.getProperty(Constants.USERNAME);
				String password = smssProp.getProperty(Constants.PASSWORD);
				classLogger.info("Connecting to remote graph: {}", Utility.cleanLogString(connectionURL));
				conn = DriverManager.getConnection(connectionURL, username, password);
				classLogger.info("Done neo4j opening graph: ");
			}
		} catch (Exception e) {
			classLogger.error("Failed to open neo4j graph database connection", e);
		}
		return this.conn;
	}

	@Override
	public void insertData(String query) throws Exception {

	}

	@Override
	public void removeData(String query) throws Exception {

	}

	@Override
	public void commit() {

	}

	@Override
	public Vector<Object> getEntityOfType(String type) {
		return null;
	}

	@Override
	public void close() throws IOException {
		super.close();
		ConnectionUtils.closeConnection(this.conn);
	}

	@Override
	public boolean holdsFileLocks() {
		return false;
	}

}

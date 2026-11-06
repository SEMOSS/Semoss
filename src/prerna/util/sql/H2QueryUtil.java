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

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.engine.impl.SmssUtilities;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.sql.H2SqlInterpreter;
import prerna.util.Constants;
import prerna.util.Utility;

public class H2QueryUtil extends AnsiSqlQueryUtil {

	private static final Logger classLogger = LogManager.getLogger(H2QueryUtil.class);

	public static final String BASE_H2_FILE_CONNECTION = "jdbc:h2:nio:" + "@" + Constants.BASE_FOLDER + "@"
			+ DIR_SEPARATOR + Constants.DATABASE_FOLDER + DIR_SEPARATOR + "@" + Constants.ENGINE + "@" + DIR_SEPARATOR
			+ "database;query_timeout=180000;cache_size=32768";

	// H2 1.x url settings that H2 2.x rejects (older smss files still carry some of these)
	private static final Set<String> UNSUPPORTED_V1_URL_SETTINGS = Set.of("EARLY_FILTER", "LOG", "UNDO_LOG", "MVCC",
			"MULTI_THREADED", "ALIAS_COLUMN_NAME", "COMPRESS_LOB", "ACCESS_MODE_LOG", "PERSISTENT",
			"BINARY_COLLATION", "UUID_COLLATION");

	// words H2 2.x reserves that the 1.x list in RDF_Map.prop is missing - merged in so
	// installs that keep an older RDF_Map.prop still escape them
	private static final List<String> H2_V2_RESERVED_WORDS = List.of("ANY", "ASYMMETRIC", "AUTHORIZATION", "CAST",
			"CURRENT_CATALOG", "CURRENT_PATH", "CURRENT_ROLE", "CURRENT_SCHEMA", "DAY", "DEFAULT", "ELSE", "END", "HOUR",
			"KEY", "MINUTE", "MONTH", "SECOND", "SESSION_USER", "SET", "SOME", "SYMMETRIC", "SYSTEM_USER", "TO",
			"UESCAPE", "UNKNOWN", "USER", "USING", "VALUE", "WHEN", "YEAR");

	private boolean forceFile;

	// strip H2 1.x only settings so H2 2.x will accept the url
	public static String removeUnsupportedUrlSettings(String connectionUrl) {
		if (connectionUrl == null || connectionUrl.indexOf(';') < 0) {
			return connectionUrl;
		}
		String[] parts = connectionUrl.split(";");
		StringBuilder cleanUrl = new StringBuilder(parts[0]);
		List<String> removed = new ArrayList<>();
		for (int i = 1; i < parts.length; i++) {
			String setting = parts[i];
			int eq = setting.indexOf('=');
			String key = (eq < 0 ? setting : setting.substring(0, eq)).trim().toUpperCase();
			// MV_STORE=FALSE is left alone on purpose - dropping it would open an empty new db
			if (UNSUPPORTED_V1_URL_SETTINGS.contains(key)) {
				removed.add(key);
			} else if (!setting.trim().isEmpty()) {
				cleanUrl.append(';').append(setting);
			}
		}
		if (!removed.isEmpty()) {
			classLogger.warn("Removed H2 1.x connection settings {} that H2 2.x does not support", removed);
		}
		return cleanUrl.toString();
	}

	H2QueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.H2_DB);
	}

	H2QueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.H2_DB);
	}

	@Override
	public void setReservedWords(List<String> reservedWords) {
		List<String> allWords = new ArrayList<>();
		if (reservedWords != null) {
			allWords.addAll(reservedWords);
		}
		for (String word : H2_V2_RESERVED_WORDS) {
			if (!allWords.contains(word)) {
				allWords.add(word);
			}
		}
		super.setReservedWords(allWords);
	}

	@Override
	public IQueryInterpreter getInterpreter(IDatabaseEngine engine) {
		return new H2SqlInterpreter(engine);
	}

	@Override
	public IQueryInterpreter getInterpreter(ITableDataFrame frame) {
		return new H2SqlInterpreter(frame);
	}

	@Override
	public String setConnectionDetailsfromMap(Map<String, Object> configMap) throws RuntimeException {
		if (configMap == null || configMap.isEmpty()) {
			throw new RuntimeException("Configuration map is null or empty");
		}

		this.forceFile = getOptionalBooleanValue(configMap, AbstractSqlQueryUtil.FORCE_FILE, false);
		this.connectionUrl = getOptionalStringValue(configMap, AbstractSqlQueryUtil.CONNECTION_URL, null);
		this.hostname = getOptionalStringValue(configMap, AbstractSqlQueryUtil.HOSTNAME, null);
		this.port = getOptionalStringValue(configMap, AbstractSqlQueryUtil.PORT, null);
		this.schema = getOptionalStringValue(configMap, AbstractSqlQueryUtil.SCHEMA, null);
		this.additionalProps = getOptionalStringValue(configMap, AbstractSqlQueryUtil.ADDITIONAL, null);
		this.username = getOptionalStringValue(configMap, AbstractSqlQueryUtil.USERNAME, null);
		this.password = getOptionalStringValue(configMap, AbstractSqlQueryUtil.PASSWORD, null);

		return buildConnectionString();
	}

	@Override
	public String setConnectionDetailsFromSMSS(CaseInsensitiveProperties prop) throws RuntimeException {
		if (prop == null || prop.isEmpty()) {
			throw new RuntimeException("Properties object is null or empty");
		}

		this.forceFile = getOptionalBooleanValue(prop, AbstractSqlQueryUtil.FORCE_FILE, false);
		this.connectionUrl = getOptionalStringValue(prop, AbstractSqlQueryUtil.CONNECTION_URL, null);
		this.hostname = getOptionalStringValue(prop, AbstractSqlQueryUtil.HOSTNAME, null);
		this.port = getOptionalStringValue(prop, AbstractSqlQueryUtil.PORT, null);
		this.schema = getOptionalStringValue(prop, AbstractSqlQueryUtil.SCHEMA, null);
		this.additionalProps = getOptionalStringValue(prop, AbstractSqlQueryUtil.ADDITIONAL, null);
		this.username = getOptionalStringValue(prop, AbstractSqlQueryUtil.USERNAME, null);
		this.password = getOptionalStringValue(prop, AbstractSqlQueryUtil.PASSWORD, null);

		return buildConnectionString();
	}

	@Override
	public String buildConnectionString() {
		if (this.connectionUrl != null && !this.connectionUrl.isEmpty()) {
			return this.connectionUrl;
		}

		this.connectionUrl = this.dbType.getUrlPrefix();

		requireNonBlank(AbstractSqlQueryUtil.HOSTNAME, this.hostname);

		String port = this.port;
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "";
		}

		File f = new File(Utility.normalizePath(hostname));
		if (this.forceFile || f.exists()) {
			hostname = hostname.replace(".mv.db", "");
			this.connectionUrl += ":nio:" + hostname;
		} else {
			this.connectionUrl += ":tcp://" + hostname + ":" + port;
		}

		if (this.schema != null && !this.schema.isEmpty()) {
			this.connectionUrl += ";SCHEMA=" + schema;
		}

		this.connectionUrl = appendAdditionalProps(this.connectionUrl);

		return this.connectionUrl;
	}

	@Override
	public void enhanceConnection(Connection con) {
		Statement stmt = null;
		try {
			stmt = con.createStatement();
			stmt.execute("DROP AGGREGATE IF EXISTS MEDIAN");
			stmt.close();
			stmt = con.createStatement();
			stmt.execute("CREATE AGGREGATE IF NOT EXISTS SMSS_MEDIAN FOR \"prerna.ds.rdbms.h2.H2MedianAggregation\";");
		} catch (SQLException e) {
			classLogger.error("Error enhancing H2 connection while registering SMSS_MEDIAN aggregate: {}",
					e.getMessage(), e);
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					classLogger.error("Error closing statement after H2 connection enhancement: {}", e.getMessage(), e);
				}
			}
		}
	}

	@Override
	public String fillFileParameterizedConnectionUrl(String connectionUrl, String engineId, String engineName) {
		if (engineId == null && engineName == null) {
			return connectionUrl;
		}

		if (connectionUrl == null || (connectionUrl = connectionUrl.trim()).isEmpty()) {
			connectionUrl = BASE_H2_FILE_CONNECTION;
		}

		String baseFolder = Utility.getBaseFolder().replace('\\', '/');
		if (baseFolder.endsWith("/")) {
			baseFolder = baseFolder.substring(0, baseFolder.length() - 1);
		}

		return connectionUrl.replace("@" + Constants.BASE_FOLDER + "@", baseFolder)
				.replace("@" + Constants.ENGINE + "@", SmssUtilities.getUniqueName(engineName, engineId));
	}

	@Override
	public String getMedianFunctionSyntax() {
		return "SMSS_MEDIAN";
	}

	@Override
	public String dropIndex(String indexName, String tableName) {
		return "DROP INDEX " + indexName;
	}

	@Override
	public String dropIndexIfExists(String indexName, String tableName) {
		return "DROP INDEX IF EXISTS " + indexName;
	}

	@Override
	public String getDateFormatFunctionSyntax() {
		return "FORMATDATETIME";
	}

	@Override
	public String escapeReferencedAlias(String alias) {
		return "\"" + alias + "\"";
	}

	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Query database scripts
	 */

	@Override
	public String tableExistsQuery(String tableName, String database, String schema) {
		// do not need to use the schema
		return "SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '"
				+ tableName.toUpperCase() + "'";
	}

	@Override
	public String tableConstraintExistsQuery(String constraintName, String tableName, String database, String schema) {
		// do not need to use the schema
		return "SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_NAME = '"
				+ constraintName.toUpperCase() + "' AND TABLE_NAME = '" + tableName.toUpperCase() + "'";
	}

	@Override
	public String referentialConstraintExistsQuery(String constraintName, String database, String schema) {
		// do not need to use the schema
		return "SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_NAME = '"
				+ constraintName.toUpperCase() + "'";
	}

	@Override
	public String getAllColumnDetails(String tableName, String database, String schema) {
		// do not need to use the schema
		return "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '"
				+ tableName.toUpperCase() + "';";
	}

	@Override
	public String columnDetailsQuery(String tableName, String columnName, String database, String schema) {
		// do not need to use the schema
		return "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '"
				+ tableName.toUpperCase() + "' AND COLUMN_NAME='" + columnName.toUpperCase() + "';";
	}

	@Override
	public String getIndexList(String database, String schema) {
		// do not need to use the schema
		return "SELECT DISTINCT INDEX_NAME, TABLE_NAME FROM INFORMATION_SCHEMA.INDEXES;";
	}

	@Override
	public String getIndexDetails(String indexName, String tableName, String database, String schema) {
		// do not use the schema
		return "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.INDEX_COLUMNS WHERE INDEX_NAME='"
				+ indexName.toUpperCase() + "' AND TABLE_NAME='" + tableName.toUpperCase() + "';";
	}

	@Override
	public String allIndexForTableQuery(String tableName, String database, String schema) {
		// do not need to use the schema
		return "SELECT INDEX_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.INDEX_COLUMNS WHERE TABLE_NAME='"
				+ tableName.toUpperCase() + "';";
	}

	@Override
	public String alterTableDropColumns(String tableName, Collection<String> columnNames) {
		// should escape keywords
		if (isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}

		StringBuilder alterString = new StringBuilder("ALTER TABLE " + tableName + " DROP COLUMN (");
		int i = 0;
		for (String newColumn : columnNames) {
			if (i > 0) {
				alterString.append(", ");
			}

			// should escape keywords
			if (isSelectorKeyword(newColumn)) {
				newColumn = getEscapeKeyword(newColumn);
			}

			alterString.append(newColumn);

			i++;
		}
		alterString.append(");");
		return alterString.toString();
	}

	@Override
	public String hashColumn(String tableName, String[] columns) {
		StringBuilder builder = new StringBuilder();
		builder.append("UPDATE " + tableName + " SET ");
		builder.append(String.join(",", Stream.of(columns)
				.map(c -> c + " = RAWTOHEX(HASH('SHA256', STRINGTOUTF8(" + c + "), 1000))").collect(Collectors.toList())));
		return builder.toString();
	}
}
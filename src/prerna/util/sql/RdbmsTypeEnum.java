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

import java.util.Locale;

public enum RdbmsTypeEnum {

	// @formatter:off
	ASTER("ASTER_DB", "com.asterdata.ncluster.jdbc.core.NClusterJDBCDriver", "jdbc:ncluster"),
	ATHENA("ATHENA", "com.amazon.athena.jdbc.AthenaDriver", "jdbc:athena"),
	BIG_QUERY("BIG_QUERY", "com.simba.googlebigquery.jdbc42.Driver", "jdbc:bigquery"),
	CASSANDRA("CASSANDRA", "com.simba.cassandra.jdbc42.Driver", "jdbc:cassandra"),
	CLICKHOUSE("CLICKHOUSE", "com.clickhouse.jdbc.ClickHouseDriver", "jdbc:clickhouse"),
	DATABRICKS("DATABRICKS", "com.databricks.client.jdbc.Driver", "jdbc:databricks"),
	DB2("DB2", "com.ibm.db2.jcc.DB2Driver", "jdbc:db2"),
	DERBY("DERBY", "org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby"),
	ELASTIC_SEARCH("ELASTIC_SEARCH", "org.elasticsearch.xpack.sql.jdbc.EsDriver", "jdbc:es"),
	H2_DB("H2_DB", "org.h2.Driver", "jdbc:h2"), 
	HIVE("HIVE", "org.apache.hive.jdbc.HiveDriver", "jdbc:hive2"),
	IMPALA("IMPALA", "com.cloudera.impala.jdbc4.Driver", "jdbc:impala"),
	REDSHIFT("REDSHIFT", "com.amazon.redshift.jdbc.Driver", "jdbc:redshift"),
	MARIADB("MARIA_DB", "org.mariadb.jdbc.Driver", "jdbc:mariadb"),
	MYSQL("MYSQL", "com.mysql.cj.jdbc.Driver", "jdbc:mysql"),
	OPEN_SEARCH("OPEN_SEARCH", "org.opensearch.jdbc.Driver", "jdbc:opensearch"),
	ORACLE("ORACLE", "oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin"),
	PHOENIX("PHOENIX", "org.apache.phoenix.jdbc.PhoenixDriver", "jdbc:phoenix"),
	POSTGRES("POSTGRES", "org.postgresql.Driver", "jdbc:postgresql"),
	SAP_HANA("SAP_HANA", "com.sap.db.jdbc.Driver", "jdbc:sap"),
	SPARK("SPARK", "com.simba.spark.jdbc41.Driver", "jdbc:spark"), 
	SQLITE("SQLITE", "org.sqlite.JDBC", "jdbc:sqlite"),
	SNOWFLAKE("SNOWFLAKE", "net.snowflake.client.jdbc.SnowflakeDriver", "jdbc:snowflake"),
	SYNAPSE("SYNAPSE", "com.microsoft.sqlserver.jdbc.SQLServerDriver", "jdbc:sqlserver"),
	SQL_SERVER("SQL_SERVER", "com.microsoft.sqlserver.jdbc.SQLServerDriver", "jdbc:sqlserver"),
	TERADATA("TERADATA", "com.teradata.jdbc.TeraDriver", "jdbc:teradata"),
	TIBCO("TIBCO", "cs.jdbc.driver.CompositeDriver", "jdbc:compositesw:dbapi"),
	TRINO("TRINO", "io.trino.jdbc.TrinoDriver", "jdbc:trino"),
	// SEMOSS to your SEMOSS
	SEMOSS("SEMOSS", "prerna.jdbc.SMSSDriver", "jdbc:smss");
	// @formatter:on

	private String label;
	private String driver;
	private String urlPrefix;

	RdbmsTypeEnum(String label, String driver, String urlPrefix) {
		this.label = label;
		this.driver = driver;
		this.urlPrefix = urlPrefix;
	}

	public String getLabel() {
		return this.label;
	}

	public String getDriver() {
		return this.driver;
	}

	public String getUrlPrefix() {
		return this.urlPrefix;
	}

	/**
	 * Get the driver based on the string
	 * 
	 * @param type
	 * @return
	 */
	public static String getDriverFromString(String type) {
		type = type.toUpperCase();
		RdbmsTypeEnum foundType = null;
		try {
			foundType = RdbmsTypeEnum.valueOf(type);
		} catch (IllegalArgumentException e) {
			// ignore
		}
		// if we found a type
		// return the driver
		if (foundType != null) {
			return foundType.getDriver();
		}

		// loop through and see if passed in is a label
		for (RdbmsTypeEnum rdbmsType : RdbmsTypeEnum.values()) {
			if (type.equals(rdbmsType.label)) {
				return rdbmsType.getDriver();
			}
		}

		return null;
	}

	/**
	 * Get the enum based on the type
	 * 
	 * @param type
	 * @return
	 */
	public static RdbmsTypeEnum getEnumFromString(String type) {
		if (type == null) {
			return null;
		}

		type = type.trim().toUpperCase(Locale.ROOT).replaceAll("[\\s-]+", "_");
		RdbmsTypeEnum foundType = null;
		try {
			foundType = RdbmsTypeEnum.valueOf(type);
		} catch (IllegalArgumentException e) {
			// ignore
		}
		// if we found a type
		// return the driver
		if (foundType != null) {
			return foundType;
		}

		// loop through and see if passed in is a label
		for (RdbmsTypeEnum rdbmsType : RdbmsTypeEnum.values()) {
			if (type.equals(rdbmsType.label)) {
				return rdbmsType;
			}
		}

		String compactType = type.replace("_", "");
		if (compactType.equals("MSSQL") || compactType.equals("SQLSERVER")
				|| compactType.equals("MICROSOFTSQLSERVER")) {
			return RdbmsTypeEnum.SQL_SERVER;
		}

		return null;
	}

	/**
	 * Get the enum from a JDBC connection URL. Longest prefix wins.
	 *
	 * @param url
	 * @return
	 */
	public static RdbmsTypeEnum getEnumFromUrl(String url) {
		if (url == null) {
			return null;
		}

		String normalizedUrl = url.trim().toLowerCase(Locale.ROOT);
		RdbmsTypeEnum foundType = null;
		int longestPrefix = -1;
		for (RdbmsTypeEnum rdbmsType : RdbmsTypeEnum.values()) {
			String prefix = rdbmsType.urlPrefix.toLowerCase(Locale.ROOT);
			if (normalizedUrl.startsWith(prefix) && (prefix.length() > longestPrefix
					|| (prefix.length() == longestPrefix && rdbmsType == RdbmsTypeEnum.SQL_SERVER))) {
				foundType = rdbmsType;
				longestPrefix = prefix.length();
			}
		}

		return foundType;
	}

	/**
	 * Get the enum from the driver
	 * 
	 * @param driver
	 * @return
	 */
	public static RdbmsTypeEnum getEnumFromDriver(String driver) {
		if (driver == null) {
			return null;
		}
		if (driver.equalsIgnoreCase(RdbmsTypeEnum.SQL_SERVER.driver)) {
			return RdbmsTypeEnum.SQL_SERVER;
		}

		for (RdbmsTypeEnum rdbmsType : RdbmsTypeEnum.values()) {
			if (driver.equalsIgnoreCase(rdbmsType.driver)) {
				return rdbmsType;
			}
		}

		return null;
	}

}

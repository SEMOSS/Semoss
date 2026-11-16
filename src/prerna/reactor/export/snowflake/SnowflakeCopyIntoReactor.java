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
package prerna.reactor.export.snowflake;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;

public class SnowflakeCopyIntoReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SnowflakeCopyIntoReactor.class);

	public SnowflakeCopyIntoReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.TABLE.getKey(), "userStage",
				"tableStage", "namedStage", "files", "pattern", "formatName", "fileType", "fileCompression",
				"parseHeader", "dateFormat", "timeFormat", "timestampFormat", "fieldOptionallyEnclosedBy",
				"matchByColumnName", "force" };
		this.keyRequired = new int[] { 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), databaseId)) {
			throw new IllegalArgumentException(
					"Database " + databaseId + " does not exist or user does not have access to database");
		}

		String tableName = this.keyValue.get(this.keysToGet[1]);
		if (tableName == null || (tableName = tableName.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must pass in the table we are inserting into");
		}
		String userStage = this.keyValue.get(this.keysToGet[2]);
		String tableStage = this.keyValue.get(this.keysToGet[3]);
		String namedStage = this.keyValue.get(this.keysToGet[4]);

		String stageQualifier = "";
		String stageDestination = "";
		if (userStage != null && !(userStage = userStage.trim()).isEmpty()) {
			stageQualifier = "@~";
			stageDestination = userStage;
		} else if (tableStage != null && !(tableStage = tableStage.trim()).isEmpty()) {
			stageQualifier = "@%";
			stageDestination = tableStage;
		} else if (namedStage != null && !(namedStage = namedStage.trim()).isEmpty()) {
			stageQualifier = "@";
			stageDestination = namedStage;
		} else {
			throw new IllegalArgumentException(
					"Must pass in userStage, tableStage, or namedStage. All values were null or empty");
		}

		String fromFiles = getFiles();
		String fromPattern = this.keyValue.get("pattern");

		String sql = "COPY INTO " + tableName + " FROM " + stageQualifier + stageDestination;
		if (fromFiles != null && !fromFiles.isEmpty()) {
			sql += " FILES = " + fromFiles;
		} else if (fromPattern != null && !(fromPattern = fromPattern.trim()).isEmpty()) {
			sql += " PATTERN = '" + fromPattern + "'";
		} else {
			throw new IllegalArgumentException(
					"Must pass in files or pattern for the FROM statement in the COPY FILES command");
		}

		{
			String fileFormat = "FILE_FORMAT = (";
			String formatName = this.keyValue.get("formatName");
			if (formatName != null && !(formatName = formatName.trim()).isEmpty()) {
				fileFormat += " FORMAT_NAME = '" + formatName + "'";
			}
			String fileType = this.keyValue.get("fileType");
			if (fileType != null && !(fileType = fileType.trim()).isEmpty()) {
				fileFormat += " TYPE = " + fileType.toUpperCase();
			}
			String fileCompression = this.keyValue.get("fileCompression");
			if (fileCompression != null && !(fileCompression = fileCompression.trim()).isEmpty()) {
				fileFormat += " COMPRESSION = " + fileCompression.toUpperCase();
			}
			Boolean parseHeader = Boolean.parseBoolean(this.keyValue.getOrDefault("parseHeader", "true") + "");
			if (parseHeader != null) {
				fileFormat += " PARSE_HEADER = " + parseHeader.toString().toUpperCase();
			}
			String dateFormat = this.keyValue.get("dateFormat");
			if (dateFormat != null && !(dateFormat = dateFormat.trim()).isEmpty()) {
				fileFormat += " DATE_FORMAT = '" + dateFormat + "'";
			}
			String timeFormat = this.keyValue.get("timeFormat");
			if (timeFormat != null && !(timeFormat = timeFormat.trim()).isEmpty()) {
				fileFormat += " TIME_FORMAT = '" + timeFormat + "'";
			}
			String timestampFormat = this.keyValue.get("timestampFormat");
			if (timestampFormat != null && !(timestampFormat = timestampFormat.trim()).isEmpty()) {
				fileFormat += " TIMESTAMP_FORMAT = '" + timestampFormat + "'";
			}
			String fieldOptionallyEnclosedBy = this.keyValue.getOrDefault("fieldOptionallyEnclosedBy", "NONE");
			if (fieldOptionallyEnclosedBy != null
					&& !(fieldOptionallyEnclosedBy = fieldOptionallyEnclosedBy.trim()).isEmpty()) {
				if (fieldOptionallyEnclosedBy.equals("'")) {
					fileFormat += " FIELD_OPTIONALLY_ENCLOSED_BY = ''''";
				} else {
					fileFormat += " FIELD_OPTIONALLY_ENCLOSED_BY = '" + fieldOptionallyEnclosedBy + "'";
				}
			}
			sql += " " + fileFormat + ")";
		}
		String matchByColumnName = this.keyValue.getOrDefault("matchByColumnName", "CASE_SENSITIVE");
		if (matchByColumnName != null && !(matchByColumnName = matchByColumnName.trim()).isEmpty()) {
			sql += " MATCH_BY_COLUMN_NAME = " + matchByColumnName.toUpperCase();
		}
		Boolean force = Boolean.parseBoolean(this.keyValue.get("force") + "");
		if (force != null) {
			sql += " FORCE = " + force.toString().toUpperCase();
		}

		IDatabaseEngine snowflake = Utility.getDatabase(databaseId);
		if (!(snowflake instanceof IRDBMSEngine)) {
			throw new IllegalArgumentException("Database is not a snowlfake db");
		}
		IRDBMSEngine snowflakeRdbms = (IRDBMSEngine) snowflake;
		if (snowflakeRdbms.getDbType() != RdbmsTypeEnum.SNOWFLAKE) {
			throw new IllegalArgumentException("Database is not a snowlfake db");
		}

		Statement stmt = null;
		try {
			stmt = snowflakeRdbms.getConnection().createStatement();
			stmt.execute(sql);
			int updatedRows = stmt.getUpdateCount();
			return new NounMetadata("Updated rows = " + updatedRows, PixelDataType.CONST_STRING);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("A SQL exception was thrown. Detailed error = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(snowflakeRdbms, stmt);
		}
	}

	/**
	 * 
	 * @return
	 */
	private String getFiles() {
		List<String> files = new ArrayList<String>();

		GenRowStruct colGrs = this.store.getGenRowStruct("files");
		if (colGrs != null && !colGrs.isEmpty()) {
			for (int selectIndex = 0; selectIndex < colGrs.size(); selectIndex++) {
				String column = colGrs.get(selectIndex) + "";
				// wrap in quotes as well
				files.add("'" + column + "'");
			}
		}

		if (files.isEmpty()) {
			return null;
		}

		StringBuilder builder = new StringBuilder("(");
		builder.append(String.join(",", files));
		builder.append(")");
		return builder.toString();
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"Utility method to execute the COPY INTO command from a file in a stage. Snowflake docs found here: https://docs.snowflake.com/en/sql-reference/sql/copy-into-table")
				.key(ReactorKeysEnum.DATABASE, "The database id for the snowflake db")
				.key(ReactorKeysEnum.TABLE, "This is a required value containing the table we are inserting into")
				.key("userStage", "The path prefix for the user stage. Do not include the '@~' qualifier")
				.key("tableStage", "The table name and path prefix. Do not enter the '@%' qualifier")
				.key("namedStage",
						"The named stage and path prefix. This will not create a stage if it does not exist. Do not enter the '@' qualifier ")
				.key("files", "Specifies a list of one or more comma-separated file names to copy")
				.key("pattern",
						"Specifies a regular expression pattern for filtering the list of files to copy. This command applies the regular expression to the entire stage location")
				.key("formatName",
						"Specifies an existing named file format to use for loading data into the table. Do not use with fileType. Please reference https://docs.snowflake.com/en/sql-reference/sql/create-file-format on creating your own file format")
				.key("fileType",
						"Specifies the type of files to load into the table. Do not use with formatName. Supported values include: CSV | JSON | AVRO | ORC | PARQUET | XML. Additional options depend on the type of file selected.")
				.key("fileCompression",
						"Specifies the current compression algorithm for the data files to be loaded. Supported values include: AUTO | GZIP | BZ2 | BROTLI | ZSTD | DEFLATE | RAW_DEFLATE | NONE. Default is AUTO")
				.key("parseHeader",
						"Boolean that specifies whether to use the first row headers in the data files to determine column names. Default value is true.")
				.key("dateFormat", "String that defines the format of date values in the data files to be loaded")
				.key("timeFormat", "String that defines the format of time values in the data files to be loaded")
				.key("timestampFormat",
						"String that defines the format of timestamp values in the data files to be loaded")
				.key("fieldOptionallyEnclosedBy",
						"Character used to enclose strings. Supported values include: NONE, single quote character ('), or double quote character (\"). Default is NONE.")
				.key("matchByColumnName",
						"String that specifies whether to load semi-structured data into columns in the target table that match corresponding columns represented in the data. Supported values are: CASE_SENSITIVE | CASE_INSENSITIVE | NONE. Default is CASE_SENSITIVE")
				.key("force",
						"Boolean that specifies to load all files, regardless of whether they’ve been loaded previously and have not changed since they were loaded. Default is false");
	}

}

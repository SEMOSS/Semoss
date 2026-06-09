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

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.rdf.engine.wrappers.RawRDBMSSelectWrapper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;

public class SnowflakePutReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SnowflakePutReactor.class);

	public SnowflakePutReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.FILE_PATH.getKey(),
				ReactorKeysEnum.SPACE.getKey(), "userStage", "tableStage", "namedStage", "autoCompress",
				"sourceCompression", "parallel", "overwrite" };
		this.keyRequired = new int[] { 1, 1, 0, 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), databaseId)) {
			throw new IllegalArgumentException(
					"Database " + databaseId + " does not exist or user does not have access to database");
		}

		// specify the folder from the base
		String fileRelativePath = Utility.normalizePath(keyValue.get(keysToGet[1]));
		String space = this.keyValue.get(this.keysToGet[2]);
		// if security enables, you need proper permissions
		// this takes in the insight and does a user check that the user has access to
		// perform the operations
		String baseFolder = AssetUtility.getRootFolderPath(this.insight, space, true);
		String filePath = (baseFolder + "/" + fileRelativePath).replace('\\', '/');
		File putFile = new File(filePath);
		if (putFile.exists() && !putFile.isFile()) {
			throw new IllegalArgumentException("Cannot find file '" + fileRelativePath + "'");
		}

		String userStage = this.keyValue.get(this.keysToGet[3]);
		String tableStage = this.keyValue.get(this.keysToGet[4]);
		String namedStage = this.keyValue.get(this.keysToGet[5]);
		Boolean compress = Boolean.parseBoolean(this.keyValue.getOrDefault(this.keysToGet[6], "true") + "");
		String sourceCompression = this.keyValue.get(this.keysToGet[7]);
		String parallelStr = this.keyValue.get(this.keysToGet[8]);
		Boolean overwrite = Boolean.parseBoolean(this.keyValue.getOrDefault(this.keysToGet[9], "false") + "");

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

		String fileUri = "file://" + filePath;
		Integer parallel = null;
		// Example statement shape for readability:
		// PUT 'file:///tmp/orders.csv' @~uploads AUTO_COMPRESS = TRUE OVERWRITE = FALSE
		// SOURCE_COMPRESSION = GZIP PARALLEL = 4
		String sql = "PUT ? " + stageQualifier + stageDestination + " AUTO_COMPRESS = ? OVERWRITE = ?";
		if (sourceCompression != null && !(sourceCompression = sourceCompression.trim()).isEmpty()) {
			sourceCompression = sourceCompression.toUpperCase();
			boolean validCompression = sourceCompression.equals("AUTO_DETECT") || sourceCompression.equals("GZIP")
					|| sourceCompression.equals("BZ2") || sourceCompression.equals("BROTLI")
					|| sourceCompression.equals("ZSTD") || sourceCompression.equals("DEFLATE")
					|| sourceCompression.equals("RAW_DEFLATE") || sourceCompression.equals("NONE");
			if (!validCompression) {
				throw new IllegalArgumentException(
						"sourceCompression must be one of: AUTO_DETECT, GZIP, BZ2, BROTLI, ZSTD, DEFLATE, RAW_DEFLATE, NONE. Current value = '"
								+ sourceCompression + "'");
			}
			sql += " SOURCE_COMPRESSION = " + sourceCompression;
		}
		if (parallelStr != null && !(parallelStr = parallelStr.trim()).isEmpty()) {
			// make sure its an integer
			try {
				parallel = Integer.parseInt(parallelStr);
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException(
						"parallel input must be a valid integer. Current value = '" + parallelStr + "'");
			}

			sql += " PARALLEL = ?";
		}

		IDatabaseEngine snowflake = Utility.getDatabase(databaseId);
		if (!(snowflake instanceof IRDBMSEngine)) {
			throw new IllegalArgumentException("Database is not a snowlfake db");
		}
		IRDBMSEngine snowflakeRdbms = (IRDBMSEngine) snowflake;
		if (snowflakeRdbms.getDbType() != RdbmsTypeEnum.SNOWFLAKE) {
			throw new IllegalArgumentException("Database is not a snowlfake db");
		}
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = snowflakeRdbms.getConnection().prepareStatement(sql);
			stmt.setString(1, fileUri);
			stmt.setBoolean(2, compress);
			stmt.setBoolean(3, overwrite);
			if (parallel != null) {
				stmt.setInt(4, parallel);
			}
			stmt.execute();
			rs = stmt.getResultSet();

			RawRDBMSSelectWrapper iterator = RawRDBMSSelectWrapper.flushRsToWrapper(rs);
			List<Map<String, Object>> results = QueryExecutionUtility.flushWrapperToMap(iterator);
			return new NounMetadata(results, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("A SQL exception was thrown. Detailed error = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(snowflakeRdbms, stmt, rs);
		}
	}

	@Override
	public String getReactorDescription() {
		return "Utility method to PUT a file into the appropirate stage. Snowflake docs found here: https://docs.snowflake.com/en/sql-reference/sql/put";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.DATABASE.getKey())) {
			return "The database id for the snowflake db";
		} else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "This is a required value containing the relative file path of a single file to be uploaded to snowflake";
		} else if (key.equals(ReactorKeysEnum.SPACE.getKey())) {
			return "This is an optional field to determine the space in which the relative file path exists (user project space, current insight space, project id space).";
		} else if (key.equals("userStage")) {
			return "The path prefix for the user stage. Do not include the '@~' qualifier";
		} else if (key.equals("tableStage")) {
			return "The table name and path prefix. Do not enter the '@%' qualifier";
		} else if (key.equals("namedStage")) {
			return "The named stage and path prefix. This will not create a stage if it does not exist. Do not enter the '@' qualifier ";
		} else if (key.equals("autoCompress")) {
			return "Boolean to determine if snowflake should compress data during upload. Default is true";
		} else if (key.equals("sourceCompression")) {
			return "Specifies the method of compression used on already-compressed files that are being stagged. Supported values include: AUTO_DETECT | GZIP | BZ2 | BROTLI | ZSTD | DEFLATE | RAW_DEFLATE | NONE. "
					+ "Default value is AUTO_DETECT";
		} else if (key.equals("parallel")) {
			return "Specifies the number of threads to use for uploading files. Default is 4";
		} else if (key.equals("overwrite")) {
			return "Specifies whether Snowflake overwrites an existing file with the same name during upload. Default is false. "
					+ "Note that the snowflake API does not throw an error with ths is set to false and a file already exists with the name in the stage";
		}

		return super.getDescriptionForKey(key);
	}

}

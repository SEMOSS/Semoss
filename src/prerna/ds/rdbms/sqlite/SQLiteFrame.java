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
package prerna.ds.rdbms.sqlite;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.crypto.Cipher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.cache.CachePropFileFrameObject;
import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.ds.rdbms.RdbmsFrameBuilder;
import prerna.om.ThreadStore;
import prerna.util.Utility;
import prerna.util.insight.InsightUtility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

public class SQLiteFrame extends AbstractRdbmsFrame {

	private static final Logger classLogger = LogManager.getLogger(SQLiteFrame.class);

	private String fileLocation;
	private String fileNameToUse;

	public SQLiteFrame() {
		super();
	}

	public SQLiteFrame(String tableName) {
		super(tableName);
	}

	public SQLiteFrame(String[] headers) {
		super(headers);
	}

	public SQLiteFrame(String[] headers, String[] types) {
		super(headers, types);
	}

	@Override
	protected void initConnAndBuilder() throws Exception {
		this.util = SqlQueryUtilFactory.initialize(RdbmsTypeEnum.SQLITE);

		String sessionId = ThreadStore.getSessionId();
		String insightId = ThreadStore.getInsightId();

		String folderToUsePath = null;
		if (sessionId != null && insightId != null) {
			sessionId = InsightUtility.getFolderDirSessionId(sessionId);
			folderToUsePath = Utility.getInsightCacheDir() + DIR_SEPARATOR + sessionId + DIR_SEPARATOR + insightId;
			this.fileNameToUse = "SQLite_Store_" + UUID.randomUUID().toString().toUpperCase().replaceAll("-", "_")
					+ ".sqlite";
		} else {
			folderToUsePath = Utility.getInsightCacheDir() + DIR_SEPARATOR + "SQLite_Store_"
					+ UUID.randomUUID().toString().toUpperCase().replaceAll("-", "_");
			this.fileNameToUse = "database.sqlite";
		}

		// create the location of the file if it doesn't exist
		File folderToUse = new File(folderToUsePath);
		if (!folderToUse.exists()) {
			folderToUse.mkdirs();
		}

		this.fileLocation = folderToUsePath + DIR_SEPARATOR + this.fileNameToUse;
		File fileToUse = new File(this.fileLocation);
		if (!fileToUse.exists()) {
			fileToUse.createNewFile();
		}

		// build the connection url
		// build the connection url
		Map<String, Object> connDetails = new HashMap<>();
		connDetails.put(AbstractSqlQueryUtil.HOSTNAME, fileLocation);
		String connectionUrl = this.util.setConnectionDetailsfromMap(connDetails);
		// get the connection
		this.conn = AbstractSqlQueryUtil.makeConnection(RdbmsTypeEnum.SQLITE, connectionUrl, "", "");

		// set the builder
		this.builder = new RdbmsFrameBuilder(this.conn, this.database, this.schema, this.util);
		this.util.enhanceConnection(this.conn);

		this.builder.runQuery("PRAGMA synchronous = OFF");
		this.builder.runQuery("PRAGMA journal_mode = MEMORY");
	}

	@Override
	public void close() {
		super.close();
		File f = new File(Utility.normalizePath(this.fileLocation));
		if (f.exists()) {
			f.delete();
		}
	}

	@Override
	public CachePropFileFrameObject save(String folderDir, Cipher cipher) throws IOException {
		CachePropFileFrameObject cf = new CachePropFileFrameObject();

		String frameName = this.getName();
		cf.setFrameName(frameName);

		// save frame
		String frameFileName = Utility.normalizePath(folderDir + DIR_SEPARATOR + frameName + ".sqlite");

		String saveScript = "backup to '" + frameFileName + "'";
		Statement stmt = null;
		try {
			stmt = this.conn.createStatement();
			stmt.executeUpdate(saveScript);
		} catch (SQLException e) {
			classLogger.error("Failed to backup SQLite frame {} to {}", frameName, frameFileName, e);
			throw new IOException("Error occurred attempting to cache SQL Frame");
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					classLogger.error("Failed to close the statement after backing up SQLite frame {}", frameName, e);
				}
			}
		}

		if (!new File(frameFileName).exists()) {
			throw new IllegalArgumentException("Unable to save the SQLite frame");
		}
		if (new File(frameFileName).length() == 0) {
			throw new IllegalArgumentException("Attempting to save an empty SQLite frame");
		}

		cf.setFrameCacheLocation(frameFileName);
		// also save the meta details
		this.saveMeta(cf, folderDir, frameName, cipher);
		return cf;
	}

	@Override
	public void open(CachePropFileFrameObject cf, Cipher cipher) throws IOException {
		// set the frame name to that of the cached frame name
		this.frameName = cf.getFrameName();

		// load the frame
		String filePath = Utility.normalizePath(cf.getFrameCacheLocation());

		// drop the aggregate if it exists since the opening of the script will
		// fail otherwise
		Statement stmt = null;
		try {
			stmt = this.conn.createStatement();
			stmt.executeUpdate("restore from '" + filePath + "'");
		} catch (SQLException e1) {
			classLogger.error("Failed to restore cached SQLite frame {} from {}", this.frameName, filePath, e1);
			throw new IOException("Error occurred opening cached SQL Frame");
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					classLogger.error("Failed to close the statement after restoring cached SQLite frame {}",
							this.frameName, e);
				}
			}
		}

		// open the meta details
		this.openCacheMeta(cf, cipher);
	}
}

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
package prerna.ds.rdbms.h2;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.zip.GZIPInputStream;

import javax.crypto.Cipher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.RunScript;

import prerna.cache.CachePropFileFrameObject;
import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.ds.rdbms.RdbmsFrameBuilder;
import prerna.om.ThreadStore;
import prerna.util.Utility;
import prerna.util.insight.InsightUtility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

public class H2Frame extends AbstractRdbmsFrame {

	private static final Logger classLogger = LogManager.getLogger(H2Frame.class);

	private String fileLocation;
	private String fileNameToUse;

	public H2Frame() {
		super();
	}

	public H2Frame(String tableName) {
		super(tableName);
	}

	public H2Frame(String[] headers) {
		super(headers);
	}

	public H2Frame(String[] headers, String[] types) {
		super(headers, types);
	}

	@Override
	protected void initConnAndBuilder() throws Exception {
		this.util = SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB);

		String sessionId = ThreadStore.getSessionId();
		String insightId = ThreadStore.getInsightId();

		String folderToUsePath = null;
		if (sessionId != null && insightId != null) {
			sessionId = InsightUtility.getFolderDirSessionId(sessionId);
			folderToUsePath = Utility.getInsightCacheDir() + DIR_SEPARATOR + sessionId + DIR_SEPARATOR + insightId;
			this.fileNameToUse = "H2_Store_" + UUID.randomUUID().toString().toUpperCase().replace("-", "_") + ".mv.db";
		} else {
			folderToUsePath = Utility.getInsightCacheDir() + DIR_SEPARATOR + "H2_Store_"
					+ UUID.randomUUID().toString().toUpperCase().replace("-", "_");
			this.fileNameToUse = "database.mv.db";
		}

		// create the location of the file if it doesn't exist
		File folderToUse = new File(folderToUsePath);
		if (!folderToUse.exists()) {
			folderToUse.mkdirs();
		}

		this.fileLocation = folderToUsePath + DIR_SEPARATOR + this.fileNameToUse;
		// make the actual file so the connection helper knows its not a tcp protocol
		File fileToUse = new File(this.fileLocation);
		if (!fileToUse.exists()) {
			fileToUse.createNewFile();
		}

		// build the connection url
		Map<String, Object> connDetails = new HashMap<>();
		connDetails.put(AbstractSqlQueryUtil.HOSTNAME, fileLocation);
		connDetails.put(AbstractSqlQueryUtil.ADDITIONAL, "CACHE_SIZE=65536;LOCK_MODE=1");
		String connectionUrl = this.util.setConnectionDetailsfromMap(connDetails);
		// get the connection
		this.conn = AbstractSqlQueryUtil.makeConnection(RdbmsTypeEnum.H2_DB, connectionUrl, "sa", "");
		// set the builder
		this.builder = new RdbmsFrameBuilder(this.conn, this.database, this.schema, this.util);
		this.util.enhanceConnection(this.conn);
	}

	@Override
	public void close() {
		super.close();
		File f = new File(Utility.normalizePath(this.fileLocation));
		DeleteDbFiles.execute(f.getParent().replace('\\', '/'), this.fileNameToUse.replace(".mv.db", ""), false);
		if (f.exists()) {
			f.delete();
		}
		// also delete the parent folder
		File pF = f.getParentFile();
		// this condition should always be the case
		if (pF.listFiles() != null && pF.listFiles().length == 0) {
			f.getParentFile().delete();
		}
	}

	@Override
	public CachePropFileFrameObject save(String folderDir, Cipher cipher) throws IOException {
		CachePropFileFrameObject cf = new CachePropFileFrameObject();

		String frameName = this.getName();
		cf.setFrameName(frameName);

		// save frame
		String frameFileName = folderDir + DIR_SEPARATOR + frameName + ".gz";

		String saveScript = "SCRIPT TO '" + frameFileName + "' COMPRESSION GZIP TABLE " + frameName;
		try (Statement stmt = this.conn.createStatement()) {
			// removing our custom aggregates so the file could be used/loaded elsewhere
			stmt.execute("DROP AGGREGATE IF EXISTS SMSS_MEDIAN");
			stmt.execute(saveScript);
		} catch (Exception e) {
			throw new IOException("Error occurred attempting to cache SQL Frame", e);
		}

		if (!new File(frameFileName).exists()) {
			throw new IllegalArgumentException("Unable to save the H2 frame");
		}
		if (new File(frameFileName).length() == 0) {
			throw new IllegalArgumentException("Attempting to save an empty H2 frame");
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

		try (FileInputStream fis = new FileInputStream(filePath);
				GZIPInputStream gis = new GZIPInputStream(fis);
				Reader r = new InputStreamReader(gis)) {
			// load the frame
			RunScript.execute(this.conn, r);
		} catch (SQLException e) {
			classLogger.error("Error opening the cached H2 frame", e);
			throw new IOException("Error occurred opening cached SQL Frame");
		} finally {
			// open the meta details
			this.openCacheMeta(cf, cipher);
		}
	}

	/*
	 * Legacy calls
	 */

	/**
	 * Execute and get back a result set Responsibility of the method to grab the
	 * statement and close it from the result set
	 * 
	 * @param query
	 * @return
	 */
	public ResultSet execQuery(String query) {
		try (PreparedStatement stmt = this.conn.prepareStatement(query)) {
			return stmt.executeQuery();
		} catch (SQLException e) {
			classLogger.error("Error executing the query {}", query, e);
		}

		return null;
	}

	public void deleteAllRows() {
		String query = "DELETE FROM " + this.frameName + " WHERE 1 != 0";
		try {
			this.builder.runQuery(query);
		} catch (Exception e) {
			classLogger.error("Error deleting all rows from the H2 frame", e);
		}
	}

}

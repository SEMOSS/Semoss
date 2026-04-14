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
package prerna.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IRDBMSEngine;

public class LocalMasterConceptIdHash {

	private static final Logger classLogger = LogManager.getLogger(LocalMasterConceptIdHash.class);

	// simple hash table that saves and gets values from the database
	private static final String TABLE_NAME = "KVSTORE";

	private static volatile LocalMasterConceptIdHash instance = null;

	private Map<String, String> dataHash = Collections.synchronizedMap(new HashMap<String, String>());
	private boolean dirty = false;

	private LocalMasterConceptIdHash() {

	}

	public static LocalMasterConceptIdHash getInstance() {
		if (instance != null) {
			return instance;
		}

		synchronized (LocalMasterConceptIdHash.class) {
			if (instance != null) {
				return instance;
			}

			instance = new LocalMasterConceptIdHash();
			instance.load();
		}

		return instance;
	}

	private void load() {
		IRDBMSEngine engine = SystemEngineRegistry.getLocalMasterDb();

		// this is only for local master!!!
		Connection conn = null;
		try {
			conn = engine.getConnection();
			try (Statement stmt = conn.createStatement()) {
				ResultSet rs = stmt.executeQuery("SELECT K, V from " + TABLE_NAME);
				while (rs.next()) {
					this.dataHash.put(rs.getString(1), rs.getString(2));
				}
			}
		} catch (SQLException e) {
			classLogger.error("Failed to load local master concept id hash from table '{}' in local master database.",
					TABLE_NAME, e);
		} finally {
			try {
				if (engine.isConnectionPooling() && conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				classLogger.error("Failed to close pooled local master database connection after loading table '{}'.",
						TABLE_NAME, e);
			}
		}
	}

	public void put(String key, String value) {
		this.dataHash.put(key, value);
		this.dirty = true;
	}

	public boolean containsKey(String key) {
		return this.dataHash.containsKey(key);
	}

	public String get(String key) {
		return this.dataHash.get(key);
	}

	public void clear() {
		this.dataHash.clear();
	}

	public void persistBack() {
		IRDBMSEngine engine = SystemEngineRegistry.getLocalMasterDb();

		if (this.dirty) {
			Connection conn = null;
			Statement stmt = null;
			PreparedStatement ps = null;
			try {
				conn = engine.getConnection();
				stmt = conn.createStatement();
				stmt.execute("DELETE FROM " + TABLE_NAME);
				Iterator<String> keys = dataHash.keySet().iterator();
				ps = conn.prepareStatement("INSERT KVSTORE(K, V) VALUES(?, ?)");
				while (keys.hasNext()) {
					String key = keys.next();
					String value = dataHash.get(key);
					int parameterIndex = 1;
					ps.setString(parameterIndex++, key);
					ps.setString(parameterIndex++, value);
					ps.addBatch();
				}
				ps.executeBatch();
				if (!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
				this.dirty = false;
			} catch (Exception e) {
				classLogger.error(
						"Failed to persist local master concept id hash back to table '{}'. attemptedEntries={}.",
						TABLE_NAME, this.dataHash.size(), e);
			} finally {
				ConnectionUtils.closeStatement(stmt);
				ConnectionUtils.closeAllConnectionsIfPooling(engine, ps);
			}
		}
	}

}

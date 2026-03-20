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
package prerna.masterdatabase;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IRDBMSEngine;
import prerna.util.ConnectionUtils;
import prerna.util.LocalMasterConceptIdHash;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

public class DeleteFromMasterDB {

	private static final Logger classLogger = LogManager.getLogger(DeleteFromMasterDB.class);

	/**
	 * 
	 * @param engineId
	 * @return
	 */
	public boolean deleteEngineRDBMS(String engineId) {
		classLogger.info("Removing engine {} from Local Master", Utility.cleanLogString(engineId));
		IRDBMSEngine localMasterEngine = SystemEngineRegistry.getLocalMasterDb();
		Connection conn = null;
		try {
			conn = localMasterEngine.getConnection();
			String metaDeleteSql = "DELETE FROM conceptmetadata WHERE physicalnameid in (SELECT physicalnameid FROM engineconcept WHERE engine = ?)";
			String relationDeleteSql = "DELETE FROM enginerelation WHERE engine = ?";
			String conceptDeleteSql = "DELETE FROM engineconcept WHERE engine = ?";
			String engineDeleteSql = "DELETE FROM engine WHERE id = ?";
			String metamodelPositionDeleteSql = "DELETE FROM METAMODELPOSITION WHERE ENGINEID = ?";
			for (String sql : new String[] { metaDeleteSql, relationDeleteSql, conceptDeleteSql, engineDeleteSql,
					metamodelPositionDeleteSql }) {
				try (PreparedStatement statement = conn.prepareStatement(sql)) {
					statement.setString(1, engineId);
					int rowsDeleted = statement.executeUpdate();
					classLogger.info("Deleted {} rows for engine {} and query {}", rowsDeleted,
							Utility.cleanLogString(engineId), sql);
				} catch (SQLException e) {
					classLogger.error("Error running delete for engine {} and query {}",
							Utility.cleanLogString(engineId), sql, e);
				}
			}
			String kvDeleteSql = "DELETE FROM kvstore WHERE k like ?";
			try (PreparedStatement statement = conn.prepareStatement(kvDeleteSql)) {
				statement.setString(1, "'%" + engineId + "%PHYSICAL'");
				int rowsDeleted = statement.executeUpdate();
				classLogger.info("Deleted {} rows for engine {} and query {}", rowsDeleted,
						Utility.cleanLogString(engineId), kvDeleteSql);
			} catch (SQLException e) {
				classLogger.error("Error running delete for engine {} and query {}", Utility.cleanLogString(engineId),
						kvDeleteSql, e);
			}

			// this is so if we load db again
			// the values are refreshed
			LocalMasterConceptIdHash.getInstance().clear();
		} catch (Exception ex) {
			classLogger.info("Error removing engine {} from Local Master", Utility.cleanLogString(engineId));
			return false;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(localMasterEngine, conn);
		}
		return true;
	}

}

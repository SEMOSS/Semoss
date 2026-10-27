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
package prerna.usertracking;

import java.sql.Connection;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IRDBMSEngine;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;

public class EngineUsageUtils extends UserTrackingUtils {

	private static Logger classLogger = LogManager.getLogger(EngineUsageUtils.class);

	private static String EU_TN = "ENGINE_USES";

	/**
	 * Records usage of each queried database within the given insight. Every row is
	 * inserted in one write on the shared executor, so the usage is recorded for
	 * all of the databases or none. A failure is logged and not thrown.
	 *
	 * @param queriedDatabaseIds the ids of the databases/engines that were queried
	 * @param insightId          the insight the queries ran in
	 * @param projectId          the project owning the insight
	 */
	public static void add(Set<String> queriedDatabaseIds, String insightId, String projectId) {
		IRDBMSEngine userTrackingDb = SystemEngineRegistry.getUserTrackingDb();
		LocalDate date = LocalDate.now(ZoneId.of("UTC"));
		try {
			QueryExecutionUtility.write(userTrackingDb, connection -> {
				for (String databaseId : queriedDatabaseIds) {
					add(connection, databaseId, insightId, projectId, date);
				}
				return null;
			});
		} catch (Exception e) {
			classLogger.error("Failed to insert engine usage rows for engines {} (insight {}, project {})",
					queriedDatabaseIds, insightId, projectId, e);
		}
	}

	/**
	 * Reconciles recorded database usage for an insight: adds newly used databases,
	 * refreshes still-used ones, and removes those no longer used. The recorded
	 * databases are read and every change is written in one write on the shared
	 * executor, so the insight's usage rows change together or not at all. A
	 * failure is logged and not thrown.
	 *
	 * @param queriedDatabaseIds the ids of the databases/engines currently in use
	 * @param insightId          the insight the queries ran in
	 * @param projectId          the project owning the insight
	 */
	public static void update(Set<String> queriedDatabaseIds, String insightId, String projectId) {
		IRDBMSEngine userTrackingDb = SystemEngineRegistry.getUserTrackingDb();
		LocalDate date = LocalDate.now(ZoneId.of("UTC"));
		try {
			QueryExecutionUtility.write(userTrackingDb, connection -> {
				List<String> existingDbs = get(connection, insightId, projectId);

				for (String db : queriedDatabaseIds) {
					if (existingDbs.remove(db)) {
						// update existing
						update(connection, db, insightId, projectId, date);
					} else {
						// add new
						add(connection, db, insightId, projectId, date);
					}
				}

				for (String db : existingDbs) {
					// remove no longer used
					remove(connection, db, insightId, projectId);
				}
				return null;
			});
		} catch (Exception e) {
			classLogger.error("Failed to reconcile engine usage rows for insight {} (project {})", insightId, projectId,
					e);
		}
	}

	/**
	 * Returns the database ids currently recorded as used by the given insight,
	 * read inside the caller's transaction.
	 *
	 * @param connection connection whose transaction the caller owns
	 * @param insightId  the insight to look up
	 * @param projectId  the project owning the insight
	 * @return the list of database ids recorded for the insight
	 * @throws Exception if the read fails
	 */
	private static List<String> get(Connection connection, String insightId, String projectId) throws Exception {
		String query = "SELECT ENGINEID FROM " + EU_TN + " WHERE INSIGHTID = ? AND PROJECTID = ?";
		return QueryExecutionUtility.queryList(connection, query, ps -> {
			int index = 1;
			ps.setString(index++, insightId);
			ps.setString(index++, projectId);
		}, rs -> rs.getString(1));
	}

	/**
	 * Inserts a usage row for a database within an insight on the given date,
	 * inside the caller's transaction.
	 *
	 * @param connection connection whose transaction the caller owns
	 * @param engineId   the id of the database/engine used
	 * @param insightId  the insight the query ran in
	 * @param projectId  the project owning the insight
	 * @param date       the date to record the usage under
	 * @throws Exception if the insert fails
	 */
	private static void add(Connection connection, String engineId, String insightId, String projectId, LocalDate date)
			throws Exception {
		String query = "INSERT INTO " + EU_TN + " VALUES (?, ?, ?, ?)";
		QueryExecutionUtility.executeUpdate(connection, query, ps -> {
			int index = 1;
			ps.setString(index++, engineId);
			ps.setString(index++, insightId);
			ps.setString(index++, projectId);
			ps.setDate(index++, java.sql.Date.valueOf(date));
		});
	}

	/**
	 * Updates the recorded usage date of a database within an insight to the given
	 * date, inside the caller's transaction.
	 *
	 * @param connection connection whose transaction the caller owns
	 * @param engineId   the id of the database/engine used
	 * @param insightId  the insight the query ran in
	 * @param projectId  the project owning the insight
	 * @param date       the new date to record
	 * @throws Exception if the update fails
	 */
	private static void update(Connection connection, String engineId, String insightId, String projectId,
			LocalDate date) throws Exception {
		String query = "UPDATE " + EU_TN + " SET DATE = ? WHERE ENGINEID = ? AND INSIGHTID = ? " + "AND PROJECTID = ?";
		QueryExecutionUtility.executeUpdate(connection, query, ps -> {
			int index = 1;

			ps.setDate(index++, java.sql.Date.valueOf(date));
			ps.setString(index++, engineId);
			ps.setString(index++, insightId);
			ps.setString(index++, projectId);
		});
	}

	/**
	 * Removes the recorded usage row for a database within an insight, inside the
	 * caller's transaction.
	 *
	 * @param connection connection whose transaction the caller owns
	 * @param engineId   the id of the database/engine to remove
	 * @param insightId  the insight the usage was recorded under
	 * @param projectId  the project owning the insight
	 * @throws Exception if the delete fails
	 */
	private static void remove(Connection connection, String engineId, String insightId, String projectId)
			throws Exception {
		String query = "DELETE FROM " + EU_TN + " where ENGINEID = ? AND INSIGHTID = ? AND PROJECTID = ?";
		QueryExecutionUtility.executeUpdate(connection, query, ps -> {
			int index = 1;
			ps.setString(index++, engineId);
			ps.setString(index++, insightId);
			ps.setString(index++, projectId);
		});
	}

}

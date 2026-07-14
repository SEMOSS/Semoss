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

import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.ConnectionUtils;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;

public class EngineViewsUtils extends UserTrackingUtils {

	private static Logger classLogger = LogManager.getLogger(EngineViewsUtils.class);

	private static String EV_TN = "ENGINE_VIEWS";
	private static String EV_PRE = "ENGINE_VIEWS__";

	/**
	 * Records a view of the given engine for the current day, incrementing today's
	 * view count.
	 * 
	 * @param databaseId the id of the engine/database that was viewed
	 */
	public static void add(String databaseId) {
		addOrUpdate(databaseId);
	}

	/**
	 * Returns the total number of views recorded for an engine across all dates.
	 *
	 * @param engineId the id of the engine/database
	 * @return the summed view count; 0 if none exist
	 */
	public static int getTotal(String engineId) {
		IRDBMSEngine userTrackingDb = SystemEngineRegistry.getUserTrackingDb();

		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector function = new QueryFunctionSelector();
		function.addInnerSelector(new QueryColumnSelector(EV_PRE + "VIEWS"));
		function.setFunction(QueryFunctionHelper.SUM);
		function.setAlias("total_views");
		qs.addSelector(function);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(EV_PRE + "ENGINEID", "==", engineId));
		Long longViews = QueryExecutionUtility.flushToLong(userTrackingDb, qs);
		if (longViews == null) {
			longViews = 0L;
		}
		return longViews.intValue();
	}

	/**
	 * Returns the daily view counts for an engine over the past year.
	 *
	 * @param engineId the id of the engine/database
	 * @return a list of (date, view count) pairs within the last year
	 */
	public static List<Pair<String, Integer>> getByDate(String engineId) {
		SelectQueryStruct qs = new SelectQueryStruct();

		qs.addSelector(new QueryColumnSelector(EV_PRE + "DATE"));
		qs.addSelector(new QueryColumnSelector(EV_PRE + "VIEWS"));

		String lastYear = LocalDate.now().minusYears(1).toString();
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(EV_PRE + "DATE", ">", lastYear));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(EV_PRE + "ENGINEID", "==", engineId));

		List<Pair<String, Integer>> viewsByDate = new ArrayList<>();
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance()
				.getRawWrapper(SystemEngineRegistry.getUserTrackingDb(), qs)) {
			while (wrapper.hasNext()) {
				IHeadersDataRow row = wrapper.next();
				String date = row.getValues()[0].toString();
				Integer view = (Integer) row.getValues()[1];
				viewsByDate.add(Pair.of(date, view));
			}
		} catch (Exception e) {
			classLogger.error("Failed to fetch engine views by date for engine {}", engineId, e);
		}

		return viewsByDate;
	}

	/**
	 * Inserts today's view row for an engine, or increments it if one already
	 * exists.
	 *
	 * @param engineId the id of the engine/database that was viewed
	 */
	private static void addOrUpdate(String engineId) {
		IRDBMSEngine userTrackingDb = SystemEngineRegistry.getUserTrackingDb();

		LocalDate date = LocalDate.now();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(EV_PRE + "VIEWS"));

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(EV_PRE + "ENGINEID", "==", engineId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(EV_PRE + "DATE", "==", date.toString()));

		Integer curr = QueryExecutionUtility.flushToInteger(userTrackingDb, qs);
		if (curr == null) {
			add(engineId, date, 1);
		} else {
			update(engineId, date, ++curr);
		}
	}

	/**
	 * Sets the view count for an engine on a specific date.
	 *
	 * @param engineId the id of the engine/database
	 * @param date     the date whose count is being set
	 * @param i        the new view count
	 */
	private static void update(String engineId, LocalDate date, int i) {
		IRDBMSEngine userTrackingDb = SystemEngineRegistry.getUserTrackingDb();

		String query = "UPDATE " + EV_TN + " SET VIEWS = ?" + " WHERE ENGINEID = ? AND DATE = ?";

		PreparedStatement ps = null;
		try {
			ps = userTrackingDb.getPreparedStatement(query);
			int index = 1;
			ps.setInt(index++, i);
			ps.setString(index++, engineId);
			ps.setDate(index++, java.sql.Date.valueOf(date));
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update engine view count for engine {} on {}", engineId, date, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(userTrackingDb, ps);
		}
	}

	/**
	 * Inserts a view-count row for an engine on a specific date.
	 *
	 * @param engineId the id of the engine/database
	 * @param date     the date to record
	 * @param i        the initial view count
	 */
	private static void add(String engineId, LocalDate date, int i) {
		String query = "INSERT INTO " + EV_TN + " VALUES (?, ?, ?)";
		IRDBMSEngine userTrackingDb = SystemEngineRegistry.getUserTrackingDb();
		PreparedStatement ps = null;
		try {
			ps = userTrackingDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, engineId);
			ps.setDate(index++, java.sql.Date.valueOf(date));
			ps.setInt(index++, i);

			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to insert engine view count for engine {} on {}", engineId, date, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(userTrackingDb, ps);
		}
	}

}

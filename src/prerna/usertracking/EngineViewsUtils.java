/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.usertracking;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;

public class EngineViewsUtils extends UserTrackingUtils {

	private static Logger logger = LogManager.getLogger(EngineViewsUtils.class);

	private static String EV_TN = "ENGINE_VIEWS";
	private static String EV_PRE = "ENGINE_VIEWS__";

	public static void add(String databaseId) {
		addOrUpdate(databaseId);
	}

	public static int getTotal(String engineId) {
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

	public static List<Pair<String, Integer>> getByDate(String engineId) {
		SelectQueryStruct qs = new SelectQueryStruct();

		qs.addSelector(new QueryColumnSelector(EV_PRE + "DATE"));
		qs.addSelector(new QueryColumnSelector(EV_PRE + "VIEWS"));

		String lastYear = LocalDate.now().minusYears(1).toString();
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(EV_PRE + "DATE", ">", lastYear));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(EV_PRE + "ENGINEID", "==", engineId));

		IRawSelectWrapper wrapper = null;
		List<Pair<String, Integer>> viewsByDate = new ArrayList<>();
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(userTrackingDb, qs);
			while (wrapper.hasNext()) {
				IHeadersDataRow row = wrapper.next();
				String date = row.getValues()[0].toString();
				Integer view = (Integer) row.getValues()[1];
				viewsByDate.add(Pair.of(date, view));
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if (wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return viewsByDate;
	}

	private static void addOrUpdate(String engineId) {
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

	private static void update(String engineId, LocalDate date, int i) {
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
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	private static void add(String engineId, LocalDate date, int i) {
		String query = "INSERT INTO " + EV_TN + " VALUES (?, ?, ?)";

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
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
}

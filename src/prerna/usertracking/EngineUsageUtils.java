package prerna.usertracking;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

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

public class EngineUsageUtils extends UserTrackingUtils {
	
	private static Logger logger = LogManager.getLogger(EngineUsageUtils.class);
	private static String EU_TN = "ENGINE_USES";
	private static String EU_PRE = "ENGINE_USES__";

	public static void add(Set<String> queriedDatabaseIds, String insightId, String projectId) {
		queriedDatabaseIds.forEach(databaseId -> add(databaseId, insightId, projectId));
	}
	
	public static void update(Set<String> queriedDatabaseIds, String insightId, String projectId) {
		List<String> existingDbs = get(insightId, projectId);
		
		for (String db : queriedDatabaseIds) {
			if (existingDbs.remove(db)) {
				// update existing
				update(db, insightId, projectId);
			} else {
				// add new
				add(db, insightId, projectId);
			}
		}
		
		for (String db : existingDbs) {
			// remove no longer used
			remove(db, insightId, projectId);
		}
	}
	
	private static List<String> get(String insightId, String projectId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(EU_PRE + "ENGINEID"));

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(EU_PRE + "INSIGHTID", "==", insightId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(EU_PRE + "PROJECTID", "==", projectId));

		return QueryExecutionUtility.flushToListString(userTrackingDb, qs);
	}

	private static void add(String engineId, String insightId, String projectId) {
		add(engineId, insightId, projectId, LocalDate.now(ZoneId.of("UTC")));
	}
	
	private static void update(String engineId, String insightId, String projectId) {
		update(engineId, insightId, projectId, LocalDate.now(ZoneId.of("UTC")));
	}

	private static void add(String engineId, String insightId, String projectId, LocalDate date) {
		String query = "INSERT INTO " + EU_TN + " VALUES (?, ?, ?, ?)";

		PreparedStatement ps = null;
		try {
			ps = userTrackingDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, engineId);
			ps.setString(index++, insightId);
			ps.setString(index++, projectId);
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

	private static void update(String engineId, String insightId, String projectId, LocalDate date) {
		String query = "UPDATE " + EU_TN + " SET DATE = ? WHERE ENGINEID = ? AND INSIGHTID = ? "
				+ "AND PROJECTID = ?";

		PreparedStatement ps = null;
		try {
			ps = userTrackingDb.getPreparedStatement(query);
			int index = 1;

			ps.setDate(index++, java.sql.Date.valueOf(date));
			ps.setString(index++, engineId);
			ps.setString(index++, insightId);
			ps.setString(index++, projectId);

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
	
	private static void remove(String engineId, String insightId, String projectId) {
		String query = "DELETE FROM " + EU_TN + " where ENGINEID = ? AND INSIGHTID = ? AND PROJECTID = ?";

		PreparedStatement ps = null;
		try {
			ps = userTrackingDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, engineId);
			ps.setString(index++, insightId);
			ps.setString(index++, projectId);
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
	
	public static List<Pair<String, String>> getInInsights(String engineId) {
		SelectQueryStruct qs = new SelectQueryStruct();

		qs.addSelector(new QueryColumnSelector(EU_PRE + "INSIGHTID", "insightid"));
		qs.addSelector(new QueryColumnSelector(EU_PRE + "PROJECTID", "projectid"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(EU_PRE + "ENGINEID", "==", engineId));

		IRawSelectWrapper wrapper = null;
		List<Pair<String, String>> insightids = new ArrayList<>();
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(userTrackingDb, qs);
			while (wrapper.hasNext()) {
				IHeadersDataRow row = wrapper.next();
				String insight = row.getValues()[0].toString();
				String project = row.getValues()[1].toString();
				insightids.add(Pair.of(insight, project));
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

		return insightids;
	}

	public static List<Pair<String, Integer>> getByDate(String engineId) {
		SelectQueryStruct qs = new SelectQueryStruct();

		QueryFunctionSelector function = new QueryFunctionSelector();
		function.addInnerSelector(new QueryColumnSelector(EU_PRE + "INSIGHTID"));
		function.setFunction(QueryFunctionHelper.COUNT);
		function.setAlias("total_uses");
		qs.addSelector(function);
		qs.addSelector(new QueryColumnSelector(EU_PRE + "DATE"));

		String lastYear = LocalDate.now().minusYears(1).toString();
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(EU_PRE + "DATE", ">", lastYear));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(EU_PRE + "ENGINEID", "==", engineId));
		qs.addGroupBy(new QueryColumnSelector(EU_PRE + "DATE"));
		qs.addGroupBy(new QueryColumnSelector(EU_PRE + "INSIGHTID"));

		IRawSelectWrapper wrapper = null;
		List<Pair<String, Integer>> viewsByDate = new ArrayList<>();
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(userTrackingDb, qs);
			while (wrapper.hasNext()) {
				IHeadersDataRow row = wrapper.next();
				Integer view = ((Long) row.getValues()[0]).intValue();
				String date = row.getValues()[1].toString();
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
	
	// return the most used catalog in the past month
	// easy solution that I think provides real value.
	// probably not the most exact/in depth solution, but I do believe its the best
	// If we went off views, then we may trending something to people to look at and not use
	// which would keep it stuck in the trending section, which would cause it to get viewed more
	// and we would never display a useful catalog.
	public static List<String> getTrendingDatabases(int limit, List<String> accessibleDbs) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(EU_PRE + "ENGINEID"));
		
		QueryFunctionSelector count = new QueryFunctionSelector();
		count.addInnerSelector(new QueryColumnSelector(EU_PRE + "ENGINEID"));
		count.setAlias("engine_uses");
		count.setFunction(QueryFunctionHelper.COUNT);
		qs.addSelector(count);
		
		// filter out any non viewable databases
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(EU_PRE + "ENGINEID", "==", accessibleDbs));

		LocalDate now = LocalDate.now(TimeZone.getTimeZone("UTC").toZoneId());
		LocalDate lastMonth = now.minusMonths(1);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(EU_PRE + "DATE", ">", lastMonth));
		
		qs.addGroupBy(new QueryColumnSelector(EU_PRE + "ENGINEID"));
		
		qs.addOrderBy("engine_uses", "desc");
		qs.setLimit(limit);
		
		
		return QueryExecutionUtility.flushToListString(userTrackingDb, qs);
	}
	
}

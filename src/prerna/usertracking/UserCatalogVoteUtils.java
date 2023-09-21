package prerna.usertracking;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;

public class UserCatalogVoteUtils extends UserTrackingUtils {

	private static Logger logger = LogManager.getLogger(UserCatalogVoteUtils.class);
	
	private static String VOTE_TN = "USER_CATALOG_VOTES";
	private static String VOTE_PRE = "USER_CATALOG_VOTES__";
	
	/**
	 * 
	 * @param creds
	 * @param catalogId
	 * @return
	 */
	public static Map<Pair<String, String>, Integer> getVote(List<Pair<String, String>> creds, String catalogId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(VOTE_PRE + "USERID"));
		qs.addSelector(new QueryColumnSelector(VOTE_PRE + "TYPE"));
		qs.addSelector(new QueryColumnSelector(VOTE_PRE + "VOTE"));
		

		OrQueryFilter of = new OrQueryFilter();
		for (Pair<String, String> cred : creds) {
			AndQueryFilter af = new AndQueryFilter();
			af.addFilter(SimpleQueryFilter.makeColToValFilter(VOTE_PRE +  "USERID", "==", cred.getValue0()));
			af.addFilter(SimpleQueryFilter.makeColToValFilter(VOTE_PRE +  "TYPE", "==", cred.getValue1()));
			of.addFilter(af);
		}
		qs.addExplicitFilter(of);

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(VOTE_PRE + "ENGINEID", "==", catalogId));
		
		IRawSelectWrapper wrapper = null;
		Map<Pair<String, String>, Integer> votes = new HashMap<>();
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(userTrackingDb, qs);
			if (wrapper.hasNext()) {
				IHeadersDataRow headerRow = wrapper.next();
				Object[] values = headerRow.getValues();
				
				if (values[0] != null && values[1] != null && values[2] != null) {
					Pair<String, String> credential = Pair.with(values[0].toString(), values[1].toString());
					Integer vote = ((Number) values[2]).intValue();
					votes.put(credential, vote);
				}
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
		
		return votes;
	}
	
	/**
	 * 
	 * @param creds
	 * @param engineId
	 * @return
	 */
	public static Map<String, Boolean> userEngineVotes(List<Pair<String, String>> creds, Set<String> engineIds) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(VOTE_PRE + "ENGINEID"));
		qs.addSelector(new QueryColumnSelector(VOTE_PRE + "USERID"));
		qs.addSelector(new QueryColumnSelector(VOTE_PRE + "TYPE"));
		qs.addSelector(new QueryColumnSelector(VOTE_PRE + "VOTE"));
		
		OrQueryFilter of = new OrQueryFilter();
		for (Pair<String, String> cred : creds) {
			AndQueryFilter af = new AndQueryFilter();
			af.addFilter(SimpleQueryFilter.makeColToValFilter(VOTE_PRE +  "USERID", "==", cred.getValue0()));
			af.addFilter(SimpleQueryFilter.makeColToValFilter(VOTE_PRE +  "TYPE", "==", cred.getValue1()));
			of.addFilter(af);
		}
		qs.addExplicitFilter(of);
		if(engineIds != null && !engineIds.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(VOTE_PRE + "ENGINEID", "==", engineIds));
		}
		
		IRawSelectWrapper wrapper = null;
		Map<String, Map<Pair<String, String>, Integer>> mappy = new HashMap<>();
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(userTrackingDb, qs);
			while (wrapper.hasNext()) {
				IHeadersDataRow headerRow = wrapper.next();
				Object[] values = headerRow.getValues();
				
				if (values[0] != null && values[1] != null && values[2] != null && values[3] != null) {
					String engine = values[0].toString();
					Pair<String, String> credential = Pair.with(values[1].toString(), values[2].toString());
					Integer vote = ((Number) values[3]).intValue();
					
					if (mappy.containsKey(engine)) {
						mappy.get(engine).put(credential, vote);
					} else {
						Map<Pair<String, String>, Integer> newMap = new HashMap<>();
						newMap.put(credential, vote);
						mappy.put(engine, newMap);
					}
				}
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
		
		Map<String, Boolean> toReturn = new HashMap<>();
		for (String x : engineIds) {
			boolean upvoted = false;
			
			if (mappy.containsKey(x)) {
				Map<Pair<String, String>, Integer> fromDB = mappy.get(x);
				boolean allUpvoted = true;
				for (Pair<String, String> cred : creds) {
					if (!fromDB.containsKey(cred) || fromDB.get(cred) == null || fromDB.get(cred) != 1) {
						allUpvoted = false;
					}
				}
				upvoted = allUpvoted;
			}

			toReturn.put(x, upvoted);
		}
		
		return toReturn;
	}

	/**
	 * 
	 * @param databaseId
	 * @return
	 */
	public static int getAllVotes(String databaseId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector sum = new QueryFunctionSelector();
		sum.addInnerSelector(new QueryColumnSelector(VOTE_PRE + "VOTE"));
		sum.setAlias("total");
		sum.setFunction(QueryFunctionHelper.SUM);
		qs.addSelector(sum);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(VOTE_PRE + "ENGINEID", "==", databaseId));
		
		int val = 0;
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(userTrackingDb, qs);

			if(wrapper.hasNext()) {
				IHeadersDataRow headerRow = wrapper.next();
				Object[] values = headerRow.getValues();
				if (values[0] != null) {
					val = ((Number) values[0]).intValue();
				}
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
		
		return val;
	}

	/**
	 * 
	 * @param databaseIds
	 * @return
	 */
	public static Map<String, Integer> getAllVotes(List<String> databaseIds) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(VOTE_PRE + "ENGINEID"));
		QueryFunctionSelector sum = new QueryFunctionSelector();
		sum.addInnerSelector(new QueryColumnSelector(VOTE_PRE + "VOTE"));
		sum.setAlias("total");
		sum.setFunction(QueryFunctionHelper.SUM);
		qs.addSelector(sum);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(VOTE_PRE + "ENGINEID", "==", databaseIds));
		qs.addGroupBy(new QueryColumnSelector(VOTE_PRE + "ENGINEID"));
		
		Map<String, Integer> votes = new HashMap<>();
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(userTrackingDb, qs);
			while(wrapper.hasNext()) {
				IHeadersDataRow headerRow = wrapper.next();
				Object[] values = headerRow.getValues();
				votes.put((String) values[0], ((Number) values[0]).intValue());
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
		
		return votes;
	}
	
	/**
	 * 
	 * @param databaseIds
	 * @return
	 * @throws Exception
	 */
	public static IRawSelectWrapper getAllVotesWrapper(Collection<String> databaseIds) throws Exception {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(VOTE_PRE + "ENGINEID"));
		QueryFunctionSelector sum = new QueryFunctionSelector();
		sum.addInnerSelector(new QueryColumnSelector(VOTE_PRE + "VOTE"));
		sum.setAlias("total");
		sum.setFunction(QueryFunctionHelper.SUM);
		qs.addSelector(sum);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(VOTE_PRE + "ENGINEID", "==", databaseIds));
		qs.addGroupBy(new QueryColumnSelector(VOTE_PRE + "ENGINEID"));
		
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(userTrackingDb, qs);
		return wrapper;
	}
	
	/**
	 * 
	 * @param creds
	 * @param catalogId
	 * @param vote
	 */
	public static void vote(List<Pair<String, String>> creds, String catalogId, int vote) {
		Map<Pair<String, String>, Integer> votes = getVote(creds, catalogId);

		List<Pair<String, String>> toUpdate = new ArrayList<>();
		List<Pair<String, String>> toInsert = new ArrayList<>();

		for (Pair<String, String> cred : creds) {
			if (votes.containsKey(cred)) {
				int existing = votes.get(cred);
				if (existing != vote) {
					toUpdate.add(cred);
				}
			} else {
				toInsert.add(cred);
			}
		}

		if (toInsert.size() != 0) {
			insert(toInsert, catalogId, vote);
		}

		if (toUpdate.size() != 0) {
			update(toUpdate, catalogId, vote);
		}
	}

	/**
	 * 
	 * @param creds
	 * @param catalogId
	 * @param vote
	 */
	private static void update(List<Pair<String, String>> creds, String catalogId, int vote) {
		String query = "UPDATE " + VOTE_TN + " SET VOTE = ?, LAST_MODIFIED = ? WHERE USERID = ? AND TYPE = ? AND ENGINEID = ?";
		
		PreparedStatement ps = null;
		try {
			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
			ps = userTrackingDb.getPreparedStatement(query);
			for (Pair<String, String> cred : creds) {
				int index = 1;
				ps.setInt(index, vote);
				ps.setTimestamp(index, Timestamp.valueOf(LocalDateTime.now()), cal);
				ps.setString(index++, cred.getValue0());
				ps.setString(index++, cred.getValue1());
				ps.setString(index++, catalogId);
				ps.addBatch();
			}
			ps.executeBatch();
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred while adding user access request detailed message = " + e.getMessage());
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(userTrackingDb.isConnectionPooling()) {
					try {
						ps.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}	
	}

	/**
	 * 
	 * @param creds
	 * @param catalogId
	 */
	public static void delete(List<Pair<String, String>> creds, String catalogId) {
		String query = "DELETE FROM " + VOTE_TN + " WHERE USERID = ? AND TYPE = ? AND ENGINEID = ?";
		PreparedStatement ps = null;
		try {
			ps = userTrackingDb.getPreparedStatement(query);
			for (Pair<String, String> cred : creds) {
				int parameterIndex = 1;
				ps.setString(parameterIndex++, cred.getValue0());
				ps.setString(parameterIndex++, cred.getValue1());
				ps.setString(parameterIndex++, catalogId);
				ps.addBatch();
			}
			ps.execute();
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(userTrackingDb.isConnectionPooling()) {
					try {
						ps.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
	}

	/**
	 * 
	 * @param creds
	 * @param cid
	 * @param vote
	 */
	private static void insert(List<Pair<String, String>> creds, String cid, int vote) {
		String query = "INSERT INTO " + VOTE_TN + " (USERID, TYPE, ENGINEID, VOTE, LAST_MODIFIED) VALUES (?, ?, ?, ?, ?)";
		
		PreparedStatement ps = null;
		try {
			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
			ps = userTrackingDb.getPreparedStatement(query);
			
			for (Pair<String, String> cred : creds) {
				int index = 1;
				ps.setString(index++, cred.getValue0());
				ps.setString(index++, cred.getValue1());
				ps.setString(index++, cid);
				ps.setInt(index++, vote);
				ps.setTimestamp(index++, Timestamp.valueOf(LocalDateTime.now()), cal);
				ps.addBatch();
			}
			ps.executeBatch();
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred while adding user access request detailed message = " + e.getMessage());
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(userTrackingDb.isConnectionPooling()) {
					try {
						ps.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
	}

	/**
	 * 
	 * @param limit
	 * @param accessibleDbs
	 * @return
	 */
	public static List<String> getRecommendedDatabases(int limit, List<String> accessibleDbs) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(VOTE_PRE + "ENGINEID"));
		
		QueryFunctionSelector sum = new QueryFunctionSelector();
		sum.addInnerSelector(new QueryColumnSelector(VOTE_PRE + "VOTE"));
		sum.setAlias("total");
		sum.setFunction(QueryFunctionHelper.SUM);
		qs.addSelector(sum);
		
		// filter out any non viewable databases
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(VOTE_PRE + "ENGINEID", "==", accessibleDbs));

		qs.addGroupBy(new QueryColumnSelector(VOTE_PRE + "ENGINEID"));
		
		qs.addOrderBy("total", "desc");
		qs.setLimit(limit);
		
		
		return QueryExecutionUtility.flushToListString(userTrackingDb, qs);
	}
	
}

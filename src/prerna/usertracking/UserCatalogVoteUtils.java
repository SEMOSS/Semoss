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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.ConnectionUtils;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

public class UserCatalogVoteUtils extends UserTrackingUtils {

	private static Logger classLogger = LogManager.getLogger(UserCatalogVoteUtils.class);

	private static String VOTE_TN = "USER_CATALOG_VOTES";
	private static String VOTE_PRE = "USER_CATALOG_VOTES__";

	/**
	 * Fetches the vote each of the given user credentials has cast on a single
	 * catalog entry (engine).
	 *
	 * @param creds     the user's (userId, type) credential pairs to look up
	 * @param catalogId the engine/catalog id being voted on
	 * @return a map from each matching credential to its vote value (1 for an
	 *         upvote, -1 for a downvote); empty if no votes exist
	 */
	public static Map<Pair<String, String>, Integer> getVote(List<Pair<String, String>> creds, String catalogId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(VOTE_PRE + "USERID"));
		qs.addSelector(new QueryColumnSelector(VOTE_PRE + "TYPE"));
		qs.addSelector(new QueryColumnSelector(VOTE_PRE + "VOTE"));

		OrQueryFilter of = new OrQueryFilter();
		for (Pair<String, String> cred : creds) {
			AndQueryFilter af = new AndQueryFilter();
			af.addFilter(SimpleQueryFilter.makeColToValFilter(VOTE_PRE + "USERID", "==", cred.getValue0()));
			af.addFilter(SimpleQueryFilter.makeColToValFilter(VOTE_PRE + "TYPE", "==", cred.getValue1()));
			of.addFilter(af);
		}
		qs.addExplicitFilter(of);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(VOTE_PRE + "ENGINEID", "==", catalogId));

		Map<Pair<String, String>, Integer> votes = new HashMap<>();
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance()
				.getRawWrapper(SystemEngineRegistry.getUserTrackingDb(), qs)) {
			while (wrapper.hasNext()) {
				IHeadersDataRow headerRow = wrapper.next();
				Object[] values = headerRow.getValues();

				if (values[0] != null && values[1] != null && values[2] != null) {
					Pair<String, String> credential = Pair.with(values[0].toString(), values[1].toString());
					Integer vote = ((Number) values[2]).intValue();
					votes.put(credential, vote);
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to fetch vote for catalog {}", catalogId, e);
		}

		return votes;
	}

	/**
	 * Determines, for each requested engine, whether every one of the user's
	 * credentials has upvoted it.
	 *
	 * @param creds     the user's (userId, type) credential pairs
	 * @param engineIds the engine ids to check
	 * @return a map from each engine id to {@code true} when all of the user's
	 *         credentials have an upvote (vote == 1) on it, otherwise {@code false}
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
			af.addFilter(SimpleQueryFilter.makeColToValFilter(VOTE_PRE + "USERID", "==", cred.getValue0()));
			af.addFilter(SimpleQueryFilter.makeColToValFilter(VOTE_PRE + "TYPE", "==", cred.getValue1()));
			of.addFilter(af);
		}
		qs.addExplicitFilter(of);
		if (engineIds != null && !engineIds.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(VOTE_PRE + "ENGINEID", "==", engineIds));
		}

		Map<String, Map<Pair<String, String>, Integer>> mappy = new HashMap<>();
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance()
				.getRawWrapper(SystemEngineRegistry.getUserTrackingDb(), qs)) {
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
			classLogger.error("Failed to fetch user engine votes for engines {}", engineIds, e);
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
	 * Returns the aggregate (summed) vote score for a single engine.
	 *
	 * @param databaseId the engine/catalog id
	 * @return the sum of all votes cast on the engine; 0 if none exist
	 */
	public static int getAllVotes(String databaseId) {
		IRDBMSEngine userTrackingDb = SystemEngineRegistry.getUserTrackingDb();

		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector sum = new QueryFunctionSelector();
		sum.addInnerSelector(new QueryColumnSelector(VOTE_PRE + "VOTE"));
		sum.setAlias("total");
		sum.setFunction(QueryFunctionHelper.SUM);
		qs.addSelector(sum);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(VOTE_PRE + "ENGINEID", "==", databaseId));

		int val = 0;
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(userTrackingDb, qs)) {
			if (wrapper.hasNext()) {
				IHeadersDataRow headerRow = wrapper.next();
				Object[] values = headerRow.getValues();
				if (values[0] != null) {
					val = ((Number) values[0]).intValue();
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to fetch total votes for engine {}", databaseId, e);
		}

		return val;
	}

	/**
	 * Returns the aggregate (summed) vote score for each of the given engines.
	 *
	 * @param databaseIds the engine/catalog ids to total
	 * @return a map from engine id to its summed vote score; engines with no votes
	 *         are omitted
	 */
	public static Map<String, Integer> getAllVotes(List<String> databaseIds) {
		IRDBMSEngine userTrackingDb = SystemEngineRegistry.getUserTrackingDb();

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
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(userTrackingDb, qs)) {
			while (wrapper.hasNext()) {
				IHeadersDataRow headerRow = wrapper.next();
				Object[] values = headerRow.getValues();
				votes.put((String) values[0], ((Number) values[1]).intValue());
			}
		} catch (Exception e) {
			classLogger.error("Failed to fetch total votes for engines {}", databaseIds, e);
		}

		return votes;
	}

	/**
	 * Builds and executes the summed-vote-per-engine query and returns the open
	 * result iterator. The caller is responsible for closing the returned wrapper.
	 *
	 * @param databaseIds the engine/catalog ids to total
	 * @return an open {@link IRawSelectWrapper} over (engineId, totalVotes) rows
	 * @throws Exception if the query fails to execute
	 */
	public static IRawSelectWrapper getAllVotesWrapper(Collection<String> databaseIds) throws Exception {
		IRDBMSEngine userTrackingDb = SystemEngineRegistry.getUserTrackingDb();

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
	 * Records the given vote for the user on a catalog entry, inserting a row for
	 * credentials that have not yet voted and updating those whose vote changed.
	 *
	 * @param creds     the user's (userId, type) credential pairs
	 * @param catalogId the engine/catalog id being voted on
	 * @param vote      the vote value to record (e.g. 1 for an upvote, -1 for a
	 *                  downvote)
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
	 * Updates the existing vote rows for the given credentials on a catalog entry.
	 *
	 * @param creds     the credentials whose vote rows should be updated
	 * @param catalogId the engine/catalog id being voted on
	 * @param vote      the new vote value
	 */
	private static void update(List<Pair<String, String>> creds, String catalogId, int vote) {
		String query = "UPDATE " + VOTE_TN
				+ " SET VOTE = ?, LAST_MODIFIED = ? WHERE USERID = ? AND TYPE = ? AND ENGINEID = ?";
		IRDBMSEngine userTrackingDb = SystemEngineRegistry.getUserTrackingDb();
		PreparedStatement ps = null;
		try {
			ps = userTrackingDb.getPreparedStatement(query);
			for (Pair<String, String> cred : creds) {
				int index = 1;
				ps.setInt(index++, vote);
				ps.setTimestamp(index++, Utility.getCurrentSqlTimestampUTC());
				ps.setString(index++, cred.getValue0());
				ps.setString(index++, cred.getValue1());
				ps.setString(index++, catalogId);
				ps.addBatch();
			}
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update votes for catalog {}", catalogId, e);
			throw new IllegalArgumentException(
					"An error occurred while updating the user's vote. See logs for details.");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(userTrackingDb, ps);
		}
	}

	/**
	 * Removes any votes the given user credentials have cast on a catalog entry.
	 *
	 * @param creds     the user's (userId, type) credential pairs
	 * @param catalogId the engine/catalog id to remove votes for
	 */
	public static void delete(List<Pair<String, String>> creds, String catalogId) {
		String query = "DELETE FROM " + VOTE_TN + " WHERE USERID = ? AND TYPE = ? AND ENGINEID = ?";
		IRDBMSEngine userTrackingDb = SystemEngineRegistry.getUserTrackingDb();
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
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to delete votes for catalog {}", catalogId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(userTrackingDb, ps);
		}
	}

	/**
	 * Inserts new vote rows for the given credentials on a catalog entry.
	 *
	 * @param creds the credentials that have not yet voted
	 * @param cid   the engine/catalog id being voted on
	 * @param vote  the vote value to record
	 */
	private static void insert(List<Pair<String, String>> creds, String cid, int vote) {
		String query = "INSERT INTO " + VOTE_TN
				+ " (USERID, TYPE, ENGINEID, VOTE, LAST_MODIFIED) VALUES (?, ?, ?, ?, ?)";
		IRDBMSEngine userTrackingDb = SystemEngineRegistry.getUserTrackingDb();
		PreparedStatement ps = null;
		try {
			ps = userTrackingDb.getPreparedStatement(query);
			for (Pair<String, String> cred : creds) {
				int index = 1;
				ps.setString(index++, cred.getValue0());
				ps.setString(index++, cred.getValue1());
				ps.setString(index++, cid);
				ps.setInt(index++, vote);
				ps.setTimestamp(index++, Utility.getCurrentSqlTimestampUTC());
				ps.addBatch();
			}
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to insert votes for catalog {}", cid, e);
			throw new IllegalArgumentException("An error occurred while saving the user's vote. See logs for details.");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(userTrackingDb, ps);
		}
	}
}
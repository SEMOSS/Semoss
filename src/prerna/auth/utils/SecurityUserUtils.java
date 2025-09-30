package prerna.auth.utils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;

public class SecurityUserUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityUserUtils.class);

	/**
	 * Get the metadata for a specific user
	 * 
	 * @param userId
	 * @param userType
	 * @param metaKeys
	 * @param ignoreMarkdown
	 * @return
	 */
	public static Map<String, Collection<String>> getAggregateUserMetadata(String userId, AuthProvider userType,
			List<String> metaKeys, boolean ignoreMarkdown) {
		Map<String, Collection<String>> retMap = new HashMap<String, Collection<String>>();
		try (IRawSelectWrapper wrapper = getUserMetadataWrapper(Lists.newArrayList(userId),
				Lists.newArrayList(userType), metaKeys, ignoreMarkdown)) {
			while (wrapper.hasNext()) {
				Object[] data = wrapper.next().getValues();
				String metaKey = (String) data[2];
				String metaValue = (String) data[3];

				if (retMap.containsKey(metaKey)) {
					retMap.get(metaKey).add(metaValue);
				} else {
					retMap.put(metaKey, Lists.newArrayList(metaValue));
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		return retMap;
	}

	/**
	 * 
	 * @param metakey
	 * @return
	 */
	public static List<Map<String, Object>> getMetakeyOptions(Collection<String> metakeys) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERMETAKEYS__METAKEY", "metakey"));
		qs.addSelector(new QueryColumnSelector("USERMETAKEYS__SINGLEMULTI", "single_multi"));
		qs.addSelector(new QueryColumnSelector("USERMETAKEYS__DISPLAYORDER", "display_order"));
		qs.addSelector(new QueryColumnSelector("USERMETAKEYS__DISPLAYOPTIONS", "display_options"));
		qs.addSelector(new QueryColumnSelector("USERMETAKEYS__DEFAULTVALUES", "display_values"));
		if (metakeys != null && !metakeys.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERMETAKEYS__METAKEY", "==", metakeys));
		}
		qs.addOrderBy("USERMETAKEYS__DISPLAYORDER");
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Get the wrapper for additional user metadata
	 * 
	 * @param userIds
	 * @param userTypes
	 * @param metaKeys
	 * @param ignoreMarkdown
	 * @return
	 * @throws Exception
	 */
	public static IRawSelectWrapper getUserMetadataWrapper(Collection<String> userIds,
			Collection<AuthProvider> userTypes, List<String> metaKeys, boolean ignoreMarkdown) throws Exception {
		SelectQueryStruct qs = new SelectQueryStruct();
		// selectors
		qs.addSelector(new QueryColumnSelector("USERMETA__USERID"));
		qs.addSelector(new QueryColumnSelector("USERMETA__TYPE"));
		qs.addSelector(new QueryColumnSelector("USERMETA__METAKEY"));
		qs.addSelector(new QueryColumnSelector("USERMETA__METAVALUE"));
		qs.addSelector(new QueryColumnSelector("USERMETA__METAORDER"));
		// filters
		if (userIds != null && !userIds.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERMETA__USERID", "==", userIds));
		}
		if (userTypes != null && !userTypes.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERMETA__TYPE", "==", userTypes));
		}
		if (metaKeys != null && !metaKeys.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERMETA__METAKEY", "==", metaKeys));
		}
		// exclude markdown metadata due to potential large data size
		if (ignoreMarkdown) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERMETA__METAKEY", "!=", Constants.MARKDOWN));
		}
		// order
		qs.addOrderBy("USERMETA__METAORDER");
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		return wrapper;
	}

	/**
	 * 
	 * @return
	 */
	public static List<String> getAllMetakeys() {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERMETAKEYS__METAKEY"));
		List<String> metakeys = QueryExecutionUtility.flushToListString(securityDb, qs);
		return metakeys;
	}

	/**
	 * Update the user metadata Will delete existing values and then perform a bulk
	 * insert
	 * 
	 * @param userId
	 * @param userType
	 * @param insightId
	 * @param tags
	 */
	@SuppressWarnings("unchecked")
	public static void updateUserMetadata(String userId, AuthProvider userType, Map<String, ?> metadata) {
		String userTypeString = userType.toString();

		// first do a delete
		String deleteQ = "DELETE FROM USERMETA WHERE METAKEY=? AND USERID=? AND TYPE=?";
		PreparedStatement deletePs = null;
		try {
			deletePs = securityDb.getPreparedStatement(deleteQ);
			for (String field : metadata.keySet()) {
				int parameterIndex = 1;
				deletePs.setString(parameterIndex++, field);
				deletePs.setString(parameterIndex++, userId);
				deletePs.setString(parameterIndex++, userTypeString);
				deletePs.addBatch();
			}
			deletePs.executeBatch();
			if (!deletePs.getConnection().getAutoCommit()) {
				deletePs.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, deletePs);
		}

		// now we do the new insert with the order of the tags
		String query = securityDb.getQueryUtil().createInsertPreparedStatementString("USERMETA",
				new String[] { "USERID", "TYPE", "METAKEY", "METAVALUE", "METAORDER" });
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			for (String field : metadata.keySet()) {
				Object val = metadata.get(field);
				List<Object> values = new ArrayList<>();
				if (val instanceof List) {
					values = (List<Object>) val;
				} else if (val instanceof Collection) {
					values.addAll((Collection<Object>) val);
				} else {
					values.add(val);
				}

				for (int i = 0; i < values.size(); i++) {
					int parameterIndex = 1;
					Object fieldVal = values.get(i);

					ps.setString(parameterIndex++, userId);
					ps.setString(parameterIndex++, userTypeString);
					ps.setString(parameterIndex++, field);
					ps.setString(parameterIndex++, fieldVal + "");
					ps.setInt(parameterIndex++, i);
					ps.addBatch();
				}
			}
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * 
	 * @param metaoptions
	 * @return
	 */
	public static boolean updateMetakeyOptions(List<Map<String, Object>> metaoptions) {
		boolean valid = false;
		PreparedStatement insertPs = null;
		try {
			// first truncate table clean
			String truncateSql = "DELETE FROM USERMETAKEYS WHERE 1=1";
			securityDb.removeData(truncateSql);
			insertPs = securityDb.bulkInsertPreparedStatement(
					new Object[] { "USERMETAKEYS", Constants.METAKEY, Constants.SINGLE_MULTI, Constants.DISPLAY_ORDER,
							Constants.DISPLAY_OPTIONS, Constants.DEFAULT_VALUES });
			// then insert latest options
			for (int i = 0; i < metaoptions.size(); i++) {
				Map<String, Object> m = metaoptions.get(i);
				insertPs.setString(1, (String) m.get("metakey"));
				insertPs.setString(2, (String) m.get("single_multi"));
				Number n = ((Number) m.get("display_order"));
				if (n == null) {
					insertPs.setNull(3, java.sql.Types.INTEGER);
				} else {
					insertPs.setInt(3, n.intValue());
				}
				insertPs.setString(4, (String) m.get("display_options"));
				insertPs.setString(5, (String) m.get("display_values"));
				insertPs.addBatch();
			}
			insertPs.executeBatch();
			if (!insertPs.getConnection().getAutoCommit()) {
				insertPs.getConnection().commit();
			}
			valid = true;
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, insertPs);
		}
		return valid;
	}

	/**
	 * Set if the model as default to user
	 * 
	 * @param user
	 * @param engineId
	 * @return
	 * @throws IllegalAccessException
	 */
	public static User setModelDefault(User user, String engineId) throws IllegalAccessException {
		String userId = user.getAccessToken(user.getLogins().get(0)).getId();
		if (!SecurityUserEngineUtils.userIsOwner(user, engineId)) {
			 // If the user is not the owner (or permission expired and got removed),
			// clear the defaultmodel mapping
	        String deleteQ = "DELETE FROM USERMETA WHERE USERID=? AND METAKEY=?";
	        try (PreparedStatement ps = securityDb.getPreparedStatement(deleteQ)) {
	            ps.setString(1, userId);
	            ps.setString(2, Constants.DEFAULT_MODEL_KEY);
	            ps.executeUpdate();
	            if (!ps.getConnection().getAutoCommit()) {
	                ps.getConnection().commit();
	            }
	        } catch (SQLException e) {
	            classLogger.error(Constants.STACKTRACE, e);
	        }

	        user.setDefaultModel(null);
	        return user;
		}
		
		String mergeQuery = "MERGE INTO USERMETA KEY(USERID, METAKEY) " + 
		                "VALUES (?, 'SYSTEM', ?, ?, 0)";

		try (PreparedStatement ps = securityDb.getPreparedStatement(mergeQuery)) {
			ps.setString(1, userId);
			ps.setString(2, Constants.DEFAULT_MODEL_KEY);
			ps.setString(3, engineId);
			ps.executeUpdate();

			user.setDefaultModel(engineId);

			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}

		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		return user;
	}
	

    /**
     * Fetch default model for a given user
     */
	public static String getDefaultModelForUser(String userId) {
	    String selectQ = "SELECT METAVALUE FROM USERMETA WHERE USERID = ? AND METAKEY = ?";
	    PreparedStatement ps = null;
	    ResultSet rs = null;

	    try {
	        ps = securityDb.getPreparedStatement(selectQ);
	        ps.setString(1, userId);
	        ps.setString(2, Constants.DEFAULT_MODEL_KEY);

	        rs = ps.executeQuery();
	        if (rs.next()) {
	            return rs.getString("METAVALUE");
	        }
	    } catch (SQLException e) {
	        classLogger.error(Constants.STACKTRACE, e);
	    } finally {
	        ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps, rs);
	    }

	    return null;
	}


}

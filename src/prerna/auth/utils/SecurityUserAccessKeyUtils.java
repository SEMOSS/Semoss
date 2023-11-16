package prerna.auth.utils;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.Utility;

public class SecurityUserAccessKeyUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityUserAccessKeyUtils.class);

	private static final String SMSS_USER_ACCESS_KEYS_TABLE_NAME = "SMSS_USER_ACCESS_KEYS";
	@Deprecated
	private static final String OLD_USERID_COL = SMSS_USER_ACCESS_KEYS_TABLE_NAME + "__ID";
	private static final String USERID_COL = SMSS_USER_ACCESS_KEYS_TABLE_NAME + "__USERID";
	private static final String TYPE_COL = SMSS_USER_ACCESS_KEYS_TABLE_NAME + "__TYPE";
	private static final String ACCESS_KEY_COL = SMSS_USER_ACCESS_KEYS_TABLE_NAME + "__ACCESSKEY";
	private static final String SECRET_KEY_COL = SMSS_USER_ACCESS_KEYS_TABLE_NAME + "__SECRETKEY";
	private static final String SECRET_KEY_SALT_COL = SMSS_USER_ACCESS_KEYS_TABLE_NAME + "__SECRETSALT";
	private static final String DATE_CREATED_COL = SMSS_USER_ACCESS_KEYS_TABLE_NAME + "__DATECREATED";
	private static final String LAST_USED_COL = SMSS_USER_ACCESS_KEYS_TABLE_NAME + "__LASTUSED";

	private static final String TOKEN_NAME_COL = SMSS_USER_ACCESS_KEYS_TABLE_NAME + "__TOKENNAME";
	private static final String TOKEN_DESCRIPTION_COL = SMSS_USER_ACCESS_KEYS_TABLE_NAME + "__TOKENDESCRIPTION";

	private SecurityUserAccessKeyUtils() {

	}
	
	/**
	 * 
	 * @param accessKey
	 * @param secretKey
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static User validateKeysAndReturnUser(String accessKey, String secretKey) throws IllegalAccessException {
		String saltedSecretKey = null;
		String salt = null;
		String userId = null;
		String oldUserId = null;
		String loginType = null;
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(SECRET_KEY_COL));
		qs.addSelector(new QueryColumnSelector(SECRET_KEY_SALT_COL));
		qs.addSelector(new QueryColumnSelector(USERID_COL));
		qs.addSelector(new QueryColumnSelector(OLD_USERID_COL));
		qs.addSelector(new QueryColumnSelector(TYPE_COL));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(ACCESS_KEY_COL, "==", accessKey));

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if (wrapper.hasNext()) {
				Object[] values = wrapper.next().getValues();
				saltedSecretKey = (String) values[0];
				salt = (String) values[1];
				userId = (String) values[2];
				oldUserId = (String) values[3];
				loginType = (String) values[4];
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		if(saltedSecretKey == null || salt == null) {
			throw new IllegalAccessException("Invalid access key");
		}

		String typedHash = hash(secretKey, salt);
		boolean validCredentials = saltedSecretKey.equals(typedHash);
		if(!validCredentials) {
			throw new IllegalAccessException("Invalid credentials");
		}
		
		// TODO: should load in all the user details from the User table
		User user = new User();
		AccessToken token = new AccessToken();
		AuthProvider provider = AuthProvider.getProviderFromString(loginType);
		token.setProvider(provider);
		if(userId == null) {
			token.setId(oldUserId);
		} else {
			token.setId(userId);
		}
		user.setAccessToken(token);
		return user;
	}

	/**
	 * 
	 * @param accessToken
	 * @return
	 * @throws SQLException 
	 */
	public static Map<String, String> createUserAccessToken(AccessToken accessToken, String tokenName, String tokenDescription) throws SQLException {
		String salt = AbstractSecurityUtils.generateSalt();
		String accessKey = UUID.randomUUID().toString();
		String secretKey = UUID.randomUUID().toString();
		String saltedSecretKey = (AbstractSecurityUtils.hash(secretKey, salt));

		java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();

		String insertQuery = "INSERT INTO "+SMSS_USER_ACCESS_KEYS_TABLE_NAME +
				" (USERID, TYPE, ACCESSKEY, SECRETKEY, SECRETSALT, DATECREATED, LASTUSED, TOKENNAME, TOKENDESCRIPTION) "
				+ "VALUES (?,?,?,?,?,?,?,?,?)";

		PreparedStatement ps = null;
		try {
			int parameterIndex = 1;
			ps = securityDb.getPreparedStatement(insertQuery);
			ps.setString(parameterIndex++, accessToken.getId()); 
			ps.setString(parameterIndex++, accessToken.getProvider().toString()); 
			ps.setString(parameterIndex++, accessKey); 
			ps.setString(parameterIndex++, saltedSecretKey); 
			ps.setString(parameterIndex++, salt); 
			ps.setTimestamp(parameterIndex++, timestamp);
			ps.setNull(parameterIndex++, java.sql.Types.TIMESTAMP);
			if(tokenName == null || (tokenName=tokenName.trim()).isEmpty()) {
				ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(parameterIndex++, tokenName); 
			}
			if(tokenDescription == null || (tokenDescription=tokenDescription.trim()).isEmpty()) {
				ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(parameterIndex++, tokenDescription); 
			}
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}

		Map<String, String> details = new HashMap<>();
		details.put("ACCESSKEY", accessKey);
		details.put("SECRETKEY", secretKey);
		details.put("TOKENNAME", tokenName);
		details.put("TOKENDESCRIPTION", tokenDescription);
		return details;
	}

	/**
	 * 
	 * @param accessKey
	 * @param token
	 */
	public static void updateAccessTokenLastUsed(String accessKey) {
		java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();

		String insertQuery = "UPDATE "+SMSS_USER_ACCESS_KEYS_TABLE_NAME+" SET LASTUSED=? WHERE ACCESSKEY=?";

		PreparedStatement ps = null;
		try {
			int parameterIndex = 1;
			ps = securityDb.getPreparedStatement(insertQuery);
			ps.setTimestamp(parameterIndex++, timestamp);
			ps.setString(parameterIndex++, accessKey);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * 
	 * @param token
	 * @param accessKey
	 * @return
	 */
	public static boolean deleteUserAccessToken(AccessToken token, String accessKey) {
		// validate user has this access key
		List<Map<String, Object>> validateAssignedToUser = getUserAccessKeyInfo(token, accessKey);
		if(validateAssignedToUser == null || validateAssignedToUser.isEmpty()) {
			throw new IllegalArgumentException("Access key does not exist for this user");
		}
		String insertQuery = "DELETE FROM "+SMSS_USER_ACCESS_KEYS_TABLE_NAME+" WHERE ACCESSKEY=?";
		PreparedStatement ps = null;
		try {
			int parameterIndex = 1;
			ps = securityDb.getPreparedStatement(insertQuery);
			ps.setString(parameterIndex++, accessKey); 
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
			return true;
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			return false;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * 
	 * @param token
	 * @return
	 */
	public static List<Map<String, Object>> getUserAccessKeyInfo(AccessToken token) {
		return getUserAccessKeyInfo(token, null);
	}
	
	/**
	 * 
	 * @param token
	 * @param accessKey
	 * @return
	 */
	public static List<Map<String, Object>> getUserAccessKeyInfo(AccessToken token, String accessKey) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(TOKEN_NAME_COL));
		qs.addSelector(new QueryColumnSelector(TOKEN_DESCRIPTION_COL));
		qs.addSelector(new QueryColumnSelector(ACCESS_KEY_COL));
		qs.addSelector(new QueryColumnSelector(DATE_CREATED_COL));
		qs.addSelector(new QueryColumnSelector(LAST_USED_COL));
		{
			// account for legacy table structure
			OrQueryFilter or = new OrQueryFilter();
			or.addFilter(SimpleQueryFilter.makeColToValFilter(OLD_USERID_COL, "==", token.getId()));
			or.addFilter(SimpleQueryFilter.makeColToValFilter(USERID_COL, "==", token.getId()));
			qs.addExplicitFilter(or);
		}
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(TYPE_COL, "==", token.getProvider().toString()));
		if(accessKey != null && !(accessKey=accessKey.trim()).isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(ACCESS_KEY_COL, "==", accessKey));
		}
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
}

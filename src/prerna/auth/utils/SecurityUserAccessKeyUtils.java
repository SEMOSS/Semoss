package prerna.auth.utils;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
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
import prerna.util.Utility;

public class SecurityUserAccessKeyUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityUserAccessKeyUtils.class);

	private static final String SMSS_USER_ACCESS_KEYS_TABLE_NAME = "SMSS_USER_ACCESS_KEYS";
	private static final String USERID_COL = SMSS_USER_ACCESS_KEYS_TABLE_NAME + "__ID";
	private static final String TYPE_COL = SMSS_USER_ACCESS_KEYS_TABLE_NAME + "__TYPE";
	private static final String ACCESS_KEY_COL = SMSS_USER_ACCESS_KEYS_TABLE_NAME + "__ACCESSKEY";
	private static final String SECRET_KEY_COL = SMSS_USER_ACCESS_KEYS_TABLE_NAME + "__SECRETKEY";
	private static final String SECRET_KEY_SALT_COL = SMSS_USER_ACCESS_KEYS_TABLE_NAME + "__SECRETSALT";
	private static final String DATE_CREATED_COL = SMSS_USER_ACCESS_KEYS_TABLE_NAME + "__DATECREATED";
	private static final String LAST_USED_COL = SMSS_USER_ACCESS_KEYS_TABLE_NAME + "__LASTUSED";

	private SecurityUserAccessKeyUtils() {

	}
	
	/**
	 * 
	 * @param accessKey
	 * @param secretKey
	 * @return
	 */
	public static User validateKeysAndReturnUser(String accessKey, String secretKey) {
		String saltedSecretKey = null;
		String salt = null;
		String userId = null;
		String loginType = null;
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(SECRET_KEY_COL));
		qs.addSelector(new QueryColumnSelector(SECRET_KEY_SALT_COL));
		qs.addSelector(new QueryColumnSelector(USERID_COL));
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
				loginType = (String) values[3];
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
			throw new IllegalAccessError("Invalid access key");
		}

		String typedHash = hash(secretKey, salt);
		boolean validCredentials = saltedSecretKey.equals(typedHash);
		if(!validCredentials) {
			throw new IllegalAccessError("Invalid credentials");
		}
		
		// TODO: should load in all the user details from the User table
		User user = new User();
		AccessToken token = new AccessToken();
		AuthProvider provider = AuthProvider.valueOf(loginType);
		token.setProvider(provider);
		token.setId(userId);
		user.setAccessToken(token);
		return user;
	}

	/**
	 * 
	 * @param token
	 * @return
	 */
	public static Map<String, String> createUserAccessToken(AccessToken token) {
		Map<String, String> details = new HashMap<>();
		String salt = AbstractSecurityUtils.generateSalt();
		String accessKey = UUID.randomUUID().toString();
		String secretKey = UUID.randomUUID().toString();
		String saltedSecretKey = (AbstractSecurityUtils.hash(secretKey, salt));

		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
		java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());

		String insertQuery = "INSERT INTO "+SMSS_USER_ACCESS_KEYS_TABLE_NAME+" (ID, TYPE, ACCESSKEY, SECRETKEY, SECRETSALT, DATECREATED, LASTUSED) "
				+ "VALUES (?,?,?,?,?,?,?)";

		PreparedStatement ps = null;
		try {
			int parameterIndex = 1;
			ps = securityDb.getPreparedStatement(insertQuery);
			ps.setString(parameterIndex++, token.getId()); 
			ps.setString(parameterIndex++, token.getProvider().toString()); 
			ps.setString(parameterIndex++, accessKey); 
			ps.setString(parameterIndex++, saltedSecretKey); 
			ps.setString(parameterIndex++, salt); 
			ps.setTimestamp(parameterIndex++, timestamp, cal);
			ps.setNull(parameterIndex++, java.sql.Types.TIMESTAMP);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}

		details.put("accessKey", accessKey);
		details.put("secretKey", secretKey);
		return details;
	}
	
	/**
	 * 
	 * @param token
	 * @param accessKey
	 * @return
	 */
	public static boolean deleteUserAccessToken(AccessToken token, String accessKey) {
		String insertQuery = "DELETE FROM "+SMSS_USER_ACCESS_KEYS_TABLE_NAME+" WHERE ID=? AND TYPE=? AND ACCESSKEY=?";
		PreparedStatement ps = null;
		try {
			int parameterIndex = 1;
			ps = securityDb.getPreparedStatement(insertQuery);
			ps.setString(parameterIndex++, token.getId()); 
			ps.setString(parameterIndex++, token.getProvider().toString()); 
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
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(ACCESS_KEY_COL));
		qs.addSelector(new QueryColumnSelector(DATE_CREATED_COL));
		qs.addSelector(new QueryColumnSelector(LAST_USED_COL));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(USERID_COL, "==", token.getId()));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(TYPE_COL, "==", token.getProvider().toString()));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	
	
}

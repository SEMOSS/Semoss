package prerna.auth.utils;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.SocialPropertiesUtil;
import prerna.util.Utility;

public class SecurityAPIUserUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityAPIUserUtils.class);

	private static final String SMSS_USER_TABLE_NAME = "SMSS_USER";
	private static final String USERID_COL = SMSS_USER_TABLE_NAME + "__ID";
	private static final String TYPE_COL = SMSS_USER_TABLE_NAME + "__TYPE";
	private static final String PASSWORD_COL = SMSS_USER_TABLE_NAME + "__PASSWORD";
	private static final String SALT_COL = SMSS_USER_TABLE_NAME + "__SALT";
	
	// check for API User Key
	public static final String API_USER_CHECK = "api_user_token_check";
	// do we require a dynamic api token or do we login
	public static final String REQUIRE_DYNAMIC_API_TOKEN = "api_user_require_dynamic_token";
	
	private SecurityAPIUserUtils() {

	}
	
	/**
	 * 
	 * @return
	 */
	public static Boolean getApplicationAPIUserTokenCheck() {
		String apiCheck = SocialPropertiesUtil.getInstance().getProperty(API_USER_CHECK); 
		if(apiCheck == null || (apiCheck=apiCheck.trim()).isEmpty()) {
			// default to true
			return true;
		}
		
		return Boolean.parseBoolean(apiCheck);
	}
	
	/**
	 * 
	 * @return
	 */
	public static Boolean getApplicationRequireDynamicToken() {
		String requireDynamicToken = SocialPropertiesUtil.getInstance().getProperty(REQUIRE_DYNAMIC_API_TOKEN);
		if(requireDynamicToken == null || (requireDynamicToken=requireDynamicToken.trim()).isEmpty()) {
			// default to true
			return true;
		}
		
		return Boolean.parseBoolean(requireDynamicToken);
	}

	/**
	 * 
	 * @param clientId
	 * @param secretKey
	 * @return
	 */
	public static boolean validCredentials(String clientId, String secretKey) {
		String saltedPassword = null;
		String salt = null;
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(PASSWORD_COL));
		qs.addSelector(new QueryColumnSelector(SALT_COL));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(USERID_COL, "==", clientId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(TYPE_COL, "==", AuthProvider.API_USER.toString()));

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if (wrapper.hasNext()) {
				Object[] values = wrapper.next().getValues();
				saltedPassword = (String) values[0];
				salt = (String) values[1];
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

		if(saltedPassword == null || salt == null) {
			return false;
		}

		String typedHash = hash(secretKey, salt);
		return saltedPassword.equals(typedHash);
	}

	/**
	 * 
	 * @param name
	 * @return
	 */
	public static Map<String, String> createAPIUser(String name) {
		Map<String, String> details = new HashMap<>();
		String salt = AbstractSecurityUtils.generateSalt();
		String clientId = UUID.randomUUID().toString();
		String secretKey = UUID.randomUUID().toString();
		String hashedPassword = (AbstractSecurityUtils.hash(secretKey, salt));

		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
		java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());

		String insertQuery = "INSERT INTO "+SMSS_USER_TABLE_NAME+" (ID, NAME, USERNAME, EMAIL, TYPE, ADMIN, PASSWORD, SALT, DATECREATED, "
				+ "LOCKED, PHONE, PHONEEXTENSION, COUNTRYCODE) "
				+ "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";

		PreparedStatement ps = null;
		try {
			int parameterIndex = 1;
			ps = securityDb.getPreparedStatement(insertQuery);
			ps.setString(parameterIndex++, clientId); // ID is the client ID
			ps.setString(parameterIndex++, name);
			ps.setNull(parameterIndex++, java.sql.Types.VARCHAR); // no username
			ps.setNull(parameterIndex++, java.sql.Types.VARCHAR); // no email
			ps.setString(parameterIndex++, AuthProvider.API_USER.toString());
			// shouldn't be adding API as an admin
			ps.setBoolean(parameterIndex++, false);
			ps.setString(parameterIndex++, hashedPassword);
			ps.setString(parameterIndex++, salt);
			ps.setTimestamp(parameterIndex++, timestamp, cal);
			// not locked ...
			ps.setBoolean(parameterIndex++, false);
			ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
			ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
			ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}

		details.put("clientId", clientId);
		details.put("secretKey", secretKey);

		return details;
	}

}

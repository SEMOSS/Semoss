package prerna.auth.utils;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.Utility;

public class SecurityShareSessionUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityShareSessionUtils.class);

	private static final String SESSION_SHARE_TABLE_NAME = "SESSION_SHARE";
	private static final String SHARE_VAL = "SHARE_VAL";
	private static final String SESSION_VAL = "SESSION_VAL";
	private static final String ROUTE_VAL = "ROUTE_VAL";
	private static final String DATE_ADDED = "DATE_ADDED";
	private static final String DATE_USED = "DATE_USED";
	private static final String USE_VALID = "USE_VALID";
	private static final String USERID = "USERID";
	private static final String TYPE = "TYPE";
	
	private static final String QS_SHARE_VAL = SESSION_SHARE_TABLE_NAME + "__" + SHARE_VAL;
	private static final String QS_SESSION_VAL = SESSION_SHARE_TABLE_NAME + "__" + SESSION_VAL;
	private static final String QS_ROUTE_VAL = SESSION_SHARE_TABLE_NAME + "__" + ROUTE_VAL;
	private static final String QS_DATE_ADDED = SESSION_SHARE_TABLE_NAME + "__" + DATE_ADDED;
	private static final String QS_DATE_USED = SESSION_SHARE_TABLE_NAME + "__" + DATE_USED;
	private static final String QS_USE_VALID = SESSION_SHARE_TABLE_NAME + "__" + USE_VALID;
	private static final String QS_USERID = SESSION_SHARE_TABLE_NAME + "__" + USERID;
	private static final String QS_TYPE = SESSION_SHARE_TABLE_NAME + "__" + TYPE;

	private SecurityShareSessionUtils() {
		
	}
	
	public static String createShareToken(User user, String sessionId, String routeId) throws SQLException {
		if(user == null || user.isAnonymous()) {
			throw new IllegalArgumentException("Cannot share a session for a user who is not logged in");
		}
		
		Pair<String, String> loginDetails = User.getPrimaryUserIdAndTypePair(user);
		
		java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
		String shareToken = UUID.randomUUID().toString();
		
		PreparedStatement ps = null;
		try {
			ps = securityDb.bulkInsertPreparedStatement(new Object[] {
					SESSION_SHARE_TABLE_NAME, 
					SHARE_VAL, SESSION_VAL, ROUTE_VAL,
					DATE_ADDED, USERID, TYPE
				});
			int parameterIndex = 1;
			ps.setString(parameterIndex++, shareToken);
			ps.setString(parameterIndex++, sessionId);
			ps.setString(parameterIndex++, routeId);
			ps.setTimestamp(parameterIndex++, timestamp);
			ps.setString(parameterIndex++, loginDetails.getValue0());
			ps.setString(parameterIndex++, loginDetails.getValue1());
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred inserting the request to update the password");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		
		return shareToken;
	}
	
	/**
	 * 
	 * @param shareToken
	 * @return
	 * @throws SQLException 
	 */
	public static boolean validateShareSessionDetails(Object[] shareDetails) throws SQLException {
		ZonedDateTime zdtCurrentUTC = Utility.getCurrentZonedDateTimeUTC();
		
		int index = 0;
		String shareToken = (String) shareDetails[index++];
		String sessionVal = (String) shareDetails[index++];
		String routeVal = (String) shareDetails[index++];
		SemossDate dateAddedVal = (SemossDate) shareDetails[index++];
		SemossDate dateUsedVal = (SemossDate) shareDetails[index++];
		Boolean useValid = (Boolean) shareDetails[index++];
		
		if(useValid != null || dateUsedVal != null) {
			throw new IllegalArgumentException("share key has already been used");
		}
		
		ZonedDateTime zdtAddedUTC = dateAddedVal.getZonedDateTime(TimeZone.getTimeZone("UTC"));
		// if when it was added + 5 min is BEFORE the current time
		// then it means this session has been shared for over 5 min
		// without anyone picking it up
		// we shouldn't allow it
		if(zdtAddedUTC.plusMinutes(5).isBefore(zdtCurrentUTC)) {
			logSessionUsed(shareToken, zdtCurrentUTC, false);
			throw new IllegalArgumentException("The share key has already expired");
		}
		logSessionUsed(shareToken, zdtCurrentUTC, true);
		return true;
	}
	
	/**
	 * 
	 * @param shareToken
	 * @return
	 */
	public static Object[] getShareSessionDetails(String shareToken) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(QS_SHARE_VAL));
		qs.addSelector(new QueryColumnSelector(QS_SESSION_VAL));
		qs.addSelector(new QueryColumnSelector(QS_ROUTE_VAL));
		qs.addSelector(new QueryColumnSelector(QS_DATE_ADDED));
		qs.addSelector(new QueryColumnSelector(QS_DATE_USED));
		qs.addSelector(new QueryColumnSelector(QS_USE_VALID));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(QS_SHARE_VAL, "==", shareToken));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				return wrapper.next().getValues();
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
		return null;
	}
	
	/**
	 * 
	 * @param shareToken
	 * @param zdt
	 * @param success
	 * @return
	 * @throws SQLException
	 */
	public static String logSessionUsed(String shareToken, ZonedDateTime zdt, boolean success) throws SQLException {
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("UPDATE " + SESSION_SHARE_TABLE_NAME + " SET " 
					+ DATE_USED+"=?, "
					+ USE_VALID+"=? "
					+ "WHERE " + SHARE_VAL + "=?"
					);
			int parameterIndex = 1;
			ps.setTimestamp(parameterIndex++, java.sql.Timestamp.from(zdt.toInstant()));
			ps.setBoolean(parameterIndex++, success);
			ps.setString(parameterIndex++, shareToken);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred logging the session share result");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		
		return shareToken;
	}

}

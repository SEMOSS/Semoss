package prerna.usertracking;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.Constants;
import prerna.util.Utility;

public abstract class AbstractUserTrackingUtils implements IUserTracking {

	private static Logger logger = LogManager.getLogger(AbstractUserTrackingUtils.class);
	
	/**
	 * 
	 * @param sessionId
	 * @param utd
	 * @param user
	 * @param ap
	 */
	protected static void saveSession(String sessionId, UserTrackingDetails utd, User user, AuthProvider ap) {
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
		java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());
		
		if(user.isAnonymous()) {
			addSession(sessionId, utd, user.getAnonymousId(), "ANONYMOUS", timestamp, cal);
		}
		List<AuthProvider> logins = user.getLogins();
		for(AuthProvider provider : logins) {
			AccessToken token = user.getAccessToken(provider);
			addSession(sessionId, utd, token.getId(), provider.toString(), timestamp, cal);
		}
	}
	
	private static void addSession(String sessionId, UserTrackingDetails utd, String userId, String type, java.sql.Timestamp timestamp, Calendar cal) {
		String query = "INSERT INTO USER_TRACKING VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		
		PreparedStatement ps = null;
		IRDBMSEngine engine = (IRDBMSEngine) Utility.getEngine(Constants.USER_TRACKING_DB);
		try {
			ps = engine.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, sessionId);
			ps.setString(index++, userId);
			ps.setString(index++, type);
			ps.setTimestamp(index++, timestamp, cal);
			ps.setNull(index++, java.sql.Types.NULL);
			ps.setString(index++, utd.getIpAddr());
			ps.setString(index++, utd.getIpLat());
			ps.setString(index++, utd.getIpLong());
			ps.setString(index++, utd.getIpCountry());
			ps.setString(index++, utd.getIpState());
			ps.setString(index++, utd.getIpCity());
			
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
			if(engine.isConnectionPooling()) {
				try {
					ps.getConnection().close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	@Override
	public void registerLogout(String sessionId) {
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
		java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());

		String query = "UPDATE USER_TRACKING SET ENDED_ON = ? WHERE SESSIONID = ?";
		
		PreparedStatement ps = null;
		IRDBMSEngine engine = (IRDBMSEngine) Utility.getEngine(Constants.USER_TRACKING_DB);
		try {
			ps = engine.getPreparedStatement(query);
			int index = 1;
			ps.setTimestamp(index++, timestamp, cal);
			ps.setString(index++, sessionId);
			
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
			if(engine.isConnectionPooling()) {
				try {
					ps.getConnection().close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

}

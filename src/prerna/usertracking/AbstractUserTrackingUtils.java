package prerna.usertracking;

import java.sql.PreparedStatement;
import java.sql.SQLException;

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
		java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
		if(user.isAnonymous()) {
			addSession(sessionId, utd, user.getAnonymousId(), "ANONYMOUS", timestamp);
		} else {
			// since we dont want to insert the same login multiple times
			// we will only store for the parameter ap
			AccessToken token = user.getAccessToken(ap);
			addSession(sessionId, utd, token.getId(), ap.toString(), timestamp);
		}
	}
	
	private static void addSession(String sessionId, UserTrackingDetails utd, String userId, String type, java.sql.Timestamp timestamp) {
		String query = "INSERT INTO USER_TRACKING "
				+ "(SESSIONID, USERID, TYPE, CREATED_ON, ENDED_ON, "
				+ "IP_ADDR, IP_LAT, IP_LONG, IP_COUNTRY, IP_STATE, IP_CITY) "
				+ "VALUES (?,?,?,?,?,?,?,?,?,?,?)";
		
		PreparedStatement ps = null;
		IRDBMSEngine engine = (IRDBMSEngine) Utility.getDatabase(Constants.USER_TRACKING_DB);
		try {
			ps = engine.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, sessionId);
			ps.setString(index++, userId);
			ps.setString(index++, type);
			ps.setTimestamp(index++, timestamp);
			ps.setNull(index++, java.sql.Types.TIMESTAMP);
			if(utd.getIpAddr() == null) {
				ps.setNull(index++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(index++, utd.getIpAddr());
			}
			if(utd.getIpLat() == null) {
				ps.setNull(index++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(index++, utd.getIpLat());
			}
			if(utd.getIpLong() == null) {
				ps.setNull(index++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(index++, utd.getIpLong());
			}
			if(utd.getIpCountry() == null) {
				ps.setNull(index++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(index++, utd.getIpCountry());
			}
			if(utd.getIpState() == null) {
				ps.setNull(index++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(index++, utd.getIpState());
			}
			if(utd.getIpCity() == null) {
				ps.setNull(index++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(index++, utd.getIpCity());
			}
			
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
					if(ps != null) {
					ps.getConnection().close();
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	@Override
	public void registerLogout(String sessionId) {
		java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
		String query = "UPDATE USER_TRACKING SET ENDED_ON = ? WHERE SESSIONID = ?";
		
		PreparedStatement ps = null;
		IRDBMSEngine engine = (IRDBMSEngine) Utility.getDatabase(Constants.USER_TRACKING_DB);
		try {
			ps = engine.getPreparedStatement(query);
			int index = 1;
			ps.setTimestamp(index++, timestamp);
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
					if(ps != null) {
					ps.getConnection().close();
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

}

package prerna.auth.utils;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.Utility;

public class SecurityTokenUtils extends AbstractSecurityUtils {

	private static final Logger logger = LogManager.getLogger(SecurityTokenUtils.class);

	/**
	 * Only used for static references
	 */
	private SecurityTokenUtils() {
		
	}
	
	/**
	 * Clear expired tokens
	 * @param expirationMinutes
	 */
	public static void clearExpiredTokens(long expirationMinutes) {
		ZonedDateTime zdt = ZonedDateTime.now(ZoneId.of("UTC")).minusMinutes(expirationMinutes);
		String query = "DELETE FROM TOKEN WHERE DATEADDED <= ?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setTimestamp(parameterIndex++, Utility.getSqlTimestampUTC(zdt));
			ps.execute();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
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
	 * Generate a new token for the IP address
	 * @param ipAddr
	 * @return
	 */
	public static Object[] generateToken(String ipAddr, String clientId) {
		String query = "INSERT INTO TOKEN (IPADDR, VAL, DATEADDED, CLIENTID) VALUES (?,?,?,?)";
		String tokenValue = UUID.randomUUID().toString();
		ZonedDateTime zdt = ZonedDateTime.now(ZoneId.of("UTC"));
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, ipAddr);
			ps.setString(parameterIndex++, tokenValue);
			ps.setTimestamp(parameterIndex++, Utility.getSqlTimestampUTC(zdt));
			ps.setString(parameterIndex++, clientId);
			ps.execute();
			logger.debug("Adding new token=" + tokenValue + " for ip=" + ipAddr);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						ps.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
		
		return new Object[] {tokenValue, ipAddr, clientId};
	}
	
	/**
	 * Get the token for the IP address
	 * @param ipAddr
	 * @return
	 */
	public static Object[] getToken(String ipAddr) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("TOKEN__VAL"));
		qs.addSelector(new QueryColumnSelector("TOKEN__IPADDR"));
		qs.addSelector(new QueryColumnSelector("TOKEN__CLIENTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("TOKEN__IPADDR", "==", ipAddr));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				return wrapper.next().getValues();
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return null;
	}
}

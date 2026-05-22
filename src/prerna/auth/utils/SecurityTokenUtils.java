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
package prerna.auth.utils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.ConnectionUtils;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

public class SecurityTokenUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityTokenUtils.class);

	/**
	 * Only used for static references
	 */
	private SecurityTokenUtils() {

	}

	/**
	 * Clear expired tokens
	 * 
	 * @param expirationMinutes
	 */
	public static void clearExpiredTokens(long expirationMinutes) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		ZonedDateTime zdt = ZonedDateTime.now(ZoneId.of("UTC")).minusMinutes(expirationMinutes);
		String query = "DELETE FROM TOKEN WHERE DATEADDED <= ?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setTimestamp(parameterIndex++, Utility.getSqlTimestampUTC(zdt));
			ps.execute();
		} catch (SQLException e) {
			classLogger.error("Unable to clear expired tokens.", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, null);
		}
	}

	/**
	 * Generate a new token for the IP address
	 * 
	 * @param ipAddr
	 * @return
	 */
	public static Object[] generateToken(String ipAddr, String clientId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
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
			classLogger.debug("Adding new token={} for ip={}", tokenValue, ipAddr);
		} catch (SQLException e) {
			classLogger.error("Unable to generate token.", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, ps, null);
		}

		return new Object[] { tokenValue, ipAddr, clientId };
	}

	/**
	 * Get the token for the IP address
	 * 
	 * @param ipAddr
	 * @return
	 */
	public static Object[] getToken(String ipAddr) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("TOKEN__VAL"));
		qs.addSelector(new QueryColumnSelector("TOKEN__IPADDR"));
		qs.addSelector(new QueryColumnSelector("TOKEN__CLIENTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("TOKEN__IPADDR", "==", ipAddr));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				return wrapper.next().getValues();
			}
		} catch (Exception e) {
			classLogger.error("Unable to retrieve token.", e);
		}

		return null;
	}
}

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

public class SecurityShareSessionUtilsUnitTests extends AbstractSecurityUtilsUnitTestsSetup {
	// table and column values (taken from SecurityShareSessionUtils)
	private static final String SESSION_SHARE_TABLE_NAME = "SESSION_SHARE";
	private static final String SHARE_VAL = "SHARE_VAL";
	private static final String SESSION_VAL = "SESSION_VAL";
	private static final String ROUTE_VAL = "ROUTE_VAL";
	private static final String DATE_ADDED = "DATE_ADDED";
	private static final String DATE_USED = "DATE_USED";
	private static final String USE_VALID = "USE_VALID";
	private static final String USERID = "USERID";
	private static final String TYPE = "TYPE";
	private static final String SESSION_SHARE = "IS_SESSION_SHARE";
	private static final String AUTH_SHARE = "IS_AUTH_SHARE";
	
	
	private User user;
	private static String id = "test user id";
	private static String sessionId = "session id";
	private static String routeId = "routeId";
	
	@BeforeEach
	void setUp() throws Exception {
		user = new User();
		user.setPrimaryLogin(prerna.auth.AuthProvider.GOOGLE);
		AccessToken accessToken = new AccessToken();
		accessToken.setProvider(prerna.auth.AuthProvider.GOOGLE);
		accessToken.setId(id);
		user.setGlobalAccessToken(accessToken);

		IRDBMSEngine securityDb = (IRDBMSEngine) SystemEngineRegistry.getSecurityDb();
		// clear the session share table before each test
		securityDb.removeData("DELETE FROM " + SESSION_SHARE_TABLE_NAME);

	}
	
	@Test
	void testCreateTokenForInvalidUser(){
		user.setAnonymous(true);
		// test anonymous user
		Exception e = assertThrows(IllegalArgumentException.class,
                () -> SecurityShareSessionUtils.createToken(user, null, null, false, false));
        assertEquals("Cannot share a session for a user who is not logged in", e.getMessage());
        // test null user
        e = assertThrows(IllegalArgumentException.class,
                () -> SecurityShareSessionUtils.createToken(null, null, null, false, false));
        assertEquals("Cannot share a session for a user who is not logged in", e.getMessage());
	}

	@Test
	void testCreateShareToken() throws Exception {
		String token = SecurityShareSessionUtils.createShareToken(user, sessionId, routeId);
		// validate entry in table
		try (IRawSelectWrapper wrapper = getSessionSharedTableWrapper(null)) {
			int rowCount = 0;
			while (wrapper.hasNext()) {
				Map<String, Object> data = wrapper.next().flushRowToMap();
				assertEquals(token, data.get(SHARE_VAL));
				assertEquals(sessionId, data.get(SESSION_VAL));
				assertEquals(routeId, data.get(ROUTE_VAL));
				assertEquals(id, data.get(USERID));
				assertEquals(prerna.auth.AuthProvider.GOOGLE.getLabel(), data.get(TYPE));
				assertEquals(true, data.get(SESSION_SHARE));
				assertEquals(false, data.get(AUTH_SHARE));
				
				rowCount++;
			}
			assertEquals(1, rowCount);
		}
	}
	
	@Test
	void testCreateAuthToken() throws Exception {
		String token = SecurityShareSessionUtils.createAuthToken(user, sessionId, routeId);
		// validate entry in table
		try (IRawSelectWrapper wrapper = getSessionSharedTableWrapper(null)) {
			int rowCount = 0;
			while (wrapper.hasNext()) {
				Map<String, Object> data = wrapper.next().flushRowToMap();
				assertEquals(token, data.get(SHARE_VAL));
				assertEquals(sessionId, data.get(SESSION_VAL));
				assertEquals(routeId, data.get(ROUTE_VAL));
				assertEquals(id, data.get(USERID));
				assertEquals(prerna.auth.AuthProvider.GOOGLE.getLabel(), data.get(TYPE));
				assertEquals(false, data.get(SESSION_SHARE));
				assertEquals(true, data.get(AUTH_SHARE));
				
				rowCount++;
			}
			assertEquals(1, rowCount);
		}
	}
	
	@Test
	void testGetAndValidateShareSessionDetails() throws SQLException {
		// create the token entry
		String token = SecurityShareSessionUtils.createShareToken(user, sessionId, routeId);
		
		// this will contain 10 values for each column in the share session table
		Object[] sessionDetails = SecurityShareSessionUtils.getShareSessionDetails(token);
		assertEquals(10, sessionDetails.length);
		assertEquals(token, sessionDetails[0]);
		assertEquals(sessionId, sessionDetails[1]); //SESSION_VAL
		assertEquals(routeId, sessionDetails[2]); //ROUTE_VAL
		assertNull(sessionDetails[5]); // USE_VALID
		assertEquals(true, sessionDetails[6]); //SESSION_SHARE
		assertEquals(false, sessionDetails[7]); //AUTH_SHARE
		assertEquals(id, sessionDetails[8]); //USERID
		assertEquals(prerna.auth.AuthProvider.GOOGLE.getLabel(), sessionDetails[9]); //TYPE
		// since the session has not been used and is less thn 5 minutes old, it is successful
		assertTrue(SecurityShareSessionUtils.validateShareSessionDetails(sessionDetails));
	}
	
	@Test
	void testGetAndValidateShareSessionDetailsFail() throws SQLException {
		// create the token entry
		String token = SecurityShareSessionUtils.createShareToken(user, sessionId, routeId);
		
		// this will contain 10 values for each column in the share session table
		Object[] sessionDetails = SecurityShareSessionUtils.getShareSessionDetails(token);
		assertEquals(10, sessionDetails.length);
		assertEquals(token, sessionDetails[0]);
		assertEquals(sessionId, sessionDetails[1]); //SESSION_VAL
		assertEquals(routeId, sessionDetails[2]); //ROUTE_VAL
		assertNull(sessionDetails[5]); // USE_VALID
		assertEquals(true, sessionDetails[6]); //SESSION_SHARE
		assertEquals(false, sessionDetails[7]); //AUTH_SHARE
		assertEquals(id, sessionDetails[8]); //USERID
		assertEquals(prerna.auth.AuthProvider.GOOGLE.getLabel(), sessionDetails[9]); //TYPE
		// set useValid boolean value to true
		sessionDetails[5] = true;
		// fails to validate due to boolean indicating session was already used
		Exception e = assertThrows(IllegalArgumentException.class,
                () -> SecurityShareSessionUtils.validateShareSessionDetails(sessionDetails));
        assertEquals("share key has already been used", e.getMessage());
	}
	
	@Test
	void testLogSessionUsed() throws Exception {
		// create the token entry
		String token = SecurityShareSessionUtils.createShareToken(user, sessionId, routeId);
		// verify session is not logged as "used"
		try (IRawSelectWrapper wrapper = getSessionSharedTableWrapper(null)) {
			int rowCount = 0;
			while (wrapper.hasNext()) {
				Map<String, Object> data = wrapper.next().flushRowToMap();
				assertTrue(data.containsKey(USE_VALID));
				assertNull(data.get(USE_VALID));
				rowCount++;
			}
			assertEquals(1, rowCount);
		}
		// log session as used
		SecurityShareSessionUtils.logSessionUsed(token, Utility.getCurrentZonedDateTimeUTC(), true);
		// verify session updated on table
		try (IRawSelectWrapper wrapper = getSessionSharedTableWrapper(null)) {
			int rowCount = 0;
			while (wrapper.hasNext()) {
				Map<String, Object> data = wrapper.next().flushRowToMap();
				assertTrue(data.containsKey(USE_VALID));
				assertTrue((Boolean)data.get(USE_VALID));
				rowCount++;
			}
			assertEquals(1, rowCount);
		}
	}
	
	@Test
	void testGenerateAccessTokenForShareAuth() throws Exception {
		// add user to security DB
		String email = "test123@test.com";
		String type = prerna.auth.AuthProvider.GOOGLE.getLabel();
		String name = "Test User";
		String password = "password123";
		String phone = "5551234567";
		String phoneextension = "001";
		String countrycode = "US";
		boolean admin = true;
		boolean publisher = false;
		boolean exporter = false;
		String modelUsageRestriction = null;
		String modelUsageFrequency = null;
		Integer modelMaxTokens = null;
		Double modelMaxResponseTime = null;

		boolean success = SecurityUpdateUtils.registerUser(id, name, email, password, type, phone, phoneextension,
				countrycode, admin, publisher, exporter, modelUsageRestriction, modelUsageFrequency, modelMaxTokens,
				modelMaxResponseTime);

		assertTrue(success, "Insertion of new user was not successful");
		// create share token
		String token = SecurityShareSessionUtils.createAuthToken(user, sessionId, routeId);
		// retrieve details
		Object[] sessionDetails = SecurityShareSessionUtils.getShareSessionDetails(token);
		AccessToken accessToken = SecurityShareSessionUtils.generateAccessTokenForShareAuth(sessionDetails);
		assertNotNull(accessToken);
		assertEquals(prerna.auth.AuthProvider.GOOGLE, accessToken.getProvider());
		assertEquals(name, accessToken.getName());
		assertEquals("ADMIN_ADDED_USER", accessToken.getUsername());
		assertEquals(email, accessToken.getEmail());
	}
	
	// used to get a wrapper on the table to validate tests
	// this wrapper needs to be closed after use
	private IRawSelectWrapper getSessionSharedTableWrapper(String shareToken) throws Exception {
		IRDBMSEngine securityDb = (IRDBMSEngine) SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(SESSION_SHARE_TABLE_NAME + "__" + SHARE_VAL));
		qs.addSelector(new QueryColumnSelector(SESSION_SHARE_TABLE_NAME + "__" + SESSION_VAL));
		qs.addSelector(new QueryColumnSelector(SESSION_SHARE_TABLE_NAME + "__" + ROUTE_VAL));
		qs.addSelector(new QueryColumnSelector(SESSION_SHARE_TABLE_NAME + "__" + DATE_ADDED));
		qs.addSelector(new QueryColumnSelector(SESSION_SHARE_TABLE_NAME + "__" + DATE_USED));
		qs.addSelector(new QueryColumnSelector(SESSION_SHARE_TABLE_NAME + "__" + USE_VALID));
		qs.addSelector(new QueryColumnSelector(SESSION_SHARE_TABLE_NAME + "__" + USERID));
		qs.addSelector(new QueryColumnSelector(SESSION_SHARE_TABLE_NAME + "__" + TYPE));
		qs.addSelector(new QueryColumnSelector(SESSION_SHARE_TABLE_NAME + "__" + SESSION_SHARE));
		qs.addSelector(new QueryColumnSelector(SESSION_SHARE_TABLE_NAME + "__" + AUTH_SHARE));
		if (shareToken != null) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(SESSION_SHARE_TABLE_NAME + "__" + SHARE_VAL, "==",
					shareToken));
		}
		return WrapperManager.getInstance().getRawWrapper(securityDb, qs);
	}
}

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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.SystemEngineRegistry;

public class SecurityUserAccessKeyUtilsUnitTests extends AbstractSecurityUtilsUnitTestsSetup {
	
	private static final String SMSS_USER_ACCESS_KEYS_TABLE_NAME = "SMSS_USER_ACCESS_KEYS";
	private static final String USERID_COL = "USERID";
	private static final String TYPE_COL = "TYPE";
	private static final String ACCESS_KEY_COL = "ACCESSKEY";
	private static final String SECRET_KEY_COL = "SECRETKEY";
	private static final String SECRET_KEY_SALT_COL =  "SECRETSALT";
	private static final String DATE_CREATED_COL =  "DATECREATED";
	private static final String LAST_USED_COL =  "LASTUSED";

	private static final String TOKEN_NAME_COL = "TOKENNAME";
	private static final String TOKEN_DESCRIPTION_COL = "TOKENDESCRIPTION";

	private static final String SMSS_USER_TABLE_NAME = "SMSS_USER";
	private static final String SMSS_USER_USERID_COL = SMSS_USER_TABLE_NAME + "__ID";
	private static final String NAME_COL = SMSS_USER_TABLE_NAME + "__NAME";
	private static final String USERNAME_COL = SMSS_USER_TABLE_NAME + "__USERNAME";
	private static final String EMAIL_COL = SMSS_USER_TABLE_NAME + "__EMAIL";
	
	private User user;
	private String id = "test user id";
	private String email = "test123@test.com";
	private String type = prerna.auth.AuthProvider.GOOGLE.getLabel();
	private String name = "Test User";
	private String password = "password123";
	private String phone = "5551234567";
	private String phoneextension = "001";
	private String countrycode = "US";
	private boolean admin = true;
	private boolean publisher = false;
	private boolean exporter = false;
	private String modelUsageRestriction = null;
	private String modelUsageFrequency = null;
	private Integer modelMaxTokens = null;
	private Double modelMaxResponseTime = null;
	AccessToken accessToken = null;

	@BeforeEach
	void setUp() throws SQLException {
		user = new User();
		user.setPrimaryLogin(prerna.auth.AuthProvider.GOOGLE);
		accessToken = new AccessToken();
		accessToken.setProvider(prerna.auth.AuthProvider.GOOGLE);
		accessToken.setId(id);
		accessToken.setName(name);
		accessToken.setUsername("ADMIN_ADDED_USER");
		accessToken.setEmail(email);
		user.setGlobalAccessToken(accessToken);

		IRDBMSEngine securityDb = (IRDBMSEngine) SystemEngineRegistry.getSecurityDb();
		// clear the session share table before each test
		Connection conn = null;
		Statement s = null;
		List<String> tables = List.of(SMSS_USER_ACCESS_KEYS_TABLE_NAME, SMSS_USER_TABLE_NAME);
		try {
			conn = securityDb.getConnection();
			s = conn.createStatement();
			for (String t : tables) {
				s.addBatch("DELETE FROM " + t);
			}
			s.executeBatch();
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, conn, s, null);
		}
		boolean success = SecurityUpdateUtils.registerUser(id, name, email, password, type, phone, phoneextension,
				countrycode, admin, publisher, exporter, modelUsageRestriction, modelUsageFrequency, modelMaxTokens,
				modelMaxResponseTime);
		assertTrue(success, "Insertion of new user was not successful");
	}
	
	@Test
	void testCreateUserAccessToken() throws Exception {
		String tokenName = "test token name";
		String tokenDescription = "test token description";
		Map<String, String> accessTokenMap = SecurityUserAccessKeyUtils.createUserAccessToken(accessToken, tokenName, tokenDescription);
		assertNotNull(accessTokenMap);
		assertFalse(accessTokenMap.isEmpty());
		try (IRawSelectWrapper wrapper = getAccessKeyTableWrapper()) {
			assertNotNull(wrapper);
			assertTrue(wrapper.hasNext());
			assertEquals(1, wrapper.getNumRows());
			Map<String, Object> data = wrapper.next().flushRowToMap();
			assertEquals(id, data.get(USERID_COL));
			assertEquals(type, data.get(TYPE_COL));
			assertNotNull(data.get(ACCESS_KEY_COL));
			assertNotNull(data.get(SECRET_KEY_COL));
			assertNotNull(data.get(SECRET_KEY_SALT_COL));
			assertNull(data.get(LAST_USED_COL));
			assertEquals(tokenName, data.get(TOKEN_NAME_COL));
			assertEquals(tokenDescription, data.get(TOKEN_DESCRIPTION_COL));

		}
	}
	
	@Test
	void testUpdateAccessTokenLastUsed() throws Exception {
		String tokenName = "test token name";
		String tokenDescription = "test token description";
		// add access token to table
		Map<String, String> accessTokenMap = SecurityUserAccessKeyUtils.createUserAccessToken(accessToken, tokenName, tokenDescription);
		assertNotNull(accessTokenMap);
		assertFalse(accessTokenMap.isEmpty());
		String accessKey = (String)accessTokenMap.get(ACCESS_KEY_COL);
		assertNotNull(accessKey); // verify valid access was retrieved
		assertFalse(accessKey.equals(""));
		// update lastUsed
		SecurityUserAccessKeyUtils.updateAccessTokenLastUsed(accessKey);
		// verify lastUsed has been updated
		try (IRawSelectWrapper wrapper = getAccessKeyTableWrapper()) {
			assertNotNull(wrapper);
			assertTrue(wrapper.hasNext());
			assertEquals(1, wrapper.getNumRows());
			Map<String, Object> data = wrapper.next().flushRowToMap();
			assertNotNull(data.get(LAST_USED_COL));
		}
	}
	
	@Test
	void testValidateKeysAndReturnUser() throws Exception {
		String tokenName = "test token name";
		String tokenDescription = "test token description";
		// add access token to table
		Map<String, String> accessTokenMap = SecurityUserAccessKeyUtils.createUserAccessToken(accessToken, tokenName, tokenDescription);
		assertNotNull(accessTokenMap);
		assertFalse(accessTokenMap.isEmpty());
		/// verify user was added to table
		try (IRawSelectWrapper wrapper = getAccessKeyTableWrapper()) {
			assertNotNull(wrapper);
			assertTrue(wrapper.hasNext());
			assertEquals(1, wrapper.getNumRows());
			Map<String, Object> data = wrapper.next().flushRowToMap();
			assertEquals(id, data.get(USERID_COL));
			assertEquals(type, data.get(TYPE_COL));
			assertNotNull(data.get(ACCESS_KEY_COL));
			assertNotNull(data.get(SECRET_KEY_COL));
			assertNotNull(data.get(SECRET_KEY_SALT_COL));
			assertNull(data.get(LAST_USED_COL));
			assertEquals(tokenName, data.get(TOKEN_NAME_COL));
			assertEquals(tokenDescription, data.get(TOKEN_DESCRIPTION_COL));
		}
		String accessKey = (String)accessTokenMap.get(ACCESS_KEY_COL);
		String secretKey = (String)accessTokenMap.get(SECRET_KEY_COL);
		User returnedUser = SecurityUserAccessKeyUtils.validateKeysAndReturnUser(accessKey, secretKey);
		AccessToken returnedAccessToken = returnedUser.getAccessToken(prerna.auth.AuthProvider.GOOGLE);
		assertNotNull(returnedAccessToken);
		assertEquals(accessToken.getProvider(), returnedAccessToken.getProvider());
		assertEquals(accessToken.getId(), returnedAccessToken.getId());
		assertEquals(accessToken.getName(), returnedAccessToken.getName());
		assertEquals(accessToken.getUsername(), returnedAccessToken.getUsername());
		assertEquals(accessToken.getEmail(), returnedAccessToken.getEmail());
	}
	
	@Test
	void testDeleteUserAccessToken() throws Exception {
		/// add user and create user access token
		String tokenName = "test token name";
		String tokenDescription = "test token description";
		// add access token to table
		Map<String, String> accessTokenMap = SecurityUserAccessKeyUtils.createUserAccessToken(accessToken, tokenName, tokenDescription);
		assertNotNull(accessTokenMap);
		assertFalse(accessTokenMap.isEmpty());
		/// verify user was added to table
		try (IRawSelectWrapper wrapper = getAccessKeyTableWrapper()) {
			assertNotNull(wrapper);
			assertTrue(wrapper.hasNext());
			assertEquals(1, wrapper.getNumRows());
		}
		String accessKey = (String) accessTokenMap.get(ACCESS_KEY_COL);
		String secretKey = (String) accessTokenMap.get(SECRET_KEY_COL);
		User returnedUser = SecurityUserAccessKeyUtils.validateKeysAndReturnUser(accessKey, secretKey);
		AccessToken returnedAccessToken = returnedUser.getAccessToken(prerna.auth.AuthProvider.GOOGLE);
		assertNotNull(returnedAccessToken);
		/// delete user from table
		SecurityUserAccessKeyUtils.deleteUserAccessToken(returnedAccessToken, accessKey);
		/// verify user has been removed from table
		try (IRawSelectWrapper wrapper = getAccessKeyTableWrapper()) {
			assertNotNull(wrapper);
			assertFalse(wrapper.hasNext());
			assertEquals(0, wrapper.getNumRows());
		}
	}
	
	@Test
	void testGetUserAccessKeyInfo() throws Exception {
		/// add user and create user access token
		String tokenName = "test token name";
		String tokenDescription = "test token description";
		// add access token to table
		Map<String, String> accessTokenMap = SecurityUserAccessKeyUtils.createUserAccessToken(accessToken, tokenName,
				tokenDescription);
		assertNotNull(accessTokenMap);
		assertFalse(accessTokenMap.isEmpty());
		/// verify user was added to table
		try (IRawSelectWrapper wrapper = getAccessKeyTableWrapper()) {
			assertNotNull(wrapper);
			assertTrue(wrapper.hasNext());
			assertEquals(1, wrapper.getNumRows());
		}
		String accessKey = (String) accessTokenMap.get(ACCESS_KEY_COL);
		String secretKey = (String) accessTokenMap.get(SECRET_KEY_COL);
		User returnedUser = SecurityUserAccessKeyUtils.validateKeysAndReturnUser(accessKey, secretKey);
		AccessToken returnedAccessToken = returnedUser.getAccessToken(prerna.auth.AuthProvider.GOOGLE);
		assertNotNull(returnedAccessToken);
		/// retrieve user access key info
		List<Map<String, Object>> retrievedUserAccessKeyInfoMap = SecurityUserAccessKeyUtils.getUserAccessKeyInfo(returnedAccessToken, accessKey);
		assertNotNull(retrievedUserAccessKeyInfoMap);
		assertEquals(1, retrievedUserAccessKeyInfoMap.size());
		Map<String, Object> retrievedUserAccessKeyMap = retrievedUserAccessKeyInfoMap.get(0);
		assertEquals(tokenName, retrievedUserAccessKeyMap.get(TOKEN_NAME_COL));
		assertEquals(tokenDescription, retrievedUserAccessKeyMap.get(TOKEN_DESCRIPTION_COL));
		assertEquals(accessKey, retrievedUserAccessKeyMap.get(ACCESS_KEY_COL));
		assertNotNull(retrievedUserAccessKeyMap.get(DATE_CREATED_COL));
		assertNull(retrievedUserAccessKeyMap.get(LAST_USED_COL));
	}

	// used to get a wrapper on the table to validate tests
	// this wrapper needs to be closed after use
	private IRawSelectWrapper getAccessKeyTableWrapper() throws Exception {
		IRDBMSEngine securityDb = (IRDBMSEngine) SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(SMSS_USER_ACCESS_KEYS_TABLE_NAME + "__" + USERID_COL));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_ACCESS_KEYS_TABLE_NAME + "__" + TYPE_COL));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_ACCESS_KEYS_TABLE_NAME + "__" + ACCESS_KEY_COL));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_ACCESS_KEYS_TABLE_NAME + "__" + SECRET_KEY_COL));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_ACCESS_KEYS_TABLE_NAME + "__" + SECRET_KEY_SALT_COL));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_ACCESS_KEYS_TABLE_NAME + "__" + DATE_CREATED_COL));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_ACCESS_KEYS_TABLE_NAME + "__" + LAST_USED_COL));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_ACCESS_KEYS_TABLE_NAME + "__" + TOKEN_NAME_COL));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_ACCESS_KEYS_TABLE_NAME + "__" + TOKEN_DESCRIPTION_COL));
		return WrapperManager.getInstance().getRawWrapper(securityDb, qs);
	}
}

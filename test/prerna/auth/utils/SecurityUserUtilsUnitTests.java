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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.auth.AccessToken;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.SystemEngineRegistry;

public class SecurityUserUtilsUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private static String userId = "test123";
	private static String metaKey = "testKey123";
	private static String metaValue = "testValue123";
	private static String type = prerna.auth.AuthProvider.GOOGLE.toString();
	private static Integer metaOrder = 5;

	private static String singleMulti = "testSingleMulti456";
	private static Integer displayOrder = 1;
	private static String displayOptions = "testDefaultOptions456";
	private static String defaultValues = "testDefaultValue456";

	@BeforeEach
	void tearDownAndSetUpUserMetaTables() throws SQLException {
		IRDBMSEngine securityDb = (IRDBMSEngine) SystemEngineRegistry.getSecurityDb();
		// clear out tables
		Connection conn = null;
		Statement s = null;
		List<String> tables = List.of("USERMETA", Constants.USER_METAKEYS);
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

		String psString = "INSERT INTO USERMETA (USERID, TYPE, METAKEY, METAVALUE, METAORDER) " + "VALUES (?,?,?,?,?)";

		PreparedStatement ps = null;
		try {
			conn = securityDb.getConnection();
			ps = conn.prepareStatement(psString);

			int index = 1;
			ps.setString(index++, userId);
			ps.setString(index++, type);
			ps.setString(index++, metaKey);
			ps.setString(index++, metaValue);
			ps.setInt(index++, metaOrder);

			int result = ps.executeUpdate();
			if (result == PreparedStatement.EXECUTE_FAILED) {
				throw new SQLException("Error inserting USERMETA data");
			}

			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, conn, ps, null);
		}

		psString = "INSERT INTO USERMETAKEYS (METAKEY, SINGLEMULTI, DISPLAYORDER, DISPLAYOPTIONS, DEFAULTVALUES) "
				+ "VALUES (?,?,?,?,?)";

		try {
			conn = securityDb.getConnection();
			ps = conn.prepareStatement(psString);

			int index = 1;
			ps.setString(index++, metaKey);
			ps.setString(index++, singleMulti);
			ps.setInt(index++, displayOrder);
			ps.setString(index++, displayOptions);
			ps.setString(index++, defaultValues);

			int result = ps.executeUpdate();
			if (result == PreparedStatement.EXECUTE_FAILED) {
				throw new SQLException("Error inserting USERMETA data");
			}

			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, conn, ps, null);
		}
	}

	@Test
	void testGetAggregateUserMetadata() throws Exception {
		Map<String, Collection<String>> aggregateData = SecurityUserUtils.getAggregateUserMetadata(userId,
				prerna.auth.AuthProvider.GOOGLE, null, true);
		assertEquals(1, aggregateData.size());
		assertTrue(aggregateData.containsKey(metaKey));
		Collection<String> values = aggregateData.get(metaKey);
		assertNotNull(values);
		assertEquals(1, values.size());
		assertTrue(values.contains(metaValue));
	}

	@Test
	void testGetUserMetaDataWrapper() throws Exception {
		List<String> userMetaCols = List.of("USERID", "TYPE", "METAKEY", "METAVALUE", "METAORDER");
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = SecurityUserUtils.getUserMetadataWrapper(null, null, null, true);
			assertTrue(wrapper.hasNext());
			int count = 0;
			while (wrapper.hasNext()) {
				count++;
				Map<String, Object> data = wrapper.next().flushRowToMap();
				assertTrue(data.keySet().containsAll(userMetaCols));
				assertEquals(userId, data.get(userMetaCols.get(0)));
				assertEquals(type, data.get(userMetaCols.get(1)));
				assertEquals(metaKey, data.get(userMetaCols.get(2)));
				assertEquals(metaValue, data.get(userMetaCols.get(3)));
				assertEquals(metaOrder, data.get(userMetaCols.get(4)));
			}
			assertEquals(1, count);
		} finally {
			wrapper.close();
		}
	}

	@Test
	void testGetMetaKeyOptions() {
		List<Map<String, Object>> options = SecurityUserUtils.getMetakeyOptions(null);
		List<String> userMetaKeyCols = List.of("metakey", "single_multi", "display_order", "display_options",
				"display_values");
		assertEquals(1, options.size());
		for (Map<String, Object> optionsMap : options) {
			assertTrue(optionsMap.keySet().containsAll(userMetaKeyCols));
			assertEquals(metaKey, optionsMap.get(userMetaKeyCols.get(0)));
			assertEquals(singleMulti, optionsMap.get(userMetaKeyCols.get(1)));
			assertEquals(displayOrder, optionsMap.get(userMetaKeyCols.get(2)));
			assertEquals(displayOptions, optionsMap.get(userMetaKeyCols.get(3)));
			assertEquals(defaultValues, optionsMap.get(userMetaKeyCols.get(4)));
		}
	}

	@Test
	void testGetAllMetaKeys() {
		List<String> allMetaKeys = SecurityUserUtils.getAllMetakeys();
		assertEquals(1, allMetaKeys.size());
		assertEquals(metaKey, allMetaKeys.get(0));
	}

	@Test
	void testUpdateUserMetadata() throws IOException, Exception {
		List<String> userMetaCols = List.of("USERID", "TYPE", "METAKEY", "METAVALUE", "METAORDER");
		try (IRawSelectWrapper wrapper = SecurityUserUtils.getUserMetadataWrapper(null, null, null, true)) {
			assertNotNull(wrapper);
			assertTrue(wrapper.hasNext());
			int count = 0;
			while (wrapper.hasNext()) {
				count++;
				Map<String, Object> data = wrapper.next().flushRowToMap();
				assertTrue(data.keySet().containsAll(userMetaCols));
				assertEquals(userId, data.get(userMetaCols.get(0)));
				assertEquals(type, data.get(userMetaCols.get(1)));
				assertEquals(metaKey, data.get(userMetaCols.get(2)));
				assertEquals(metaValue, data.get(userMetaCols.get(3)));
				assertEquals(metaOrder, data.get(userMetaCols.get(4)));
			}
			assertEquals(1, count);
		}

		String updatedMetaValue = "NEW testValue123";
		int updatedMetaOrder = 0;
		Map<String, Collection<String>> metaData = Map.of(metaKey, Arrays.asList(updatedMetaValue));
		AccessToken token = new AccessToken();
		token.setId(userId);
		token.setProvider(prerna.auth.AuthProvider.GOOGLE);
		SecurityUserUtils.updateUserMetadata(token, metaData);
		// verify user had meta data value modified
		try (IRawSelectWrapper wrapper = SecurityUserUtils.getUserMetadataWrapper(null, null, null, true)) {
			assertNotNull(wrapper);
			assertTrue(wrapper.hasNext());
			int count = 0;
			while (wrapper.hasNext()) {
				count++;
				Map<String, Object> data = wrapper.next().flushRowToMap();
				assertTrue(data.keySet().containsAll(userMetaCols));
				assertEquals(userId, data.get(userMetaCols.get(0)));
				assertEquals(type, data.get(userMetaCols.get(1)));
				assertEquals(metaKey, data.get(userMetaCols.get(2)));
				assertEquals(updatedMetaValue, data.get(userMetaCols.get(3)));
				assertEquals(updatedMetaOrder, data.get(userMetaCols.get(4)));
			}
			assertEquals(1, count);
		}
	}

	@Test
	void testUpdateMetakeyOptions() {
		List<String> userMetaKeyCols = List.of("metakey", "single_multi", "display_order", "display_options",
				"display_values");
		// check initial meta key options
		List<Map<String, Object>> options = SecurityUserUtils.getMetakeyOptions(null);
		assertEquals(1, options.size());
		for (Map<String, Object> optionsMap : options) {
			assertTrue(optionsMap.keySet().containsAll(userMetaKeyCols));
			assertEquals(metaKey, optionsMap.get(userMetaKeyCols.get(0)));
			assertEquals(singleMulti, optionsMap.get(userMetaKeyCols.get(1)));
			assertEquals(displayOrder, optionsMap.get(userMetaKeyCols.get(2)));
			assertEquals(displayOptions, optionsMap.get(userMetaKeyCols.get(3)));
			assertEquals(defaultValues, optionsMap.get(userMetaKeyCols.get(4)));
		}
		String updatedMetaKey = "NEW META KEY";
		String updatedSingleMulti = "NEW testSingleMulti456";
		Integer updatedDisplayOrder = 10;
		String updatedDisplayOptions = "NEW testDefaultOptions456";
		String updatedDefaultValues = "NEW testDefaultValue456";
		Map<String, Object> updatedMetaKeyOptions = Map.of("metakey", updatedMetaKey, "single_multi",
				updatedSingleMulti, "display_order", updatedDisplayOrder, "display_options", updatedDisplayOptions,
				"display_values", updatedDefaultValues);
		assertTrue(SecurityUserUtils.updateMetakeyOptions(List.of(updatedMetaKeyOptions)));
		// validate new entry
		List<Map<String, Object>> updatedOptions = SecurityUserUtils.getMetakeyOptions(null);
		assertEquals(1, updatedOptions.size());
		for (Map<String, Object> optionsMap : updatedOptions) {
			assertTrue(optionsMap.keySet().containsAll(userMetaKeyCols));
			assertEquals(updatedMetaKey, optionsMap.get(userMetaKeyCols.get(0)));
			assertEquals(updatedSingleMulti, optionsMap.get(userMetaKeyCols.get(1)));
			assertEquals(updatedDisplayOrder, optionsMap.get(userMetaKeyCols.get(2)));
			assertEquals(updatedDisplayOptions, optionsMap.get(userMetaKeyCols.get(3)));
			assertEquals(updatedDefaultValues, optionsMap.get(userMetaKeyCols.get(4)));
		}
	}
}

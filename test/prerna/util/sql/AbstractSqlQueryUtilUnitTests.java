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
package prerna.util.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import prerna.engine.impl.CaseInsensitiveProperties;

class AbstractSqlQueryUtilUnitTests {

	/**
	 * These helpers are plain, stateless logic with no instance fields involved -
	 * any concrete dialect works as the receiver. Deliberately called through an
	 * instance (not statically): PGVectorDatabaseEngineUnitTests mockStatics
	 * AbstractSqlQueryUtil to stub makeConnection(), and unstubbed static calls on
	 * a Mockito static mock silently return null/false - these must stay instance
	 * methods so that mock can't intercept them.
	 */
	private final AbstractSqlQueryUtil util = new H2QueryUtil();

	@Test
	void h2RoundTripsExactValues() throws Exception {
		try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:binding_" + UUID.randomUUID())) {
			NullableParameterBindingUnitTests.roundTrip(connection, new H2QueryUtil());
		}
	}

	@Test
	void nullableStringVariantsPreserveTheirDistinctEmptyAndWhitespaceContracts() throws Exception {
		AbstractSqlQueryUtil[] dialects = { new H2QueryUtil(), new PostgresQueryUtil(), new SQLiteQueryUtil(),
				new MicrosoftSqlServerQueryUtil() };
		for (AbstractSqlQueryUtil dialect : dialects) {
			var ps = mock(java.sql.PreparedStatement.class);
			dialect.setNullableString(ps, 1, null);
			dialect.setNullableString(ps, 2, "");
			dialect.setNullableString(ps, 3, " 	 ");
			dialect.setStringEmptyAsNullable(ps, 4, null);
			dialect.setStringEmptyAsNullable(ps, 5, "");
			dialect.setStringEmptyAsNullable(ps, 6, " 	 ");
			dialect.setStringEmptyAsNullable(ps, 7, "  café 世界  ");
			verify(ps).setNull(1, java.sql.Types.VARCHAR);
			verify(ps).setString(2, "");
			verify(ps).setString(3, " 	 ");
			verify(ps).setNull(4, java.sql.Types.VARCHAR);
			verify(ps).setNull(5, java.sql.Types.VARCHAR);
			verify(ps).setString(6, " 	 ");
			verify(ps).setString(7, "  café 世界  ");
			verifyNoMoreInteractions(ps);
		}
	}

	@Test
	void optionalStringValueFromMapReturnsDefaultWhenKeyAbsent() {
		Map<String, Object> configMap = new HashMap<>();
		assertEquals("fallback", util.getOptionalStringValue(configMap, "missingKey", "fallback"));
	}

	@Test
	void optionalStringValueFromMapReturnsValueWhenPresent() {
		Map<String, Object> configMap = new HashMap<>();
		configMap.put("hostname", "db.example.com");
		assertEquals("db.example.com", util.getOptionalStringValue(configMap, "hostname", null));
	}

	@Test
	void optionalStringValueFromMapRejectsWrongTypeNamingTheKey() {
		Map<String, Object> configMap = new HashMap<>();
		configMap.put("port", Integer.valueOf(5432));
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
				() -> util.getOptionalStringValue(configMap, "port", null));
		assertTrue(ex.getMessage().contains("port"), "message should name the offending key: " + ex.getMessage());
		assertTrue(ex.getMessage().contains("Integer"), "message should name the actual type: " + ex.getMessage());
	}

	@Test
	void optionalStringValueFromPropertiesMirrorsMapBehavior() {
		CaseInsensitiveProperties prop = new CaseInsensitiveProperties();
		assertEquals("fallback", util.getOptionalStringValue(prop, "missingKey", "fallback"));

		prop.put("hostname", "db.example.com");
		assertEquals("db.example.com", util.getOptionalStringValue(prop, "hostname", null));

		prop.put("port", Integer.valueOf(5432));
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
				() -> util.getOptionalStringValue(prop, "port", null));
		assertTrue(ex.getMessage().contains("port"), "message should name the offending key: " + ex.getMessage());
	}

	@Test
	void optionalBooleanValueFromMapReturnsDefaultWhenKeyAbsent() {
		Map<String, Object> configMap = new HashMap<>();
		assertTrue(util.getOptionalBooleanValue(configMap, "forceFile", true));
		assertFalse(util.getOptionalBooleanValue(configMap, "forceFile", false));
	}

	@Test
	void optionalBooleanValueFromMapAcceptsActualBooleanAndStringForms() {
		Map<String, Object> configMap = new HashMap<>();
		configMap.put("forceFile", Boolean.TRUE);
		assertTrue(util.getOptionalBooleanValue(configMap, "forceFile", false));

		configMap.put("forceFile", "true");
		assertTrue(util.getOptionalBooleanValue(configMap, "forceFile", false));

		configMap.put("forceFile", "false");
		assertFalse(util.getOptionalBooleanValue(configMap, "forceFile", true));
	}

	@Test
	void optionalBooleanValueFromMapRejectsWrongTypeNamingTheKey() {
		Map<String, Object> configMap = new HashMap<>();
		configMap.put("forceFile", java.util.List.of("not", "a", "boolean"));
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
				() -> util.getOptionalBooleanValue(configMap, "forceFile", false));
		assertTrue(ex.getMessage().contains("forceFile"), "message should name the offending key: " + ex.getMessage());
	}

	@Test
	void requireNonBlankPassesThroughNonEmptyValueUnchanged() {
		assertEquals("db.example.com", util.requireNonBlank("hostname", "db.example.com"));
	}

	@Test
	void requireNonBlankRejectsNullAndEmptyNamingTheKey() {
		IllegalStateException nullEx = assertThrows(IllegalStateException.class,
				() -> util.requireNonBlank("hostname", null));
		assertTrue(nullEx.getMessage().contains("hostname"), "message should name the key: " + nullEx.getMessage());

		IllegalStateException emptyEx = assertThrows(IllegalStateException.class,
				() -> util.requireNonBlank("hostname", ""));
		assertTrue(emptyEx.getMessage().contains("hostname"), "message should name the key: " + emptyEx.getMessage());
	}
}

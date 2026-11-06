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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import prerna.engine.impl.CaseInsensitiveProperties;

class H2QueryUtilUnitTests {

	@Test
	void missingHostnameWithoutConnectionUrlFailsFastNamingTheKey() {
		Map<String, Object> connDetails = new HashMap<>();
		// present but irrelevant key, so this exercises the hostname check and not
		// the null-or-empty-map guard
		connDetails.put(AbstractSqlQueryUtil.USERNAME, "sa");

		H2QueryUtil util = new H2QueryUtil();
		IllegalStateException ex = assertThrows(IllegalStateException.class,
				() -> util.setConnectionDetailsfromMap(connDetails));
		assertTrue(ex.getMessage().contains(AbstractSqlQueryUtil.HOSTNAME),
				"message should name the missing key: " + ex.getMessage());
	}

	@Test
	void wrongTypeHostnameFailsFastNamingTheKeyAndType() {
		Map<String, Object> connDetails = new HashMap<>();
		connDetails.put(AbstractSqlQueryUtil.HOSTNAME, Integer.valueOf(12345));

		H2QueryUtil util = new H2QueryUtil();
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
				() -> util.setConnectionDetailsfromMap(connDetails));
		assertTrue(ex.getMessage().contains(AbstractSqlQueryUtil.HOSTNAME),
				"message should name the offending key: " + ex.getMessage());
		assertTrue(ex.getMessage().contains("Integer"), "message should name the actual type: " + ex.getMessage());
	}

	@Test
	void wrongTypeForceFileFailsFastNamingTheKey() {
		Map<String, Object> connDetails = new HashMap<>();
		connDetails.put(AbstractSqlQueryUtil.HOSTNAME, "db.example.com");
		connDetails.put(AbstractSqlQueryUtil.FORCE_FILE, java.util.List.of("not", "a", "boolean"));

		H2QueryUtil util = new H2QueryUtil();
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
				() -> util.setConnectionDetailsfromMap(connDetails));
		assertTrue(ex.getMessage().contains(AbstractSqlQueryUtil.FORCE_FILE),
				"message should name the offending key: " + ex.getMessage());
	}

	@Test
	void connectionUrlProvidedDirectlyBypassesHostnameRequirement() {
		Map<String, Object> connDetails = new HashMap<>();
		String rawUrl = "jdbc:h2:mem:bypass_" + UUID.randomUUID();
		connDetails.put(AbstractSqlQueryUtil.CONNECTION_URL, rawUrl);

		H2QueryUtil util = new H2QueryUtil();
		String connectionUrl = util.setConnectionDetailsfromMap(connDetails);
		assertEquals(rawUrl, connectionUrl);
	}

	@Test
	void forceFileDefaultsToFalseWhenAbsent() {
		Map<String, Object> connDetails = new HashMap<>();
		connDetails.put(AbstractSqlQueryUtil.HOSTNAME, "/path/that/definitely/does/not/exist/" + UUID.randomUUID());

		H2QueryUtil util = new H2QueryUtil();
		String connectionUrl = util.setConnectionDetailsfromMap(connDetails);
		assertTrue(connectionUrl.contains(":tcp://"),
				"forceFile should default to false, using tcp mode for a nonexistent path: " + connectionUrl);
	}

	@Test
	void missingHostnameFromSmssPropertiesFailsFastNamingTheKey() {
		CaseInsensitiveProperties prop = new CaseInsensitiveProperties();
		prop.put(AbstractSqlQueryUtil.USERNAME, "sa");

		H2QueryUtil util = new H2QueryUtil();
		IllegalStateException ex = assertThrows(IllegalStateException.class,
				() -> util.setConnectionDetailsFromSMSS(prop));
		assertTrue(ex.getMessage().contains(AbstractSqlQueryUtil.HOSTNAME),
				"message should name the missing key: " + ex.getMessage());
	}

	@Test
	void wrongTypeHostnameFromSmssPropertiesFailsFastNamingTheKey() {
		CaseInsensitiveProperties prop = new CaseInsensitiveProperties();
		prop.put(AbstractSqlQueryUtil.HOSTNAME, Integer.valueOf(12345));

		H2QueryUtil util = new H2QueryUtil();
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
				() -> util.setConnectionDetailsFromSMSS(prop));
		assertTrue(ex.getMessage().contains(AbstractSqlQueryUtil.HOSTNAME),
				"message should name the offending key: " + ex.getMessage());
	}
}

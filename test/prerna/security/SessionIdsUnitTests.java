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
package prerna.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import prerna.util.Utility;

class SessionIdsUnitTests {

	private static final String SESSION_ID = "3F2A9C1B7E6D5A4F8B0C1D2E3F4A5B6C";

	@Test
	void withASaltTheHashIsAnHmacKeyedByIt() throws Exception {
		try (MockedStatic<Utility> utility = saltIs("pepper")) {
			Mac mac = Mac.getInstance("HmacSHA256");
			mac.init(new SecretKeySpec("pepper".getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
			String expected = HexFormat.of().formatHex(mac.doFinal(SESSION_ID.getBytes(StandardCharsets.UTF_8)));

			assertEquals(expected, SessionIds.hash(SESSION_ID));
		}
	}

	@Test
	void withoutASaltTheHashIsAPlainSha256() throws Exception {
		try (MockedStatic<Utility> utility = saltIs(null)) {
			String expected = HexFormat.of().formatHex(
					MessageDigest.getInstance("SHA-256").digest(SESSION_ID.getBytes(StandardCharsets.UTF_8)));

			assertEquals(expected, SessionIds.hash(SESSION_ID));
		}
	}

	@Test
	void theSaltChangesTheHash() {
		String salted;
		try (MockedStatic<Utility> utility = saltIs("pepper")) {
			salted = SessionIds.hash(SESSION_ID);
		}
		try (MockedStatic<Utility> utility = saltIs(null)) {
			assertNotEquals(salted, SessionIds.hash(SESSION_ID));
		}
	}

	@Test
	void aSessionAlwaysGetsTheSameHashAndNeverItsId() {
		try (MockedStatic<Utility> utility = saltIs("pepper")) {
			String hash = SessionIds.hash(SESSION_ID);

			assertTrue(hash.matches("[0-9a-f]{64}"), hash);
			assertEquals(hash, SessionIds.hash(SESSION_ID));
			assertNotEquals(hash, SessionIds.hash(SESSION_ID + "0"));
			assertFalse(hash.contains(SESSION_ID.toLowerCase()));
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { "UNKNOWN", "STARTUP", "SHUTDOWN", "NO_SESSION_ID", "NULL" })
	void aPlaceholderIsWrittenAsItIs(String placeholder) {
		assertEquals(placeholder, SessionIds.hash(placeholder));
	}

	@Test
	void noSessionIdGivesNoHash() {
		assertNull(SessionIds.hash(null));
		assertNull(SessionIds.hash(" "));
	}

	private static MockedStatic<Utility> saltIs(String salt) {
		MockedStatic<Utility> utility = mockStatic(Utility.class);
		utility.when(() -> Utility.getDIHelperProperty(SessionIds.SALT_PROPERTY)).thenReturn(salt);
		return utility;
	}
}

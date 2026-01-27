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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class SecurityTokenUtilsUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	//@Test
	void testClearExpiredTokens() {
		String ipAddr = "ipAddress";
		String clientId = "clientId";
		Object[] tokenObj = SecurityTokenUtils.generateToken(ipAddr, clientId);
		SecurityTokenUtils.clearExpiredTokens(0);
		// test invalid token
		tokenObj = SecurityTokenUtils.getToken(ipAddr);
		assertNull(tokenObj);
	}

	@Test
	void testGenerateToken() {
		String ipAddr = "ipAddress";
		String clientId = "clientId";
		Object[] tokenObj = SecurityTokenUtils.generateToken(ipAddr, clientId);
		String tokenValue = (String) tokenObj[0];
		String ipActual = (String) tokenObj[1];
		String cliendIdActual = (String) tokenObj[2];

		assertTrue(tokenValue != null && !tokenValue.isEmpty());
		assertEquals(ipAddr, ipActual);
		assertEquals(clientId, cliendIdActual);
	}

	@Test
	void testGetToken() {
		String ipAddr = "ipAddress";
		String clientId = "clientId";
		SecurityTokenUtils.generateToken(ipAddr, clientId);
		Object[] tokenObj = SecurityTokenUtils.getToken(ipAddr);
		String tokenValue = (String) tokenObj[0];
		String ipActual = (String) tokenObj[1];
		String cliendIdActual = (String) tokenObj[2];

		assertTrue(tokenValue != null && !tokenValue.isEmpty());
		assertEquals(ipAddr, ipActual);
		assertEquals(clientId, cliendIdActual);

		// test invalid token
		tokenObj = SecurityTokenUtils.getToken("badIP");
		assertNull(tokenObj);
	}
}

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
package prerna.util.linotp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.http.cookie.ClientCookie;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.junit.jupiter.api.Test;

/**
 * Covers the CodeQL "Sensitive Cookie in HTTPS Session Without 'Secure' Attribute /
 * Without 'HttpOnly' Flag" finding on the admin_session cookie built for the LinOTP
 * admin reset call. The cookie-building logic was extracted into
 * {@link LinOTPUtil#buildAdminSessionCookie(String, String)} so it can be verified
 * directly without needing a live LinOTP server or static SocialPropertiesUtil state.
 */
class LinOTPUtilUnitTests {

	@Test
	void testBuildAdminSessionCookie_isSecure() {
		BasicClientCookie cookie = LinOTPUtil.buildAdminSessionCookie("linotp.example.com", "abc123token");
		assertTrue(cookie.isSecure(), "admin_session cookie must be marked Secure");
	}

	@Test
	void testBuildAdminSessionCookie_isHttpOnly() {
		BasicClientCookie cookie = LinOTPUtil.buildAdminSessionCookie("linotp.example.com", "abc123token");
		assertEquals("true", cookie.getAttribute("httponly"));
	}

	@Test
	void testBuildAdminSessionCookie_hasExpectedNameAndValue() {
		BasicClientCookie cookie = LinOTPUtil.buildAdminSessionCookie("linotp.example.com", "my-token-value");
		assertEquals("admin_session", cookie.getName());
		assertEquals("my-token-value", cookie.getValue());
	}

	@Test
	void testBuildAdminSessionCookie_hasDomainSetAndMarkedExplicit() {
		BasicClientCookie cookie = LinOTPUtil.buildAdminSessionCookie("linotp.example.com", "tok");
		assertEquals("linotp.example.com", cookie.getDomain());
		assertEquals("true", cookie.getAttribute(ClientCookie.DOMAIN_ATTR));
	}

	@Test
	void testBuildAdminSessionCookie_differentTokensProduceDifferentValues() {
		BasicClientCookie cookie1 = LinOTPUtil.buildAdminSessionCookie("host", "tokenA");
		BasicClientCookie cookie2 = LinOTPUtil.buildAdminSessionCookie("host", "tokenB");
		assertNotEquals(cookie1.getValue(), cookie2.getValue());
	}
}

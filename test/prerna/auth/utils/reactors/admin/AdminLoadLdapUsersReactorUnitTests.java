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
package prerna.auth.utils.reactors.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityUpdateUtils;
import prerna.om.Insight;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.SocialPropertiesUtil;
import prerna.util.ldap.ILdapAuthenticator;

public class AdminLoadLdapUsersReactorUnitTests {

	private AdminLoadLdapUsersReactor reactor;
	private Insight insight;
	private User user;
	private NounStore ns;

	@BeforeEach
	void setup() {
		reactor = new AdminLoadLdapUsersReactor();
		insight = mock(Insight.class);
		user = mock(User.class);
		ns = mock(NounStore.class);
		reactor.setInsight(insight);
		reactor.setNounStore(ns);
		when(insight.getUser()).thenReturn(user);
	}

	@Test
	void testKeysToGet() {
		assertEquals(5, reactor.keysToGet.length);
		assertEquals("searchContextName", reactor.keysToGet[0]);
	}

	@Test
	void testNonAdminThrowsException() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(null);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("User must be an admin to perform this function", e.getMessage());
		}
	}

	@Test
	void testInvalidSearchContextScopeThrowsException() {
		reactor.keyValue.put("searchContextScope", "notAnInt");

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("searchContextScope must be an integer value. Received input = notAnInt", e.getMessage());
		}
	}

	@Test
	void testInvalidAuthProviderThrowsException() {
		reactor.keyValue.put("authProvider", "INVALID_PROVIDER");

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("New provider INVALID_PROVIDER is not a valid auth provider", e.getMessage());
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	void testSuccessNoUsersFound() throws Exception {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<SocialPropertiesUtil> spu = Mockito.mockStatic(SocialPropertiesUtil.class);
				MockedStatic<SecurityUpdateUtils> suu = Mockito.mockStatic(SecurityUpdateUtils.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			SocialPropertiesUtil mockSpu = mock(SocialPropertiesUtil.class);
			spu.when(SocialPropertiesUtil::getInstance).thenReturn(mockSpu);

			ILdapAuthenticator ldap = mock(ILdapAuthenticator.class);
			when(mockSpu.getLdapAuthenticator()).thenReturn(ldap);
			when(ldap.findUsers(anyString(), anyString(), anyInt())).thenReturn(new ArrayList<>());

			NounMetadata result = reactor.execute();

			assertNotNull(result);
			assertEquals(PixelDataType.CUSTOM_DATA_STRUCTURE, result.getNounType());
			Map<String, ?> retMap = (Map<String, ?>) result.getValue();
			assertNotNull(retMap.get("addedUsers"));
			assertNotNull(retMap.get("updatedUsers"));
			assertNotNull(retMap.get("foundUsers"));
		}
	}
}

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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.PasswordRequirements;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.impl.function.mail.engine.SMTPFunctionEngine;
import prerna.om.Insight;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.SocialPropertiesUtil;
import prerna.util.Utility;

public class AdminLockAccountWarningReactorUnitTests {

	private AdminLockAccountWarningReactor reactor;
	private Insight insight;
	private User user;
	private NounStore ns;

	@BeforeEach
	void setup() {
		reactor = new AdminLockAccountWarningReactor();
		insight = mock(Insight.class);
		user = mock(User.class);
		ns = mock(NounStore.class);
		reactor.setInsight(insight);
		reactor.setNounStore(ns);
		when(insight.getUser()).thenReturn(user);
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
	void testDaysToLockNegativeThrowsException() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<PasswordRequirements> pr = Mockito.mockStatic(PasswordRequirements.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			PasswordRequirements mockPr = mock(PasswordRequirements.class);
			pr.when(PasswordRequirements::getInstance).thenReturn(mockPr);
			when(mockPr.getDaysToLock()).thenReturn(-1);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("No value set to lock accounts", e.getMessage());
		}
	}

	@Test
	void testPasswordRequirementsExceptionThrowsNoValueSet() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<PasswordRequirements> pr = Mockito.mockStatic(PasswordRequirements.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			pr.when(PasswordRequirements::getInstance).thenThrow(new RuntimeException("no config"));

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("No value set to lock accounts", e.getMessage());
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	void testSuccessNoUsersToEmail() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<PasswordRequirements> pr = Mockito.mockStatic(PasswordRequirements.class);
				MockedStatic<SocialPropertiesUtil> spu = Mockito.mockStatic(SocialPropertiesUtil.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			PasswordRequirements mockPr = mock(PasswordRequirements.class);
			pr.when(PasswordRequirements::getInstance).thenReturn(mockPr);
			when(mockPr.getDaysToLock()).thenReturn(90);

			SocialPropertiesUtil mockSpu = mock(SocialPropertiesUtil.class);
			spu.when(SocialPropertiesUtil::getInstance).thenReturn(mockSpu);

			when(s.getUserEmailsGettingLocked()).thenReturn(new ArrayList<>());

			NounMetadata result = reactor.execute();

			assertNotNull(result);
			assertEquals(PixelDataType.CONST_STRING, result.getNounType());
			List<String> emails = (List<String>) result.getValue();
			assertEquals(0, emails.size());
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	void testSuccessWithUsersToEmail() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<PasswordRequirements> pr = Mockito.mockStatic(PasswordRequirements.class);
				MockedStatic<SocialPropertiesUtil> spu = Mockito.mockStatic(SocialPropertiesUtil.class);
				MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			PasswordRequirements mockPr = mock(PasswordRequirements.class);
			pr.when(PasswordRequirements::getInstance).thenReturn(mockPr);
			when(mockPr.getDaysToLock()).thenReturn(90);

			SocialPropertiesUtil mockSpu = mock(SocialPropertiesUtil.class);
			spu.when(SocialPropertiesUtil::getInstance).thenReturn(mockSpu);
			SMTPFunctionEngine mockMailEngine = mock(SMTPFunctionEngine.class);
			when(mockSpu.getSmtpEngine()).thenReturn(mockMailEngine);
			when(mockSpu.getEmailStaticProps()).thenReturn(new HashMap<>());
			when(mockSpu.getSmtpSender()).thenReturn("admin@test.com");

			List<Object[]> listToEmail = new ArrayList<>();
			listToEmail.add(new Object[] { "user@test.com", Long.valueOf(80) });
			when(s.getUserEmailsGettingLocked()).thenReturn(listToEmail);

			util.when(() -> Utility.getDIHelperProperty(Constants.EMAIL_TEMPLATES)).thenReturn("/nonexistent/path/");

			NounMetadata result = reactor.execute();

			assertNotNull(result);
			List<String> emails = (List<String>) result.getValue();
			assertEquals(1, emails.size());
			assertEquals("user@test.com", emails.get(0));
			verify(mockMailEngine, times(1)).sendEmail(eq(new String[] { "user@test.com" }), isNull(), isNull(),
					eq("admin@test.com"), eq("WARNING! Account Locking Soon"), anyString(), eq(true), isNull());
		}
	}
}

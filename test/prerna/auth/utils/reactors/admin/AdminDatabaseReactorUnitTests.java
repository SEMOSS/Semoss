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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.om.Insight;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AdminDatabaseReactorUnitTests {

	private AdminDatabaseReactor reactor;
	private Insight insight;
	private User user;
	private SelectQueryStruct qs;
	private NounStore ns;

	@BeforeEach
	void setup() {
		reactor = new AdminDatabaseReactor();
		insight = mock(Insight.class);
		user = mock(User.class);
		qs = mock(SelectQueryStruct.class);
		ns = mock(NounStore.class);

		reactor.setNounStore(ns);
		reactor.setInsight(insight);

		when(insight.getUser()).thenReturn(user);
	}

	@Test
	void testAdminUtilsNull() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(null);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("User must be an admin to perform this function", e.getMessage());
		}
	}

	@Test
	void testEngineIdEmpty() throws Exception {
		reactor.keyValue.put(ReactorKeysEnum.DATABASE.getKey(), "");

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			NullPointerException e = assertThrows(NullPointerException.class, reactor::execute);
			assertEquals("The engine id cannot be null for this operation", e.getMessage());
		}
	}

	@Test
	void testEngineIdNull() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			NullPointerException e = assertThrows(NullPointerException.class, reactor::execute);
			assertEquals("The engine id cannot be null for this operation", e.getMessage());
		}
	}

	@Test
	void testExecute() {
		reactor.keyValue.put(ReactorKeysEnum.DATABASE.getKey(), "testEngineId");

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			when(qs.getQsType()).thenReturn(AbstractQueryStruct.QUERY_STRUCT_TYPE.ENGINE);

			NounMetadata result = reactor.execute();

			assertNotNull(result);
		}
	}

	@Test
	void testRawEngineQuery() {
		reactor.keyValue.put(ReactorKeysEnum.DATABASE.getKey(), "testEngineId");

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			qs.setQsType(AbstractQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);
			System.out.println(qs.getQsType());

			when(qs.getQsType()).thenReturn(AbstractQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);

			reactor.execute();

			verify(qs).setQsType(AbstractQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);
		}
	}
}

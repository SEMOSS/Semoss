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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.om.Insight;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AdminProjectInfoReactorUnitTests {

	private AdminProjectInfoReactor reactor;
	private Insight insight;
	private User user;

	private NounStore ns;
	private GenRowStruct grs;

	private Map<String, String> keyValues;

	@BeforeEach
	void setup() {
		reactor = new AdminProjectInfoReactor();
		keyValues = reactor.keyValue;

		insight = mock(Insight.class);
		user = mock(User.class);
		reactor.setInsight(insight);
		when(insight.getUser()).thenReturn(user);

		ns = mock(NounStore.class);

		grs = mock(GenRowStruct.class);

		reactor.setNounStore(ns);
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
	void testProjectIdNull() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Must input an project id", e.getMessage());
		}
	}

	@Test
	void testProjectIdEmpty() {
		keyValues.put(ReactorKeysEnum.PROJECT.getKey(), "");
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Must input an project id", e.getMessage());
		}
	}

	@Test
	void testBaseInfoNull() {
		Map<String, String> keyvalues = reactor.keyValue;
		keyvalues.put("project", "test");
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			List<Map<String, Object>> baseInfo = new ArrayList<>();
			when(s.getAllProjectSettings(any(List.class), eq(null), eq(null), eq(null), eq(null), eq(null)))
					.thenReturn(null);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Could not find any project data", e.getMessage());

			ArgumentCaptor<List<String>> listCaptor = ArgumentCaptor.forClass(List.class);
			verify(s, times(1)).getAllProjectSettings(listCaptor.capture(), eq(null), eq(null), eq(null), eq(null),
					eq(null));
			assertEquals(1, listCaptor.getValue().size());
		}
	}

	@Test
	void testBaseInfoEmpty() {
		Map<String, String> keyvalues = reactor.keyValue;
		keyvalues.put("project", "test");
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			List<Map<String, Object>> baseInfo = new ArrayList<>();
			when(s.getAllProjectSettings(any(List.class), any(List.class), eq(null), eq(null), eq(null), eq(null)))
					.thenReturn(baseInfo);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Could not find any project data", e.getMessage());

			ArgumentCaptor<List<String>> listCaptor = ArgumentCaptor.forClass(List.class);
			verify(s, times(1)).getAllProjectSettings(listCaptor.capture(), eq(null), eq(null), eq(null), eq(null),
					eq(null));
			assertEquals(1, listCaptor.getValue().size());
		}
	}

	@Test
	void testMetaKeysNull() {
		Map<String, String> keyvalues = reactor.keyValue;
		keyvalues.put("project", "test");
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			List<Map<String, Object>> baseInfo = new ArrayList<>();
			Map<String, Object> temp = new HashMap<>();
			temp.put("test", "test");
			baseInfo.add(temp);
			when(s.getAllProjectSettings(any(List.class), eq(null), eq(null), eq(null), eq(null), eq(null)))
					.thenReturn(baseInfo);

			GenRowStruct grss = new GenRowStruct();
			NounMetadata n = new NounMetadata(keyvalues, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.PROJECT_INFO);
			grss.add(n);
			when(ns.getGenRowStruct(ReactorKeysEnum.META_KEYS.getKey())).thenReturn(grss);

			NounMetadata result = reactor.execute();
			assertNotNull(result.getValue());
			assertTrue(result.getValue().equals(temp));

		}
	}

	@Test
	void testMetaKeysEmpty() {
		Map<String, String> keyvalues = reactor.keyValue;
		keyvalues.put("project", "test");
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			List<Map<String, Object>> baseInfo = new ArrayList<>();
			Map<String, Object> temp = new HashMap<>();
			temp.put("test", "test");
			baseInfo.add(temp);
			when(s.getAllProjectSettings(any(List.class), eq(null), eq(null), eq(null), eq(null), eq(null)))
					.thenReturn(baseInfo);

			GenRowStruct grss = new GenRowStruct();
			when(ns.getGenRowStruct(ReactorKeysEnum.META_KEYS.getKey())).thenReturn(grss);

			NounMetadata result = reactor.execute();
			assertNotNull(result.getValue());
			assertTrue(result.getValue().equals(temp));

		}
	}

	@Test
	void testProjectInfo() {
		Map<String, String> keyvalues = reactor.keyValue;
		keyvalues.put("project", "test");
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			List<Map<String, Object>> baseInfo = new ArrayList<>();
			Map<String, Object> temp = new HashMap<>();
			temp.put("test", "test");
			baseInfo.add(temp);
			when(s.getAllProjectSettings(any(List.class), eq(null), eq(null), eq(null), eq(null), eq(null)))
					.thenReturn(baseInfo);

			NounMetadata result = reactor.execute();
			assertNotNull(result.getValue());

			assertTrue(result.getValue().equals(temp));

		}
	}
}

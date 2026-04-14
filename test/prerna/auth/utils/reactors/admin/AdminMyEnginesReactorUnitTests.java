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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.Insight;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.UserCatalogVoteUtils;

public class AdminMyEnginesReactorUnitTests {

	private AdminMyEnginesReactor reactor;
	private Insight insight;
	private User user;
	private NounStore ns;

	@BeforeEach
	void setup() throws Exception {
		reactor = new AdminMyEnginesReactor();
		insight = mock(Insight.class);
		user = mock(User.class);
		ns = mock(NounStore.class);
		reactor.setInsight(insight);
		reactor.setNounStore(ns);
		when(insight.getUser()).thenReturn(user);

		// curRow is accessed by getMetaMap() — use reflection since it's protected in
		// AbstractReactor
		GenRowStruct curRow = mock(GenRowStruct.class);
		Field curRowField = AbstractReactor.class.getDeclaredField("curRow");
		curRowField.setAccessible(true);
		curRowField.set(reactor, curRow);
	}

	@Test
	void testKeysToGet() {
		assertEquals(10, reactor.keysToGet.length);
		assertEquals(ReactorKeysEnum.FILTER_WORD.getKey(), reactor.keysToGet[0]);
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
	void testSuccessEmptyResults() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			when(s.getAllEngineSettings(isNull(), isNull(), isNull(), isNull(), isNull(), isNull(), isNull()))
					.thenReturn(new ArrayList<>());

			NounMetadata result = reactor.execute();

			assertNotNull(result);
			assertEquals(PixelDataType.CUSTOM_DATA_STRUCTURE, result.getNounType());
		}
	}

	@Test
	void testGetDescriptionForSortKey() {
		String desc = reactor.getDescriptionForKey(ReactorKeysEnum.SORT.getKey());
		assertNotNull(desc);
		assertEquals(
				"The sort is a map with key and direction. Supported keys are 'ENGINENAME' and 'DATECREATED'. Use values like 'ASC' or 'DESC'. 'ENGINENAME' sorting is case-insensitive.",
				desc);
	}

	@Test
	void testGetDescriptionForEngineKey() {
		String desc = reactor.getDescriptionForKey(ReactorKeysEnum.ENGINE.getKey());
		assertEquals("This is an optional engine filter", desc);
	}

	@Test
	void testGetDescriptionForOtherKey() {
		// Unknown keys delegate to super, which returns null
		String desc = reactor.getDescriptionForKey("unknownKey");
		assertEquals(null, desc);
	}

	@Test
	@SuppressWarnings("unchecked")
	void testSuccessWithNonEmptyResultsAndMetadata() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<SecurityEngineUtils> seu = Mockito.mockStatic(SecurityEngineUtils.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			List<Map<String, Object>> results = new ArrayList<>();
			Map<String, Object> engine = new HashMap<>();
			engine.put("database_id", "eng1");
			results.add(engine);

			when(s.getAllEngineSettings(isNull(), isNull(), isNull(), isNull(), isNull(), isNull(), isNull()))
					.thenReturn(results);

			IRawSelectWrapper wrapper = mock(IRawSelectWrapper.class);
			when(wrapper.hasNext()).thenReturn(true, true, false);
			IHeadersDataRow row1 = mock(IHeadersDataRow.class);
			when(row1.getValues()).thenReturn(new Object[] { "eng1", "description", "Test engine" });
			IHeadersDataRow row2 = mock(IHeadersDataRow.class);
			when(row2.getValues()).thenReturn(new Object[] { "eng1", "description", "Another value" });
			when(wrapper.next()).thenReturn(row1, row2);
			seu.when(() -> SecurityEngineUtils.getEngineMetadataWrapper(any(), isNull(), eq(true))).thenReturn(wrapper);

			NounMetadata result = reactor.execute();

			assertNotNull(result);
			assertEquals(PixelDataType.CUSTOM_DATA_STRUCTURE, result.getNounType());
			List<Map<String, Object>> value = (List<Map<String, Object>>) result.getValue();
			assertEquals(1, value.size());
			// Verify metadata was added — second value should create a list
			Object desc = value.get(0).get("description");
			assertNotNull(desc);
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	void testSuccessWithNonEmptyResultsNullMetaValue() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<SecurityEngineUtils> seu = Mockito.mockStatic(SecurityEngineUtils.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			List<Map<String, Object>> results = new ArrayList<>();
			Map<String, Object> engine = new HashMap<>();
			engine.put("database_id", "eng1");
			results.add(engine);

			when(s.getAllEngineSettings(isNull(), isNull(), isNull(), isNull(), isNull(), isNull(), isNull()))
					.thenReturn(results);

			IRawSelectWrapper wrapper = mock(IRawSelectWrapper.class);
			when(wrapper.hasNext()).thenReturn(true, false);
			IHeadersDataRow row1 = mock(IHeadersDataRow.class);
			when(row1.getValues()).thenReturn(new Object[] { "eng1", "description", null });
			when(wrapper.next()).thenReturn(row1);
			seu.when(() -> SecurityEngineUtils.getEngineMetadataWrapper(any(), isNull(), eq(true))).thenReturn(wrapper);

			NounMetadata result = reactor.execute();

			assertNotNull(result);
			List<Map<String, Object>> value = (List<Map<String, Object>>) result.getValue();
			assertEquals(1, value.size());
			// Null meta value should be skipped
			assertEquals(false, value.get(0).containsKey("description"));
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	void testSuccessWithUserTracking() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<SecurityEngineUtils> seu = Mockito.mockStatic(SecurityEngineUtils.class);
				MockedStatic<UserCatalogVoteUtils> ucvu = Mockito.mockStatic(UserCatalogVoteUtils.class);
				MockedStatic<prerna.auth.User> userStatic = Mockito.mockStatic(prerna.auth.User.class)) {

			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			List<Map<String, Object>> results = new ArrayList<>();
			Map<String, Object> engine = new HashMap<>();
			engine.put("database_id", "eng1");
			results.add(engine);

			when(s.getAllEngineSettings(isNull(), isNull(), isNull(), isNull(), isNull(), isNull(), isNull()))
					.thenReturn(results);

			// noMeta=true (skip metadata), includeUserT=true (include user tracking)
			reactor.keyValue.put(ReactorKeysEnum.NO_META.getKey(), "true");
			reactor.keyValue.put(ReactorKeysEnum.INCLUDE_USERTRACKING_KEY.getKey(), "true");

			IRawSelectWrapper voteWrapper = mock(IRawSelectWrapper.class);
			when(voteWrapper.hasNext()).thenReturn(true, false);
			IHeadersDataRow voteRow = mock(IHeadersDataRow.class);
			when(voteRow.getValues()).thenReturn(new Object[] { "eng1", Integer.valueOf(5) });
			when(voteWrapper.next()).thenReturn(voteRow);
			ucvu.when(() -> UserCatalogVoteUtils.getAllVotesWrapper(any())).thenReturn(voteWrapper);

			userStatic.when(() -> User.getUserIdAndType(user)).thenReturn(null);
			Map<String, Boolean> voted = new HashMap<>();
			voted.put("eng1", Boolean.TRUE);
			ucvu.when(() -> UserCatalogVoteUtils.userEngineVotes(any(), any())).thenReturn(voted);

			NounMetadata result = reactor.execute();

			assertNotNull(result);
			List<Map<String, Object>> value = (List<Map<String, Object>>) result.getValue();
			assertEquals(1, value.size());
			assertEquals(5, value.get(0).get("upvotes"));
			assertEquals(true, value.get(0).get("hasUpvoted"));
		}
	}
}

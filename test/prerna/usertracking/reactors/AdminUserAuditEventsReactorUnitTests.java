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
package prerna.usertracking.reactors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.om.Insight;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.Constants;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

class AdminUserAuditEventsReactorUnitTests {

	private static AdminUserAuditEventsReactor reactor(User user) {
		Insight insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(user);
		AdminUserAuditEventsReactor reactor = new AdminUserAuditEventsReactor();
		reactor.setInsight(insight);
		return reactor;
	}

	@Test
	void nonAdminCannotAccessAuditEvents() {
		User user = mock(User.class);
		try (var security = mockStatic(SecurityAdminUtils.class); var registry = mockStatic(SystemEngineRegistry.class)) {
			security.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(false);
			assertThrows(SemossPixelException.class, () -> reactor(user).execute());
			registry.verifyNoInteractions();
		}
	}

	@Test
	void disabledTrackingReturnsAClearErrorForAdmins() {
		User user = mock(User.class);
		try (var security = mockStatic(SecurityAdminUtils.class); var utility = mockStatic(Utility.class)) {
			security.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(true);
			utility.when(Utility::isUserTrackingDisabled).thenReturn(true);
			assertThrows(IllegalArgumentException.class, () -> reactor(user).execute());
		}
	}

	@Test
	void unloadedDatabaseReturnsAClearErrorForAdmins() {
		User user = mock(User.class);
		try (var security = mockStatic(SecurityAdminUtils.class); var utility = mockStatic(Utility.class);
				var registry = mockStatic(SystemEngineRegistry.class)) {
			security.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(true);
			utility.when(Utility::isUserTrackingDisabled).thenReturn(false);
			registry.when(SystemEngineRegistry::isUserTrackingDbLoaded).thenReturn(false);
			assertThrows(IllegalStateException.class, () -> reactor(user).execute());
		}
	}

	@Test
	void auditQueryResolvesRegisteredEngineWhenOrdinaryLookupReturnsNull() throws Exception {
		User user = mock(User.class);
		var engine = mock(IRDBMSEngine.class);
		var manager = mock(WrapperManager.class);
		var wrapper = mock(IRawSelectWrapper.class);
		try (var security = mockStatic(SecurityAdminUtils.class);
				var utility = mockStatic(Utility.class);
				var registry = mockStatic(SystemEngineRegistry.class);
				var wrappers = mockStatic(WrapperManager.class);
				var metadata = mockStatic(MasterDatabaseUtility.class)) {
			security.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(true);
			utility.when(() -> Utility.getDatabase(Constants.USER_TRACKING_DB)).thenReturn(null);
			registry.when(SystemEngineRegistry::isUserTrackingDbLoaded).thenReturn(true);
			registry.when(SystemEngineRegistry::getUserTrackingDb).thenReturn(engine);
			wrappers.when(WrapperManager::getInstance).thenReturn(manager);

			var query = (SelectQueryStruct) reactor(user).execute().getValue();
			assertEquals(Constants.USER_TRACKING_DB, query.getEngineId());
			assertEquals(AbstractQueryStruct.QUERY_STRUCT_TYPE.ENGINE, query.getQsType());
			assertEquals(UserAuditTrailUtils.COLUMNS.size(), query.getSelectors().size());
			var order = (QueryColumnOrderBySelector) query.getOrderBy().get(0);
			assertEquals("USER_AUDIT_EVENTS__EVENT_TIME", order.getQueryStructName());
			assertSame(engine, query.retrieveQueryStructEngine());

			// Query operations and merging must preserve the engine used by Collect.
			var paginated = new SelectQueryStruct();
			paginated.merge(query);
			paginated.setQsType(query.getQsType());
			paginated.setOffSet(0);
			paginated.setLimit(26);
			assertSame(engine, paginated.retrieveQueryStructEngine());
			when(manager.getRawWrapper(engine, paginated)).thenReturn(wrapper);
			var types = new SemossDataType[query.getSelectors().size()];
			Arrays.fill(types, SemossDataType.STRING);
			when(wrapper.getTypes()).thenReturn(types);
			try (var task = new BasicIteratorTask(paginated)) {
				assertFalse(task.hasNext());
			}
			verify(manager).getRawWrapper(engine, paginated);
			utility.verify(() -> Utility.getDatabase(Constants.USER_TRACKING_DB), never());
		}
	}
}

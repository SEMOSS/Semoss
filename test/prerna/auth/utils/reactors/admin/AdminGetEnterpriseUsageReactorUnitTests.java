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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.User;
import prerna.auth.utils.EnterpriseUsageUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.om.Insight;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;

class AdminGetEnterpriseUsageReactorUnitTests {

	@Test
	void allReadReactorsDenyNonAdminsBeforeReadingAnyCatalogOrLog() {
		var insight = mock(Insight.class);
		var user = mock(User.class);
		when(insight.getUser()).thenReturn(user);
		try (MockedStatic<SecurityAdminUtils> security = mockStatic(SecurityAdminUtils.class);
				MockedStatic<EnterpriseUsageUtils> usage = mockStatic(EnterpriseUsageUtils.class)) {
			security.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(null);
			for (AbstractReactor reactor : List.of(new AdminGetEnterpriseUsageReactor(),
					new AdminGetEnterpriseUsageDetailReactor(), new AdminGetEnterpriseUsageFilterOptionsReactor())) {
				reactor.setInsight(insight);
				assertThrows(IllegalArgumentException.class, reactor::execute);
			}
			usage.verifyNoInteractions();
		}
	}

	@Test
	void authorizedRequestsUseTheTypedServiceContract() {
		var insight = mock(Insight.class);
		var user = mock(User.class);
		when(insight.getUser()).thenReturn(user);
		var reactor = new AdminGetEnterpriseUsageReactor();
		reactor.setInsight(insight);
		reactor.keyValue.putAll(
				Map.of("source", "model", "view", "summary", "startDate", "2024-03-01", "endDate", "2024-03-31"));
		var detail = new AdminGetEnterpriseUsageDetailReactor();
		detail.setInsight(insight);
		detail.keyValue.putAll(Map.of("source", "model", "recordId", "message-1"));
		try (MockedStatic<SecurityAdminUtils> security = mockStatic(SecurityAdminUtils.class);
				MockedStatic<EnterpriseUsageUtils> usage = mockStatic(EnterpriseUsageUtils.class)) {
			security.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(mock(SecurityAdminUtils.class));
			usage.when(() -> EnterpriseUsageUtils.option(EnterpriseUsageUtils.Source.class, "model"))
					.thenReturn(EnterpriseUsageUtils.Source.MODEL);
			usage.when(() -> EnterpriseUsageUtils.option(EnterpriseUsageUtils.View.class, "summary"))
					.thenReturn(EnterpriseUsageUtils.View.SUMMARY);
			usage.when(() -> EnterpriseUsageUtils.boundedInteger(null, 25, 1, 5000)).thenReturn(25);
			usage.when(() -> EnterpriseUsageUtils.boundedInteger(null, 0, 0, Integer.MAX_VALUE - 5000)).thenReturn(0);
			var response = Map.<String, Object>of("rows", List.of(Map.of("REQUESTS", 1)));
			usage.when(() -> EnterpriseUsageUtils.report(EnterpriseUsageUtils.Source.MODEL,
					EnterpriseUsageUtils.View.SUMMARY, "2024-03-01", "2024-03-31", null, null, null, null, 25, 0))
					.thenReturn(response);
			usage.when(() -> EnterpriseUsageUtils.detail(EnterpriseUsageUtils.Source.MODEL, "message-1"))
					.thenReturn(response);
			assertEquals(response, reactor.execute().getValue());
			assertEquals(PixelDataType.MAP, detail.execute().getNounType());
		}
	}

	@Test
	void catalogReactorUsesBoundedTypedChoices() {
		var insight = mock(Insight.class);
		var user = mock(User.class);
		when(insight.getUser()).thenReturn(user);
		var reactor = new AdminGetEnterpriseUsageFilterOptionsReactor();
		reactor.setInsight(insight);
		reactor.keyValue.putAll(Map.of("dimension", "user", "search", "O'Brien"));
		try (var security = mockStatic(SecurityAdminUtils.class); var usage = mockStatic(EnterpriseUsageUtils.class)) {
			security.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(mock(SecurityAdminUtils.class));
			usage.when(() -> EnterpriseUsageUtils.option(EnterpriseUsageUtils.FilterDimension.class, "user"))
					.thenCallRealMethod();
			usage.when(() -> EnterpriseUsageUtils.option(EnterpriseUsageUtils.FilterDimension.class, "unsupported"))
					.thenCallRealMethod();
			usage.when(() -> EnterpriseUsageUtils.boundedInteger(null, 50, 1, 100)).thenCallRealMethod();
			usage.when(() -> EnterpriseUsageUtils.boundedInteger(null, 0, 0, Integer.MAX_VALUE - 101))
					.thenCallRealMethod();
			var response = Map.<String, Object>of("rows", List.of(), "hasMore", false);
			usage.when(() -> EnterpriseUsageUtils.filterOptions(EnterpriseUsageUtils.FilterDimension.USER, "O'Brien",
					null, 50, 0)).thenReturn(response);
			assertEquals(response, reactor.execute().getValue());
			reactor.keyValue.put("dimension", "unsupported");
			assertThrows(IllegalArgumentException.class, reactor::execute);
		}
	}

	@Test
	void engineArgumentTakesPrecedenceOverLegacyModelAlias() {
		var insight = mock(Insight.class);
		var user = mock(User.class);
		when(insight.getUser()).thenReturn(user);
		var reactor = new AdminGetEnterpriseUsageReactor();
		reactor.setInsight(insight);
		reactor.keyValue.putAll(Map.of("source", "activity", "view", "summary", "startDate", "2024-03-01", "endDate",
				"2024-03-31", "model", "=model-1"));
		try (var security = mockStatic(SecurityAdminUtils.class); var usage = mockStatic(EnterpriseUsageUtils.class)) {
			security.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(mock(SecurityAdminUtils.class));
			usage.when(() -> EnterpriseUsageUtils.option(EnterpriseUsageUtils.Source.class, "activity"))
					.thenReturn(EnterpriseUsageUtils.Source.ACTIVITY);
			usage.when(() -> EnterpriseUsageUtils.option(EnterpriseUsageUtils.View.class, "summary"))
					.thenReturn(EnterpriseUsageUtils.View.SUMMARY);
			usage.when(() -> EnterpriseUsageUtils.boundedInteger(null, 25, 1, 5000)).thenReturn(25);
			usage.when(() -> EnterpriseUsageUtils.boundedInteger(null, 0, 0, Integer.MAX_VALUE - 5000)).thenReturn(0);
			for (String engine : List.of("=model-1", "=function-1")) {
				var response = Map.<String, Object>of("rows", List.of(Map.of("ENGINE_ID", engine)));
				usage.when(() -> EnterpriseUsageUtils.report(EnterpriseUsageUtils.Source.ACTIVITY,
						EnterpriseUsageUtils.View.SUMMARY, "2024-03-01", "2024-03-31", null, null, engine, null, 25, 0))
						.thenReturn(response);
				if (engine.equals("=function-1")) {
					reactor.keyValue.put("engine", engine);
				}
				assertEquals(response, reactor.execute().getValue());
			}
		}
	}

}

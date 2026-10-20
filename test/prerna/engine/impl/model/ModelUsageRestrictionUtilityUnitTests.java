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
package prerna.engine.impl.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import prerna.engine.impl.model.responses.AskStringModelEngineResponse;
import prerna.engine.impl.model.usage.ModelUsageReport;
import prerna.engine.impl.model.usage.ModelUsageTicket;

public class ModelUsageRestrictionUtilityUnitTests {

	@Test
	void callsWithNoLimitGetNoCeiling() {
		Map<String, Object> parameters = new HashMap<>();
		parameters.put("max_tokens", 100);
		assertSame(parameters,
				ModelUsageRestrictionUtility.withUsageCeiling(parameters, ModelUsageTicket.none(), null));
		assertFalse(parameters.containsKey(ModelUsageTicket.USAGE_CEILING));
	}

	@Test
	void theCeilingIsAddedToACopyOfTheParameters() {
		Map<String, Object> parameters = new HashMap<>();
		parameters.put("temperature", 0.2);
		ModelUsageTicket ticket = mock(ModelUsageTicket.class);
		when(ticket.pythonCeiling()).thenReturn(ceiling(5000L, null));

		Map<String, Object> withCeiling = ModelUsageRestrictionUtility.withUsageCeiling(parameters, ticket, null);

		assertFalse(parameters.containsKey(ModelUsageTicket.USAGE_CEILING));
		assertEquals(0.2, withCeiling.get("temperature"));
		assertEquals(5000L, ((Map<?, ?>) withCeiling.get(ModelUsageTicket.USAGE_CEILING)).get("tokens"));
	}

	@Test
	void aRoutersCeilingIsMergedWithTheLesserOfEach() {
		Map<String, Object> ours = ceiling(8000L, 2_000_000L);
		ours.put("rates", Map.of("input", 2.5, "output", 10.0));
		Map<String, Object> router = ceiling(5000L, null);

		Map<String, Object> merged = ModelUsageRestrictionUtility.mergeCeilings(ours, router);

		assertEquals(5000L, merged.get("tokens"));
		assertEquals(2_000_000L, merged.get("cost_micros"));
		assertEquals(Map.of("input", 2.5, "output", 10.0), merged.get("rates"));
		assertEquals(256L, merged.get("min_output_tokens"));
	}

	@Test
	void aCeilingPassedOnAsJsonIsRead() {
		ModelUsageTicket ticket = ModelUsageTicket.none();
		Map<String, Object> withCeiling = ModelUsageRestrictionUtility.withUsageCeiling(null, ticket,
				"{\"tokens\": 3000, \"cost_micros\": null, \"min_output_tokens\": 256, \"margin\": 0.05}");
		Map<?, ?> ceiling = (Map<?, ?>) withCeiling.get(ModelUsageTicket.USAGE_CEILING);
		assertEquals(3000.0, ((Number) ceiling.get("tokens")).doubleValue());
	}

	@Test
	void takeUsageCeilingRemovesIt() {
		Map<String, Object> parameters = new HashMap<>();
		Map<String, Object> passedOn = ceiling(10L, null);
		parameters.put(ModelUsageTicket.USAGE_CEILING, passedOn);
		assertSame(passedOn, ModelUsageRestrictionUtility.takeUsageCeiling(parameters));
		assertTrue(parameters.isEmpty());
		assertNull(ModelUsageRestrictionUtility.takeUsageCeiling(null));
	}

	@Test
	void requestedMaxTokensReadsEveryAlias() {
		assertEquals(500L, ModelUsageRestrictionUtility.requestedMaxTokens(Map.of("max_tokens", 500), 4096));
		assertEquals(700L,
				ModelUsageRestrictionUtility.requestedMaxTokens(Map.of("max_completion_tokens", "700"), 4096));
		assertEquals(900L, ModelUsageRestrictionUtility.requestedMaxTokens(Map.of("max_output_tokens", 900L), 0));
		assertEquals(4096L, ModelUsageRestrictionUtility.requestedMaxTokens(Map.of("max_tokens", 0), 4096));
		assertEquals(4096L, ModelUsageRestrictionUtility.requestedMaxTokens(null, 4096));
		assertNull(ModelUsageRestrictionUtility.requestedMaxTokens(Map.of(), 0));
	}

	@Test
	void billPutsTheUsageOnTheResponse() {
		ModelUsageTicket ticket = mock(ModelUsageTicket.class);
		Map<String, Object> restriction = Map.of("restrictedBy", "token", "currentValue", 120L, "maxValue", 1000L);
		Map<String, Object> limits = Map.of("capped", true);
		when(ticket.bill(any())).thenReturn(42L);
		when(ticket.responseUsageRestriction()).thenReturn(restriction);
		when(ticket.responseUsageLimits(true)).thenReturn(limits);
		AskStringModelEngineResponse response = new AskStringModelEngineResponse("hi", 100, 20);
		response.setUsageCapped(true);
		ZonedDateTime start = ZonedDateTime.now();

		Long cost = ModelUsageRestrictionUtility.bill(ticket, response, start, start.plusNanos(1_500_000_000L));

		assertEquals(42L, cost);
		ArgumentCaptor<ModelUsageReport> report = ArgumentCaptor.forClass(ModelUsageReport.class);
		verify(ticket).bill(report.capture());
		assertEquals(120L, report.getValue().totalTokens());
		assertEquals(1500L, report.getValue().getResponseTimeMillis());
		assertSame(restriction, response.getUsageRestriction());
		assertSame(limits, response.getUsageLimits());
		assertSame(restriction, response.toMap().get("usageRestriction"));
		assertSame(limits, response.toMap().get("usageLimits"));
	}

	@Test
	void billWithNoTicketDoesNothing() {
		assertNull(ModelUsageRestrictionUtility.bill(null, null, null, null));
	}

	private static Map<String, Object> ceiling(Long tokens, Long costMicros) {
		Map<String, Object> ceiling = new LinkedHashMap<>();
		ceiling.put("tokens", tokens);
		ceiling.put("cost_micros", costMicros);
		ceiling.put("rates", null);
		ceiling.put("min_output_tokens", 256L);
		ceiling.put("margin", 0.05);
		return ceiling;
	}
}

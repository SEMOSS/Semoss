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
package prerna.engine.impl.model.inferencetracking;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import prerna.auth.access.ModelUsageLimitRule.Metric;
import prerna.util.JdbcTestDatabase;

/**
 * The queries usage limit counters are filled from, on a real database.
 */
class ModelUsageLogsJdbcUnitTests {

	private static final Instant DAY_START = Instant.parse("2026-10-12T00:00:00Z");
	private static final Instant DAY_END = Instant.parse("2026-10-13T00:00:00Z");

	private void messages(JdbcTestDatabase db) throws Exception {
		db.execute("CREATE TABLE MESSAGE (MESSAGE_ID VARCHAR, TRANSACTION_ID VARCHAR, MESSAGE_TYPE VARCHAR, "
				+ "MESSAGE_TOKENS INT, CACHE_READ_TOKENS INT, CACHE_CREATION_TOKENS INT, THINKING_TOKENS INT, "
				+ "MESSAGE_METHOD VARCHAR, RESPONSE_TIME DOUBLE, DATE_CREATED TIMESTAMP, AGENT_ID VARCHAR, "
				+ "USER_ID VARCHAR, USER_TYPE VARCHAR, BILLED_SOURCE_TYPE VARCHAR, BILLED_SUBJECT_ID VARCHAR, BILLED_SUBJECT_TYPE VARCHAR, "
				+ "COST_MICROS BIGINT)",
				// a call billed to team Ops: the response time and the cost are on the RESPONSE
				// row
				"INSERT INTO MESSAGE VALUES ('1', 't1', 'INPUT', 100, 0, 0, NULL, 'ask', NULL, "
						+ "TIMESTAMP '2026-10-12 10:00:00', 'model-m', 'eli', 'NATIVE', 'TEAM', 'Ops', 'CUSTOM', NULL)",
				"INSERT INTO MESSAGE VALUES ('2', 't1', 'RESPONSE', 50, NULL, NULL, 10, 'ask', 2000, "
						+ "TIMESTAMP '2026-10-12 10:00:02', 'model-m', 'eli', 'NATIVE', 'TEAM', 'Ops', 'CUSTOM', 300)",
				// a call from before login types and costs were recorded, with its time on both
				// rows
				"INSERT INTO MESSAGE VALUES ('3', 't2', 'INPUT', 40, 10, 5, NULL, 'ask', 500, "
						+ "TIMESTAMP '2026-10-12 11:00:00', 'model-m', 'eli', NULL, NULL, NULL, NULL, NULL)",
				"INSERT INTO MESSAGE VALUES ('4', 't2', 'RESPONSE', 20, NULL, NULL, 4, 'ask', 500, "
						+ "TIMESTAMP '2026-10-12 11:00:01', 'model-m', 'eli', NULL, NULL, NULL, NULL, NULL)",
				// a router's own rows for a routed call
				"INSERT INTO MESSAGE VALUES ('5', 't3', 'INPUT', 100, 0, 0, NULL, 'route_ask', NULL, "
						+ "TIMESTAMP '2026-10-12 12:00:00', 'router-r', 'eli', 'NATIVE', NULL, NULL, NULL, NULL)",
				"INSERT INTO MESSAGE VALUES ('6', 't3', 'RESPONSE', 50, NULL, NULL, NULL, 'route_ask', 900, "
						+ "TIMESTAMP '2026-10-12 12:00:01', 'router-r', 'eli', 'NATIVE', NULL, NULL, NULL, NULL)",
				// the day before, and the same id under another login type
				"INSERT INTO MESSAGE VALUES ('7', 't4', 'INPUT', 1000, 0, 0, NULL, 'ask', NULL, "
						+ "TIMESTAMP '2026-10-11 23:59:59', 'model-m', 'eli', 'NATIVE', NULL, NULL, NULL, NULL)",
				"INSERT INTO MESSAGE VALUES ('8', 't5', 'INPUT', 7, 0, 0, NULL, 'ask', NULL, "
						+ "TIMESTAMP '2026-10-12 13:00:00', 'model-m', 'eli', 'GOOGLE', NULL, NULL, NULL, NULL)");
	}

	@Test
	void aPersonsUseOfAModelCountsEveryCallInTheWindow() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			messages(db);
			ModelUsageLogFilter person = new ModelUsageLogFilter("model-m", "eli", "NATIVE", null, null, null, false);
			assertEquals(210, ModelInferenceLogsUtils.sumModelUsage(Metric.TOKENS, person, DAY_START, DAY_END));
			// each call's time counts once, from its RESPONSE row
			assertEquals(2500, ModelInferenceLogsUtils.sumModelUsage(Metric.COMPUTE_TIME, person, DAY_START, DAY_END));
			assertEquals(300, ModelInferenceLogsUtils.sumModelUsage(Metric.COST, person, DAY_START, DAY_END));
		}
	}

	@Test
	void teamCountersCountOnlyCallsBilledToTheTeam() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			messages(db);
			ModelUsageLogFilter ops = new ModelUsageLogFilter("model-m", null, null, "TEAM", "Ops", "CUSTOM", false);
			assertEquals(150, ModelInferenceLogsUtils.sumModelUsage(Metric.TOKENS, ops, DAY_START, DAY_END));
			ModelUsageLogFilter data = new ModelUsageLogFilter(null, null, null, "TEAM", "Data", "CUSTOM", false);
			assertEquals(0, ModelInferenceLogsUtils.sumModelUsage(Metric.TOKENS, data, DAY_START, DAY_END));
		}
	}

	@Test
	void allModelsCountersLeaveOutARoutersOwnRows() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			messages(db);
			ModelUsageLogFilter allModels = new ModelUsageLogFilter(null, "eli", "NATIVE", null, null, null, true);
			assertEquals(210, ModelInferenceLogsUtils.sumModelUsage(Metric.TOKENS, allModels, DAY_START, DAY_END));
			ModelUsageLogFilter router = new ModelUsageLogFilter("router-r", null, null, null, null, null, false);
			assertEquals(150, ModelInferenceLogsUtils.sumModelUsage(Metric.TOKENS, router, DAY_START, DAY_END));
		}
	}

	@Test
	void callsFromBeforeCostsWereRecordedAreTotalledForPricing() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			messages(db);
			ModelUsageLogFilter person = new ModelUsageLogFilter("model-m", "eli", "NATIVE", null, null, null, false);
			List<ModelUsageLogTotals> totals = ModelInferenceLogsUtils.sumModelUsageBeforeCost(person, DAY_START,
					DAY_END);
			assertEquals(List.of(new ModelUsageLogTotals("model-m", 40, 10, 5, 20, 4)), totals);
		}
	}
}

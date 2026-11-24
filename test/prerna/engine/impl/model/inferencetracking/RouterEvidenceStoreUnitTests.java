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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/** A routed step's cost joins on the router's transaction id although the served model writes its own rows. */
class RouterEvidenceStoreUnitTests {

	private static Map<String, Object> row(String tx, String messageId, String type, String method, Long cost) {
		Map<String, Object> row = new HashMap<>();
		row.put("TRANSACTION_ID", tx);
		row.put("MESSAGE_ID", messageId);
		row.put("MESSAGE_TYPE", type);
		row.put("MESSAGE_METHOD", method);
		row.put("COST_MICROS", cost);
		return row;
	}

	@Test
	void servedRowsMoveToTheRoutersTransactionAndTheRoutersOwnRowsAreDropped() {
		List<Map<String, Object>> rows = List.of(
				row("t-route", "m1", "INPUT", "route_ask", null), row("t-route", "t-route", "RESPONSE", "route_ask", null),
				row("t-served", "m1", "INPUT", "ask", null), row("t-served", "t-served", "RESPONSE", "ask", 407L),
				// a call that was not routed (classifier, room title) keeps its own transaction
				row("t-clf", "m0", "INPUT", "ask", null), row("t-clf", "t-clf", "RESPONSE", "ask", 26L));

		List<Map<String, Object>> out = RouterEvidenceStore.attribute(rows);

		assertEquals(2, out.size(), "only charge-bearing response rows are attributed");
		long routed = out.stream().filter(r -> "t-route".equals(r.get("TRANSACTION_ID")) && r.get("COST_MICROS") != null)
				.mapToLong(r -> (Long) r.get("COST_MICROS")).sum();
		assertEquals(407L, routed);
		assertEquals(26L, out.stream().filter(r -> "t-clf".equals(r.get("TRANSACTION_ID")) && r.get("COST_MICROS") != null)
				.mapToLong(r -> (Long) r.get("COST_MICROS")).sum());
		assertEquals(0, out.stream().filter(r -> "t-served".equals(r.get("TRANSACTION_ID"))).count());
	}
}

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
package prerna.reactor.masterdatabase.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class GenerateMetamodelLayoutUnitTests {

	@Test
	void tablesWithASavedPositionKeepItAndNewTablesGoBelow() {
		List<Map<String, Object>> tables = List.of(table("orders", 3), table("customers", 2), table("invoices", 4),
				table("refunds", 1));
		Map<String, Object> saved = new HashMap<>();
		saved.put("orders", position(100, 40));
		// saved positions can come back as other kinds of numbers, or as text
		saved.put("customers", Map.of("top", "400", "left", 500.0f));

		Map<String, Map<String, Double>> positions = GenerateMetamodelLayout.generateMetamodelLayoutForExternal(tables,
				List.of(), saved);

		assertEquals(100.0, positions.get("orders").get("top"));
		assertEquals(40.0, positions.get("orders").get("left"));
		assertEquals(400.0, positions.get("customers").get("top"));
		assertEquals(500.0, positions.get("customers").get("left"));
		// customers is the lowest saved table: 400 + a header and two rows
		double savedBottom = 400 + 50 + 2 * 54;
		for (String newTable : List.of("invoices", "refunds")) {
			assertTrue(positions.get(newTable).get("top") > savedBottom, newTable + " is below the saved tables");
			assertTrue(positions.get(newTable).get("left") >= 40, newTable + " starts at the saved tables' left edge");
		}
		// the two new tables sit side by side in one row
		assertEquals(positions.get("invoices").get("top"), positions.get("refunds").get("top"));
		assertTrue(positions.get("refunds").get("left") > positions.get("invoices").get("left") + 320);
	}

	private static Map<String, Object> table(String name, int columnCount) {
		Map<String, Object> table = new HashMap<>();
		table.put("table", name);
		table.put("columns", Collections.nCopies(columnCount, "col"));
		return table;
	}

	private static Map<String, Object> position(double top, double left) {
		Map<String, Object> position = new HashMap<>();
		position.put("top", top);
		position.put("left", left);
		return position;
	}
}

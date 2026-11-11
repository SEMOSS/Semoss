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
package prerna.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class AccessPermissionEnumUnitTests {

	private static Map<String, Object> row(String user, Object permission) {
		Map<String, Object> row = new HashMap<>();
		row.put("userid", user);
		row.put("permission", permission);
		return row;
	}

	@ParameterizedTest
	@CsvSource({ "OWNER,OWNER", "edit,EDIT", "' Read_Only ',READ_ONLY" })
	void requireReturnsTheNameOfALevelInAnyCase(String sent, String name) {
		assertEquals(name, AccessPermissionEnum.require(sent));
	}

	@ParameterizedTest
	@NullAndEmptySource
	@ValueSource(strings = { " ", "OWNR", "1", "ADMIN" })
	void requireRefusesAMissingOrUnknownLevel(String sent) {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> AccessPermissionEnum.require(sent));
		assertEquals("The permission must be OWNER, EDIT or READ_ONLY. Received value = " + sent, e.getMessage());
	}

	@Test
	void theLenientLookupsStillReadAnUnknownLevelAsReadOnly() {
		assertEquals(AccessPermissionEnum.READ_ONLY.getId(), AccessPermissionEnum.getIdByPermission("OWNR"));
		assertEquals("READ_ONLY", AccessPermissionEnum.getPermissionValueById(7));
	}

	@Test
	void requireEachWritesTheNameOfEachRowsLevelBack() {
		List<Map<String, Object>> rows = new ArrayList<>(List.of(row("u1", "owner"), row("u2", " edit ")));

		assertEquals(rows, AccessPermissionEnum.requireEach(rows));
		assertEquals("OWNER", rows.get(0).get("permission"));
		assertEquals("EDIT", rows.get(1).get("permission"));
	}

	@Test
	void requireEachRefusesARowWithoutAValidLevel() {
		List<Map<String, Object>> rows = new ArrayList<>(List.of(row("u1", "EDIT"), row("u2", 7.0)));

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> AccessPermissionEnum.requireEach(rows));
		assertEquals("The permission must be OWNER, EDIT or READ_ONLY. Received value = 7.0", e.getMessage());
	}

	@Test
	void requireEachRefusesAMissingRowAndLeavesNoRowsAlone() {
		List<Map<String, Object>> rows = new ArrayList<>();
		rows.add(null);

		assertThrows(IllegalArgumentException.class, () -> AccessPermissionEnum.requireEach(rows));
		assertNull(AccessPermissionEnum.requireEach(null));
	}

	@Test
	void requireByIdReturnsTheLevelWithTheId() {
		assertEquals(AccessPermissionEnum.OWNER, AccessPermissionEnum.requireById(1));
		assertEquals(AccessPermissionEnum.EDIT, AccessPermissionEnum.requireById(2));
		assertEquals(AccessPermissionEnum.READ_ONLY, AccessPermissionEnum.requireById(3));
	}

	@ParameterizedTest
	@ValueSource(ints = { 0, 4, 7, -1 })
	void requireByIdRefusesAnIdThatIsNotALevel(int id) {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> AccessPermissionEnum.requireById(id));
		assertEquals("The permission must be 1 (OWNER), 2 (EDIT) or 3 (READ_ONLY). Received value = " + id,
				e.getMessage());
	}
}

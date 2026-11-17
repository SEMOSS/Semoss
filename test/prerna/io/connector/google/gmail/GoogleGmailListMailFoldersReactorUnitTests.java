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
package prerna.io.connector.google.gmail;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class GoogleGmailListMailFoldersReactorUnitTests {

	@Test
	void eachLabelSitsUnderTheNearestLabelItsNameContinues() {
		List<GoogleGmailListMailFoldersReactor.PlacedLabel> placed = GoogleGmailListMailFoldersReactor
				.place(List.of(label("INBOX", "INBOX", "system"), label("L1", "Projects", "user"),
						label("L2", "Projects/Alpha", "user"), label("L3", "Projects/Alpha/Specs", "user"),
						label("L4", "Projects/Beta/Notes", "user"), label("L5", "Orphan/Child", "user")));

		assertNull(placed.get(0).parentId());
		assertEquals(0, placed.get(0).depth());
		assertNull(placed.get(1).parentId());
		assertEquals("L1", placed.get(2).parentId());
		assertEquals("Projects", placed.get(2).parentPath());
		assertEquals(1, placed.get(2).depth());
		assertEquals("L2", placed.get(3).parentId());
		assertEquals(2, placed.get(3).depth());
		// Projects/Beta was never made, so the label sits under the nearest one that
		// was
		assertEquals("L1", placed.get(4).parentId());
		assertEquals(1, placed.get(4).depth());
		assertNull(placed.get(5).parentId());
		assertEquals(0, placed.get(5).depth());
	}

	@Test
	void aLabelGmailKeepsIsNeverNestedUnderAnother() {
		List<GoogleGmailListMailFoldersReactor.PlacedLabel> placed = GoogleGmailListMailFoldersReactor
				.place(List.of(label("L1", "CATEGORY", "user"), label("CATEGORY_SOCIAL", "CATEGORY/SOCIAL", "system")));

		assertNull(placed.get(1).parentId());
		assertEquals(0, placed.get(1).depth());
	}

	private static Map<String, Object> label(String id, String name, String type) {
		return Map.of("id", id, "name", name, "type", type);
	}
}

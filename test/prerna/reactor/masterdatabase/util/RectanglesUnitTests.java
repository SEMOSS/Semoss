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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.awt.geom.Rectangle2D;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class RectanglesUnitTests {

	@Test
	void overlappingRectanglesEndUpApart() {
		Map<String, Rectangle2D> rectangles = new HashMap<>();
		for (int index = 0; index < 200; index++) {
			// a pile of tables around the same spot, as an unsettled layout leaves them
			rectangles.put("table" + index,
					new Rectangle2D.Double(index % 7 * 20, index % 5 * 15, 160, 40 + index % 9 * 10));
		}

		Map<String, Rectangle2D> fixed = new Rectangles().fix(rectangles);

		assertEquals(rectangles.keySet(), fixed.keySet());
		assertNoOverlaps(fixed);
	}

	@Test
	void identicalRectanglesAreSeparatedToo() {
		Map<String, Rectangle2D> rectangles = new HashMap<>();
		rectangles.put("a", new Rectangle2D.Double(0, 0, 160, 50));
		rectangles.put("b", new Rectangle2D.Double(0, 0, 160, 50));

		assertNoOverlaps(new Rectangles().fix(rectangles));
	}

	@Test
	void eachRectangleKeepsItsLeftEdgeAndOnlyMovesDown() {
		Map<String, Rectangle2D> rectangles = new HashMap<>();
		rectangles.put("a", new Rectangle2D.Double(0, 0, 160, 50));
		rectangles.put("b", new Rectangle2D.Double(40, 20, 160, 50));
		rectangles.put("apart", new Rectangle2D.Double(500, 0, 160, 50));

		Map<String, Rectangle2D> fixed = new Rectangles().fix(rectangles);

		for (Map.Entry<String, Rectangle2D> entry : rectangles.entrySet()) {
			Rectangle2D moved = fixed.get(entry.getKey());
			assertEquals(entry.getValue().getX(), moved.getX());
			assertTrue(moved.getY() >= entry.getValue().getY());
		}
		// one that overlapped nothing stays where it was
		assertEquals(0, fixed.get("apart").getY());
		// the input is not changed
		assertEquals(20, rectangles.get("b").getY());
	}

	private static void assertNoOverlaps(Map<String, Rectangle2D> rectangles) {
		List<Rectangle2D> all = new ArrayList<>(rectangles.values());
		for (int first = 0; first < all.size(); first++) {
			for (int second = first + 1; second < all.size(); second++) {
				assertFalse(all.get(first).intersects(all.get(second)),
						all.get(first) + " overlaps " + all.get(second));
			}
		}
	}
}

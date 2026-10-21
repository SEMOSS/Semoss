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

import java.awt.geom.Rectangle2D;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Separates the rectangles of a metamodel layout so no two overlap.
 */
public class Rectangles {

	/** The space kept between two rectangles, in the layout's units */
	private static final double MARGIN = 10;

	/**
	 * Moves rectangles apart so none overlap, keeping each one's left edge and
	 * moving it only down. The rectangles are placed from the top down, in one
	 * pass: each goes at the first height, from its own down, where it fits beside
	 * the rectangles already placed that share its columns. So the separation
	 * always finishes, in time that grows with the square of the number of
	 * rectangles.
	 *
	 * @param rectanglesToFix the rectangles to separate, by name; they are not
	 *                        changed
	 * @return a separated copy of each rectangle, by name
	 */
	public Map<String, Rectangle2D> fix(Map<String, Rectangle2D> rectanglesToFix) {
		List<Entry<String, Rectangle2D>> fromTheTop = new ArrayList<>(rectanglesToFix.entrySet());
		fromTheTop.sort(Comparator.comparingDouble((Entry<String, Rectangle2D> entry) -> entry.getValue().getY())
				.thenComparingDouble(entry -> entry.getValue().getX()).thenComparing(entry -> entry.getKey()));

		Map<String, Rectangle2D> fixed = new HashMap<>(rectanglesToFix.size());
		// the rectangles placed so far, by their top edge
		List<Rectangle2D> placed = new ArrayList<>(rectanglesToFix.size());
		for (Entry<String, Rectangle2D> entry : fromTheTop) {
			Rectangle2D rectangle = entry.getValue();
			double left = rectangle.getX();
			double width = rectangle.getWidth();
			double height = rectangle.getHeight();
			double top = rectangle.getY();
			for (Rectangle2D other : placed) {
				// sorted by top, so once one starts below the spot, every later one does too
				if (other.getY() >= top + height + MARGIN) {
					break;
				}
				boolean sharesColumns = other.getX() < left + width + MARGIN
						&& left < other.getX() + other.getWidth() + MARGIN;
				double otherBottom = other.getY() + other.getHeight();
				if (sharesColumns && otherBottom + MARGIN > top) {
					top = otherBottom + MARGIN;
				}
			}
			Rectangle2D moved = new Rectangle2D.Double(left, top, width, height);
			fixed.put(entry.getKey(), moved);
			placed.add(insertionPoint(placed, top), moved);
		}
		return fixed;
	}

	/**
	 * Where a rectangle with the given top edge goes in a list kept in order of top
	 * edge.
	 */
	private static int insertionPoint(List<Rectangle2D> byTop, double top) {
		int low = 0;
		int high = byTop.size();
		while (low < high) {
			int middle = (low + high) >>> 1;
			if (byTop.get(middle).getY() <= top) {
				low = middle + 1;
			} else {
				high = middle;
			}
		}
		return low;
	}
}

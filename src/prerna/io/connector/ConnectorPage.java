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
package prerna.io.connector;

import java.util.List;

/**
 * One page of what a listing found, and whether there is more after it.
 *
 * @param items   what is on this page
 * @param hasMore whether asking again with a larger offset finds more
 * @param <T>     what was listed
 */
public record ConnectorPage<T>(List<T> items, boolean hasMore) {

	public ConnectorPage {
		items = items == null ? List.of() : List.copyOf(items);
	}

	/**
	 * Cut one page out of everything a provider returned.
	 *
	 * <p>
	 * A provider that cannot skip on the server returns everything up to one past
	 * the end of the page, and the one past the end is how this knows there is
	 * more.
	 * </p>
	 *
	 * @param all    what was read, starting from the first item
	 * @param offset how many items to skip
	 * @param limit  how many items the page holds
	 * @param <T>    what was listed
	 * @return the page
	 */
	public static <T> ConnectorPage<T> of(List<T> all, int offset, int limit) {
		if (all == null || offset >= all.size()) {
			return new ConnectorPage<>(List.of(), false);
		}
		int end = Math.min(all.size(), offset + limit);
		return new ConnectorPage<>(all.subList(offset, end), all.size() > end);
	}

	/**
	 * Read a page a provider already skipped to, which holds one item past the end
	 * when there is more.
	 *
	 * @param window what was read, starting at the offset
	 * @param limit  how many items the page holds
	 * @param <T>    what was listed
	 * @return the page
	 */
	public static <T> ConnectorPage<T> ofWindow(List<T> window, int limit) {
		return of(window, 0, limit);
	}
}

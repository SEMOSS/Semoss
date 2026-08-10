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
package prerna.reactor.playwright;

import java.util.Map;
import java.util.Objects;

/**
 * Represents a patch for updating metadata, specifically the title,
 * description, and intent. This record is an immutable data carrier used for
 * modifying the metadata of a Playwright recording or session.
 *
 * @param title       The new title for the metadata. If null, the existing
 *                    title will be preserved.
 * @param description The new description for the metadata. If null, the
 *                    existing description will be preserved.
 * @param intent      The new intent for the metadata. If null, the existing
 *                    intent will be preserved.
 */
public record MetaPatch(String title, String description, String intent) {

	/**
	 * Builds a patch from a map. Keys that are absent stay null so the caller keeps
	 * the existing metadata value for that field.
	 *
	 * @param paramValues The paramValues map typically provided from the reactor
	 *                    noun store. May be null, which yields an empty patch.
	 * @return A {@link MetaPatch} holding only the supplied fields.
	 */
	public static MetaPatch fromMap(Map<String, ?> paramValues) {
		if (paramValues == null) {
			return new MetaPatch(null, null, null);
		}
		return new MetaPatch(stringValue(paramValues.get("title")), stringValue(paramValues.get("description")),
				stringValue(paramValues.get("intent")));
	}

	private static String stringValue(Object value) {
		return value == null ? null : Objects.toString(value);
	}
}

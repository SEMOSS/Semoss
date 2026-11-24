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
package prerna.auth.access;

import java.util.Locale;

/**
 * What to list of the permissions engines and projects give apps, agents and
 * engines, for the pages that list them all: text to search for, each side's
 * kind and type, and the page. The kind splits the list into what projects use
 * of projects, what projects use of engines, and what engines use of engines.
 *
 * @param search      text the name or id of the target or grantee contains,
 *                    ignoring case, or blank for any
 * @param targetKind  {@code PROJECT} or {@code ENGINE}, what gives the
 *                    permission, or blank for either
 * @param targetType  the target's type: a project's type, such as SKILL, or an
 *                    engine's catalog type, such as MODEL; blank for any
 * @param granteeKind {@code PROJECT} or {@code ENGINE}, what is given the
 *                    permission, or blank for either
 * @param granteeType the grantee's type in the same way, such as WORKSPACE for
 *                    an agent; blank for any
 * @param limit       how many to return, or 0 or less for all
 * @param offset      how many to skip
 */
public record GranteePermissionQuery(String search, String targetKind, String targetType, String granteeKind,
		String granteeType, int limit, int offset) {

	/** A project, on either side */
	public static final String PROJECT_KIND = "PROJECT";
	/** An engine, on either side */
	public static final String ENGINE_KIND = "ENGINE";

	/**
	 * Checks each kind given.
	 *
	 * @throws IllegalArgumentException when a kind is neither PROJECT nor ENGINE
	 */
	public GranteePermissionQuery {
		targetKind = readKind(targetKind);
		granteeKind = readKind(granteeKind);
	}

	/**
	 * A query for every permission, unfiltered and in one page.
	 *
	 * @return the query
	 */
	public static GranteePermissionQuery all() {
		return new GranteePermissionQuery(null, null, null, null, null, 0, 0);
	}

	/**
	 * Whether a side is of the kind asked for.
	 *
	 * @param kind      the kind asked for, or null for either
	 * @param isProject whether the side is a project
	 * @return whether it matches
	 */
	public static boolean matchesKind(String kind, boolean isProject) {
		return kind == null || kind.equals(isProject ? PROJECT_KIND : ENGINE_KIND);
	}

	private static String readKind(String kind) {
		if (kind == null || kind.isBlank()) {
			return null;
		}
		String read = kind.trim().toUpperCase(Locale.ROOT);
		if (!PROJECT_KIND.equals(read) && !ENGINE_KIND.equals(read)) {
			throw new IllegalArgumentException("A kind is PROJECT or ENGINE, not " + kind);
		}
		return read;
	}
}

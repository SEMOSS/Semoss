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
package prerna.reactor;

import java.util.Set;

/**
 * How a reactor class is named in Pixel and whether it belongs to one frame
 * type. {@link ReactorFactory} registers reactors with these rules, and the
 * OpenAPI export reads contracts with them, so both always agree.
 */
public final class ReactorNames {

	private static final String REACTOR_SUFFIX = "REACTOR";
	private static final Set<String> FRAME_TYPES = Set.of("rdbms", "r", "py", "tinker", "graph");

	private ReactorNames() {
	}

	/** The name a reactor is called by in Pixel: its class name without "Reactor". */
	public static String pixelName(String simpleClassName) {
		if (simpleClassName.toUpperCase().endsWith(REACTOR_SUFFIX)) {
			return simpleClassName.substring(0, simpleClassName.length() - REACTOR_SUFFIX.length());
		}
		return simpleClassName;
	}

	/**
	 * The frame type a reactor in this package is specific to, as written in the
	 * package (such as {@code r} for {@code prerna.reactor.frame.r}), or null for
	 * a general reactor. A package one level below {@code frame}, or two
	 * ({@code *.frame.r.util}), names its frame type.
	 */
	public static String frameType(String packageName) {
		String[] packagePaths = packageName.split("\\.");
		String last = packagePaths[packagePaths.length - 1];
		boolean frame = false;
		if (packagePaths.length > 2) {
			frame = packagePaths[packagePaths.length - 2].equalsIgnoreCase("frame");
		}
		// we will allow for 1 more level
		// i.e. things of the form so *.frame.r.?
		if (!frame && packagePaths.length > 3) {
			last = packagePaths[packagePaths.length - 2];
			frame = packagePaths[packagePaths.length - 3].equalsIgnoreCase("frame");
		}
		if (frame && FRAME_TYPES.contains(last.toLowerCase())) {
			return last;
		}
		// not under frame, or a general reactor under it (such as frame filters)
		return null;
	}
}

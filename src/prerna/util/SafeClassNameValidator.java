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
package prerna.util;

import java.util.Set;

/**
 * Gatekeeper for the various spots across the codebase that resolve a class
 * name originating in JSON, an smss property, or another externally-influenced
 * string (a "class"/"reactorType"/"outputDataClass"/interceptor or engine-type
 * field) via {@code Class.forName(...)} before instantiating or reflectively
 * populating it. Those names can end up attacker-controlled through a pixel
 * argument or a user-supplied engine property map, so the name is checked
 * against this allow-list before resolution: restricting to first-party
 * (prerna.*) classes plus a short list of common JDK value types blocks the
 * well-known deserialization gadget classes that live in third-party libraries
 * on the classpath, without requiring an exhaustive enumeration of every
 * legitimate class each call site could legitimately see.
 */
public final class SafeClassNameValidator {

	private static final Set<String> SAFE_JDK_VALUE_CLASSES = Set.of(
			"java.lang.String",
			"java.lang.Boolean",
			"java.lang.Integer",
			"java.lang.Long",
			"java.lang.Short",
			"java.lang.Byte",
			"java.lang.Double",
			"java.lang.Float",
			"java.lang.Character",
			"java.math.BigDecimal",
			"java.math.BigInteger",
			"java.util.HashMap",
			"java.util.LinkedHashMap",
			"java.util.TreeMap",
			"java.util.ArrayList",
			"java.util.LinkedList",
			"java.util.Vector",
			"java.util.HashSet",
			"java.util.LinkedHashSet",
			"java.util.TreeSet"
	);

	private SafeClassNameValidator() {
	}

	/**
	 * @param className fully qualified class name read from untrusted input
	 * @return true if it is safe to pass to Class.forName for resolution/instantiation
	 */
	public static boolean isAllowed(String className) {
		if (className == null || className.isEmpty()) {
			return false;
		}
		if (className.startsWith("prerna.")) {
			return true;
		}
		return SAFE_JDK_VALUE_CLASSES.contains(className);
	}
}

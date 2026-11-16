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

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The sandbox rules for Jackson, which project reactor code shares with the
 * platform. A mapper reads and writes JSON, and every way to have it create
 * objects of the classes the data names is refused: the default typing calls,
 * the type factory's class lookups, the helper that finds and creates classes by
 * name, and the {@code @JsonTypeInfo} settings that name a class.
 */
final class SandboxedJackson {

	/**
	 * The packages project reactor code shares with the platform, as name
	 * prefixes: Jackson 2 and Jackson 3.
	 */
	static final List<String> SHARED_PACKAGES = List.of("com.fasterxml.jackson.", "tools.jackson.");

	/**
	 * Single classes kept out of the shared packages, so a project gets its own
	 * sandboxed copy: the helper that finds and creates classes by name.
	 */
	static final Set<String> UNSHARED_CLASSES = Set.of("com.fasterxml.jackson.databind.util.ClassUtil",
			"tools.jackson.databind.util.ClassUtil");

	/**
	 * Calls project reactor code cannot make, by owner then method name.
	 */
	static final Map<String, Set<String>> BLOCKED_CALLS = Map.of("com/fasterxml/jackson/databind/type/TypeFactory",
			Set.of("findClass"), "tools/jackson/databind/type/TypeFactory", Set.of("findClass"));

	/**
	 * Calls project reactor code cannot make on any class, by method name. Each
	 * tells a Jackson mapper to create objects of the classes that the JSON, or a
	 * string, names.
	 */
	static final Set<String> BLOCKED_CALLS_ON_ANY_CLASS = Set.of("enableDefaultTyping",
			"enableDefaultTypingAsProperty", "activateDefaultTyping", "activateDefaultTypingAsProperty",
			"setDefaultTyping", "constructFromCanonical");

	/**
	 * The Jackson annotation that picks how a type is named in JSON.
	 */
	static final String JSON_TYPE_INFO = "Lcom/fasterxml/jackson/annotation/JsonTypeInfo;";

	/**
	 * The two settings of {@link #JSON_TYPE_INFO} that name a class.
	 */
	static final Set<String> JSON_TYPE_BY_CLASS_NAME = Set.of("CLASS", "MINIMAL_CLASS");

	private SandboxedJackson() {
	}
}

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

import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

import com.google.gson.Gson;
import com.google.gson.ReflectionAccessFilter;
import com.google.gson.ReflectionAccessFilter.FilterResult;

/**
 * The sandbox rules for Gson, which project reactor code shares with the
 * platform. Gson reads and writes the fields of an object by reflection, for
 * whoever hands it the object or names its class, so for project code it does
 * that for the project's own classes only. Every other value is JSON data:
 * maps, lists, text, numbers and the JSON tree. A platform object is turned
 * into JSON through its own methods, and built through its own constructors.
 *
 * <p>
 * The rule is applied where a Gson is used. Each call project code makes on a
 * Gson runs on a twin of it that has the same settings and the rule, so it holds
 * for a Gson the project builds any way it likes and for one the platform hands
 * it.
 */
public final class SandboxedGson {

	/**
	 * Internal name of this class, which the rewritten calls name.
	 */
	static final String INTERNAL_NAME = "prerna/util/SandboxedGson";

	/**
	 * Internal name of the Gson class whose calls run on a twin.
	 */
	static final String GSON = "com/google/gson/Gson";

	/**
	 * Lets Gson use reflection on a class the project's sandbox loaded, and on no
	 * other.
	 */
	private static final ReflectionAccessFilter PROJECT_CLASSES_ONLY = type -> SandboxedClassLoading
			.isProjectClass(type) ? FilterResult.ALLOW : FilterResult.BLOCK_ALL;

	/**
	 * The twin of each Gson project code has used, kept for as long as the Gson
	 * itself.
	 */
	private static final Map<Gson, Gson> TWINS = Collections.synchronizedMap(new WeakHashMap<>());

	private SandboxedGson() {
	}

	/**
	 * Stands in for a Gson project code is about to call: the twin with the same
	 * settings that uses reflection on the project's own classes only. The rule is
	 * added last, and Gson asks the filter added last first, so a filter the
	 * project adds does not change it.
	 *
	 * @param gson the Gson the project holds, or null
	 * @return its twin, or null
	 */
	public static Gson guarded(Gson gson) {
		if (gson == null) {
			return null;
		}
		return TWINS.computeIfAbsent(gson,
				original -> original.newBuilder().addReflectionAccessFilter(PROJECT_CLASSES_ONLY).create());
	}
}

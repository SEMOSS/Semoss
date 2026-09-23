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
package prerna.reactor.interceptor;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;

public class ReactorInputHelper {

	private static final Object PATH_NOT_FOUND = new Object();

	private final NounStore nounStore;
	// The map containing ENGINE, METHOD_NAME, CONFIG, etc.
	private final Map<String, Object> argumentsMap;

	public ReactorInputHelper(NounStore nounStore) {
		this.nounStore = nounStore;
		// Assuming the main arguments map is stored under
		// PipelineReactorUtils.ARGUMENTS
		GenRowStruct grs = nounStore.getGenRowStruct(PipelineReactorUtils.ARGUMENTS);
		if (grs != null && grs.size() > 0) {
			this.argumentsMap = (Map<String, Object>) grs.get(0);
		} else {
			this.argumentsMap = null;
		}
	}

	public Method getMethod() {
		if (argumentsMap != null && argumentsMap.containsKey(PipelineReactorUtils.METHOD_NAME)) {
			return (Method) argumentsMap.get(PipelineReactorUtils.METHOD_NAME);
		}
		return null;
	}

	public Map<String, Object> getConfig() {
		if (argumentsMap != null && argumentsMap.containsKey(PipelineReactorUtils.CONFIG)) {
			return (Map<String, Object>) argumentsMap.get(PipelineReactorUtils.CONFIG);
		}
		return null;
	}

	/**
	 * Retrieves a configuration parameter from the reactor's config map.
	 * 
	 * @param key  The key of the configuration parameter.
	 * @param type The expected class type of the parameter.
	 * @return The configuration parameter, or null if not found or type mismatch.
	 */
	public <T> T getConfigParameter(String key, Class<T> type) {
		Map<String, Object> config = getConfig();
		if (config != null && config.containsKey(key)) {
			Object value = config.get(key);
			if (value != null && type.isInstance(value)) {
				return type.cast(value);
			}
		}
		return null;
	}

	/**
	 * Retrieves a method argument by its name (e.g., {@code arg0} or
	 * {@code question}). A dot-separated path can select values inside map-shaped
	 * arguments and results. Numeric segments index lists, while {@code *} projects
	 * the rest of the path across every item in a list.
	 *
	 * <p>
	 * Examples are {@code arg0.message}, {@code result.messages.0.subject} and
	 * {@code result.messages.*.body}. This relies on how arguments are mapped in
	 * {@link prerna.engine.impl.pipeline.PipelineInvocationHandler}.
	 * 
	 * @param argName The name of the argument.
	 * @return The argument value, or null if not found.
	 */
	public Object getMethodArgument(String argName) {
		if (argumentsMap == null || argName == null || argName.isEmpty()) {
			return null;
		}
		if (argumentsMap.containsKey(argName)) {
			return argumentsMap.get(argName);
		}

		String[] path = argName.split("\\.");
		if (path.length < 2 || !argumentsMap.containsKey(path[0])) {
			return null;
		}
		Object resolved = resolvePath(argumentsMap.get(path[0]), path, 1);
		return resolved == PATH_NOT_FOUND ? null : resolved;
	}

	/**
	 * Replaces a method argument or a value nested inside one. Nested containers
	 * are copied on the way down, so masking a function parameter does not mutate
	 * the map supplied by its caller.
	 *
	 * @param argName argument name or dot-separated path
	 * @param value   replacement value
	 * @return whether the path existed and was replaced
	 */
	public boolean setMethodArgument(String argName, Object value) {
		if (argumentsMap == null || argName == null || argName.isEmpty()) {
			return false;
		}
		if (argumentsMap.containsKey(argName)) {
			argumentsMap.put(argName, value);
			return true;
		}

		String[] path = argName.split("\\.");
		if (path.length < 2 || !argumentsMap.containsKey(path[0])) {
			return false;
		}
		Object replacement = replacePath(argumentsMap.get(path[0]), path, 1, value);
		if (replacement == PATH_NOT_FOUND) {
			return false;
		}
		argumentsMap.put(path[0], replacement);
		return true;
	}

	/**
	 * Retrieves a method argument by its name and casts it to the specified type.
	 * 
	 * @param argName The name of the argument.
	 * @param type    The expected class type of the argument.
	 * @return The argument value, or null if not found or type mismatch.
	 */
	public <T> T getMethodArgument(String argName, Class<T> type) {
		Object value = getMethodArgument(argName);
		if (value != null && type.isInstance(value)) {
			return type.cast(value);
		}
		return null;
	}

	/**
	 * Retrieves the entire argument map
	 * 
	 * @return The argument map
	 */
	public Map<String, Object> getArgumentsMap() {
		return argumentsMap;
	}

	/**
	 * Walks a dot-separated path into a map or list shaped value. Numeric segments
	 * index lists and {@code *} projects the rest of the path across every item in
	 * a list, matching the paths {@link #getMethodArgument(String)} accepts.
	 *
	 * <p>
	 * This reads a path relative to a value the caller already holds, which is how
	 * a guardrail reaches inside a payload that is not itself a method argument.
	 *
	 * @param value value to read from
	 * @param path  dot-separated path relative to value, or null/empty for the
	 *              value itself
	 * @return the resolved value, or null when the path does not exist
	 */
	public static Object resolveValuePath(Object value, String path) {
		if (value == null) {
			return null;
		}
		if (path == null || path.isEmpty()) {
			return value;
		}
		Object resolved = resolvePath(value, path.split("\\."), 0);
		return resolved == PATH_NOT_FOUND ? null : resolved;
	}

	/**
	 * Reads one segment of an already split path and recurses into the rest. Maps
	 * are read by key and lists by numeric index, so the walk can only step into a
	 * map or a list; a String, a POJO, or null ends it.
	 *
	 * <p>
	 * A {@code *} segment projects the remaining path across every item of a list
	 * and collects the results, skipping items the rest of the path does not
	 * resolve for and flattening one level when an item resolves to a list of its
	 * own. A projection therefore reports an empty list rather than a missing path
	 * when nothing matched.
	 *
	 * @param current   the value this segment is read from
	 * @param path      the whole path, already split on dots
	 * @param pathIndex the segment to read now, equal to the path length once every
	 *                  segment has been walked
	 * @return the resolved value, or {@code PATH_NOT_FOUND} when the path does not
	 *         exist. The sentinel is returned instead of null so callers can tell a
	 *         missing path apart from a path that resolves to null.
	 */
	private static Object resolvePath(Object current, String[] path, int pathIndex) {
		if (pathIndex == path.length) {
			return current;
		}
		if (current instanceof Map) {
			Map<?, ?> currentMap = (Map<?, ?>) current;
			String segment = path[pathIndex];
			if (!currentMap.containsKey(segment)) {
				return PATH_NOT_FOUND;
			}
			return resolvePath(currentMap.get(segment), path, pathIndex + 1);
		}
		if (current instanceof List) {
			List<?> currentList = (List<?>) current;
			String segment = path[pathIndex];
			if ("*".equals(segment)) {
				List<Object> projected = new ArrayList<>();
				for (Object item : currentList) {
					Object resolved = resolvePath(item, path, pathIndex + 1);
					if (resolved == PATH_NOT_FOUND) {
						continue;
					}
					if (resolved instanceof List) {
						projected.addAll((List<?>) resolved);
					} else {
						projected.add(resolved);
					}
				}
				return projected;
			}
			try {
				int listIndex = Integer.parseInt(segment);
				if (listIndex < 0 || listIndex >= currentList.size()) {
					return PATH_NOT_FOUND;
				}
				return resolvePath(currentList.get(listIndex), path, pathIndex + 1);
			} catch (NumberFormatException e) {
				return PATH_NOT_FOUND;
			}
		}
		return PATH_NOT_FOUND;
	}

	/**
	 * Rebuilds the path with a new value at the end of it. Nested containers are
	 * copied rather than modified, so the map or list a caller passed into the
	 * intercepted method is left alone; each copy is returned to the level above to
	 * be stored in place of the original.
	 *
	 * <p>
	 * A {@code *} segment is refused, since one value cannot be written back across
	 * every item of a list.
	 *
	 * @param current   the value this segment is read from
	 * @param path      the whole path, already split on dots
	 * @param pathIndex the segment to replace now
	 * @param value     the replacement for the value at the end of the path
	 * @return a copy of {@code current} carrying the replacement, or
	 *         {@code PATH_NOT_FOUND} when the path does not exist, in which case
	 *         nothing has been copied or changed
	 */
	private Object replacePath(Object current, String[] path, int pathIndex, Object value) {
		if (pathIndex == path.length) {
			return value;
		}
		String segment = path[pathIndex];
		if (current instanceof Map) {
			Map<?, ?> currentMap = (Map<?, ?>) current;
			if (!currentMap.containsKey(segment)) {
				return PATH_NOT_FOUND;
			}
			Object replacement = replacePath(currentMap.get(segment), path, pathIndex + 1, value);
			if (replacement == PATH_NOT_FOUND) {
				return PATH_NOT_FOUND;
			}
			Map<Object, Object> copy = new LinkedHashMap<>(currentMap);
			copy.put(segment, replacement);
			return copy;
		}
		if (current instanceof List && !"*".equals(segment)) {
			List<?> currentList = (List<?>) current;
			try {
				int listIndex = Integer.parseInt(segment);
				if (listIndex < 0 || listIndex >= currentList.size()) {
					return PATH_NOT_FOUND;
				}
				Object replacement = replacePath(currentList.get(listIndex), path, pathIndex + 1, value);
				if (replacement == PATH_NOT_FOUND) {
					return PATH_NOT_FOUND;
				}
				List<Object> copy = new ArrayList<>(currentList);
				copy.set(listIndex, replacement);
				return copy;
			} catch (NumberFormatException e) {
				return PATH_NOT_FOUND;
			}
		}
		return PATH_NOT_FOUND;
	}

}

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

import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.json.JSONArray;
import org.json.JSONObject;

import prerna.reactor.IReactor.MCP_KEY_TYPE;
import prerna.sablecc2.om.ReactorKeysEnum;

/**
 * What a reactor tells its MCP tool, its help text and the SDKs about itself:
 * its description, its keys and its output.
 * {@link AbstractReactor#getReactorDefinition()} starts it from
 * {@code keysToGet} and {@code keyRequired}; a reactor overrides that method,
 * calls super, and adds what it knows.
 *
 * <pre>{@code
 * @Override
 * public ReactorDefinition getReactorDefinition() {
 * 	return super.getReactorDefinition().description("Stop the current execution of a pixel job")
 * 			.key(ReactorKeysEnum.ID, "The id for the job").output(String.class, "Whether the job was stopped");
 * }
 * }</pre>
 *
 * It is metadata only: {@code keysToGet} and {@code keyRequired} still decide
 * how a call's inputs are read.
 * <p>
 * A reactor that still overrides the deprecated {@code getReactorDescription},
 * {@code getDescriptionForKey} or {@code getKeyTypeForMCP} keeps working, since
 * {@code super.getReactorDefinition()} reads those overrides. To migrate one,
 * move each value into a single {@code getReactorDefinition()} override and
 * delete the old methods:
 * <ul>
 * <li>{@code getReactorDescription()} returning text becomes
 * {@code .description(text)};</li>
 * <li>{@code getDescriptionForKey} returning text for a key becomes
 * {@code .key(key, text)}, with a {@link ReactorKeysEnum} constant or the key
 * name;</li>
 * <li>{@code getKeyTypeForMCP} returning a type for a key becomes
 * {@code .key(key, k -> k.type(...))}, with the Java type for each MCP type
 * listed on {@link AbstractReactor#getKeyTypeForMCP};</li>
 * <li>a JSON Schema of the output, which {@code getResponseSchema()} returned,
 * becomes {@code .outputSchema(schema)}.</li>
 * </ul>
 * Code that computes a value, such as text built from a field set in the
 * constructor, moves into the method as it is. Migrate a class and its parents
 * together: when a subclass still overrides a deprecated method and its parent
 * overrides {@code getReactorDefinition()}, the parent's definition wins for
 * the keys it describes. To read what a reactor declares, call
 * {@code getReactorDefinition()} rather than the deprecated methods, which see
 * only overrides.
 */
public final class ReactorDefinition {

	/**
	 * The property a non-object output is wrapped in for MCP, whose structured
	 * results are objects.
	 */
	public static final String WRAPPED_RESULT = "result";

	/**
	 * Marks an MCP output schema whose result is wrapped in
	 * {@link #WRAPPED_RESULT}.
	 */
	public static final String WRAP_RESULT_MARKER = "x-semoss-wrap-result";

	/**
	 * Finds the class whose code made a problem, so the report says where to fix
	 * it.
	 */
	private static final StackWalker STACK = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);

	private final Map<String, Key> keys = new LinkedHashMap<>();
	private final List<Problem> problems = new ArrayList<>();
	private String description;
	private Output output;
	private boolean noKeys;

	/** Something wrong with the definition, and the class whose code did it. */
	private record Problem(Class<?> madeBy, String message) {
	}

	/**
	 * Starts a definition with one key for each canonical name in
	 * {@code keysToGet}, in order, required as {@code keyRequired} says. An empty
	 * key, or a canonical name listed twice, is left out and recorded as a problem
	 * of {@code reactorClass}.
	 */
	ReactorDefinition(Class<?> reactorClass, String[] keysToGet, int[] keyRequired) {
		if (keysToGet == null) {
			return;
		}
		for (int i = 0; i < keysToGet.length; i++) {
			String[] names = keysToGet[i] == null ? new String[] { "" } : keysToGet[i].split(",", -1);
			String name = names[0].trim();
			if (name.isEmpty()) {
				this.problems.add(new Problem(reactorClass, "has an empty key at position " + i + " of its keysToGet"));
				continue;
			}
			if (this.keys.containsKey(name)) {
				this.problems.add(new Problem(reactorClass, "lists the key \"" + name + "\" twice in its keysToGet"));
				continue;
			}
			List<String> aliases = new ArrayList<>();
			for (int j = 1; j < names.length; j++) {
				if (!names[j].isBlank()) {
					aliases.add(names[j].trim());
				}
			}
			Boolean required = keyRequired == null ? null : i < keyRequired.length && keyRequired[i] == 1;
			this.keys.put(name, new Key(name, List.copyOf(aliases), required));
		}
	}

	/** Sets what the reactor does. */
	public ReactorDefinition description(String description) {
		this.description = description;
		return this;
	}

	/** Describes one of the reactor's keys. */
	public ReactorDefinition key(String name, String description) {
		return key(name, key -> key.description(description));
	}

	/** Describes one of the reactor's standard keys. */
	public ReactorDefinition key(ReactorKeysEnum key, String description) {
		return key(key.getKey(), description);
	}

	/** Sets the details of one of the reactor's standard keys. */
	public ReactorDefinition key(ReactorKeysEnum key, Consumer<Key> details) {
		return key(key.getKey(), details);
	}

	/**
	 * Sets the details of one of the reactor's keys. A key that is not in
	 * {@code keysToGet} is skipped and recorded as a problem, with the class whose
	 * code named it; the OpenAPI export fails on it. A parent describes only the
	 * keys all its reactors take, and each reactor describes the rest of its own.
	 */
	public ReactorDefinition key(String name, Consumer<Key> details) {
		Key key = this.keys.get(name);
		if (key == null) {
			this.problems
					.add(new Problem(caller(), "describes the key \"" + name + "\", which is not in its keysToGet"));
			return this;
		}
		details.accept(key);
		return this;
	}

	/**
	 * Declares what the reactor returns: {@code String}, {@code Integer},
	 * {@code Long}, {@code Double}, {@code Boolean}, an enum, {@code Map}, a
	 * record, or {@code Object} for any value.
	 */
	public ReactorDefinition output(Class<?> type, String description) {
		this.output = new Output(type, Void.class, null, description);
		return this;
	}

	/** Declares that the reactor returns a list of {@code items}. */
	public ReactorDefinition outputList(Class<?> items, String description) {
		this.output = new Output(List.class, items, null, description);
		return this;
	}

	/** Declares what the reactor returns as a JSON Schema, used as is. */
	public ReactorDefinition outputSchema(JSONObject schema) {
		JSONObject copy = new JSONObject(schema.toString());
		this.output = new Output(null, Void.class, copy, copy.optString("description", null));
		return this;
	}

	/**
	 * Says the reactor takes no keys on purpose. It applies only while
	 * {@code keysToGet} is empty; said of a reactor with keys, it is recorded as a
	 * problem, with the class whose code said it, and the OpenAPI export fails on
	 * it.
	 */
	public ReactorDefinition noKeys() {
		if (!this.keys.isEmpty()) {
			this.problems.add(
					new Problem(caller(), "says it takes no keys, but the reactor declares " + this.keys.keySet()));
		}
		this.noKeys = true;
		return this;
	}

	/** What the reactor does, or null when it does not say. */
	public String getDescription() {
		return this.description;
	}

	/** Its keys, in {@code keysToGet} order. */
	public List<Key> getKeys() {
		return List.copyOf(this.keys.values());
	}

	/** One of its keys by canonical name, or null. */
	public Key getKey(String name) {
		return this.keys.get(name);
	}

	/** Its declared output, or null. */
	public Output getOutput() {
		return this.output;
	}

	/** Whether it says it takes no keys on purpose and has none. */
	public boolean isNoKeys() {
		return this.noKeys && this.keys.isEmpty();
	}

	/**
	 * What is wrong with the definition: an empty key or a key listed twice in
	 * {@code keysToGet}, a key described that is not in {@code keysToGet}, or
	 * {@code noKeys()} on a reactor with keys. Each starts with the class whose
	 * code made it, which may be a parent of the reactor.
	 *
	 * @return the problems, empty when there are none
	 */
	public List<String> getProblems() {
		return this.problems.stream()
				.map(problem -> (problem.madeBy() == null ? "" : problem.madeBy().getName() + " ") + problem.message())
				.toList();
	}

	/**
	 * The first class on the stack outside this one: the reactor code that called
	 * into the definition.
	 */
	private static Class<?> caller() {
		return STACK.walk(frames -> frames.map(frame -> frame.getDeclaringClass())
				.filter(type -> type != ReactorDefinition.class).findFirst().orElse(null));
	}

	/** One of the reactor's keys. */
	public static final class Key {

		private final String name;
		private final List<String> aliases;
		private Boolean required;
		private Class<?> type;
		private Class<?> items = Void.class;
		private String description;

		private Key(String name, List<String> aliases, Boolean required) {
			this.name = name;
			this.aliases = aliases;
			this.required = required;
		}

		/**
		 * Sets its type: {@code String}, {@code Integer}, {@code Long}, {@code Double},
		 * {@code Boolean}, an enum, {@code Map}, {@code List}, a record, or
		 * {@code Object} for any value.
		 */
		public Key type(Class<?> type) {
			this.type = type;
			this.items = Void.class;
			return this;
		}

		/** Makes it a list of {@code items}. */
		public Key list(Class<?> items) {
			this.type = List.class;
			this.items = items;
			return this;
		}

		/** Sets its description. */
		public Key description(String description) {
			this.description = description;
			return this;
		}

		/** Sets whether it must be passed, whatever {@code keyRequired} says. */
		public Key required(boolean required) {
			this.required = required;
			return this;
		}

		/** Its canonical name in {@code keysToGet}. */
		public String getName() {
			return this.name;
		}

		/** The other names it can be passed under. */
		public List<String> getAliases() {
			return this.aliases;
		}

		/** Its declared type, or null when the reactor does not declare one. */
		public Class<?> getType() {
			return this.type;
		}

		/**
		 * Its declared type, or when it has none, the type its MCP tool gives it:
		 * {@code Map} for the map and metadata keys, {@code Integer} for limit and
		 * offset, and {@code String} for the rest.
		 */
		public Class<?> getTypeOrDefault() {
			return this.type != null ? this.type : javaType(AbstractReactor.defaultKeyType(this.name));
		}

		/** The element type when it is a {@code List}, or {@code Void}. */
		public Class<?> getItems() {
			return this.items;
		}

		/** Its description, or null. */
		public String getDescription() {
			return this.description;
		}

		/**
		 * Whether it must be passed, or null when neither {@code keyRequired} nor the
		 * definition says.
		 */
		public Boolean getRequired() {
			return this.required;
		}
	}

	/** What the reactor returns: a Java type, or a JSON Schema given as is. */
	public static final class Output {

		private final Class<?> type;
		private final Class<?> items;
		private final JSONObject schema;
		private final String description;

		private Output(Class<?> type, Class<?> items, JSONObject schema, String description) {
			this.type = type;
			this.items = items;
			this.schema = schema;
			this.description = description;
		}

		/** The Java type, or null when the output is given as a JSON Schema. */
		public Class<?> getType() {
			return this.type;
		}

		/** The element type when the type is a {@code List}, or {@code Void}. */
		public Class<?> getItems() {
			return this.items;
		}

		/** The JSON Schema it was given as, or null. */
		public JSONObject getSchema() {
			return this.schema == null ? null : new JSONObject(this.schema.toString());
		}

		/** What the output is, or null. */
		public String getDescription() {
			return this.description;
		}

		/**
		 * The output as JSON Schema: the schema it was given as, or one built from its
		 * type.
		 */
		public JSONObject toJsonSchema() {
			if (this.schema != null) {
				return getSchema();
			}
			JSONObject built = List.class.isAssignableFrom(this.type) ? listSchema(this.items) : schemaOf(this.type);
			if (this.description != null && !this.description.isBlank()) {
				built.put("description", this.description);
			}
			return built;
		}

		/**
		 * The output schema for an MCP tool. MCP structured results are objects, so an
		 * output that is not one is wrapped in {@link #WRAPPED_RESULT} and marked
		 * {@link #WRAP_RESULT_MARKER}.
		 */
		public JSONObject toMcpOutputSchema() {
			JSONObject schema = toJsonSchema();
			if ("object".equals(schema.opt("type"))) {
				return schema;
			}
			JSONObject wrapped = new JSONObject();
			wrapped.put("type", "object");
			wrapped.put("properties", new JSONObject().put(WRAPPED_RESULT, schema));
			wrapped.put("required", new JSONArray().put(WRAPPED_RESULT));
			wrapped.put(WRAP_RESULT_MARKER, true);
			return wrapped;
		}
	}

	/** Maps the type MCP is told a key has to a Java type. */
	static Class<?> javaType(MCP_KEY_TYPE type) {
		if (type == null) {
			return String.class;
		}
		return switch (type) {
		case INTEGER -> Integer.class;
		case NUMBER -> Double.class;
		case BOOLEAN -> Boolean.class;
		case ARRAY -> List.class;
		case OBJECT -> Map.class;
		default -> String.class;
		};
	}

	/**
	 * Maps a Java type to the type MCP is told, or null for {@code Object}, which
	 * MCP has no type for.
	 */
	static MCP_KEY_TYPE mcpType(Class<?> type) {
		if (type == String.class || type.isEnum() || CharSequence.class.isAssignableFrom(type)) {
			return MCP_KEY_TYPE.STRING;
		}
		if (type == Integer.class || type == int.class || type == Long.class || type == long.class
				|| type == Short.class || type == short.class) {
			return MCP_KEY_TYPE.INTEGER;
		}
		if (Number.class.isAssignableFrom(type) || type == double.class || type == float.class) {
			return MCP_KEY_TYPE.NUMBER;
		}
		if (type == Boolean.class || type == boolean.class) {
			return MCP_KEY_TYPE.BOOLEAN;
		}
		if (Collection.class.isAssignableFrom(type) || type.isArray()) {
			return MCP_KEY_TYPE.ARRAY;
		}
		if (Map.class.isAssignableFrom(type) || type.isRecord()) {
			return MCP_KEY_TYPE.OBJECT;
		}
		return null;
	}

	/**
	 * The JSON Schema of a list of {@code items}, with no item type for
	 * {@code Void}.
	 */
	static JSONObject listSchema(Class<?> items) {
		JSONObject schema = new JSONObject().put("type", "array");
		if (items != null && items != Void.class) {
			schema.put("items", schemaOf(items));
		}
		return schema;
	}

	/** The JSON Schema of a Java type; {@code Object} is any value. */
	static JSONObject schemaOf(Class<?> type) {
		if (type.isEnum()) {
			JSONArray values = new JSONArray();
			for (Object constant : type.getEnumConstants()) {
				values.put(((Enum<?>) constant).name());
			}
			return new JSONObject().put("type", "string").put("enum", values);
		}
		if (type.isRecord()) {
			JSONObject properties = new JSONObject();
			JSONArray required = new JSONArray();
			for (RecordComponent component : type.getRecordComponents()) {
				properties.put(component.getName(), schemaOf(component.getType()));
				required.put(component.getName());
			}
			return new JSONObject().put("type", "object").put("properties", properties).put("required", required);
		}
		MCP_KEY_TYPE mapped = mcpType(type);
		if (mapped == MCP_KEY_TYPE.ARRAY) {
			return listSchema(type.isArray() ? type.getComponentType() : Void.class);
		}
		return mapped == null ? new JSONObject() : new JSONObject().put("type", mapped.getValue());
	}
}

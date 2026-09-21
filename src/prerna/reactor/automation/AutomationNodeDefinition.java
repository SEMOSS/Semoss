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
package prerna.reactor.automation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import prerna.engine.api.IEngine;

/**
 * Immutable authoring metadata for one supported Automation node type.
 *
 * @param nodeType        stable node identity and server-owned capabilities
 * @param label           default user-facing label
 * @param description     concise authoring description
 * @param defaultCodeMode generated or custom source mode used for new nodes
 * @param defaultConfig   server-owned initial configuration
 * @param configFields    ordered configuration contract
 * @param inputs          ordered input ports
 * @param outputs         ordered output ports
 */
public record AutomationNodeDefinition(AutomationNodeType nodeType, String label, String description,
		String defaultCodeMode, Map<String, Object> defaultConfig, List<ConfigField> configFields, List<Port> inputs,
		List<Port> outputs) {

	public AutomationNodeDefinition {
		defaultConfig = Collections.unmodifiableMap(new LinkedHashMap<>(defaultConfig));
		configFields = List.copyOf(configFields);
		inputs = List.copyOf(inputs);
		outputs = List.copyOf(outputs);
	}

	/** Supported configuration value shapes exposed to authoring clients. */
	public enum ConfigFieldType {
		ENGINE("engine"), STRING("string"), STRING_LIST("string[]"), TEXT("textarea"), CODE("code"),
		INTEGER("number"), JSON("json"), GLOBALS("globals"), BRANCH_CLAUSES("branch-clauses");

		private final String value;

		ConfigFieldType(String value) {
			this.value = value;
		}

		String getValue() {
			return this.value;
		}
	}

	/** Supported port kinds in the persisted graph. */
	public enum PortKind {
		CONTROL, DATA
	}

	/** Port direction in the persisted graph. */
	public enum PortDirection {
		INPUT, OUTPUT
	}

	/**
	 * One ordered node configuration field.
	 *
	 * @param key          persisted config key
	 * @param type         field value shape
	 * @param label        user-facing label
	 * @param required     whether authoring validation requires a value
	 * @param defaultValue initial value, or {@code null}
	 * @param minimum      optional inclusive numeric minimum
	 * @param maximum      optional inclusive numeric maximum
	 * @param engineType   optional engine catalog restriction
	 */
	public record ConfigField(String key, ConfigFieldType type, String label, boolean required, Object defaultValue,
			Number minimum, Number maximum, IEngine.CATALOG_TYPE engineType) {

		Map<String, Object> toMap() {
			Map<String, Object> field = new LinkedHashMap<>();
			field.put("type", this.type.getValue());
			field.put("label", this.label);
			field.put("required", this.required);
			if (this.defaultValue != null) {
				field.put("defaultValue", this.defaultValue);
			}
			if (this.minimum != null) {
				field.put("minimum", this.minimum);
			}
			if (this.maximum != null) {
				field.put("maximum", this.maximum);
			}
			if (this.engineType != null) {
				field.put("engineType", this.engineType.name());
			}
			return field;
		}
	}

	/**
	 * One graph port advertised to authoring clients.
	 *
	 * @param id        persisted port id or id template
	 * @param label     user-facing label
	 * @param kind      control or data port
	 * @param direction input or output port
	 * @param dataType  optional data type for data ports
	 */
	public record Port(String id, String label, PortKind kind, PortDirection direction, String dataType) {

		Map<String, Object> toMap() {
			Map<String, Object> port = new LinkedHashMap<>();
			port.put("id", this.id);
			port.put("label", this.label);
			port.put("kind", this.kind.name().toLowerCase(Locale.ROOT));
			port.put("direction", this.direction.name().toLowerCase(Locale.ROOT));
			if (this.dataType != null) {
				port.put("dataType", this.dataType);
			}
			return port;
		}
	}

	/**
	 * Converts this internal contract to the stable Pixel response shape.
	 *
	 * @return ordered response map
	 */
	public Map<String, Object> toMap() {
		Map<String, Object> definition = new LinkedHashMap<>();
		definition.put("type", this.nodeType.getType());
		definition.put("category", this.nodeType.getCategory().name().toLowerCase(Locale.ROOT));
		definition.put("label", this.label);
		definition.put("description", this.description);
		definition.put("defaultCodeMode", this.defaultCodeMode);
		definition.put("requiredPermission", this.nodeType.getPermission().name());
		definition.put("supportsOutput", this.nodeType.supportsOutput());
		definition.put("supportsCustomCode", this.nodeType.supportsCustomCode());
		if (this.nodeType.getEngineType() != null) {
			definition.put("engineType", this.nodeType.getEngineType().name());
		}
		definition.put("defaultConfig", new LinkedHashMap<>(this.defaultConfig));
		definition.put("configSchema", configSchema(this.configFields));
		definition.put("inputs", portMaps(this.inputs));
		definition.put("outputs", portMaps(this.outputs));
		return definition;
	}

	private static Map<String, Object> configSchema(List<ConfigField> fields) {
		Map<String, Object> schema = new LinkedHashMap<>();
		for (ConfigField field : fields) {
			schema.put(field.key(), field.toMap());
		}
		return schema;
	}

	private static List<Map<String, Object>> portMaps(List<Port> ports) {
		List<Map<String, Object>> maps = new ArrayList<>(ports.size());
		for (Port port : ports) {
			maps.add(port.toMap());
		}
		return maps;
	}
}

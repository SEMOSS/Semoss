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
package prerna.contract;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import org.json.JSONArray;
import org.json.JSONObject;

import prerna.contract.TypeContract.*;

/** Fresh JSON Schema views for reactor help/MCP metadata, not a catalog exporter. */
public final class ContractSchemas {
	private ContractSchemas() {}

	/** Render an output type with all referenced definitions. */
	public static JSONObject output(OperationContract contract) {
		if (contract.outputs().size() != 1) throw new IllegalArgumentException("Select a specific REST response; output() requires one result");
		JSONObject schema = type(contract.outputs().getFirst().type());
		if (!contract.outputs().getFirst().description().isBlank()) schema.put("description", contract.outputs().getFirst().description());
		attachDefinitions(schema, contract);
		return schema;
	}

	/** Render canonical named inputs, retaining aliases as descriptive metadata. */
	public static JSONObject inputs(OperationContract contract) {
		JSONObject properties = new JSONObject();
		JSONArray required = new JSONArray();
		for (OperationContract.Input input : contract.inputs()) {
			JSONObject property = type(input.type());
			if (!input.description().isBlank()) property.put("description", input.description());
			property.put("title", input.name());
			if (!input.aliases().isEmpty()) property.put("x-aliases", input.aliases());
			if (input.requirement() == OperationContract.Requirement.REQUIRED) required.put(input.name());
			if (input.requirement() == OperationContract.Requirement.UNKNOWN) property.put("x-requiredness", "unknown");
			properties.put(input.name(), property);
		}
		JSONObject schema = new JSONObject().put("type", "object").put("properties", properties).put("required", required);
		attachDefinitions(schema, contract);
		return schema;
	}

	private static void attachDefinitions(JSONObject schema, OperationContract contract) {
		if (contract.definitions().isEmpty()) return;
		JSONObject definitions = new JSONObject();
		for (ObjectType definition : contract.definitions().values()) {
			JSONObject properties = new JSONObject();
			JSONArray required = new JSONArray();
			for (Property property : definition.properties()) {
				properties.put(property.name(), type(property.type()));
				if (property.required()) required.put(property.name());
			}
			definitions.put(definition.id(), new JSONObject().put("type", "object").put("properties", properties).put("required", required));
		}
		schema.put("$defs", definitions);
	}

	private static JSONObject type(TypeUse value) {
		JSONObject schema = switch (value.shape()) {
			case Scalar scalar -> {
				String name = switch (scalar.kind()) {
					case STRING -> "string";
					case BOOLEAN -> "boolean";
					case INTEGER -> "integer";
					case DECIMAL -> "number";
				};
				JSONObject object = new JSONObject().put("type", name);
				if (!scalar.values().isEmpty()) object.put("enum", scalar.values());
				yield object;
			}
			case Sequence sequence -> new JSONObject().put("type", "array").put("items", type(sequence.element()));
			case Dictionary dictionary -> new JSONObject().put("type", "object").put("additionalProperties", type(dictionary.value()));
			case Union union -> new JSONObject().put("anyOf", union.alternatives().stream().map(ContractSchemas::type).toList());
			case Reference reference -> {
				String pointerSegment = reference.id().replace("~", "~0").replace("/", "~1");
				// Definition IDs may contain method signatures or generic type expressions.
				String fragment = URLEncoder.encode(pointerSegment, StandardCharsets.UTF_8).replace("+", "%20");
				yield new JSONObject().put("$ref", "#/$defs/" + fragment);
			}
			case Empty ignored -> new JSONObject().put("x-no-content", true);
			case Unknown ignored -> new JSONObject();
		};
		if (value.nullable()) schema = new JSONObject().put("anyOf", new JSONArray().put(schema).put(new JSONObject().put("type", "null")));
		if (!value.description().isBlank()) schema.put("description", value.description());
		if (!value.format().isBlank()) schema.put("format", value.format());
		if (value.knowledge() != Knowledge.VERIFIED) {
			schema.put("x-contract-status", value.knowledge().name().toLowerCase(java.util.Locale.ROOT));
			schema.put("x-contract-reason", value.reason());
		}
		return schema;
	}
}

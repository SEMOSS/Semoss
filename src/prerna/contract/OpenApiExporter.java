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

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Set;
import java.util.function.Function;

import org.json.JSONArray;
import org.json.JSONObject;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.contract.OperationContract.*;
import prerna.contract.TypeContract.*;

/** Builds an OpenAPI 3.1 artifact from explicitly selected, already-read contracts. */
public final class OpenApiExporter {
	private final JSONObject document;
	private final JSONObject paths = new JSONObject();
	private final JSONObject schemas = new JSONObject();
	private final JSONObject reactors = new JSONObject();
	private final Set<String> operationIds = new HashSet<>();

	public OpenApiExporter(String title, String version) {
		if (title == null || title.isBlank() || version == null || version.isBlank()) {
			throw new IllegalArgumentException("OpenAPI title and version are required");
		}
		document = new JSONObject().put("openapi", "3.1.0")
				.put("info", new JSONObject().put("title", title).put("version", version))
				.put("paths", paths).put("components", new JSONObject().put("schemas", schemas))
				.put("x-semoss-pixels", new JSONObject().put("schemaVersion", 1).put("reactors", reactors));
	}

	/** JAX-RS application paths are relative to the deployment's existing module URL. */
	public void addRest(String applicationPath, String operationId, OperationContract contract) {
		check(contract, Kind.REST);
		if (applicationPath == null || !applicationPath.matches("/[a-z][a-z0-9-]*")) {
			throw new IllegalArgumentException("Unsupported JAX-RS application path: " + applicationPath);
		}
		if (operationId == null || operationId.isBlank() || !operationIds.add(operationId)) {
			throw new IllegalArgumentException("Missing or duplicate operationId: " + operationId);
		}
		RestInfo rest = contract.rest();
		String method = rest.method().toLowerCase(java.util.Locale.ROOT);
		if (!Set.of("get", "post", "put", "patch", "delete", "head", "options", "trace").contains(method)) {
			throw new IllegalArgumentException("Unsupported HTTP method: " + rest.method());
		}
		String route = (applicationPath + "/" + rest.path()).replaceAll("/{2,}", "/");
		JSONObject path = paths.optJSONObject(route);
		if (path == null) { path = new JSONObject(); paths.put(route, path); }
		if (path.has(method)) throw new IllegalArgumentException("Duplicate HTTP operation: " + method + " " + route);
		Function<String, String> reference = definitions(contract);
		JSONObject operation = metadata(contract).put("operationId", operationId)
				.put("tags", new JSONArray().put(applicationPath.substring(1)));
		JSONArray parameters = new JSONArray();
		Set<String> pathNames = new HashSet<>();
		for (Input input : contract.inputs()) {
			String location = switch (input.binding()) {
				case PATH -> "path";
				case QUERY -> "query";
				case HEADER -> "header";
				case COOKIE -> "cookie";
				default -> throw new IllegalArgumentException("Add an OpenAPI request adapter for binding " + input.binding());
			};
			if (!input.aliases().isEmpty()) throw new IllegalArgumentException("REST aliases need an explicit binding adapter");
			if (input.binding() == Parameter.Binding.PATH) pathNames.add(input.name());
			JSONObject schema = ContractSchemas.type(input.type(), reference);
			if (input.defaultValue() != null) schema.put("default", defaultValue(input));
			JSONObject parameter = new JSONObject().put("name", input.name()).put("in", location)
					.put("required", input.requirement() == Requirement.REQUIRED).put("description", input.description());
			if (input.encoding() == Parameter.Encoding.JSON) {
				parameter.put("content", new JSONObject().put("application/json", new JSONObject().put("schema", schema)));
			} else {
				if (input.encoding() != Parameter.Encoding.DEFAULT && input.encoding() != Parameter.Encoding.REPEATED) {
					throw new IllegalArgumentException("Unsupported OpenAPI parameter encoding: " + input.encoding());
				}
				parameter.put("schema", schema);
				if (input.type().shape() instanceof Sequence || input.encoding() == Parameter.Encoding.REPEATED) {
					if (input.binding() != Parameter.Binding.QUERY) throw new IllegalArgumentException("Repeated values require query binding");
					parameter.put("style", "form").put("explode", true);
				}
			}
			parameters.put(parameter);
		}
		Set<String> placeholders = new HashSet<>();
		var matcher = java.util.regex.Pattern.compile("\\{([^{}]+)\\}").matcher(route);
		while (matcher.find()) placeholders.add(matcher.group(1));
		if (!placeholders.equals(pathNames)) throw new IllegalArgumentException("Path bindings disagree with " + route);
		if (!parameters.isEmpty()) operation.put("parameters", parameters);
		JSONObject responses = new JSONObject();
		for (OperationContract.Output output : contract.outputs()) {
			if (output.status() < 100 || output.status() > 599) throw new IllegalArgumentException("Explicit HTTP response status required");
			String status = Integer.toString(output.status());
			String description = output.description().isBlank() ? "HTTP " + status + " response." : output.description();
			JSONObject response = responses.optJSONObject(status);
			if (response == null) { response = new JSONObject().put("description", description); responses.put(status, response); }
			if (!response.getString("description").equals(description)) throw new IllegalArgumentException("Conflicting descriptions for HTTP " + status);
			if (output.type().shape() instanceof Empty) {
				if (response.has("content")) throw new IllegalArgumentException("Conflicting response bodies for HTTP " + status);
				continue;
			}
			var media = output.mediaType().isBlank() ? rest.produces() : java.util.List.of(output.mediaType());
			if (media.isEmpty()) throw new IllegalArgumentException("Explicit response media type required");
			JSONObject content = response.optJSONObject("content");
			if (content == null) { content = new JSONObject(); response.put("content", content); }
			for (String mediaType : media) {
				if (content.has(mediaType)) throw new IllegalArgumentException("Duplicate response media type: " + mediaType);
				content.put(mediaType, new JSONObject().put("schema", ContractSchemas.type(output.type(), reference)));
			}
		}
		operation.put("responses", responses);
		path.put(method, operation);
	}

	/** Pixel schemas describe canonical named values; invocation metadata describes the DSL. */
	public void addPixel(OperationContract contract) {
		if (contract.name().isBlank()) throw new IllegalArgumentException("Pixel contract must declare its name");
		addPixel(contract.name(), contract);
		for (String alias : contract.aliases()) addPixel(alias, contract);
	}

	public void addPixel(String name, OperationContract contract) {
		check(contract, Kind.PIXEL);
		if (!contract.name().isBlank() && !contract.name().equals(name) && !contract.aliases().contains(name))
			throw new IllegalArgumentException("Pixel export name conflicts with @Reactor: " + name);
		if (name == null || !name.matches("[A-Za-z_][A-Za-z0-9_]*") || reactors.has(name)) {
			throw new IllegalArgumentException("Invalid or duplicate Pixel name: " + name);
		}
		if (contract.invocation() == null || contract.outputs().size() != 1 || contract.outputs().getFirst().status() != -1) {
			throw new IllegalArgumentException("Pixel requires invocation metadata and one output");
		}
		Function<String, String> reference = definitions(contract);
		JSONObject properties = new JSONObject();
		JSONArray required = new JSONArray();
		for (Input input : contract.inputs()) {
			if (input.binding() != Parameter.Binding.NOUN || input.encoding() != Parameter.Encoding.DEFAULT) {
				throw new IllegalArgumentException("Unsupported Pixel binding: " + input.name());
			}
			JSONObject schema = ContractSchemas.type(input.type(), reference);
			if (!input.description().isBlank()) schema.put("description", input.description());
			if (!input.aliases().isEmpty()) schema.put("x-semoss-aliases", input.aliases());
			properties.put(input.name(), schema);
			if (input.requirement() == Requirement.REQUIRED) required.put(input.name());
		}
		InvocationInfo invocation = contract.invocation();
		OperationContract.Output output = contract.outputs().getFirst();
		JSONObject outputSchema = ContractSchemas.type(output.type(), reference);
		if (!output.name().isBlank()) outputSchema.put("title", output.name());
		if (!output.description().isBlank()) outputSchema.put("description", output.description());
		reactors.put(name, metadata(contract)
				.put("inputSchema", new JSONObject().put("type", "object").put("properties", properties).put("required", required))
				.put("outputSchema", outputSchema)
				.put("invocation", new JSONObject().put("mode", invocation.mode().name()).put("order", invocation.order())
						.put("variadic", invocation.variadic().isBlank() ? JSONObject.NULL : invocation.variadic())
						.put("canOmitParameters", invocation.canOmitParameters())));
	}

	/** Stable UTF-8 JSON content; no filesystem, clock, registration or operation execution. */
	public String json() {
		return new GsonBuilder().serializeNulls().disableHtmlEscaping().setPrettyPrinting().create()
				.toJson(sorted(JsonParser.parseString(document.toString()))) + "\n";
	}

	private static JsonElement sorted(JsonElement value) {
		if (value.isJsonObject()) {
			JsonObject result = new JsonObject();
			value.getAsJsonObject().keySet().stream().sorted().forEach(key -> result.add(key, sorted(value.getAsJsonObject().get(key))));
			return result;
		}
		if (value.isJsonArray()) {
			JsonArray result = new JsonArray();
			value.getAsJsonArray().forEach(item -> result.add(sorted(item)));
			return result;
		}
		return value;
	}

	private static JSONObject metadata(OperationContract contract) {
		JSONObject metadata = new JSONObject().put("description", contract.description()).put("x-semoss-source", contract.source());
		if (contract.policy().ignored()) metadata.put("x-semoss-ignore", contract.policy().ignoreReasons());
		if (contract.policy().internal()) metadata.put("x-semoss-internal", contract.policy().internalReasons());
		if (contract.deprecation().deprecated()) {
			DeprecationInfo d = contract.deprecation();
			metadata.put("deprecated", true).put("x-semoss-deprecation", new JSONObject().put("since", d.since())
					.put("forRemoval", d.forRemoval()).put("reason", d.reason()).put("replacement", d.replacement()));
		}
		return metadata;
	}

	private Function<String, String> definitions(OperationContract contract) {
		Function<String, String> reference = id -> {
			if (!contract.definitions().containsKey(id)) throw new IllegalArgumentException("Unresolved schema reference: " + id);
			return "#/components/schemas/" + schemaName(id);
		};
		for (ObjectType definition : contract.definitions().values()) {
			String key = schemaName(definition.id());
			JSONObject schema = ContractSchemas.object(definition, reference);
			if (schemas.has(key) && !schemas.getJSONObject(key).similar(schema)) {
				throw new IllegalArgumentException("Conflicting shared schema: " + definition.id());
			}
			schemas.put(key, schema);
		}
		return reference;
	}

	private static String schemaName(String id) {
		try {
			String label = id.replaceAll("[^A-Za-z0-9_.-]", "_");
			if (label.length() > 80) label = label.substring(0, 80);
			String hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(id.getBytes(StandardCharsets.UTF_8)));
			return label + "_" + hash.substring(0, 16);
		} catch (NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
	}

	private static Object defaultValue(Input input) {
		if (!(input.type().shape() instanceof Scalar scalar)) throw new IllegalArgumentException("Unsupported parameter default: " + input.name());
		return switch (scalar.kind()) {
			case STRING -> input.defaultValue();
			case BOOLEAN -> {
				if (!Set.of("true", "false").contains(input.defaultValue())) throw new IllegalArgumentException("Invalid boolean default");
				yield Boolean.valueOf(input.defaultValue());
			}
			case INTEGER -> new BigDecimal(input.defaultValue()).toBigIntegerExact();
			case DECIMAL -> new BigDecimal(input.defaultValue());
		};
	}

	private static void check(OperationContract contract, Kind kind) {
		if (contract.kind() != kind || contract.description().isBlank() || contract.outputs().isEmpty()
				|| !contract.diagnostics().isEmpty() || contract.inputs().stream().anyMatch(input -> input.requirement() == Requirement.UNKNOWN)) {
			throw new IllegalArgumentException("Incomplete " + kind + " contract: " + contract.source() + " " + contract.diagnostics());
		}
	}
}

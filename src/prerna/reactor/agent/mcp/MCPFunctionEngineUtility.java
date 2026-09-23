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
package prerna.reactor.agent.mcp;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.engine.api.IFunctionEngine;
import prerna.engine.impl.function.FunctionParameter;
import prerna.reactor.IReactor.MCP_KEY_TYPE;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.util.Utility;

/**
 * Builds MCP tool schemas out of a function engine's own function definition.
 *
 * <p>
 * Kept apart from {@link MCPUtility}, which is about MCP mechanics that apply
 * to every engine type - reading and merging definition files, running pixel
 * and python tools, naming and metadata conventions. Everything here instead
 * translates one specific thing, the {@code FUNCTION_NAME} /
 * {@code FUNCTION_DESCRIPTION} / {@code FUNCTION_PARAMETERS} contract that
 * {@code IFunctionEngine} exposes, into that schema.
 */
public final class MCPFunctionEngineUtility {

	private static final Logger classLogger = LogManager.getLogger(MCPFunctionEngineUtility.class);

	/**
	 * Rewrite a generated {@code ExecuteFunctionEngine} tool so it presents the
	 * function the way the engine itself describes it: named after the function,
	 * carrying the function's description, and taking the function's parameters as
	 * flat named arguments.
	 *
	 * <p>
	 * This is what makes a purpose built function engine usable as a tool without a
	 * reactor written for it. The stock schema takes an opaque {@code map}
	 * argument, so a model would have to already know the parameter names before it
	 * could call the function - which is exactly what the function definition
	 * already spells out. Rewriting the schema from that definition hands the model
	 * the same contract, and {@code ExecuteFunctionEngineReactor} accepts the named
	 * arguments the schema advertises.
	 *
	 * <p>
	 * The tool's {@code _meta} is left alone, so
	 * {@link MCPUtility#SMSS_FUNCTION_NAME} keeps pointing at
	 * {@code ExecuteFunctionEngine} and the rename is only what the model sees.
	 * Callers must therefore apply this after they have stamped the meta.
	 *
	 * <p>
	 * Engines that declare no parameters are left as the generic map tool. There is
	 * nothing to build a schema out of for those, and an argument-less tool would
	 * be strictly worse than the map form it replaced.
	 *
	 * @param tool           a tool generated from
	 *                       {@code ExecuteFunctionEngineReactor}
	 * @param functionEngine the engine the tool calls
	 * @return true when the tool was rewritten, false when the engine declares no
	 *         parameters and the tool was left as is
	 */
	public static boolean applyFunctionEngineDefinition(JSONObject tool, IFunctionEngine functionEngine) {
		List<FunctionParameter> parameters = functionEngine.getParameters();
		if (parameters == null || parameters.isEmpty()) {
			return false;
		}

		String toolName = sanitizeToolName(functionEngine.getFunctionName());
		if (toolName == null) {
			classLogger.warn("Function engine '{}' has no usable function name, leaving the generic tool in place",
					functionEngine.getEngineId());
			return false;
		}

		String engineId = functionEngine.getEngineId();
		JSONObject properties = new JSONObject();
		JSONArray required = new JSONArray();

		// the engine is pinned to a single value so the model never picks it, the
		// same way every other engine scoped tool is generated
		JSONObject engineProperty = new JSONObject();
		String engineKey = ReactorKeysEnum.ENGINE.getKey();
		engineProperty.put("title", engineKey);
		engineProperty.put("type", MCP_KEY_TYPE.STRING.getValue());
		engineProperty.put("description", "Id of the engine that backs this tool");
		engineProperty.put("enum", new JSONArray().put(engineId));
		engineProperty.put("default", engineId);
		properties.put(engineKey, engineProperty);
		required.put(engineKey);

		for (FunctionParameter parameter : parameters) {
			String parameterName = parameter.getParameterName();
			if (parameterName == null || (parameterName = parameterName.trim()).isEmpty()) {
				continue;
			}
			// a parameter named engine would shadow the pinned id above and leave
			// the tool unable to say which engine to run
			if (parameterName.equals(engineKey)) {
				classLogger.warn(
						"Function engine '{}' declares a parameter named '{}', which is reserved - skipping it",
						engineId, engineKey);
				continue;
			}
			JSONObject property = new JSONObject();
			property.put("title", parameterName);
			property.put("type", toSchemaType(parameter.getParameterType()));
			String parameterFormat = toSchemaFormat(parameter.getParameterType());
			if (parameterFormat != null) {
				property.put("format", parameterFormat);
			}
			String parameterDescription = parameter.getParameterDescription();
			property.put("description",
					(parameterDescription != null && !parameterDescription.trim().isEmpty()) ? parameterDescription
							: "No description present");
			properties.put(parameterName, property);
		}

		List<String> requiredParameters = functionEngine.getRequiredParameters();
		if (requiredParameters != null) {
			for (String requiredParameter : requiredParameters) {
				if (requiredParameter != null && properties.has(requiredParameter)) {
					required.put(requiredParameter);
				}
			}
		}

		String description = functionEngine.getFunctionDescription();
		if (description == null || (description = description.trim()).isEmpty()) {
			description = "Runs the " + toolName + " function";
		}

		JSONObject inputSchema = new JSONObject();
		inputSchema.put("type", "object");
		inputSchema.put("title", toolName + "_Arguments");
		inputSchema.put("properties", properties);
		inputSchema.put("required", required);

		tool.put("name", toolName);
		tool.put("title", MCPUtility.formatToTitleCase(toolName));
		tool.put("description", description);
		tool.put("inputSchema", inputSchema);
		return true;
	}

	/**
	 * Reduce a function name to the characters a tool name is allowed to contain.
	 * Providers accept letters, digits, underscores, and dashes, so a function
	 * named "Bing Web Search" has to become "Bing_Web_Search" before it can be
	 * published.
	 *
	 * @param functionName the name off the function definition
	 * @return a usable tool name, or null when nothing usable is left
	 */
	private static String sanitizeToolName(String functionName) {
		if (functionName == null || (functionName = functionName.trim()).isEmpty()) {
			return null;
		}
		String sanitized = functionName.replaceAll("[^A-Za-z0-9_-]", "_");
		// collapse the runs a replacement can leave behind so the name stays readable
		sanitized = sanitized.replaceAll("_{2,}", "_").replaceAll("^_+|_+$", "");
		return sanitized.isEmpty() ? null : sanitized;
	}

	/**
	 * Map a function parameter type onto a JSON schema type. The type on a function
	 * definition is free text an admin typed, so a java-ism like "double" or a sql
	 * name like "bigint" shows up routinely and would be an invalid schema if
	 * published as is.
	 *
	 * <p>
	 * Classifying the scalar types is deferred to {@link Utility}, which already
	 * maintains the mapping for every sql and pandas type name the platform reads.
	 * That is a list worth having in one place rather than two, and going through
	 * it means a parameter typed "numeric" or "bit" lands correctly without this
	 * method having to know those names.
	 *
	 * @param parameterType the type off the function definition
	 * @return the JSON schema type to publish
	 */
	private static String toSchemaType(String parameterType) {
		if (parameterType == null || (parameterType = parameterType.trim()).isEmpty()) {
			return MCP_KEY_TYPE.STRING.getValue();
		}

		// nothing that fits in a table cell is a collection, so Utility has no
		// notion of these two and they have to be recognized here
		switch (parameterType.toLowerCase()) {
		case "array":
		case "list":
			return MCP_KEY_TYPE.ARRAY.getValue();
		case "map":
		case "object":
		case "json":
			return MCP_KEY_TYPE.OBJECT.getValue();
		default:
			break;
		}

		// the integer and double prefix sets do not overlap, so the order of these
		// three is a readability choice rather than a correctness one
		if (Utility.isIntegerType(parameterType)) {
			return MCP_KEY_TYPE.INTEGER.getValue();
		}
		if (Utility.isDoubleType(parameterType)) {
			return MCP_KEY_TYPE.NUMBER.getValue();
		}
		if (Utility.isBoolean(parameterType)) {
			return MCP_KEY_TYPE.BOOLEAN.getValue();
		}

		// string covers everything left, so there is no isStringType check to make
		// here - a date is a string with a format, and an unrecognized type is a
		// string because guessing anything narrower could only make the schema wrong
		return MCP_KEY_TYPE.STRING.getValue();
	}

	/**
	 * The JSON schema {@code format} that goes with a parameter type, for the types
	 * that have no schema type of their own. A date travels as a string, so without
	 * this the model is told only that the parameter is text and has to infer from
	 * the description that it wants a date.
	 *
	 * @param parameterType the type off the function definition
	 * @return the format to publish, or null when the type needs none
	 */
	private static String toSchemaFormat(String parameterType) {
		if (parameterType == null || (parameterType = parameterType.trim()).isEmpty()) {
			return null;
		}
		if (Utility.isTimeStamp(parameterType)) {
			return "date-time";
		}
		if (Utility.isDateType(parameterType)) {
			return "date";
		}
		return null;
	}

	private MCPFunctionEngineUtility() {

	}
}

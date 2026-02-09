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
package prerna.engine.impl.model.responses;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

import prerna.util.Constants;

public class AskToolModelEngineResponse extends AskModelEngineResponse<List<Map<String, Object>>> {

	private static final Logger classLogger = LogManager.getLogger(AskToolModelEngineResponse.class);
	private static final long serialVersionUID = 1L;

	private static final Gson GSON = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.disableHtmlEscaping().create();

	public static final String ID_KEY = "id";
	public static final String TYPE_KEY = "type";
	public static final String NAME_KEY = "name";
	public static final String ARGUMENTS_KEY = "arguments";

	List<Map<String, Object>> toolResponse;
	private List<ToolResponse> tools;

	/**
	 * 
	 * @param response
	 * @param numberOfTokensInPrompt
	 * @param numberOfTokensInResponse
	 */
	public AskToolModelEngineResponse(List<Map<String, Object>> response, Integer numberOfTokensInPrompt,
			Integer numberOfTokensInResponse) {
		super(response, numberOfTokensInPrompt, numberOfTokensInResponse);
		this.toolResponse = response;
		this.tools = new ArrayList<>();
		for (Map<String, Object> toolResponse : response) {
			String id = null;
			String type = null;
			String name = null;
			Map<String, Object> arguments = null;

			if (toolResponse.containsKey(ID_KEY) && toolResponse.get(ID_KEY) instanceof String) {
				id = (String) toolResponse.get(ID_KEY);
			}

			if (toolResponse.containsKey(TYPE_KEY) && toolResponse.get(TYPE_KEY) instanceof String) {
				type = (String) toolResponse.get(TYPE_KEY);
			}

			if (toolResponse.containsKey(NAME_KEY) && toolResponse.get(NAME_KEY) instanceof String) {
				name = (String) toolResponse.get(NAME_KEY);
			}

			if (toolResponse.containsKey(ARGUMENTS_KEY)) {
				Object toolArguments = toolResponse.get(ARGUMENTS_KEY);
				if (toolArguments == null) {
					arguments = new HashMap<>();
				} else if (toolArguments instanceof Map) {
					arguments = (Map<String, Object>) toolArguments;
				} else {
					String argumentsJsonStr = toolArguments + "";
					try {
						arguments = GSON.fromJson(argumentsJsonStr, Map.class);
					} catch (Exception e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}

			ToolResponse tool = new ToolResponse(id, type, name, arguments);
			this.tools.add(tool);
		}

		this.messageType = TOOL;
	}

	@Deprecated
	public String getToolCallId() {
		return this.tools.get(0).getId();
	}

	@Deprecated
	public String getToolCallArgumentsAsString() {
		Map<String, Object> arguments = this.tools.get(0).getArguments();
		if (arguments == null) {
			return "{}";
		}
		return new Gson().toJson(arguments);
	}

	@Deprecated
	public String getToolCallName() {
		return this.tools.get(0).getName();
	}

	@Override
	public String getStringResponse() {
		if (this.response != null) {
			return new Gson().toJson(this.response);
		}
		return "[]";
	}

	/**
	 * 
	 * @return
	 */
	public List<ToolResponse> getTools() {
		return tools;
	}

	/**
	 * 
	 * @return
	 */
	public List<Map<String, Object>> getToolResponse() {
		return toolResponse;
	}

	/**
	 * 
	 */
	public class ToolResponse {

		private String id;
		private String type;
		private String name;
		private Map<String, Object> arguments;

		public ToolResponse(String id, String type, String name, Map<String, Object> arguments) {
			this.id = id;
			this.type = type;
			this.name = name;
			this.arguments = arguments;
		}

		public String getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		public String getType() {
			return type;
		}

		public Map<String, Object> getArguments() {
			return arguments;
		}

	}
}
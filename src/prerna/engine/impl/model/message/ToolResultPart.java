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
package prerna.engine.impl.model.message;

import java.util.Map;

import com.google.gson.annotations.SerializedName;

public class ToolResultPart {

	@SerializedName(value = "toolCallId", alternate = { "id" })
	private String toolCallId;

	@SerializedName(value = "toolName", alternate = { "tool_name" })
	private String toolName;

	@SerializedName("output")
	private String output;

	@SerializedName(value = "toolParameterValues", alternate = { "tool_parameter_values" })
	private Map<String, Object> toolParameterValues;

	@SerializedName(value = "toolStatus", alternate = { "tool_status" })
	private String toolStatus;

	@SerializedName(value = "serverTool", alternate = { "server_tool" })
	private Boolean serverTool;

	public ToolResultPart() {
	}

	public ToolResultPart(String toolCallId, String toolName, String output, Map<String, Object> toolParameterValues,
			String toolStatus, Boolean serverTool) {
		this.toolCallId = toolCallId;
		this.toolName = toolName;
		this.output = output;
		this.toolParameterValues = toolParameterValues;
		this.toolStatus = toolStatus;
		this.serverTool = serverTool;
	}

	public String getToolCallId() {
		return toolCallId;
	}

	public void setToolCallId(String toolCallId) {
		this.toolCallId = toolCallId;
	}

	public String getToolName() {
		return toolName;
	}

	public void setToolName(String toolName) {
		this.toolName = toolName;
	}

	public String getOutput() {
		return output;
	}

	public void setOutput(String output) {
		this.output = output;
	}

	public Map<String, Object> getToolParameterValues() {
		return toolParameterValues;
	}

	public void setToolParameterValues(Map<String, Object> toolParameterValues) {
		this.toolParameterValues = toolParameterValues;
	}

	public String getToolStatus() {
		return toolStatus;
	}

	public void setToolStatus(String toolStatus) {
		this.toolStatus = toolStatus;
	}

	public Boolean getServerTool() {
		return serverTool;
	}

	public void setServerTool(Boolean serverTool) {
		this.serverTool = serverTool;
	}
}

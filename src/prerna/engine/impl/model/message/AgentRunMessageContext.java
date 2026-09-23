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

/** Durable agent-run attribution carried directly by a room message. */
public final class AgentRunMessageContext {

	@SerializedName("runId")
	private String runId;

	@SerializedName("role")
	private String role;

	@SerializedName("originatingRunId")
	private String originatingRunId;

	@SerializedName("childRunId")
	private String childRunId;

	@SerializedName("completionMode")
	private String completionMode;

	@SerializedName("childStatus")
	private String childStatus;

	public AgentRunMessageContext(String runId, String role) {
		setRunId(runId);
		this.role = trimToNull(role);
	}

	static AgentRunMessageContext fromLegacyOrnaments(Map<String, Object> ornaments) {
		if (ornaments == null) {
			return null;
		}
		String runId = trimToNull(ornaments.get("agentRunId"));
		if (runId == null) {
			return null;
		}
		AgentRunMessageContext context = new AgentRunMessageContext(runId, trimToNull(ornaments.get("agentRunRole")));
		context.originatingRunId = trimToNull(ornaments.get("originatingAgentRunId"));
		context.childRunId = trimToNull(ornaments.get("childRunId"));
		context.completionMode = trimToNull(ornaments.get("completionMode"));
		context.childStatus = trimToNull(ornaments.get("childStatus"));
		return context;
	}

	void validate() {
		if (trimToNull(runId) == null) {
			throw new IllegalStateException("agentRun.runId is required");
		}
	}

	public String getRunId() {
		return runId;
	}

	public void setRunId(String runId) {
		String normalized = trimToNull(runId);
		if (normalized == null) {
			throw new IllegalArgumentException("runId is required");
		}
		this.runId = normalized;
	}

	public String getRole() {
		return role;
	}

	public void setRole(String role) {
		this.role = trimToNull(role);
	}

	public String getOriginatingRunId() {
		return originatingRunId;
	}

	public void setOriginatingRunId(String originatingRunId) {
		this.originatingRunId = trimToNull(originatingRunId);
	}

	public String getChildRunId() {
		return childRunId;
	}

	public void setChildRunId(String childRunId) {
		this.childRunId = trimToNull(childRunId);
	}

	public String getCompletionMode() {
		return completionMode;
	}

	public void setCompletionMode(String completionMode) {
		this.completionMode = trimToNull(completionMode);
	}

	public String getChildStatus() {
		return childStatus;
	}

	public void setChildStatus(String childStatus) {
		this.childStatus = trimToNull(childStatus);
	}

	private static String trimToNull(Object value) {
		if (value == null) {
			return null;
		}
		String text = String.valueOf(value).trim();
		return text.isEmpty() ? null : text;
	}
}

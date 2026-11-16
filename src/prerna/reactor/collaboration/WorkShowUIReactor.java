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
package prerna.reactor.collaboration;

import java.util.Map;

import org.json.JSONObject;

import prerna.reactor.ReactorDefinition;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** An agent-authored interface whose result is the owner's explicit response. */
public class WorkShowUIReactor extends AbstractCollaborationReactor {

	public WorkShowUIReactor() {
		this.keysToGet = new String[] { "title", "messages" };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		throw new IllegalArgumentException("ShowUI requires a response from the owner's interface. "
				+ "Use decision=respond or reject; approving it does not perform an action.");
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		Map<String, String> meta = super.getMcpToolMetadata();
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.ASK.getValue());
		meta.put(MCPUtility.UI_RESOURCE_URI, "component://collaboration/a2ui");
		meta.put(MCPUtility.UI_DISPLAY_LOCATION, "inline");
		return meta;
	}

	@Override
	public JSONObject getMcpProperties() {
		JSONObject properties = super.getMcpProperties();
		properties.getJSONObject("title").put("maxLength", 200);
		properties.getJSONObject("messages").put("type", "array").put("minItems", 1).put("maxItems", 32)
				.put("items", new JSONObject().put("type", "object"));
		return properties;
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("Show an interactive A2UI interface in this chat and wait for "
				+ "the owner's response. Use for a useful choice, form, or structured review, not a plain answer. "
				+ "Read collaboration/references/ui/a2ui.md with LoadSkill before calling. "
				+ "Use A2UI 1.0 JSON and catalog urn:semoss:collaboration:a2ui:1. "
				+ "Available components: Text, Row, Column, Card, Divider, Button, TextField, CheckBox, ChoicePicker, "
				+ "Slider, DateTimeInput. No scripts, HTML, remote resources, or function calls. "
				+ "Every surface needs root and a labeled Button with action.event. "
				+ "The response contains the selected action and its resolved context. It is input, never permission "
				+ "to bypass the normal approval of an email, calendar edit, or other external operation.")
				.key("title", "A short, descriptive title for the interface")
				.key("messages", key -> key.list(Map.class).description("Ordered A2UI 1.0 createSurface, "
						+ "updateComponents, updateDataModel, or deleteSurface messages. Exactly one active surface, "
						+ "at most 128 components and 64 KiB of JSON. Use @path bindings for form values."));
	}
}

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
package prerna.collaboration;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.engine.api.ToolExecutionResult;
import prerna.engine.impl.model.Room;
import prerna.io.connector.ms.calendar.MicrosoftCalendarCreateEventReactor;
import prerna.io.connector.ms.calendar.MicrosoftCalendarDeleteEventReactor;
import prerna.io.connector.ms.calendar.MicrosoftCalendarGetEventReactor;
import prerna.io.connector.ms.calendar.MicrosoftCalendarGetScheduleReactor;
import prerna.io.connector.ms.calendar.MicrosoftCalendarListCalendarsReactor;
import prerna.io.connector.ms.calendar.MicrosoftCalendarListEventsReactor;
import prerna.io.connector.ms.calendar.MicrosoftCalendarRespondToEventReactor;
import prerna.io.connector.ms.calendar.MicrosoftCalendarUpdateEventReactor;
import prerna.io.connector.ms.onedrive.MicrosoftOneDriveDownloadFileReactor;
import prerna.io.connector.ms.onedrive.MicrosoftOneDriveGetFileReactor;
import prerna.io.connector.ms.onedrive.MicrosoftOneDriveListDrivesReactor;
import prerna.io.connector.ms.onedrive.MicrosoftOneDriveListFilesReactor;
import prerna.io.connector.ms.onedrive.MicrosoftOneDriveListSharedFilesReactor;
import prerna.io.connector.ms.onedrive.MicrosoftOneDriveSearchFilesReactor;
import prerna.io.connector.ms.outlook.MicrosoftOutlookListMailReactor;
import prerna.io.connector.ms.teams.MicrosoftTeamsDownloadFileReactor;
import prerna.io.connector.ms.teams.MicrosoftTeamsDownloadMessageAttachmentReactor;
import prerna.io.connector.ms.teams.MicrosoftTeamsGetChatMessageReactor;
import prerna.io.connector.ms.teams.MicrosoftTeamsGetChatReactor;
import prerna.io.connector.ms.teams.MicrosoftTeamsListChannelsReactor;
import prerna.io.connector.ms.teams.MicrosoftTeamsListChatMessagesReactor;
import prerna.io.connector.ms.teams.MicrosoftTeamsListChatsReactor;
import prerna.io.connector.ms.teams.MicrosoftTeamsListFilesReactor;
import prerna.io.connector.ms.teams.MicrosoftTeamsListTeamsReactor;
import prerna.io.connector.ms.teams.MicrosoftTeamsSendChatMessageReactor;
import prerna.io.connector.ms.teams.MicrosoftTeamsUploadFileReactor;
import prerna.om.Insight;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.reactor.collaboration.BrainEditThreadReactor;
import prerna.reactor.collaboration.BrainEditTopicReactor;
import prerna.reactor.collaboration.BrainGetThreadMessagesReactor;
import prerna.reactor.collaboration.BrainListTopicsReactor;
import prerna.reactor.collaboration.BrainSearchThreadsReactor;
import prerna.reactor.collaboration.WorkComposeEmailReactor;
import prerna.reactor.collaboration.WorkDownloadAttachmentReactor;
import prerna.reactor.collaboration.WorkSendEmailReactor;

/**
 * Tools every agent gets in a collaboration room, next to DelegateToPerson and
 * FindPerson: ComposeEmail and SendEmail, which work on the email in Work's
 * editor and pick the mailbox themselves, and the Microsoft 365 mail reading,
 * calendar, Teams and OneDrive reactors. Provider mail-writing reactors stay out,
 * so the model never chooses between providers' send tools.
 * Each definition is built from its reactor (description, arguments, ask or
 * auto) under a short name; each call runs the reactor as the room's user.
 */
public final class CollaborationAgentTools {

	private static final Logger classLogger = LogManager.getLogger(CollaborationAgentTools.class);

	/** Stamped into each tool's _meta so an approved call can be told apart from a room tool. */
	public static final String TOOL_KIND = "semoss_collaboration_tool";

	// tool name the model sees -> reactor it runs
	private static final Map<String, Class<? extends AbstractReactor>> REACTORS = new LinkedHashMap<>();
	static {
		REACTORS.put("ListCalendars", MicrosoftCalendarListCalendarsReactor.class);
		REACTORS.put("ListEvents", MicrosoftCalendarListEventsReactor.class);
		REACTORS.put("GetEvent", MicrosoftCalendarGetEventReactor.class);
		REACTORS.put("GetSchedule", MicrosoftCalendarGetScheduleReactor.class);
		REACTORS.put("CreateEvent", MicrosoftCalendarCreateEventReactor.class);
		REACTORS.put("UpdateEvent", MicrosoftCalendarUpdateEventReactor.class);
		REACTORS.put("DeleteEvent", MicrosoftCalendarDeleteEventReactor.class);
		REACTORS.put("RespondToEvent", MicrosoftCalendarRespondToEventReactor.class);
		// mail through Brain first: the owner's classified threads, with never-ingest and exclusions applied
		REACTORS.put("ListTopics", BrainListTopicsReactor.class);
		REACTORS.put("SearchMail", BrainSearchThreadsReactor.class);
		REACTORS.put("ReadThread", BrainGetThreadMessagesReactor.class);
		// change the owner's Brain; both wait for the owner to approve
		REACTORS.put("EditTopic", BrainEditTopicReactor.class);
		REACTORS.put("EditThread", BrainEditThreadReactor.class);
		// then Outlook directly, for what Brain does not hold; Brain's rules do not apply to it
		REACTORS.put("ListM365Mail", MicrosoftOutlookListMailReactor.class);
		// the email in the Work editor: written there, sent from it once the owner presses Send
		REACTORS.put("ComposeEmail", WorkComposeEmailReactor.class);
		REACTORS.put("SendEmail", WorkSendEmailReactor.class);
		// one email attachment of this thread, on request, into the room's working directory
		REACTORS.put("DownloadAttachment", WorkDownloadAttachmentReactor.class);
		REACTORS.put("ListTeams", MicrosoftTeamsListTeamsReactor.class);
		REACTORS.put("ListChannels", MicrosoftTeamsListChannelsReactor.class);
		REACTORS.put("ListChannelFiles", MicrosoftTeamsListFilesReactor.class);
		REACTORS.put("DownloadChannelFile", MicrosoftTeamsDownloadFileReactor.class);
		REACTORS.put("UploadChannelFile", MicrosoftTeamsUploadFileReactor.class);
		REACTORS.put("ListChats", MicrosoftTeamsListChatsReactor.class);
		REACTORS.put("GetChat", MicrosoftTeamsGetChatReactor.class);
		REACTORS.put("ListChatMessages", MicrosoftTeamsListChatMessagesReactor.class);
		REACTORS.put("GetChatMessage", MicrosoftTeamsGetChatMessageReactor.class);
		REACTORS.put("SendChatMessage", MicrosoftTeamsSendChatMessageReactor.class);
		REACTORS.put("DownloadMessageAttachment", MicrosoftTeamsDownloadMessageAttachmentReactor.class);
		REACTORS.put("ListDrives", MicrosoftOneDriveListDrivesReactor.class);
		REACTORS.put("ListDriveFiles", MicrosoftOneDriveListFilesReactor.class);
		REACTORS.put("ListSharedFiles", MicrosoftOneDriveListSharedFilesReactor.class);
		REACTORS.put("SearchFiles", MicrosoftOneDriveSearchFilesReactor.class);
		REACTORS.put("GetFile", MicrosoftOneDriveGetFileReactor.class);
		REACTORS.put("DownloadDriveFile", MicrosoftOneDriveDownloadFileReactor.class);
	}

	// tools the model loads on demand: direct Microsoft 365 mail, after Brain's own search
	private static final Set<String> DEFERRED = Set.of("ListM365Mail");

	// said first in a mail tool's description, so the model knows which side of Brain it is on
	private static final Map<String, String> SOURCE_NOTES = Map.of(
			"ListTopics", "[Brain: topics, or one topic in full with its people]",
			"EditTopic", "[Brain: changes a topic]",
			"EditThread", "[Brain: changes a thread's topics]",
			"SearchMail", "[Brain: the owner's classified mail, start here; one topic or all]",
			"ReadThread", "[Brain: reads a thread found in Brain]",
			"ListM365Mail", "[Microsoft 365, direct: Outlook as it is now, outside Brain. Brain's never-ingest rules "
					+ "and exclusions do NOT apply here, and mail Brain has not classified can show up. Use it after "
					+ "SearchMail finds nothing, or when the owner asks about Outlook itself.]");

	private static volatile Map<String, JSONObject> toolsByName;

	private CollaborationAgentTools() {
	}

	/** Whether agents in this room get the collaboration tools. */
	public static boolean appliesTo(Room room) {
		return CollaborationUtils.isCollaborationRoom(room);
	}

	/** Fresh copies of the tool definitions, safe for the caller to change. */
	public static List<Map<String, Object>> definitions() {
		List<Map<String, Object>> tools = new ArrayList<>();
		for (JSONObject tool : tools().values()) {
			tools.add(tool.toMap());
		}
		return tools;
	}

	public static boolean isTool(String toolName) {
		return toolName != null && tools().containsKey(toolName);
	}

	/** Whether a stored pending action's _meta came from one of these tools. */
	public static boolean isToolMeta(Map<String, Object> toolMeta) {
		return toolMeta != null && TOOL_KIND.equals(toolMeta.get("SMSS_TOOL_KIND"));
	}

	/** Runs the tool's reactor with the model's arguments as the room's user. */
	public static ToolExecutionResult execute(String toolName, Map<String, Object> params, Insight insight) {
		JSONObject tool = tools().get(toolName);
		if (tool == null) {
			return ToolExecutionResult.error(null, "Unknown collaboration tool: " + toolName);
		}
		JSONObject meta = tool.getJSONObject("_meta");
		JSONObject inputSchema = tool.optJSONObject("inputSchema");
		JSONObject properties = inputSchema != null && inputSchema.has("properties")
				? inputSchema.getJSONObject("properties")
				: new JSONObject();
		// the pixel call drops names it does not know, so a wrong name (body for
		// message) would run with that value missing; refuse it so the model retries
		List<String> unknown = new ArrayList<>();
		if (params != null) {
			for (String name : params.keySet()) {
				if (!properties.has(name)) {
					unknown.add(name);
				}
			}
		}
		if (!unknown.isEmpty()) {
			String message = toolName + " has no argument " + String.join(", ", unknown) + ". Its arguments are: "
					+ String.join(", ", properties.keySet()) + ". Nothing was run; call it again with those names.";
			return ToolExecutionResult.error(message, message);
		}
		try {
			Object output = MCPUtility.runPixelTool(null, insight, meta.getString(MCPUtility.SMSS_FUNCTION_NAME),
					properties, params);
			return ToolExecutionResult.success(output);
		} catch (RuntimeException e) {
			String message = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
			return ToolExecutionResult.error(message, message);
		}
	}

	private static Map<String, JSONObject> tools() {
		Map<String, JSONObject> loaded = toolsByName;
		if (loaded == null) {
			synchronized (CollaborationAgentTools.class) {
				if (toolsByName == null) {
					toolsByName = load();
				}
				loaded = toolsByName;
			}
		}
		return loaded;
	}

	private static Map<String, JSONObject> load() {
		// reactor descriptions name other reactors; the model knows them by tool name
		Map<String, String> toolNameOf = new LinkedHashMap<>();
		Map<String, JSONObject> built = new LinkedHashMap<>();
		REACTORS.forEach((name, type) -> {
			try {
				JSONObject tool = type.getDeclaredConstructor().newInstance().asMcpTool();
				JSONObject meta = tool.optJSONObject("_meta");
				if (meta == null || meta.optString(MCPUtility.SMSS_FUNCTION_NAME).isBlank()) {
					classLogger.warn("Reactor {} has no MCP metadata; rooms do not get {}", type.getSimpleName(), name);
					return;
				}
				toolNameOf.put(meta.getString(MCPUtility.SMSS_FUNCTION_NAME), name);
				meta.put("SMSS_TOOL_KIND", TOOL_KIND);
				// hidden from the model until it finds and loads it (SearchTools, LoadTools)
				if (DEFERRED.contains(name)) {
					meta.put(MCPUtility.SMSS_MCP_DEFERRED, true);
				}
				tool.put("name", name);
				tool.put("title", MCPUtility.formatToTitleCase(name));
				tool.getJSONObject("inputSchema").put("title", name + "_Arguments");
				built.put(name, tool);
			} catch (ReflectiveOperationException | RuntimeException e) {
				classLogger.warn("Could not build collaboration tool {} from {}", name, type.getSimpleName(), e);
			}
		});
		if (built.isEmpty()) {
			return built;
		}
		Pattern reactorName = Pattern.compile("\\b(" + String.join("|", toolNameOf.keySet()) + ")\\b");
		for (JSONObject tool : built.values()) {
			String note = SOURCE_NOTES.get(tool.getString("name"));
			tool.put("description", (note == null ? "" : note + " ")
					+ renamed(tool.getString("description"), reactorName, toolNameOf));
			JSONObject properties = tool.getJSONObject("inputSchema").optJSONObject("properties");
			if (properties != null) {
				for (String key : properties.keySet()) {
					JSONObject property = properties.getJSONObject(key);
					property.put("description", renamed(property.optString("description"), reactorName, toolNameOf));
				}
			}
		}
		return built;
	}

	private static String renamed(String text, Pattern reactorName, Map<String, String> toolNameOf) {
		Matcher m = reactorName.matcher(text);
		StringBuilder out = new StringBuilder();
		while (m.find()) {
			m.appendReplacement(out, Matcher.quoteReplacement(toolNameOf.get(m.group(1))));
		}
		m.appendTail(out);
		return out.toString();
	}
}

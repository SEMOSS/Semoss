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
package prerna.io.connector;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.reactor.agent.mcp.MCPUtility.MCPDisplayOption;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * What every mail and calendar reactor has in common, whichever provider it
 * calls.
 *
 * <p>
 * Each operation, such as listing mail or creating an event, is one abstract
 * reactor under this one, and each provider supplies only the call to its own
 * API. What a caller sees is settled here and in those abstracts, and cannot be
 * overridden by a provider: the description, the MCP metadata and the view the
 * tool renders with, the types of the keys, and how a bad value is reported.
 * That is what keeps an Outlook tool and its Gmail twin interchangeable.
 * </p>
 */
public abstract class AbstractConnectorAppReactor extends AbstractConnectorReactor {

	private static final Logger classLogger = LogManager.getLogger(AbstractConnectorAppReactor.class);

	/** The view URI parameter naming the provider the view reads from. */
	public static final String PROVIDER_PARAM = "provider";

	/** Ends the description of every tool that waits for the user to approve it. */
	public static final String ASK_NOTE = "The user reviews this before it runs and may change any value. "
			+ "The result shows what was actually done; treat it as the outcome, not as an error.";

	/**
	 * The type of every key a mail or calendar reactor takes, for its MCP schema.
	 */
	private static final Map<String, MCP_KEY_TYPE> KEY_TYPES = Map.ofEntries(
			Map.entry("unreadOnly", MCP_KEY_TYPE.BOOLEAN), Map.entry("includeBody", MCP_KEY_TYPE.BOOLEAN),
			Map.entry("includeAttachments", MCP_KEY_TYPE.BOOLEAN),
			Map.entry("includeDisplayBody", MCP_KEY_TYPE.BOOLEAN),
			Map.entry("includeReplyRecipients", MCP_KEY_TYPE.BOOLEAN), Map.entry("html", MCP_KEY_TYPE.BOOLEAN),
			Map.entry("replyAll", MCP_KEY_TYPE.BOOLEAN), Map.entry("asDraft", MCP_KEY_TYPE.BOOLEAN),
			Map.entry("overrideRecipients", MCP_KEY_TYPE.BOOLEAN), Map.entry("read", MCP_KEY_TYPE.BOOLEAN),
			Map.entry("saveToSentItems", MCP_KEY_TYPE.BOOLEAN), Map.entry("isAllDay", MCP_KEY_TYPE.BOOLEAN),
			Map.entry("isOnlineMeeting", MCP_KEY_TYPE.BOOLEAN), Map.entry("sendResponse", MCP_KEY_TYPE.BOOLEAN),
			Map.entry("limit", MCP_KEY_TYPE.INTEGER), Map.entry("offset", MCP_KEY_TYPE.INTEGER),
			Map.entry("sinceDays", MCP_KEY_TYPE.INTEGER), Map.entry("maxBodyChars", MCP_KEY_TYPE.INTEGER),
			Map.entry("days", MCP_KEY_TYPE.INTEGER), Map.entry("reminderMinutesBeforeStart", MCP_KEY_TYPE.INTEGER),
			Map.entry("to", MCP_KEY_TYPE.ARRAY), Map.entry("cc", MCP_KEY_TYPE.ARRAY),
			Map.entry("bcc", MCP_KEY_TYPE.ARRAY), Map.entry("attachments", MCP_KEY_TYPE.ARRAY),
			Map.entry("attendees", MCP_KEY_TYPE.ARRAY), Map.entry("optionalAttendees", MCP_KEY_TYPE.ARRAY),
			Map.entry("categories", MCP_KEY_TYPE.ARRAY), Map.entry("schedules", MCP_KEY_TYPE.ARRAY));

	/**
	 * @return the app this reactor works against
	 */
	protected abstract IConnectorApp getApp();

	/**
	 * @return whether an agent runs the tool straight away or asks the user first
	 */
	protected abstract MCPExecution getExecution();

	/**
	 * @return the component the tool renders with, as its library and view with any
	 *         parameters, such as {@code mail/compose?intent=send}, or null when it
	 *         renders with the generic tool view
	 */
	protected abstract String getView();

	/**
	 * @return what the operation does, naming the app it works against
	 */
	protected abstract String describe();

	/**
	 * What one key means for this operation. An operation abstract overrides this
	 * and makes it final, so every provider describes its keys the same way.
	 *
	 * @param key the key
	 * @return the description, or null for the standard one
	 */
	protected String describeKey(String key) {
		return null;
	}

	@Override
	protected final AuthProvider getAuthProvider() {
		return getApp().getAuthProvider();
	}

	@Override
	public final String getReactorDescription() {
		String description = describe();
		if (getExecution() == MCPExecution.ASK) {
			description = description + " " + ASK_NOTE;
		}
		return description;
	}

	@Override
	protected final String getDescriptionForKey(String key) {
		String description = describeKey(key);
		return description == null ? super.getDescriptionForKey(key) : description;
	}

	@Override
	public final Map<String, String> getMcpToolMetadata() {
		Map<String, String> meta = new HashMap<>();
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, getExecution().getValue());
		String view = getView();
		if (view == null) {
			meta.put(MCPUtility.UI_DISPLAY_LOCATION, MCPDisplayOption.SIDEBAR.getValue());
		} else {
			// a component view is the result itself, so it sits in the conversation
			meta.put(MCPUtility.UI_DISPLAY_LOCATION, MCPDisplayOption.INLINE.getValue());
			meta.put(MCPUtility.UI_RESOURCE_URI, viewUri(view, getApp()));
		}
		return meta;
	}

	/**
	 * The URI a tool names its component view with.
	 *
	 * @param view the library and view, with any parameters
	 * @param app  the app the view reads from
	 * @return the URI, such as
	 *         {@code component://mail/compose?intent=send&provider=google}
	 */
	public static String viewUri(String view, IConnectorApp app) {
		return MCPUtility.UI_COMPONENT_SCHEME + view + (view.contains("?") ? "&" : "?") + PROVIDER_PARAM + "="
				+ app.getProviderId();
	}

	@Override
	protected final MCP_KEY_TYPE getKeyTypeForMCP(String key) {
		MCP_KEY_TYPE type = KEY_TYPES.get(key);
		return type == null ? super.getKeyTypeForMCP(key) : type;
	}

	@Override
	public final JSONObject getMcpProperties() {
		JSONObject properties = super.getMcpProperties();
		for (String key : properties.keySet()) {
			if (getKeyTypeForMCP(key) == MCP_KEY_TYPE.ARRAY) {
				// a schema that says array without saying of what is refused by some
				// model APIs, and every list these reactors take is a list of text
				properties.getJSONObject(key).put("items", new JSONObject().put("type", "string"));
			}
		}
		return properties;
	}

	/**
	 * @param user the signed in user
	 * @return the address of the account they signed in to the provider with, or
	 *         null when the provider did not say
	 */
	protected final String accountEmail(User user) {
		AccessToken token = user == null ? null : user.getAccessToken(getAuthProvider());
		return token == null ? null : token.getEmail();
	}

	/**
	 * Do the operation, reporting what goes wrong the same way for every provider.
	 *
	 * @param toDo what is being done, used in the errors and the log
	 * @param work the operation, answering with what the reactor returns
	 * @return what the reactor returns
	 */
	protected NounMetadata run(String toDo, Callable<Object> work) {
		try {
			return new NounMetadata(work.call(), PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while trying to {} in {}", toDo, getApp().getDisplayName(), e);
			throw e;
		} catch (IllegalArgumentException e) {
			// the provider refused the request, or a value could not be read, and the
			// message says which
			classLogger.error("Could not {} in {}", toDo, getApp().getDisplayName(), e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to {} in {}", toDo, getApp().getDisplayName(), e);
			throw new SemossPixelException(
					"An error occurred trying to " + toDo + ". Error message: " + e.getMessage());
		}
	}

	/**
	 * @return how many items a listing skips, which is 0 when the caller left it
	 *         out
	 */
	protected int readOffset() {
		Integer value = readOptionalInt("offset");
		if (value == null) {
			return 0;
		}
		if (value < 0) {
			throw new SemossPixelException("offset cannot be negative.");
		}
		return value;
	}
}

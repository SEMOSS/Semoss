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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

import org.json.JSONObject;
import prerna.collaboration.EmailAttachmentFiles;
import prerna.sablecc2.om.GenRowStruct;

import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkComposeEmail(to=["a@x.com"], subject=["..."], message=["..."]);
// Puts an email in the owner's Work email editor; saves and sends nothing. The
// FE reads the call's arguments, so the result only confirms the hand-off.
public class WorkComposeEmailReactor extends AbstractCollaborationReactor {

	private static final String TO = "to";
	private static final String CC = "cc";
	private static final String BCC = "bcc";
	private static final String SUBJECT = "subject";
	private static final String MESSAGE = "message";
	private static final String REPLY_TO = "replyTo";
	private static final String FORWARD = "forward";
	private static final String OPEN_EMAIL_ID = "openEmailId";
	private static final String ATTACHMENTS = "attachments";

	public WorkComposeEmailReactor() {
		this.keysToGet = new String[] { MESSAGE, TO, CC, BCC, SUBJECT, REPLY_TO, FORWARD, OPEN_EMAIL_ID, ATTACHMENTS };
		this.keyRequired = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		getUser();
		String message = getString(MESSAGE);
		String openEmailId = getString(OPEN_EMAIL_ID);
		String forward = getString(FORWARD);
		if (!isBlank(forward) && !isBlank(getString(REPLY_TO))) {
			throw new IllegalArgumentException("Pass replyTo or forward, not both");
		}
		// a change to the open email passes only the fields that change; a forward may have no note
		if (isBlank(message) && isBlank(openEmailId) && isBlank(forward)) {
			throw new IllegalArgumentException("Must pass the email as message");
		}
		Map<String, Object> out = new LinkedHashMap<>();
		GenRowStruct files = this.store.getGenRowStruct(ATTACHMENTS);
		if (files != null && !files.isEmpty()) {
			if (this.insight.getRoomId() == null) throw new IllegalArgumentException("Open the email's room first.");
			List<String> paths = new ArrayList<>();
			for (int i = 0; i < files.size(); i++) {
				Object value = files.getNoun(i).getValue();
				if (!(value instanceof String)) throw new IllegalArgumentException("Attachments must be room-relative file paths.");
				paths.add((String) value);
			}
			try {
				out.put(ATTACHMENTS, EmailAttachmentFiles.snapshot(this.insight.getInsightFolder(), paths));
			} catch (IOException e) {
				throw new IllegalArgumentException("An attachment could not be prepared. Check that every file exists in this room.", e);
			}
		}
		out.put("shown", true);
		out.put("note", "The email is in the owner's email editor, where they can edit it. Nothing was saved or "
				+ "sent; SendEmail sends it once they press Send.");
		return mapResult(out);
	}

	private static boolean isBlank(String value) {
		return value == null || value.isBlank();
	}

	@Override
	public JSONObject getMcpProperties() {
		JSONObject properties = super.getMcpProperties();
		properties.getJSONObject(ATTACHMENTS).put("type", "array").put("items", new JSONObject().put("type", "string"));
		return properties;
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		Map<String, String> meta = super.getMcpToolMetadata();
		meta.put(MCPUtility.UI_COMPONENT, MCPUtility.COMPONENT_EMAIL_COMPOSE);
		return meta;
	}

	@Override
	public String getReactorDescription() {
		return "Write an email for the owner and open it in their email editor, where they review, edit, and send it. "
				+ "Use it to write a new email, reply to or forward one, or change the email they have open, passing only the fields "
				+ "that change. It saves and sends nothing.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ATTACHMENTS.equals(key)) {
			return "Optional array of files in this room's working directory, using relative paths such as hello.txt. "
					+ "Adds files to the email's attachment list for the owner to review; existing attachments stay. "
					+ "Use openEmailId to attach to the open email without changing its text. At most 10 files, 2.5 MB total per call. "
					+ "Never pass absolute paths. Nothing is sent until the owner presses Send.";
		} else if (MESSAGE.equals(key)) {
			return "The whole email in plain text, greeting to sign-off, written as the owner. No Markdown. "
					+ "For a forward, the short note above the forwarded email. Required, except for a forward or "
					+ "when openEmailId is set and the text does not change.";
		} else if (TO.equals(key)) {
			return "Optional recipients, as one comma separated value: the whole new list. Use only addresses from the "
					+ "Work context, FindPerson, or the owner; never guess one, leave it empty instead. Leave it out for "
					+ "a reply, which goes to whoever Outlook replies to, unless the owner asks to change who gets it. "
					+ "Required for a forward.";
		} else if (CC.equals(key)) {
			return "Optional recipients to copy, as one comma separated value: the whole new list. Same address rules "
					+ "as to; leave it out for a reply unless the owner asks to change it.";
		} else if (BCC.equals(key)) {
			return "Optional recipients to blind copy, as one comma separated value.";
		} else if (SUBJECT.equals(key)) {
			return "Optional subject line of a new email, saying what it is about. Never start it with Re: or Fwd:. "
					+ "A reply or forward keeps its thread's subject; Outlook adds RE: or FW:.";
		} else if (REPLY_TO.equals(key)) {
			return "Optional id of the email to reply to, taken from the messages in the Work context. "
					+ "The reply stays in that email's thread. Leave it out for a new email.";
		} else if (FORWARD.equals(key)) {
			return "Optional id of the email to forward, taken from the messages in the Work context. Outlook includes "
					+ "that email and its attachments below the note, so do not copy its text into message. Set to. "
					+ "Leave it out unless the owner asks to forward.";
		} else if (OPEN_EMAIL_ID.equals(key)) {
			return "Optional id of the email the owner has open (openEmail.id in the Work context), to change it. "
					+ "Fields left out keep their current values. Leave it out to start a separate email.";
		}
		return super.getDescriptionForKey(key);
	}
}

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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import prerna.collaboration.email.MailProvider;
import prerna.collaboration.email.MailProviders;
import prerna.collaboration.email.OutgoingEmail;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkSendEmail(openEmailId=["assistant-draft:..."]);
// Sends an email from the owner's mailbox once they approve. In Work the owner
// presses Send on the email in their editor, which saves it and approves with its
// draftId, so what goes out is what the editor holds.
public class WorkSendEmailReactor extends AbstractCollaborationReactor {

	private static final String OPEN_EMAIL_ID = "openEmailId";
	private static final String TO = "to";
	private static final String CC = "cc";
	private static final String BCC = "bcc";
	private static final String SUBJECT = "subject";
	private static final String MESSAGE = "message";
	private static final String REPLY_TO = "replyTo";
	private static final String FROM = "from";
	private static final String DRAFT_ID = "draftId";

	public WorkSendEmailReactor() {
		this.keysToGet = new String[] { OPEN_EMAIL_ID, TO, CC, BCC, SUBJECT, MESSAGE, REPLY_TO, FROM, DRAFT_ID };
		this.keyRequired = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		String draftId = trimmed(DRAFT_ID);
		String openEmailId = trimmed(OPEN_EMAIL_ID);
		String message = trimmed(MESSAGE);
		// the open email lives in the editor, so only the editor's saved draft may send it
		if (draftId == null && openEmailId != null) {
			throw new IllegalArgumentException(
					"The open email is sent from the owner's email editor. Ask them to press Send there.");
		}
		if (draftId == null && message == null) {
			throw new IllegalArgumentException("Must pass the email as message, or openEmailId for the open email");
		}
		try {
			MailProvider mail = MailProviders.forUser(getUser(), trimmed(FROM));
			if (draftId == null) {
				OutgoingEmail email = new OutgoingEmail(addresses(TO), addresses(CC), addresses(BCC), trimmed(SUBJECT),
						message, trimmed(REPLY_TO));
				if (!email.isReply() && !email.hasRecipients()) {
					throw new IllegalArgumentException("A new email needs at least one recipient");
				}
				draftId = mail.saveDraft(email);
			}
			Map<String, Object> out = new LinkedHashMap<>();
			out.put("sent", true);
			out.put(FROM, mail.account());
			out.putAll(mail.sendDraft(draftId));
			return mapResult(out);
		} catch (SemossPixelException | IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			throw new SemossPixelException("The email could not be sent: " + e.getMessage());
		}
	}

	private String trimmed(String key) {
		String value = getString(key);
		return value == null || value.isBlank() ? null : value.trim();
	}

	// one comma or semicolon separated value
	private String[] addresses(String key) {
		String value = trimmed(key);
		return value == null ? new String[0]
				: Arrays.stream(value.split("[,;]")).map(String::trim).filter(s -> !s.isEmpty()).toArray(String[]::new);
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		// sends mail as the owner, so it waits for their approval
		Map<String, String> meta = super.getMcpToolMetadata();
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.ASK.getValue());
		meta.put(MCPUtility.UI_COMPONENT, MCPUtility.COMPONENT_EMAIL_SEND);
		return meta;
	}

	@Override
	public String getReactorDescription() {
		return "Send an email from the owner's mailbox. It waits for the owner: for the email open in their editor "
				+ "they press Send on it, which sends exactly what the editor holds. Use it only when they ask to send.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (OPEN_EMAIL_ID.equals(key)) {
			return "Id of the email the owner has open (openEmail.id in the Work context). Pass only this to send it.";
		} else if (TO.equals(key)) {
			return "Recipients, as one comma separated value, when there is no open email to send.";
		} else if (CC.equals(key)) {
			return "Recipients to copy, as one comma separated value, when there is no open email to send.";
		} else if (BCC.equals(key)) {
			return "Recipients to blind copy on a new email, as one comma separated value.";
		} else if (SUBJECT.equals(key)) {
			return "Subject of a new email, when there is no open email to send. A reply keeps its thread's subject.";
		} else if (MESSAGE.equals(key)) {
			return "The whole email in plain text, when there is no open email to send. No Markdown.";
		} else if (REPLY_TO.equals(key)) {
			return "Id of the email to reply to, when there is no open email to send. The reply stays in its thread.";
		} else if (FROM.equals(key)) {
			return "Address to send from, only when the owner names one. Defaults to the owner's mailbox.";
		} else if (DRAFT_ID.equals(key)) {
			return "Set by the owner's email editor when they press Send. Never set it yourself.";
		}
		return super.getDescriptionForKey(key);
	}
}

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
package prerna.io.connector.ms.outlook;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.Utility;

/**
 * What the reactors that write a message have in common.
 *
 * <p>
 * Sending a message and saving one as a draft differ only in the Graph call at
 * the end. Everything before it - reading the recipients, resolving the
 * attachments against the insight folder, and assembling the message - is the
 * same, and is here so the two cannot drift apart.
 * </p>
 *
 * <p>
 * Neither takes a sender. The message belongs to whoever is signed in because
 * the token says who that is, so nothing a caller passes can compose as
 * somebody else.
 * </p>
 */
public abstract class AbstractMicrosoftOutlookComposeReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AbstractMicrosoftOutlookComposeReactor.class);

	protected static final String TO = "to";
	protected static final String CC = "cc";
	protected static final String BCC = "bcc";
	protected static final String SUBJECT = "subject";
	protected static final String MESSAGE = "message";
	protected static final String HTML = "html";
	protected static final String ATTACHMENTS = "attachments";

	/**
	 * The keys every composing reactor takes, in the order it takes them. Clone it
	 * rather than assigning it straight to {@code keysToGet}, which belongs to the
	 * reactor instance.
	 */
	protected static final String[] COMPOSE_KEYS = { TO, CC, BCC, SUBJECT, MESSAGE, HTML, ATTACHMENTS };

	/**
	 * A message that has been assembled but not yet handed to Graph.
	 */
	protected static class ComposedMail {

		/** The message in the shape Graph reads. */
		protected Map<String, Object> message;

		protected String[] to;
		protected String[] cc;
		protected String[] bcc;
		protected String subject;
		protected String body;
		protected boolean html;

		/** The resolved paths of the files attached, or null when there are none. */
		protected String[] attachments;

		/**
		 * Report what was composed, so a caller can see what it ended up with without
		 * reading the message back out of Graph.
		 *
		 * @param output the map being answered with
		 */
		protected void describe(Map<String, Object> output) {
			putIfPresent(output, TO, this.to);
			putIfPresent(output, CC, this.cc);
			putIfPresent(output, BCC, this.bcc);
			if (this.subject != null) {
				output.put(SUBJECT, this.subject);
			}
			if (this.attachments != null) {
				List<String> names = new ArrayList<>();
				for (String attachment : this.attachments) {
					// the name rather than the resolved path, so the caller is not handed
					// the server side layout of the insight folder
					names.add(new File(attachment).getName());
				}
				output.put(ATTACHMENTS, names);
			}
		}

	}

	/**
	 * Read the inputs and assemble the message.
	 *
	 * @param requireContent whether a recipient and a body have to be there, which
	 *                       they do to send and do not to save a draft somebody is
	 *                       going to finish
	 * @param toDo           what this is being composed for, used in the errors
	 * @return the assembled message
	 * @throws IOException when an attachment cannot be read
	 */
	protected ComposedMail compose(boolean requireContent, String toDo) throws IOException {
		ComposedMail composed = new ComposedMail();
		composed.to = recipients(TO);
		composed.cc = recipients(CC);
		composed.bcc = recipients(BCC);
		composed.subject = this.keyValue.get(SUBJECT);
		composed.attachments = resolveAttachments();

		composed.body = this.keyValue.get(MESSAGE);
		composed.html = Boolean.parseBoolean(this.keyValue.get(HTML));
		if (requireContent) {
			if (composed.to == null && composed.cc == null && composed.bcc == null) {
				throw new SemossPixelException(
						"At least one of " + TO + ", " + CC + " or " + BCC + " is required to " + toDo + ".");
			}
			if (composed.body == null || composed.body.trim().isEmpty()) {
				throw new SemossPixelException("A " + MESSAGE + " is required to " + toDo + ".");
			}
		}

		// no from address, so the message belongs to the mailbox the token belongs to
		composed.message = MicrosoftOutlookMailHelper.buildMessage(composed.subject, composed.body, composed.html,
				composed.to, composed.cc, composed.bcc, null, null, composed.attachments);
		return composed;
	}

	/**
	 * Read one set of recipients, which a caller may pass as several values.
	 *
	 * @param key the recipient key to read
	 * @return the addresses, or null when none were passed
	 */
	private String[] recipients(String key) {
		GenRowStruct grs = this.store.getGenRowStruct(key);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		List<String> addresses = new ArrayList<>();
		for (int i = 0; i < grs.size(); i++) {
			Object value = grs.getNoun(i).getValue();
			if (value == null) {
				continue;
			}
			// a single value can still be a comma separated list, since that is how
			// somebody writing the pixel by hand tends to pass more than one
			for (String address : value.toString().split(",")) {
				if (!address.trim().isEmpty()) {
					addresses.add(address.trim());
				}
			}
		}
		if (addresses.isEmpty()) {
			return null;
		}
		return addresses.toArray(new String[0]);
	}

	/**
	 * Resolve the files to attach against the insight folder, which is the only
	 * place these reactors will read one from.
	 *
	 * <p>
	 * {@link Utility#normalizePath} collapses any {@code ..} segments and rejects a
	 * path that climbs above its root, which is the same guard the insight asset
	 * reactors use. Stripping a leading slash afterwards keeps an absolute path
	 * from being read as one.
	 * </p>
	 *
	 * @return the file paths to attach, or null when there are none
	 */
	private String[] resolveAttachments() {
		GenRowStruct grs = this.store.getGenRowStruct(ATTACHMENTS);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		if (this.insight == null || this.insight.getInsightFolder() == null) {
			throw new SemossPixelException(
					"Attachments can only be added from within an insight that holds the files.");
		}

		File insightFolder = new File(this.insight.getInsightFolder());
		List<String> attachments = new ArrayList<>();
		for (int i = 0; i < grs.size(); i++) {
			Object value = grs.getNoun(i).getValue();
			if (value == null || value.toString().trim().isEmpty()) {
				continue;
			}
			String requestedPath = value.toString().trim();

			String relativePath;
			try {
				relativePath = Utility.normalizePath(requestedPath);
			} catch (IllegalArgumentException e) {
				classLogger.error("Rejected an invalid attachment path '{}'", requestedPath, e);
				throw new SemossPixelException("The attachment path is not valid: " + requestedPath);
			}
			while (relativePath.startsWith("/")) {
				relativePath = relativePath.substring(1);
			}
			if (relativePath.isEmpty()) {
				continue;
			}

			File attachment = new File(insightFolder, relativePath);
			if (!attachment.exists() || !attachment.isFile()) {
				throw new SemossPixelException("No file exists in the insight folder at: " + relativePath);
			}
			attachments.add(attachment.getAbsolutePath());
		}

		if (attachments.isEmpty()) {
			return null;
		}
		return attachments.toArray(new String[0]);
	}

	/**
	 * Report back one set of recipients, joined, and only when there were any.
	 *
	 * @param output     the map being built
	 * @param key        the key to set
	 * @param recipients the addresses, or null when there were none
	 */
	private static void putIfPresent(Map<String, Object> output, String key, String[] recipients) {
		if (recipients != null && recipients.length > 0) {
			output.put(key, String.join(", ", recipients));
		}
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(TO)) {
			return "Recipients of the email.";
		} else if (key.equals(CC)) {
			return "Recipients to copy on the email.";
		} else if (key.equals(BCC)) {
			return "Recipients to blind copy on the email.";
		} else if (key.equals(SUBJECT)) {
			return "Subject line of the email.";
		} else if (key.equals(MESSAGE)) {
			return "Body of the email.";
		} else if (key.equals(HTML)) {
			return "Optional boolean for whether the body is html rather than plain text. Defaults to false.";
		} else if (key.equals(ATTACHMENTS)) {
			return "Optional files to attach, named relative to the insight folder. Roughly 4MB in total is accepted.";
		}
		return super.getDescriptionForKey(key);
	}

}

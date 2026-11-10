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
package prerna.io.connector.mail;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.EncryptedDocumentException;

import prerna.auth.User;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Saves a file attached to a message into the insight folder.
 */
public abstract class AbstractDownloadAttachmentReactor extends AbstractMailReactor {

	private static final Logger classLogger = LogManager.getLogger(AbstractDownloadAttachmentReactor.class);

	/** Whether to also write a plain text copy of an Office or mail file beside it. */
	public static final String INCLUDE_TEXT = "includeText";

	private static final String[] KEYS = { ID, ATTACHMENT_ID, FILE_NAME, INCLUDE_TEXT };

	protected AbstractDownloadAttachmentReactor(String... extraKeys) {
		this.keysToGet = withExtraKeys(KEYS, extraKeys);
		this.keyRequired = requiredKeys(this.keysToGet, ID);
	}

	/**
	 * Read the bytes of one file attached to a message.
	 *
	 * @param user         the signed in user
	 * @param id           the message
	 * @param attachmentId the id or name of the attachment, or null for the only
	 *                     one the message carries
	 * @return the file, or null when there is no such attachment, or when none was
	 *         named and the message carries more than one
	 * @throws Exception when the message cannot be read, or the attachment is not a
	 *                   file
	 */
	protected abstract MailAttachmentContent downloadAttachment(User user, String id, String attachmentId)
			throws Exception;

	@Override
	protected final NounMetadata executeAuthenticated() {
		this.organizeKeys();
		return run("download the attachment", () -> {
			String id = requireId("download an attachment");
			String attachmentId = readString(ATTACHMENT_ID);
			MailAttachmentContent attachment = downloadAttachment(this.insight.getUser(), id, attachmentId);
			if (attachment == null) {
				throw new SemossPixelException(attachmentId == null
						? "The message does not carry exactly one attachment, so name the one to download."
						: "The message has no attachment with that id or name: " + attachmentId);
			}
			byte[] bytes = attachment.bytes() == null ? new byte[0] : attachment.bytes();
			String saved = saveToInsightFolder(readString(FILE_NAME), attachment.name(), bytes);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put(ID, id);
			output.put(ATTACHMENT_ID, attachment.id());
			output.put("name", attachment.name());
			output.put("filePath", saved);
			output.put("size", bytes.length);
			if (readBoolean(INCLUDE_TEXT, false)) {
				writeText(output, Path.of(this.insight.getInsightFolder(), saved), attachment.name());
			}
			output.put("success", true);
			return output;
		});
	}

	/**
	 * Write a {@code <file>.txt} copy of a Word, Excel, PowerPoint, .msg or .eml
	 * file beside it, as {@code textPath}, read by {@link AttachmentText},
	 * as Collaboration reads mail attachments. A copy
	 * that cannot be written says why in {@code textError}; the file itself is
	 * still saved.
	 *
	 * @param output what the reactor answers with, added to
	 * @param file   the saved file
	 * @param name   the attachment's own name, whose extension says what it is
	 */
	private static void writeText(Map<String, Object> output, Path file, String name) {
		if (!AttachmentText.supports(name)) {
			return;
		}
		try {
			// a text document reaches a model without its file name
			AttachmentText.Extracted text = AttachmentText.extract(file, name, "Attachment: " + name + "\n\n",
					AttachmentText.MAX_CHARS);
			String textName = file.getFileName() + ".txt";
			Files.writeString(file.resolveSibling(textName), text.text(), StandardCharsets.UTF_8);
			output.put("textPath", textName);
			if (text.truncated()) {
				output.put("textTruncated", true);
			}
		} catch (EncryptedDocumentException e) {
			output.put("textError", "This file is password protected, so its text could not be read.");
		} catch (Exception e) {
			classLogger.warn("Could not read the text of an attachment", e);
			output.put("textError", "The text of this file could not be read.");
		}
	}


	@Override
	protected final MCPExecution getExecution() {
		return MCPExecution.AUTO;
	}

	@Override
	protected final String getView() {
		return null;
	}

	// only the room UI asks for the copy; offered to an agent, its schema would type this boolean as a string
	@Override
	protected final Set<String> getViewerKeys() {
		return Set.of(INCLUDE_TEXT);
	}

	@Override
	protected final String describe() {
		return "Download a file attached to a message in the signed in user's own " + mailbox()
				+ " into the insight folder.";
	}

	@Override
	protected final String describeKey(String key) {
		switch (key) {
		case ATTACHMENT_ID:
			return "Optional id or name of the attachment, as returned by " + reactorName("GetMail")
					+ ". The only attachment is taken when the message has one.";
		case FILE_NAME:
			return "Optional name to save the file as in the insight folder. The attachment's own name is used when omitted.";
		case INCLUDE_TEXT:
			return "Optional boolean to also write a plain text copy of a Word, Excel, PowerPoint, .msg or .eml file "
					+ "beside it, returned as textPath, or textError when it cannot be read. Defaults to false.";
		default:
			return super.describeKey(key);
		}
	}
}

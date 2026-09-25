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
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.PathSecurityUtils;

/**
 * Downloads a file attached to a message into the insight folder.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Mail.Read} for {@code GET /me/messages/{id}/attachments} and
 * {@code GET /me/messages/{id}/attachments/{id}}. {@code Mail.ReadWrite} also
 * satisfies both.</li>
 * </ul>
 *
 * <p>
 * Unlike a file in Teams or OneDrive, a mail attachment is carried inside the
 * message rather than pointed at, so it arrives as bytes on the attachment
 * itself and there is no drive to fetch it from. Only a file attachment has
 * those bytes: an attachment that is another message, or a link to a file in a
 * drive, is reported as such rather than written out as something it is not.
 * </p>
 *
 * <p>
 * Naming the attachment is optional when the message carries only one, which is
 * the common case. Otherwise pass either the id or the name that
 * {@code MicrosoftOutlookGetMail} listed.
 * </p>
 */
public class MicrosoftOutlookDownloadAttachmentReactor extends AbstractMicrosoftOutlookMessageReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftOutlookDownloadAttachmentReactor.class);

	private static final String ATTACHMENT_ID = "attachmentId";
	private static final String NAME = "name";
	private static final String CONTENT_BYTES = "contentBytes";

	public MicrosoftOutlookDownloadAttachmentReactor() {
		this.keysToGet = new String[] { UID, ATTACHMENT_ID, ReactorKeysEnum.FILE_NAME.getKey() };
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String uid = requiredUid("download an attachment");
		String attachmentId = trimToNull(this.keyValue.get(ATTACHMENT_ID));
		// the file lands in the insight folder, so only the base name is honored. A
		// name carrying separators would otherwise write outside that folder
		String fileName = toBaseName(this.keyValue.get(ReactorKeysEnum.FILE_NAME.getKey()));

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);
			MicrosoftOutlookMailHelper helper = new MicrosoftOutlookMailHelper();

			Map<String, Object> attachment = findAttachment(helper, accessToken, uid, attachmentId);
			if (attachment == null) {
				throw new SemossPixelException(attachmentId == null
						? "The message carries more than one attachment, so name the one to download."
						: "The message has no attachment with that id or name: " + attachmentId);
			}
			if (!MicrosoftOutlookMessageMapper.isFileAttachment(attachment)) {
				throw new SemossPixelException("The attachment '" + attachment.get(NAME)
						+ "' is not a file. An embedded message or a link to a file in a drive has no bytes to save.");
			}
			Object contentBytes = attachment.get(CONTENT_BYTES);
			if (contentBytes == null) {
				throw new SemossPixelException(
						"Microsoft Graph returned no content for the attachment: " + attachment.get(NAME));
			}

			String insightFolder = this.insight.getInsightFolder();
			File insightFolderFile = new File(insightFolder);
			if (!insightFolderFile.exists() && !insightFolderFile.mkdirs()) {
				throw new SemossPixelException("Unable to create the insight folder at: " + insightFolder);
			}

			byte[] bytes = Base64.getDecoder().decode(contentBytes.toString());
			String name = fileName == null ? toBaseName(String.valueOf(attachment.get(NAME))) : fileName;
			if (name == null) {
				name = "attachment";
			}
			name = PathSecurityUtils.requireSinglePathSegment(name, "Downloaded attachment name");
			Path canonicalInsightFolder = insightFolderFile.getCanonicalFile().toPath();
			Path target = canonicalInsightFolder.resolve(name).normalize().toFile().getCanonicalFile().toPath();
			if (target.equals(canonicalInsightFolder) || !target.startsWith(canonicalInsightFolder)
					|| !canonicalInsightFolder.equals(target.getParent())) {
				throw new IllegalArgumentException("Attachment must be a direct child of the insight folder");
			}
			File file = PathSecurityUtils.requireDirectChild(canonicalInsightFolder.toFile(), target.toFile());
			try (FileOutputStream fos = new FileOutputStream(file)) {
				fos.write(bytes);
				fos.flush();
			}

			Map<String, Object> output = new LinkedHashMap<>();
			output.put(UID, uid);
			output.put(ATTACHMENT_ID, attachment.get("id"));
			output.put(NAME, attachment.get(NAME));
			output.put("filePath", name);
			output.put("size", bytes.length);
			output.put("success", true);
			return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while downloading an attachment of the message '{}'", uid, e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to download an attachment", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to download an attachment of the message '{}'", uid, e);
			throw new SemossPixelException(
					"An error occurred downloading the attachment. Error message: " + e.getMessage());
		}
	}

	/**
	 * Finds the attachment a caller asked for.
	 *
	 * <p>
	 * An id is read directly, since that is one call rather than pulling the bytes
	 * of everything on the message. A name, or nothing at all, has to read the
	 * attachments to match against, and what comes back already carries the bytes,
	 * so there is nothing further to fetch.
	 * </p>
	 *
	 * @param helper       the mail helper to read with
	 * @param accessToken  Microsoft Graph access token for the user
	 * @param uid          the message holding the attachment
	 * @param attachmentId the id or name asked for, or null for the only one
	 * @return the attachment as Graph returned it, or null when there is no such
	 *         thing to take
	 */
	private Map<String, Object> findAttachment(MicrosoftOutlookMailHelper helper, String accessToken, String uid,
			String attachmentId) {
		if (attachmentId != null) {
			try {
				Map<String, Object> attachment = helper.getAttachment(accessToken, null, uid, attachmentId);
				if (attachment != null && attachment.get("id") != null) {
					return attachment;
				}
			} catch (IllegalArgumentException e) {
				// not an id, so it is the name somebody read off the message
				classLogger.debug("'{}' is not an attachment id, matching it against the names instead", attachmentId,
						e);
			}
		}

		List<Map<String, Object>> attachments = helper.listAttachments(accessToken, null, uid);
		if (attachmentId == null) {
			return attachments.size() == 1 ? attachments.get(0) : null;
		}
		for (Map<String, Object> attachment : attachments) {
			if (attachmentId.equalsIgnoreCase(String.valueOf(attachment.get(NAME)))) {
				return attachment;
			}
		}
		return null;
	}

	/**
	 * Reduces a requested file name to its base name, so the download cannot be
	 * steered outside the insight folder.
	 *
	 * @param fileName the requested name, may be null or blank
	 * @return the base name, or null when nothing usable was supplied
	 */
	private static String toBaseName(String fileName) {
		if (fileName == null) {
			return null;
		}
		String trimmed = fileName.trim().replace('\\', '/');
		int lastSlash = trimmed.lastIndexOf('/');
		if (lastSlash >= 0) {
			trimmed = trimmed.substring(lastSlash + 1);
		}
		trimmed = trimmed.trim();
		return trimmed.isEmpty() || trimmed.equals(".") || trimmed.equals("..") ? null : trimmed;
	}

	@Override
	public String getReactorDescription() {
		return "Download a file attached to a message in the signed in user's own Microsoft 365 mailbox into the insight folder.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ATTACHMENT_ID)) {
			return "Optional id or name of the attachment, as returned by MicrosoftOutlookGetMail. The only attachment is taken when the message has one.";
		} else if (key.equals(ReactorKeysEnum.FILE_NAME.getKey())) {
			return "Optional name to save the file as in the insight folder. The name the attachment carries is used when omitted.";
		}
		return super.getDescriptionForKey(key);
	}
}

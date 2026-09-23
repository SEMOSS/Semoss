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
package prerna.io.connector.ms.teams;

import java.io.File;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Downloads something attached to a Teams message into the insight folder.
 *
 * <p>
 * Required delegated Microsoft Graph scopes:
 * </p>
 * <ul>
 * <li>{@code Chat.Read} to read a chat message, or
 * {@code ChannelMessage.Read.All} to read a channel message</li>
 * <li>{@code Files.Read.All} for {@code GET /shares/{id}/driveItem} and the
 * file content, since an attached file lives in OneDrive or SharePoint rather
 * than in the message. {@code Files.ReadWrite.All} also satisfies it.</li>
 * </ul>
 *
 * <p>
 * One reactor serves a chat and a channel because pulling a file out of a
 * message is the same act either way: name the chat, or name the team and
 * channel, and it finds the message from there.
 * </p>
 *
 * <p>
 * A message can hold two different things worth downloading, and both are
 * handled. A file is an attachment pointing at a drive, and comes back with the
 * name it has there. A pasted image is hosted content, which carries no name of
 * its own, so pass a {@code fileName} to say what it should be saved as. An
 * attachment that is an adaptive card is neither, and is refused with an
 * explanation rather than written out as json.
 * </p>
 */
public class MicrosoftTeamsDownloadMessageAttachmentReactor extends AbstractMicrosoftTeamsMessageReactor {

	private static final Logger classLogger = LogManager
			.getLogger(MicrosoftTeamsDownloadMessageAttachmentReactor.class);

	private static final String ATTACHMENT_ID = "attachmentId";

	public MicrosoftTeamsDownloadMessageAttachmentReactor() {
		this.keysToGet = new String[] { CHAT_ID, TEAM_ID, CHANNEL_ID, MESSAGE_ID, REPLY_ID, ATTACHMENT_ID,
				ReactorKeysEnum.FILE_NAME.getKey() };
		this.keyRequired = new int[] { 0, 0, 0, 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String chatId = trimToNull(this.keyValue.get(CHAT_ID));
		String teamId = trimToNull(this.keyValue.get(TEAM_ID));
		String channelId = trimToNull(this.keyValue.get(CHANNEL_ID));
		String messageId = trimToNull(this.keyValue.get(MESSAGE_ID));
		String replyId = trimToNull(this.keyValue.get(REPLY_ID));
		String attachmentId = trimToNull(this.keyValue.get(ATTACHMENT_ID));

		if (messageId == null) {
			throw new SemossPixelException(
					"A " + MESSAGE_ID + " is required to download a Microsoft Teams attachment.");
		}
		if (chatId == null && (teamId == null || channelId == null)) {
			throw new SemossPixelException("Either a " + CHAT_ID + " or both a " + TEAM_ID + " and a " + CHANNEL_ID
					+ " are required to say where the message is.");
		}
		// the file lands in the insight folder, so only the base name is honored. A
		// name carrying separators would otherwise write outside that folder
		String fileName = toBaseName(this.keyValue.get(ReactorKeysEnum.FILE_NAME.getKey()));

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);

			// create it up front: for an unsaved insight the folder may not exist yet,
			// and the download writes into it rather than creating it
			String insightFolder = this.insight.getInsightFolder();
			File insightFolderFile = new File(insightFolder);
			if (!insightFolderFile.exists() && !insightFolderFile.mkdirs()) {
				throw new SemossPixelException("Unable to create the insight folder at: " + insightFolder);
			}

			Map<String, Object> result = MicrosoftTeamsMessageHelper.downloadAttachment(accessToken, chatId, teamId,
					channelId, messageId, replyId, attachmentId, insightFolder, fileName);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while downloading a Microsoft Teams message attachment", e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to download a Microsoft Teams message attachment", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to download a Microsoft Teams message attachment", e);
			throw new SemossPixelException(
					"An error occurred downloading the attachment. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Download a file or an image attached to a Microsoft Teams chat or channel message into the insight folder.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(CHAT_ID)) {
			return "Id of the chat holding the message. Not needed when a team id and channel id are given.";
		} else if (key.equals(TEAM_ID)) {
			return "Id of the team holding the message. Not needed when a chat id is given.";
		} else if (key.equals(CHANNEL_ID)) {
			return "Id of the channel holding the message. Not needed when a chat id is given.";
		} else if (key.equals(ATTACHMENT_ID)) {
			return "Optional id or name of the attachment, as returned on the message. The only attachment is taken when the message has one.";
		} else if (key.equals(ReactorKeysEnum.FILE_NAME.getKey())) {
			return "Optional name to save the file as in the insight folder. The name the attachment carries is used when omitted, which a pasted image does not have.";
		}
		return super.getDescriptionForKey(key);
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
}

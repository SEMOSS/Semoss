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

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Posts a message in a Teams channel, as the signed in user.
 *
 * <p>
 * Required delegated Microsoft Graph scopes:
 * </p>
 * <ul>
 * <li>{@code ChannelMessage.Send} for
 * {@code POST /teams/{id}/channels/{id}/messages} and for the replies
 * underneath a message</li>
 * <li>{@code User.Read.All} as well, when {@code mentions} names people, since
 * a mention carries the directory id of whoever is being notified</li>
 * </ul>
 *
 * <p>
 * Naming a {@code replyToId} posts underneath that message instead of starting
 * a new conversation in the channel, which is the difference between answering
 * a thread and opening one. A subject belongs to a new conversation only;
 * Microsoft Teams shows no subject on a reply.
 * </p>
 *
 * <p>
 * A file has to be in the channel's SharePoint folder before it can be
 * attached, so upload it with {@code MicrosoftTeamsUploadFile} first and pass
 * the {@code webUrl} that comes back. Attaching a file from somewhere the
 * channel's members cannot reach posts a link they cannot open.
 * </p>
 */
public class MicrosoftTeamsSendChannelMessageReactor extends AbstractMicrosoftTeamsMessageReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftTeamsSendChannelMessageReactor.class);

	private static final String REPLY_TO_ID = "replyToId";
	private static final String SUBJECT = "subject";

	public MicrosoftTeamsSendChannelMessageReactor() {
		this.keysToGet = new String[] { TEAM_ID, CHANNEL_ID, MESSAGE, SUBJECT, REPLY_TO_ID, HTML, MENTIONS,
				ATTACHMENT_URLS, ATTACHMENT_NAMES };
		this.keyRequired = new int[] { 1, 1, 1, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String teamId = trimToNull(this.keyValue.get(TEAM_ID));
		String channelId = trimToNull(this.keyValue.get(CHANNEL_ID));
		String content = this.keyValue.get(MESSAGE);
		String subject = trimToNull(this.keyValue.get(SUBJECT));
		String replyToId = trimToNull(this.keyValue.get(REPLY_TO_ID));
		boolean html = Boolean.parseBoolean(this.keyValue.get(HTML));
		String[] mentions = values(MENTIONS);
		String[] attachmentUrls = values(ATTACHMENT_URLS);
		String[] attachmentNames = values(ATTACHMENT_NAMES);

		if (teamId == null) {
			throw new SemossPixelException("A " + TEAM_ID + " is required to post in a Microsoft Teams channel.");
		}
		if (channelId == null) {
			throw new SemossPixelException("A " + CHANNEL_ID + " is required to post in a Microsoft Teams channel.");
		}

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);
			Map<String, Object> sent = MicrosoftTeamsMessageHelper.sendChannelMessage(accessToken, teamId, channelId,
					replyToId, subject, content, html, mentions, attachmentUrls, attachmentNames,
					DEFAULT_MAX_BODY_CHARS);
			return new NounMetadata(sent, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while posting in Microsoft Teams channel '{}'", channelId, e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to post in a Microsoft Teams channel", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to post in Microsoft Teams channel '{}'", channelId, e);
			throw new SemossPixelException("An error occurred sending the message. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Post a message in a Microsoft Teams channel, or a reply underneath a message already there.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(SUBJECT)) {
			return "Optional subject line for a new conversation in the channel. Microsoft Teams shows no subject on a reply.";
		} else if (key.equals(REPLY_TO_ID)) {
			return "Optional id of the message to reply to. A new conversation is started in the channel when omitted.";
		}
		return super.getDescriptionForKey(key);
	}
}

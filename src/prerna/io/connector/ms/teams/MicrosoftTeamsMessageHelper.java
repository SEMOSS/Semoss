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
import java.io.FileOutputStream;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hc.core5.http.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;

import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.io.connector.ms.MicrosoftTokenFiller;
import prerna.io.connector.ms.onedrive.MicrosoftOneDriveHelper;
import prerna.security.HttpHelperUtility;

/**
 * The Teams messaging operations of Microsoft Graph, as plain calls.
 *
 * <p>
 * Everything here is delegated: the token says who the signed in user is, so a
 * message is posted as that person and the chats that can be listed are theirs.
 * This is the counterpart of {@link MicrosoftTeamsBotClient}, which posts as a
 * registered bot through the Bot Framework connector; that one answers a
 * conversation the bot was addressed in, and this one acts as the person using
 * the product.
 * </p>
 *
 * <p>
 * A chat and a channel hold the same {@code chatMessage} but hang it off
 * different urls, and the differences that follow from that are worth knowing:
 * </p>
 * <ul>
 * <li>A channel message can carry a subject and can have replies underneath it,
 * so a reply is posted to the message rather than to the channel.</li>
 * <li>A chat message has neither. A chat is instead identified by its id, which
 * is why sending to a person means finding or creating the chat first.</li>
 * <li>Deleting a chat message is the one call Graph documents only under
 * {@code /users/{id}/chats}, so the signed in user's id is read for it.</li>
 * </ul>
 *
 * <p>
 * There is no edit here because Graph has none. A {@code PATCH} of a message
 * body answers "PATCH is not supported on specified properties": the only
 * property the update accepts is the data loss prevention
 * {@code policyViolation}. Changing what a message says means deleting it and
 * sending another.
 * </p>
 *
 * <p>
 * A file in a message is a reference rather than a payload: the file lives in a
 * drive and the message points at it. So sending one means uploading it first,
 * with {@code MicrosoftOneDriveUploadFile} for a chat or
 * {@code MicrosoftTeamsUploadFile} for a channel, and passing the url that
 * comes back. Reading one means downloading from that url, which is why the
 * attachment download delegates to {@link MicrosoftOneDriveHelper} rather than
 * fetching bytes of its own.
 * </p>
 */
public class MicrosoftTeamsMessageHelper {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftTeamsMessageHelper.class);

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

	private static final String GRAPH_BASE = MicrosoftTokenFiller.MS_GRAPH_BASE_API + "/v1.0";

	private static final String ID = "id";
	private static final String NAME = "name";
	private static final String USER = "user";
	private static final String BODY = "body";
	private static final String VALUE = "value";
	private static final String TOPIC = "topic";
	private static final String CHATS = "/chats/";
	private static final String CONTENT = "content";
	private static final String MESSAGES = "/messages/";
	private static final String REPLIES = "/replies/";
	private static final String CONTENT_TYPE = "contentType";
	private static final String CONTENT_URL = "contentUrl";
	private static final String DISPLAY_NAME = "displayName";
	private static final String ATTACHMENTS = "attachments";
	private static final String NEXT_LINK = "@odata.nextLink";
	private static final String SOFT_DELETE = "/softDelete";
	private static final String HOSTED_CONTENTS = "/hostedContents/";

	private static final String ONE_ON_ONE = "oneOnOne";
	private static final String GROUP = "group";
	private static final List<String> CHAT_TYPES = Arrays.asList(ONE_ON_ONE, GROUP);

	private static final String MEMBERS = "members";
	private static final String LAST_MESSAGE_PREVIEW = "lastMessagePreview";
	private static final String MEMBERS_AND_PREVIEW = MEMBERS + "," + LAST_MESSAGE_PREVIEW;
	private static final String HAS_UNREAD = "hasUnread";

	/**
	 * How many chats a listing reads through before giving up. Narrowing to the
	 * unread ones walks the list until it finds enough of them, so this is what
	 * stops a user with nothing unread and a long history from paging forever.
	 */
	private static final int MAX_CHATS_SCANNED = 500;

	/**
	 * How many messages one page of a listing asks Graph for. Graph rejects a
	 * larger page than this on both message collections.
	 */
	private static final int PAGE_SIZE = 50;

	/**
	 * How many chats one page asks for. Graph caps a chat listing that expands the
	 * members at 25 however large a page is asked for, so this asks for what it
	 * will actually answer and follows the paging from there.
	 */
	private static final int CHAT_PAGE_SIZE = 25;

	/**
	 * Utility class constructor intentionally hidden.
	 */
	private MicrosoftTeamsMessageHelper() {

	}

	/**
	 * Lists the chats the signed in user is in.
	 *
	 * <p>
	 * Whether a chat holds anything unread is a comparison of two things Graph
	 * keeps apart: the {@code viewpoint} on the chat says when the user last read
	 * it, and the last message says when something last arrived. The read marker
	 * comes back on its own, so asking for unread state is really asking for the
	 * last message of every chat, which is why it is a choice rather than the
	 * default.
	 * </p>
	 *
	 * @param accessToken        Microsoft Graph access token for the user
	 * @param userEmail          optional address of the signed in user, so a chat
	 *                           is named after whoever else is in it
	 * @param includeLastMessage whether the last message of each chat comes back,
	 *                           which is also what decides {@code hasUnread}
	 * @param unreadOnly         whether to keep only the chats holding something
	 *                           the user has not read, which implies reading the
	 *                           last message
	 * @param limit              maximum number of chats to return; values less than
	 *                           or equal to 0 return every chat
	 * @return the chats, most recently active first, as Graph orders them
	 * @throws Exception if the list retrieval fails
	 */
	public static List<Map<String, Object>> listChats(String accessToken, String userEmail, boolean includeLastMessage,
			boolean unreadOnly, int limit) throws Exception {
		try {
			boolean withPreview = includeLastMessage || unreadOnly;
			String nextUrl = chatsUrl(withPreview ? MEMBERS_AND_PREVIEW : MEMBERS);

			List<Map<String, Object>> chats = new ArrayList<>();
			int scanned = 0;
			boolean firstPage = true;
			while (nextUrl != null) {
				Map<String, Object> json;
				try {
					json = readMap(HttpHelperUtility.getRequest(nextUrl, headers(accessToken), null, null, null));
				} catch (Exception e) {
					// expanding both is what the unread comparison needs and is not
					// answered everywhere. The members only shape the caller of the
					// read marker asked for, so the members are what gets dropped
					if (!firstPage || !withPreview) {
						throw e;
					}
					classLogger.warn("Reading the chats with both the members and the last message failed, "
							+ "reading them without the members", e);
					nextUrl = chatsUrl(LAST_MESSAGE_PREVIEW);
					json = readMap(HttpHelperUtility.getRequest(nextUrl, headers(accessToken), null, null, null));
				}
				firstPage = false;
				if (json == null) {
					break;
				}

				for (Map<String, Object> chat : mapList(json.get(VALUE))) {
					scanned++;
					Map<String, Object> described = MicrosoftTeamsMessageMapper.toChat(chat, userEmail);
					if (unreadOnly && !Boolean.TRUE.equals(described.get(HAS_UNREAD))) {
						continue;
					}
					chats.add(described);
					if (limit > 0 && chats.size() >= limit) {
						return chats;
					}
				}
				// narrowing to the unread ones walks the whole list when nothing is
				// unread, so the walk is bounded rather than left to the chat count
				if (scanned >= MAX_CHATS_SCANNED) {
					classLogger.warn("Stopped after reading {} Microsoft Teams chats", scanned);
					break;
				}
				Object next = json.get(NEXT_LINK);
				nextUrl = next == null ? null : next.toString();
			}
			return chats;
		} catch (Exception e) {
			classLogger.error("Failed to list the Microsoft Teams chats of the signed in user.", e);
			throw e;
		}
	}

	/**
	 * The url a chat listing starts at.
	 *
	 * @param expand what to expand alongside each chat
	 * @return the url
	 */
	private static String chatsUrl(String expand) {
		return GRAPH_BASE + "/me/chats?$expand=" + expand + "&$top=" + CHAT_PAGE_SIZE;
	}

	/**
	 * Reads one chat, including who is in it.
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param chatId      id of the chat
	 * @param userEmail   optional address of the signed in user
	 * @return the chat
	 * @throws IllegalArgumentException if required inputs are missing
	 * @throws Exception                if the read fails
	 */
	public static Map<String, Object> getChat(String accessToken, String chatId, String userEmail) throws Exception {
		try {
			requireValue(chatId, "Chat ID is required to read a Microsoft Teams chat.");

			String url = chatPath(chatId) + "?$expand=members";
			Map<String, Object> chat = readMap(
					HttpHelperUtility.getRequest(url, headers(accessToken), null, null, null));
			if (chat == null) {
				throw new IllegalStateException("Microsoft Graph returned no chat for chat id = " + chatId);
			}
			return MicrosoftTeamsMessageMapper.toChat(chat, userEmail);
		} catch (Exception e) {
			classLogger.error("Failed to read Microsoft Teams chat '{}'.", chatId, e);
			throw e;
		}
	}

	/**
	 * Starts a chat with one or more people.
	 *
	 * <p>
	 * The signed in user is always in the chat, whether or not they were named, so
	 * the members passed are the other people. Asking for a one on one chat with
	 * somebody the user already has one with answers that same chat rather than a
	 * second one, which is what makes this safe to call before every send.
	 * </p>
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param chatType    {@code oneOnOne} or {@code group}
	 * @param members     email addresses or user ids of the other people
	 * @param topic       optional title, which Graph accepts for a group chat only
	 * @param userEmail   optional address of the signed in user
	 * @return the chat as Graph created or found it
	 * @throws IllegalArgumentException if required inputs are missing or invalid
	 * @throws Exception                if the create fails
	 */
	public static Map<String, Object> createChat(String accessToken, String chatType, String[] members, String topic,
			String userEmail) throws Exception {
		try {
			if (members == null || members.length == 0) {
				throw new IllegalArgumentException(
						"At least one other person is required to start a Microsoft Teams chat.");
			}
			String type = isBlank(chatType) ? ONE_ON_ONE : oneOf(chatType, CHAT_TYPES, "Chat type");
			if (ONE_ON_ONE.equals(type) && members.length > 1) {
				throw new IllegalArgumentException(
						"A one on one chat holds the signed in user and one other person. Ask for a group chat to include "
								+ members.length + " others.");
			}

			List<Map<String, Object>> conversationMembers = new ArrayList<>();
			conversationMembers.add(conversationMember(signedInUserId(accessToken)));
			for (String member : members) {
				conversationMembers.add(conversationMember(resolveUserId(accessToken, member)));
			}

			Map<String, Object> request = new LinkedHashMap<>();
			request.put("chatType", type);
			if (!isBlank(topic)) {
				if (ONE_ON_ONE.equals(type)) {
					// Graph refuses a topic on a one on one chat, and dropping it is
					// friendlier than failing a send over a label nobody sees
					classLogger.warn("Ignoring the topic '{}' asked for on a one on one Microsoft Teams chat", topic);
				} else {
					request.put(TOPIC, topic.trim());
				}
			}
			request.put("members", conversationMembers);

			String response = HttpHelperUtility.postRequestStringBody(GRAPH_BASE + "/chats", headers(accessToken),
					GSON.toJson(request), ContentType.APPLICATION_JSON, null, null, null);
			Map<String, Object> chat = readMap(response);
			if (chat == null) {
				throw new IllegalStateException("Microsoft Graph returned no chat for the create request.");
			}
			return MicrosoftTeamsMessageMapper.toChat(chat, userEmail);
		} catch (Exception e) {
			classLogger.error("Failed to start a Microsoft Teams chat with {}.", Arrays.toString(members), e);
			throw e;
		}
	}

	/**
	 * Reads the messages of a chat.
	 *
	 * @param accessToken  Microsoft Graph access token for the user
	 * @param chatId       id of the chat to read
	 * @param maxBodyChars the longest body to return before truncating it, or 0 to
	 *                     return whatever length it is
	 * @param limit        maximum number of messages to return; values less than or
	 *                     equal to 0 return every message
	 * @return the messages, most recent first, as Graph orders them
	 * @throws IllegalArgumentException if required inputs are missing
	 * @throws Exception                if the list retrieval fails
	 */
	public static List<Map<String, Object>> listChatMessages(String accessToken, String chatId, int maxBodyChars,
			int limit) throws Exception {
		try {
			requireValue(chatId, "Chat ID is required to read Microsoft Teams chat messages.");

			String url = chatPath(chatId) + "/messages?$top=" + pageSize(limit);
			return describeAll(getPagedValues(accessToken, url, limit), maxBodyChars);
		} catch (Exception e) {
			classLogger.error("Failed to read the messages of Microsoft Teams chat '{}'.", chatId, e);
			throw e;
		}
	}

	/**
	 * Reads one message of a chat.
	 *
	 * @param accessToken  Microsoft Graph access token for the user
	 * @param chatId       id of the chat holding the message
	 * @param messageId    id of the message
	 * @param maxBodyChars the longest body to return before truncating it, or 0 to
	 *                     return whatever length it is
	 * @return the message
	 * @throws IllegalArgumentException if required inputs are missing
	 * @throws Exception                if the read fails
	 */
	public static Map<String, Object> getChatMessage(String accessToken, String chatId, String messageId,
			int maxBodyChars) throws Exception {
		try {
			requireValue(chatId, "Chat ID is required to read a Microsoft Teams chat message.");
			requireValue(messageId, "Message ID is required to read a Microsoft Teams chat message.");

			String url = chatPath(chatId) + MESSAGES + messageId.trim();
			return readMessage(accessToken, url, maxBodyChars, false);
		} catch (Exception e) {
			classLogger.error("Failed to read message '{}' of Microsoft Teams chat '{}'.", messageId, chatId, e);
			throw e;
		}
	}

	/**
	 * Sends a message to a chat.
	 *
	 * @param accessToken     Microsoft Graph access token for the user
	 * @param chatId          id of the chat to post in
	 * @param content         what the message says
	 * @param html            whether the content is html rather than plain text
	 * @param mentions        optional email addresses or user ids to mention, which
	 *                        is what notifies those people
	 * @param attachmentUrls  optional urls of files already in a drive to attach
	 * @param attachmentNames optional names for those files, in the same order
	 * @param maxBodyChars    the longest body to return before truncating it, or 0
	 *                        to return whatever length it is
	 * @return the message as Graph posted it
	 * @throws IllegalArgumentException if required inputs are missing or invalid
	 * @throws Exception                if the send fails
	 */
	public static Map<String, Object> sendChatMessage(String accessToken, String chatId, String content, boolean html,
			String[] mentions, String[] attachmentUrls, String[] attachmentNames, int maxBodyChars) throws Exception {
		try {
			requireValue(chatId, "Chat ID is required to send a Microsoft Teams chat message.");

			Map<String, Object> request = buildMessage(accessToken, null, content, html, mentions, attachmentUrls,
					attachmentNames);
			String url = chatPath(chatId) + "/messages";
			return postMessage(accessToken, url, request, maxBodyChars);
		} catch (Exception e) {
			classLogger.error("Failed to send a message to Microsoft Teams chat '{}'.", chatId, e);
			throw e;
		}
	}

	/**
	 * Deletes a message of a chat.
	 *
	 * <p>
	 * This is the soft delete the Teams client performs when somebody deletes their
	 * own message: it disappears from the conversation for everybody, and only the
	 * person who sent it can delete it at all.
	 * </p>
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param chatId      id of the chat holding the message
	 * @param messageId   id of the message to delete
	 * @throws IllegalArgumentException if required inputs are missing
	 * @throws Exception                if the delete fails
	 */
	public static void deleteChatMessage(String accessToken, String chatId, String messageId) throws Exception {
		try {
			requireValue(chatId, "Chat ID is required to delete a Microsoft Teams chat message.");
			requireValue(messageId, "Message ID is required to delete a Microsoft Teams chat message.");

			// Graph documents this one only under a named user rather than under /me,
			// so the signed in user's id is read rather than assumed to be aliasable
			String url = GRAPH_BASE + "/users/" + signedInUserId(accessToken) + CHATS + chatId.trim() + MESSAGES
					+ messageId.trim() + SOFT_DELETE;
			// answers 204 with no body
			HttpHelperUtility.postRequestStringBody(url, headers(accessToken), "", ContentType.APPLICATION_JSON, null,
					null, null);
		} catch (Exception e) {
			classLogger.error("Failed to delete message '{}' of Microsoft Teams chat '{}'.", messageId, chatId, e);
			throw e;
		}
	}

	/**
	 * Reads the messages posted in a channel.
	 *
	 * @param accessToken    Microsoft Graph access token for the user
	 * @param teamId         id of the team that owns the channel
	 * @param channelId      id of the channel to read
	 * @param includeReplies whether the replies underneath each message come back
	 *                       as well
	 * @param maxBodyChars   the longest body to return before truncating it, or 0
	 *                       to return whatever length it is
	 * @param limit          maximum number of messages to return; values less than
	 *                       or equal to 0 return every message
	 * @return the messages, most recent first, as Graph orders them
	 * @throws IllegalArgumentException if required inputs are missing
	 * @throws Exception                if the list retrieval fails
	 */
	public static List<Map<String, Object>> listChannelMessages(String accessToken, String teamId, String channelId,
			boolean includeReplies, int maxBodyChars, int limit) throws Exception {
		try {
			requireValue(teamId, "Team ID is required to read Microsoft Teams channel messages.");
			requireValue(channelId, "Channel ID is required to read Microsoft Teams channel messages.");

			String url = channelPath(teamId, channelId) + "/messages?$top=" + pageSize(limit)
					+ (includeReplies ? "&$expand=replies" : "");
			List<Map<String, Object>> messages = new ArrayList<>();
			for (Map<String, Object> message : getPagedValues(accessToken, url, limit)) {
				messages.add(MicrosoftTeamsMessageMapper.toMessage(message, maxBodyChars, includeReplies));
			}
			return messages;
		} catch (Exception e) {
			classLogger.error("Failed to read the messages of channel '{}' in Microsoft Team '{}'.", channelId, teamId,
					e);
			throw e;
		}
	}

	/**
	 * Reads one message posted in a channel, or one reply to it.
	 *
	 * @param accessToken    Microsoft Graph access token for the user
	 * @param teamId         id of the team that owns the channel
	 * @param channelId      id of the channel holding the message
	 * @param messageId      id of the message
	 * @param replyId        optional id of a reply to that message, which is what
	 *                       is read when it is given
	 * @param includeReplies whether the replies underneath the message come back as
	 *                       well, which does not apply when a reply was named
	 * @param maxBodyChars   the longest body to return before truncating it, or 0
	 *                       to return whatever length it is
	 * @return the message
	 * @throws IllegalArgumentException if required inputs are missing
	 * @throws Exception                if the read fails
	 */
	public static Map<String, Object> getChannelMessage(String accessToken, String teamId, String channelId,
			String messageId, String replyId, boolean includeReplies, int maxBodyChars) throws Exception {
		try {
			requireValue(teamId, "Team ID is required to read a Microsoft Teams channel message.");
			requireValue(channelId, "Channel ID is required to read a Microsoft Teams channel message.");
			requireValue(messageId, "Message ID is required to read a Microsoft Teams channel message.");

			boolean withReplies = includeReplies && isBlank(replyId);
			String url = messagePath(teamId, channelId, messageId, replyId) + (withReplies ? "?$expand=replies" : "");
			return readMessage(accessToken, url, maxBodyChars, withReplies);
		} catch (Exception e) {
			classLogger.error("Failed to read message '{}' of channel '{}' in Microsoft Team '{}'.", messageId,
					channelId, teamId, e);
			throw e;
		}
	}

	/**
	 * Posts a message in a channel, or a reply underneath one.
	 *
	 * @param accessToken     Microsoft Graph access token for the user
	 * @param teamId          id of the team that owns the channel
	 * @param channelId       id of the channel to post in
	 * @param replyToId       optional id of the message to reply to; a new
	 *                        conversation is started in the channel when blank
	 * @param subject         optional subject line, which Graph keeps for a new
	 *                        conversation and ignores on a reply
	 * @param content         what the message says
	 * @param html            whether the content is html rather than plain text
	 * @param mentions        optional email addresses or user ids to mention
	 * @param attachmentUrls  optional urls of files already in a drive to attach
	 * @param attachmentNames optional names for those files, in the same order
	 * @param maxBodyChars    the longest body to return before truncating it, or 0
	 *                        to return whatever length it is
	 * @return the message as Graph posted it
	 * @throws IllegalArgumentException if required inputs are missing or invalid
	 * @throws Exception                if the send fails
	 */
	public static Map<String, Object> sendChannelMessage(String accessToken, String teamId, String channelId,
			String replyToId, String subject, String content, boolean html, String[] mentions, String[] attachmentUrls,
			String[] attachmentNames, int maxBodyChars) throws Exception {
		try {
			requireValue(teamId, "Team ID is required to send a Microsoft Teams channel message.");
			requireValue(channelId, "Channel ID is required to send a Microsoft Teams channel message.");

			Map<String, Object> request = buildMessage(accessToken, subject, content, html, mentions, attachmentUrls,
					attachmentNames);
			String url = isBlank(replyToId) ? channelPath(teamId, channelId) + "/messages"
					: channelPath(teamId, channelId) + MESSAGES + replyToId.trim() + "/replies";
			return postMessage(accessToken, url, request, maxBodyChars);
		} catch (Exception e) {
			classLogger.error("Failed to send a message to channel '{}' in Microsoft Team '{}'.", channelId, teamId, e);
			throw e;
		}
	}

	/**
	 * Deletes a message posted in a channel, or a reply to one.
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param teamId      id of the team that owns the channel
	 * @param channelId   id of the channel holding the message
	 * @param messageId   id of the message
	 * @param replyId     optional id of a reply to that message, which is what is
	 *                    deleted when it is given
	 * @throws IllegalArgumentException if required inputs are missing
	 * @throws Exception                if the delete fails
	 */
	public static void deleteChannelMessage(String accessToken, String teamId, String channelId, String messageId,
			String replyId) throws Exception {
		try {
			requireValue(teamId, "Team ID is required to delete a Microsoft Teams channel message.");
			requireValue(channelId, "Channel ID is required to delete a Microsoft Teams channel message.");
			requireValue(messageId, "Message ID is required to delete a Microsoft Teams channel message.");

			String url = messagePath(teamId, channelId, messageId, replyId) + SOFT_DELETE;
			// answers 204 with no body
			HttpHelperUtility.postRequestStringBody(url, headers(accessToken), "", ContentType.APPLICATION_JSON, null,
					null, null);
		} catch (Exception e) {
			classLogger.error("Failed to delete message '{}' of channel '{}' in Microsoft Team '{}'.", messageId,
					channelId, teamId, e);
			throw e;
		}
	}

	/**
	 * Downloads something attached to a message.
	 *
	 * <p>
	 * Two kinds of thing can be attached, and both are handled here because a
	 * caller looking at a message cannot always tell which it is looking at. A file
	 * is an attachment pointing at a drive, and is fetched from that drive. A
	 * pasted image is hosted content, and is fetched from Graph's own endpoint for
	 * it. An attachment that is neither is a card, which is json rather than a
	 * file, and saying so is more use than writing it out.
	 * </p>
	 *
	 * @param accessToken  Microsoft Graph access token for the user
	 * @param chatId       id of the chat holding the message, or blank for a
	 *                     channel message
	 * @param teamId       id of the team, for a channel message
	 * @param channelId    id of the channel, for a channel message
	 * @param messageId    id of the message
	 * @param replyId      optional id of the reply holding the attachment
	 * @param attachmentId optional id of the attachment or of the hosted content;
	 *                     the only attachment is taken when the message has one and
	 *                     this is blank
	 * @param destination  local directory the file is written into
	 * @param fileName     optional local file name to write as; the name the
	 *                     attachment carries is used when blank
	 * @return map carrying the attachment {@code id}, its {@code name}, the local
	 *         {@code filePath} written, the {@code size} in bytes and
	 *         {@code success}
	 * @throws IllegalArgumentException if required inputs are missing, or the
	 *                                  attachment is not something that can be
	 *                                  downloaded
	 * @throws Exception                if the download or the file write fails
	 */
	public static Map<String, Object> downloadAttachment(String accessToken, String chatId, String teamId,
			String channelId, String messageId, String replyId, String attachmentId, String destination,
			String fileName) throws Exception {
		try {
			requireValue(messageId, "Message ID is required to download a Microsoft Teams attachment.");
			requireValue(destination, "A destination path is required to download a Microsoft Teams attachment.");

			String messageUrl = isBlank(chatId) ? messagePath(teamId, channelId, messageId, replyId)
					: chatPath(chatId) + MESSAGES + messageId.trim();
			Map<String, Object> message = readMap(
					HttpHelperUtility.getRequest(messageUrl, headers(accessToken), null, null, null));
			if (message == null) {
				throw new IllegalStateException("Microsoft Graph returned no message for message id = " + messageId);
			}

			Map<String, Object> attachment = findAttachment(message, attachmentId);
			if (attachment != null) {
				if (!MicrosoftTeamsMessageMapper.isFileAttachment(attachment)) {
					throw new IllegalArgumentException("The attachment '" + attachment.get(NAME)
							+ "' is a card rather than a file, so there is nothing to download. Its content is on the message itself.");
				}
				String url = attachment.get(CONTENT_URL).toString();
				String name = attachment.get(NAME) == null ? fileNameOf(url) : attachment.get(NAME).toString();
				// the file lives in a drive, so the drive connector fetches it: the
				// content url is exactly what its sharing link path takes
				Map<String, Object> downloaded = MicrosoftOneDriveHelper.downloadFile(accessToken, null, null, null,
						url, destination, isBlank(fileName) ? name : fileName);
				Map<String, Object> result = new LinkedHashMap<>(downloaded);
				result.put("attachmentId", attachment.get(ID));
				return result;
			}

			return downloadHostedContent(accessToken, messageUrl, attachmentId, destination, fileName, message);
		} catch (Exception e) {
			classLogger.error("Failed to download attachment '{}' of Microsoft Teams message '{}'.", attachmentId,
					messageId, e);
			throw e;
		}
	}

	/**
	 * Fetches the bytes of something pasted into a message, such as an image.
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param messageUrl  the url of the message that holds it
	 * @param contentId   optional id of the hosted content; the only one is taken
	 *                    when the message has one and this is blank
	 * @param destination local directory the file is written into
	 * @param fileName    optional local file name to write as
	 * @param message     the message, already read, so what it holds can be
	 *                    described when nothing matches
	 * @return what was written
	 * @throws IllegalArgumentException if there is no such hosted content
	 * @throws Exception                if the download or the file write fails
	 */
	private static Map<String, Object> downloadHostedContent(String accessToken, String messageUrl, String contentId,
			String destination, String fileName, Map<String, Object> message) throws Exception {
		String hostedContentId = contentId;
		if (isBlank(hostedContentId)) {
			List<Map<String, Object>> hostedContents = mapList(message.get("hostedContents"));
			if (hostedContents.size() == 1 && hostedContents.get(0).get(ID) != null) {
				hostedContentId = hostedContents.get(0).get(ID).toString();
			}
		}
		if (isBlank(hostedContentId)) {
			throw new IllegalArgumentException(
					"The message holds no attachment of that name or id. Read the message to see what it has on it.");
		}

		String url = messageUrl + HOSTED_CONTENTS + hostedContentId.trim() + "/$value";
		byte[] bytes = HttpHelperUtility.getRequestBytes(url, headers(accessToken), null, null, null);
		if (bytes == null || bytes.length == 0) {
			throw new IllegalStateException("Downloaded content is empty for hosted content id = " + hostedContentId);
		}

		// hosted content is unnamed, so it is written under the name the caller
		// asked for and under the id it has otherwise
		String name = isBlank(fileName) ? "hostedContent-" + Math.abs(hostedContentId.trim().hashCode()) : fileName;
		File file = write(bytes, destination, name);

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("attachmentId", hostedContentId);
		result.put(NAME, name);
		result.put("filePath", Path.of(destination).relativize(file.toPath()).toString());
		result.put("size", bytes.length);
		result.put("success", true);
		return result;
	}

	/**
	 * Finds the attachment a caller asked for, by id, by name, or by it being the
	 * only one there.
	 *
	 * @param message      the message as Graph returned it
	 * @param attachmentId the id or name asked for, or null
	 * @return the attachment, or null when the message has no such thing
	 */
	private static Map<String, Object> findAttachment(Map<String, Object> message, String attachmentId) {
		List<Map<String, Object>> attachments = mapList(message.get(ATTACHMENTS));
		if (attachments.isEmpty()) {
			return null;
		}
		if (isBlank(attachmentId)) {
			return attachments.size() == 1 ? attachments.get(0) : null;
		}
		String wanted = attachmentId.trim();
		for (Map<String, Object> attachment : attachments) {
			if (wanted.equalsIgnoreCase(String.valueOf(attachment.get(ID)))) {
				return attachment;
			}
		}
		// a name is what somebody reading the message has in front of them, so it is
		// accepted in place of the id it was listed with
		for (Map<String, Object> attachment : attachments) {
			if (wanted.equalsIgnoreCase(String.valueOf(attachment.get(NAME)))) {
				return attachment;
			}
		}
		return null;
	}

	/**
	 * Builds a message in the shape Graph reads.
	 *
	 * <p>
	 * Mentions and attachments are both markup in the body as well as entries
	 * beside it: the {@code at} and {@code attachment} tags are what make them
	 * appear in the conversation, and a body without them posts a message that
	 * notifies nobody and shows no file. So the content becomes html whenever
	 * either is asked for, and plain text passed alongside them is escaped rather
	 * than left to be read as markup.
	 * </p>
	 */
	private static Map<String, Object> buildMessage(String accessToken, String subject, String content, boolean html,
			String[] mentions, String[] attachmentUrls, String[] attachmentNames) throws Exception {
		boolean hasMentions = mentions != null && mentions.length > 0;
		boolean hasAttachments = attachmentUrls != null && attachmentUrls.length > 0;
		if (isBlank(content) && !hasAttachments) {
			throw new IllegalArgumentException("A message needs something to say or something attached to it.");
		}

		boolean asHtml = html || hasMentions || hasAttachments;
		StringBuilder body = new StringBuilder();
		Map<String, Object> request = new LinkedHashMap<>();

		if (hasMentions) {
			List<Map<String, Object>> mentioned = new ArrayList<>();
			for (int i = 0; i < mentions.length; i++) {
				Map<String, Object> user = resolveUser(accessToken, mentions[i]);
				String name = String.valueOf(user.get(DISPLAY_NAME));
				body.append("<at id=\"").append(i).append("\">").append(escapeHtml(name)).append("</at> ");

				Map<String, Object> identity = new LinkedHashMap<>();
				identity.put(ID, user.get(ID));
				identity.put(DISPLAY_NAME, name);
				identity.put("userIdentityType", "aadUser");
				Map<String, Object> mention = new LinkedHashMap<>();
				mention.put(ID, i);
				mention.put("mentionText", name);
				mention.put("mentioned", Map.of(USER, identity));
				mentioned.add(mention);
			}
			request.put("mentions", mentioned);
		}

		if (content != null) {
			body.append(html ? content : escapeHtml(content));
		}

		if (hasAttachments) {
			List<Map<String, Object>> attached = new ArrayList<>();
			for (int i = 0; i < attachmentUrls.length; i++) {
				String url = attachmentUrls[i];
				if (isBlank(url)) {
					continue;
				}
				// Graph ties the entry to the body by this id, and makes up neither
				// side of that pairing itself
				String attachmentId = UUID.randomUUID().toString();
				String name = attachmentNames != null && attachmentNames.length > i && !isBlank(attachmentNames[i])
						? attachmentNames[i].trim()
						: fileNameOf(url);

				Map<String, Object> attachment = new LinkedHashMap<>();
				attachment.put(ID, attachmentId);
				attachment.put(CONTENT_TYPE, "reference");
				attachment.put(CONTENT_URL, url.trim());
				attachment.put(NAME, name);
				attached.add(attachment);

				body.append("<attachment id=\"").append(attachmentId).append("\"></attachment>");
			}
			if (!attached.isEmpty()) {
				request.put(ATTACHMENTS, attached);
			}
		}

		if (!isBlank(subject)) {
			request.put("subject", subject.trim());
		}
		Map<String, Object> messageBody = new LinkedHashMap<>();
		messageBody.put(CONTENT_TYPE, asHtml ? "html" : "text");
		messageBody.put(CONTENT, body.toString());
		request.put(BODY, messageBody);
		return request;
	}

	/**
	 * Posts a message and describes what came back.
	 */
	private static Map<String, Object> postMessage(String accessToken, String url, Map<String, Object> request,
			int maxBodyChars) throws Exception {
		String response = HttpHelperUtility.postRequestStringBody(url, headers(accessToken), GSON.toJson(request),
				ContentType.APPLICATION_JSON, null, null, null);
		Map<String, Object> sent = readMap(response);
		if (sent == null) {
			throw new IllegalStateException("Microsoft Graph returned no message for the send request.");
		}
		return MicrosoftTeamsMessageMapper.toMessage(sent, maxBodyChars, false);
	}

	/**
	 * Reads one message and describes it.
	 */
	private static Map<String, Object> readMessage(String accessToken, String url, int maxBodyChars,
			boolean includeReplies) throws Exception {
		Map<String, Object> message = readMap(
				HttpHelperUtility.getRequest(url, headers(accessToken), null, null, null));
		if (message == null) {
			throw new IllegalStateException("Microsoft Graph returned no message for url = " + url);
		}
		return MicrosoftTeamsMessageMapper.toMessage(message, maxBodyChars, includeReplies);
	}

	/**
	 * Describes a collection of messages that carry no replies.
	 */
	private static List<Map<String, Object>> describeAll(List<Map<String, Object>> messages, int maxBodyChars) {
		List<Map<String, Object>> described = new ArrayList<>();
		for (Map<String, Object> message : messages) {
			described.add(MicrosoftTeamsMessageMapper.toMessage(message, maxBodyChars, false));
		}
		return described;
	}

	/**
	 * Finds somebody in the directory by their email address, their user principal
	 * name or their id.
	 *
	 * <p>
	 * Both forms are tried because the address people use is not always the one
	 * they sign in with: a mailbox alias is not a principal name, and Graph
	 * addresses a user by id or principal name only, so an alias has to be looked
	 * up by filter instead.
	 * </p>
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param address     an email address, a user principal name or a user id
	 * @return the user, carrying at least an {@code id} and a {@code displayName}
	 * @throws IllegalArgumentException if nobody matches
	 * @throws Exception                if the lookup fails
	 */
	public static Map<String, Object> resolveUser(String accessToken, String address) throws Exception {
		requireValue(address, "An email address or user id is required to name somebody in Microsoft Teams.");
		final String FIELDS = "$select=id,displayName,mail,userPrincipalName";
		String wanted = address.trim();

		try {
			String url = GRAPH_BASE + "/users/" + encode(wanted) + "?" + FIELDS;
			Map<String, Object> user = readMap(
					HttpHelperUtility.getRequest(url, headers(accessToken), null, null, null));
			if (user != null && user.get(ID) != null) {
				return user;
			}
		} catch (Exception e) {
			classLogger.debug("'{}' is not a user id or principal name, looking it up as a mail address", wanted, e);
		}

		String url = GRAPH_BASE + "/users?$filter=" + encode("mail eq '" + wanted.replace("'", "''") + "'") + "&"
				+ FIELDS + "&$top=1";
		for (Map<String, Object> user : mapList(
				readValue(HttpHelperUtility.getRequest(url, headers(accessToken), null, null, null)))) {
			if (user.get(ID) != null) {
				return user;
			}
		}
		throw new IllegalArgumentException("Nobody in the directory matches: " + address);
	}

	/**
	 * @param accessToken Microsoft Graph access token for the user
	 * @param address     an email address, a user principal name or a user id
	 * @return that person's id
	 * @throws Exception if the lookup fails
	 */
	public static String resolveUserId(String accessToken, String address) throws Exception {
		return String.valueOf(resolveUser(accessToken, address).get(ID));
	}

	/**
	 * @param accessToken Microsoft Graph access token for the user
	 * @return the id of the signed in user, which a handful of Graph urls name
	 *         rather than accepting {@code /me}
	 * @throws Exception if the read fails
	 */
	public static String signedInUserId(String accessToken) throws Exception {
		Map<String, Object> me = readMap(
				HttpHelperUtility.getRequest(GRAPH_BASE + "/me?$select=id", headers(accessToken), null, null, null));
		if (me == null || me.get(ID) == null) {
			throw new IllegalStateException("Microsoft Graph returned no id for the signed in user.");
		}
		return me.get(ID).toString();
	}

	/**
	 * One member of a chat being created, in the shape Graph reads.
	 */
	private static Map<String, Object> conversationMember(String userId) {
		Map<String, Object> member = new LinkedHashMap<>();
		member.put("@odata.type", "#microsoft.graph.aadUserConversationMember");
		member.put("roles", List.of("owner"));
		// Graph binds the member to a directory user by url rather than by a plain id
		member.put("user@odata.bind", GRAPH_BASE + "/users('" + userId + "')");
		return member;
	}

	/**
	 * Writes downloaded bytes into a directory.
	 */
	private static File write(byte[] bytes, String destination, String fileName) throws Exception {
		File file = new File(Paths.get(destination, fileName).toString());
		File parent = file.getParentFile();
		if (parent != null && !parent.exists() && !parent.mkdirs()) {
			throw new IllegalStateException("Unable to create destination directory at: " + parent.getAbsolutePath());
		}
		try (FileOutputStream fos = new FileOutputStream(file)) {
			fos.write(bytes);
			fos.flush();
		}
		return file;
	}

	/**
	 * The part of a Graph url that says which chat this is.
	 */
	private static String chatPath(String chatId) {
		return GRAPH_BASE + CHATS + chatId.trim();
	}

	/**
	 * The part of a Graph url that says which channel this is.
	 */
	private static String channelPath(String teamId, String channelId) {
		return GRAPH_BASE + "/teams/" + teamId.trim() + "/channels/" + channelId.trim();
	}

	/**
	 * The url of one channel message, or of one reply to it.
	 */
	private static String messagePath(String teamId, String channelId, String messageId, String replyId) {
		requireValue(teamId, "Team ID is required to address a Microsoft Teams channel message.");
		requireValue(channelId, "Channel ID is required to address a Microsoft Teams channel message.");
		String message = channelPath(teamId, channelId) + MESSAGES + messageId.trim();
		return isBlank(replyId) ? message : message + REPLIES + replyId.trim();
	}

	/**
	 * Builds the headers a Teams messaging call carries.
	 */
	private static Map<String, String> headers(String accessToken) {
		return MicrosoftLoginUtils.getBearerHeader(accessToken);
	}

	/**
	 * Runs a Graph collection request, following {@code @odata.nextLink} until the
	 * limit is met or the collection runs out.
	 */
	private static List<Map<String, Object>> getPagedValues(String accessToken, String url, int limit)
			throws Exception {
		List<Map<String, Object>> collected = new ArrayList<>();
		String nextUrl = url;
		while (nextUrl != null) {
			Map<String, Object> json = readMap(
					HttpHelperUtility.getRequest(nextUrl, headers(accessToken), null, null, null));
			if (json == null) {
				break;
			}
			for (Map<String, Object> item : mapList(json.get(VALUE))) {
				collected.add(item);
				if (limit > 0 && collected.size() >= limit) {
					return collected;
				}
			}
			Object next = json.get(NEXT_LINK);
			nextUrl = next == null ? null : next.toString();
		}
		return collected;
	}

	/**
	 * How large a page to ask Graph for, which is the limit itself when the caller
	 * wants fewer messages than a page holds.
	 */
	private static int pageSize(int limit) {
		return limit > 0 && limit < PAGE_SIZE ? limit : PAGE_SIZE;
	}

	/**
	 * The name a url ends in, which is what an attached file is called when nothing
	 * else says.
	 */
	private static String fileNameOf(String url) {
		String path = url.trim();
		int query = path.indexOf('?');
		if (query >= 0) {
			path = path.substring(0, query);
		}
		int lastSlash = path.lastIndexOf('/');
		if (lastSlash >= 0) {
			path = path.substring(lastSlash + 1);
		}
		String name = URLDecoder.decode(path, StandardCharsets.UTF_8).trim();
		return name.isEmpty() ? "attachment" : name;
	}

	/**
	 * Escapes text that is going into an html body, so something somebody typed
	 * cannot be read as markup.
	 */
	private static String escapeHtml(String text) {
		return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
	}

	/**
	 * Reads the {@code value} collection out of a Graph response.
	 */
	private static Object readValue(String response) {
		Map<String, Object> json = readMap(response);
		return json == null ? null : json.get(VALUE);
	}

	/**
	 * Reads a json array of objects, skipping anything in it that is not one.
	 */
	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> mapList(Object value) {
		List<Map<String, Object>> maps = new ArrayList<>();
		if (!(value instanceof List)) {
			return maps;
		}
		for (Object entry : (List<?>) value) {
			if (entry instanceof Map) {
				maps.add((Map<String, Object>) entry);
			}
		}
		return maps;
	}

	/**
	 * @param response the response body
	 * @return the response as a map, or null when there is nothing to read
	 */
	private static Map<String, Object> readMap(String response) {
		if (isBlank(response)) {
			return null;
		}
		return GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
		}.getType());
	}

	/**
	 * Validates a value against a fixed set of words, matched however the caller
	 * happened to capitalize it.
	 */
	private static String oneOf(String value, List<String> accepted, String what) {
		for (String candidate : accepted) {
			if (candidate.equalsIgnoreCase(value.trim())) {
				return candidate;
			}
		}
		throw new IllegalArgumentException(what + " must be one of " + accepted + " but received: " + value);
	}

	/**
	 * URL encodes a value going into the path or the query.
	 */
	private static String encode(String value) {
		return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
	}

	/**
	 * Guards against missing required string inputs.
	 */
	private static void requireValue(String value, String message) {
		if (isBlank(value)) {
			throw new IllegalArgumentException(message);
		}
	}

	private static boolean isBlank(String value) {
		return value == null || value.trim().isEmpty();
	}

}

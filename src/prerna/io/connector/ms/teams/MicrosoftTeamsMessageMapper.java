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

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;

/**
 * Turns the json Graph returns for Teams chats and messages into the maps the
 * message reactors answer with.
 *
 * <p>
 * Kept apart from {@link MicrosoftTeamsMessageHelper} for the same reason the
 * mail and calendar mappers are kept apart from their helpers: what a message
 * looks like on the way out is a decision about this codebase's shape, not
 * about how Graph is called, and a reactor that reads a message back after
 * sending one should see it described the same way a listing describes it.
 * </p>
 *
 * <p>
 * Two things about a Teams message are worth knowing before reading what comes
 * back. The sender is an identity rather than an address: Graph gives a display
 * name and a user id and no email, so that is what {@code fromName} and
 * {@code fromId} carry. And the body is html for anything written in the Teams
 * client, so it is reduced to its text the same way a mail body is, since the
 * markup is noise to whoever asked what was said.
 * </p>
 */
public class MicrosoftTeamsMessageMapper {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftTeamsMessageMapper.class);

	private static final String ID = "id";
	private static final String NAME = "name";
	private static final String USER = "user";
	private static final String BODY = "body";
	private static final String TOPIC = "topic";
	private static final String EMAIL = "email";
	private static final String CONTENT = "content";
	private static final String WEB_URL = "webUrl";
	private static final String REPLIES = "replies";
	private static final String DISPLAY_NAME = "displayName";
	private static final String CONTENT_TYPE = "contentType";
	private static final String CONTENT_URL = "contentUrl";
	private static final String ATTACHMENTS = "attachments";
	private static final String HTML = "html";
	private static final String VIEWPOINT = "viewpoint";
	private static final String LAST_MESSAGE_READ = "lastMessageReadDateTime";
	private static final String LAST_MESSAGE_PREVIEW = "lastMessagePreview";

	/**
	 * How much of the last message a chat listing carries. It is there to say what
	 * the chat is about rather than to be read in full, and the whole message is a
	 * request away.
	 */
	private static final int PREVIEW_BODY_CHARS = 500;

	private MicrosoftTeamsMessageMapper() {

	}

	/**
	 * Describe one message, whether it was posted in a chat or in a channel.
	 *
	 * @param message        the message as Graph returned it
	 * @param maxBodyChars   the longest body to return before truncating it, or 0
	 *                       to return whatever length it is
	 * @param includeReplies whether the replies underneath it are described as
	 *                       well, which only applies to a channel message that was
	 *                       read with them
	 * @return the message as a map
	 */
	public static Map<String, Object> toMessage(Map<String, Object> message, int maxBodyChars, boolean includeReplies) {
		Map<String, Object> output = new LinkedHashMap<>();
		output.put(ID, message.get(ID));
		putIfPresent(output, "replyToId", message.get("replyToId"));
		putIfPresent(output, "messageType", message.get("messageType"));
		putIfPresent(output, "subject", message.get("subject"));
		putIfPresent(output, "importance", message.get("importance"));
		putIfPresent(output, "createdDateTime", message.get("createdDateTime"));
		putIfPresent(output, "lastModifiedDateTime", message.get("lastModifiedDateTime"));
		putIfPresent(output, "lastEditedDateTime", message.get("lastEditedDateTime"));
		putIfPresent(output, "deletedDateTime", message.get("deletedDateTime"));
		output.put("isDeleted", message.get("deletedDateTime") != null);
		putIfPresent(output, "fromName", senderName(message.get("from")));
		putIfPresent(output, "fromId", senderId(message.get("from")));
		putIfPresent(output, "fromType", senderType(message.get("from")));
		putIfPresent(output, WEB_URL, message.get(WEB_URL));

		// where a channel message came from, which is what lets a listing of one
		// team's messages be traced back to the channel that holds each of them
		Object channelIdentity = message.get("channelIdentity");
		if (channelIdentity instanceof Map) {
			Map<?, ?> identity = (Map<?, ?>) channelIdentity;
			putIfPresent(output, "teamId", identity.get("teamId"));
			putIfPresent(output, "channelId", identity.get("channelId"));
		}

		String body = bodyOf(message);
		if (maxBodyChars > 0 && body.length() > maxBodyChars) {
			body = body.substring(0, maxBodyChars) + " ... [truncated]";
			output.put("bodyTruncated", true);
		}
		output.put(BODY, body);

		List<Map<String, Object>> attachments = attachments(message.get(ATTACHMENTS));
		if (!attachments.isEmpty()) {
			output.put(ATTACHMENTS, attachments);
		}
		List<Map<String, Object>> mentions = mentions(message.get("mentions"));
		if (!mentions.isEmpty()) {
			output.put("mentions", mentions);
		}
		List<Map<String, Object>> reactions = reactions(message.get("reactions"));
		if (!reactions.isEmpty()) {
			output.put("reactions", reactions);
		}
		List<Map<String, Object>> hostedContents = hostedContents(message.get("hostedContents"));
		if (!hostedContents.isEmpty()) {
			output.put("hostedContents", hostedContents);
		}

		if (includeReplies) {
			List<Map<String, Object>> replies = new ArrayList<>();
			for (Map<String, Object> reply : mapList(message.get(REPLIES))) {
				replies.add(toMessage(reply, maxBodyChars, false));
			}
			output.put("replyCount", replies.size());
			output.put(REPLIES, replies);
		}
		return output;
	}

	/**
	 * Describe one chat.
	 *
	 * <p>
	 * A one on one chat has no topic, so it has nothing to call itself. The
	 * {@code displayName} is therefore the topic where there is one and the names
	 * of everybody else in the chat where there is not, which is how the Teams
	 * client labels it too.
	 * </p>
	 *
	 * <p>
	 * Read state lives on the chat rather than on its messages. The signed in
	 * user's {@code viewpoint} says when they last read the chat, and comparing
	 * that with when the last message arrived is what {@code hasUnread} answers.
	 * That comparison needs the last message, so {@code hasUnread} is only set when
	 * the chat was read with it.
	 * </p>
	 *
	 * @param chat      the chat as Graph returned it
	 * @param userEmail optional address of the signed in user, so they are left out
	 *                  of the name the chat is given
	 * @return the chat as a map
	 */
	public static Map<String, Object> toChat(Map<String, Object> chat, String userEmail) {
		Map<String, Object> output = new LinkedHashMap<>();
		output.put(ID, chat.get(ID));
		putIfPresent(output, "chatType", chat.get("chatType"));
		putIfPresent(output, TOPIC, chat.get(TOPIC));
		putIfPresent(output, "createdDateTime", chat.get("createdDateTime"));
		putIfPresent(output, "lastUpdatedDateTime", chat.get("lastUpdatedDateTime"));
		putIfPresent(output, WEB_URL, chat.get(WEB_URL));

		String lastReadDateTime = null;
		Object viewpoint = chat.get(VIEWPOINT);
		if (viewpoint instanceof Map) {
			Map<?, ?> viewpointMap = (Map<?, ?>) viewpoint;
			lastReadDateTime = asString(viewpointMap.get(LAST_MESSAGE_READ));
			putIfPresent(output, LAST_MESSAGE_READ, lastReadDateTime);
			output.put("isHidden", Boolean.TRUE.equals(viewpointMap.get("isHidden")));
		}

		Object preview = chat.get(LAST_MESSAGE_PREVIEW);
		if (preview instanceof Map) {
			Map<String, Object> lastMessage = toMessage(asStringKeyedMap(preview), PREVIEW_BODY_CHARS, false);
			output.put("lastMessage", lastMessage);
			output.put("hasUnread", isUnread(asString(lastMessage.get("createdDateTime")), lastReadDateTime));
		}

		List<Map<String, Object>> members = new ArrayList<>();
		List<String> otherNames = new ArrayList<>();
		for (Map<String, Object> member : mapList(chat.get("members"))) {
			Map<String, Object> described = toMember(member);
			members.add(described);
			Object address = described.get(EMAIL);
			boolean isSignedInUser = userEmail != null && address != null
					&& userEmail.trim().equalsIgnoreCase(address.toString());
			Object name = described.get(NAME);
			if (!isSignedInUser && name != null) {
				otherNames.add(name.toString());
			}
		}
		if (!members.isEmpty()) {
			output.put("members", members);
		}

		Object topic = chat.get(TOPIC);
		if (topic != null && !topic.toString().trim().isEmpty()) {
			output.put(DISPLAY_NAME, topic.toString());
		} else if (!otherNames.isEmpty()) {
			output.put(DISPLAY_NAME, String.join(", ", otherNames));
		}
		return output;
	}

	/**
	 * Describe one member of a chat.
	 *
	 * @param member the conversation member as Graph returned it
	 * @return the member as a map
	 */
	public static Map<String, Object> toMember(Map<String, Object> member) {
		Map<String, Object> output = new LinkedHashMap<>();
		putIfPresent(output, "userId", member.get("userId"));
		putIfPresent(output, NAME, member.get(DISPLAY_NAME));
		putIfPresent(output, EMAIL, member.get(EMAIL));
		putIfPresent(output, "roles", member.get("roles"));
		return output;
	}

	/**
	 * The readable text of a message, with the markup taken out of an html body the
	 * same way the mail mapper does it.
	 *
	 * @param message the message as Graph returned it
	 * @return the body text, empty when there is none
	 */
	public static String bodyOf(Map<String, Object> message) {
		Object body = message.get(BODY);
		if (!(body instanceof Map)) {
			return "";
		}
		Map<?, ?> bodyMap = (Map<?, ?>) body;
		String content = bodyMap.get(CONTENT) == null ? "" : bodyMap.get(CONTENT).toString();
		if (HTML.equalsIgnoreCase(String.valueOf(bodyMap.get(CONTENT_TYPE)))) {
			return Jsoup.parse(content).text().trim();
		}
		return content.trim();
	}

	/**
	 * The attachments of a message.
	 *
	 * <p>
	 * A file attachment is a {@code reference}: the file itself lives in OneDrive
	 * or in the channel's SharePoint folder and the message only points at it,
	 * which is why {@code contentUrl} is the field that matters and is what
	 * {@code MicrosoftTeamsDownloadMessageAttachment} reads. An attachment of any
	 * other kind is a card, and its {@code content} is json rather than a file.
	 * </p>
	 *
	 * @param attachments the collection as Graph returned it
	 * @return the attachments, empty when there are none
	 */
	private static List<Map<String, Object>> attachments(Object attachments) {
		List<Map<String, Object>> described = new ArrayList<>();
		for (Map<String, Object> attachment : mapList(attachments)) {
			Map<String, Object> output = new LinkedHashMap<>();
			putIfPresent(output, ID, attachment.get(ID));
			putIfPresent(output, NAME, attachment.get(NAME));
			putIfPresent(output, CONTENT_TYPE, attachment.get(CONTENT_TYPE));
			putIfPresent(output, CONTENT_URL, attachment.get(CONTENT_URL));
			output.put("isFile", isFileAttachment(attachment));
			if (!output.isEmpty()) {
				described.add(output);
			}
		}
		return described;
	}

	/**
	 * @param attachment an attachment as Graph returned it
	 * @return true when the attachment points at a file that can be downloaded
	 */
	public static boolean isFileAttachment(Map<String, Object> attachment) {
		Object contentType = attachment.get(CONTENT_TYPE);
		Object contentUrl = attachment.get(CONTENT_URL);
		if (contentUrl == null || contentUrl.toString().trim().isEmpty()) {
			return false;
		}
		// a card carries its content inline and names a card content type, while a
		// file is a reference to something living in a drive
		return contentType == null || contentType.toString().toLowerCase(Locale.ROOT).contains("reference");
	}

	/**
	 * Who a message mentioned.
	 *
	 * @param mentions the collection as Graph returned it
	 * @return the mentions, empty when there are none
	 */
	private static List<Map<String, Object>> mentions(Object mentions) {
		List<Map<String, Object>> described = new ArrayList<>();
		for (Map<String, Object> mention : mapList(mentions)) {
			Map<String, Object> output = new LinkedHashMap<>();
			putIfPresent(output, ID, mention.get(ID));
			putIfPresent(output, "text", mention.get("mentionText"));
			putIfPresent(output, "userId", senderId(mention.get("mentioned")));
			putIfPresent(output, NAME, senderName(mention.get("mentioned")));
			if (!output.isEmpty()) {
				described.add(output);
			}
		}
		return described;
	}

	/**
	 * How a message was reacted to.
	 *
	 * @param reactions the collection as Graph returned it
	 * @return the reactions, empty when there are none
	 */
	private static List<Map<String, Object>> reactions(Object reactions) {
		List<Map<String, Object>> described = new ArrayList<>();
		for (Map<String, Object> reaction : mapList(reactions)) {
			Map<String, Object> output = new LinkedHashMap<>();
			putIfPresent(output, "type", reaction.get("reactionType"));
			putIfPresent(output, "createdDateTime", reaction.get("createdDateTime"));
			putIfPresent(output, NAME, senderName(reaction.get(USER)));
			if (!output.isEmpty()) {
				described.add(output);
			}
		}
		return described;
	}

	/**
	 * The inline content of a message, such as a pasted image.
	 *
	 * <p>
	 * Graph answers the bytes of these only from their own endpoint, and always
	 * reports {@code contentBytes} as null here, so what is worth returning is the
	 * id that endpoint takes.
	 * </p>
	 *
	 * @param hostedContents the collection as Graph returned it
	 * @return the hosted contents, empty when there are none
	 */
	private static List<Map<String, Object>> hostedContents(Object hostedContents) {
		List<Map<String, Object>> described = new ArrayList<>();
		for (Map<String, Object> hostedContent : mapList(hostedContents)) {
			Map<String, Object> output = new LinkedHashMap<>();
			putIfPresent(output, ID, hostedContent.get(ID));
			putIfPresent(output, CONTENT_TYPE, hostedContent.get(CONTENT_TYPE));
			if (!output.isEmpty()) {
				described.add(output);
			}
		}
		return described;
	}

	/**
	 * @param identitySet an {@code identitySet} as Graph returned it, such as the
	 *                    sender of a message
	 * @return the display name, or null when there is none
	 */
	private static String senderName(Object identitySet) {
		Map<String, Object> identity = identityOf(identitySet);
		if (identity == null) {
			return null;
		}
		Object displayName = identity.get(DISPLAY_NAME);
		return displayName == null ? null : displayName.toString();
	}

	/**
	 * @param identitySet an {@code identitySet} as Graph returned it
	 * @return the id of whoever it names, or null when there is none
	 */
	private static String senderId(Object identitySet) {
		Map<String, Object> identity = identityOf(identitySet);
		if (identity == null) {
			return null;
		}
		Object id = identity.get(ID);
		return id == null ? null : id.toString();
	}

	/**
	 * Whether a message came from a person, an app or the service itself. Worth
	 * reporting because a channel is full of messages nobody typed.
	 *
	 * @param identitySet an {@code identitySet} as Graph returned it
	 * @return {@code user}, {@code application} or {@code device}, or null when it
	 *         names nobody
	 */
	private static String senderType(Object identitySet) {
		if (!(identitySet instanceof Map)) {
			return null;
		}
		Map<?, ?> set = (Map<?, ?>) identitySet;
		for (String kind : new String[] { USER, "application", "device" }) {
			if (set.get(kind) instanceof Map) {
				return kind;
			}
		}
		return null;
	}

	/**
	 * The one identity inside an identity set, whichever kind it turned out to be.
	 *
	 * @param identitySet an {@code identitySet} as Graph returned it
	 * @return the identity, or null when the set names nobody
	 */
	@SuppressWarnings("unchecked")
	private static Map<String, Object> identityOf(Object identitySet) {
		if (!(identitySet instanceof Map)) {
			return null;
		}
		Map<String, Object> set = (Map<String, Object>) identitySet;
		for (String kind : new String[] { USER, "application", "device" }) {
			Object identity = set.get(kind);
			if (identity instanceof Map) {
				return (Map<String, Object>) identity;
			}
		}
		return null;
	}

	/**
	 * Whether a chat holds something the signed in user has not read.
	 *
	 * <p>
	 * A chat nobody has ever opened reports a read time of {@code 0001-01-01},
	 * which is older than any message and so reads as unread without being special
	 * cased. A time that cannot be read at all is treated the same way as never
	 * having read it: saying there might be something new is the safer of the two
	 * wrong answers.
	 * </p>
	 *
	 * @param lastMessageDateTime when the last message arrived
	 * @param lastReadDateTime    when the user last read the chat
	 * @return true when the last message is newer than the last read
	 */
	private static boolean isUnread(String lastMessageDateTime, String lastReadDateTime) {
		if (lastMessageDateTime == null) {
			return false;
		}
		if (lastReadDateTime == null) {
			return true;
		}
		try {
			return Instant.parse(lastMessageDateTime).isAfter(Instant.parse(lastReadDateTime));
		} catch (DateTimeParseException e) {
			classLogger.debug("Could not compare the chat read time '{}' with the message time '{}'", lastReadDateTime,
					lastMessageDateTime, e);
			return true;
		}
	}

	/**
	 * @param value the value to read
	 * @return the value as a string, or null when there is nothing to read
	 */
	private static String asString(Object value) {
		if (value == null || value.toString().trim().isEmpty()) {
			return null;
		}
		return value.toString();
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> asStringKeyedMap(Object value) {
		return (Map<String, Object>) value;
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
	 * Set a value, and only when there is one.
	 *
	 * @param output the map being built
	 * @param key    the key to set
	 * @param value  the value, which is left out when it is null
	 */
	private static void putIfPresent(Map<String, Object> output, String key, Object value) {
		if (value != null) {
			output.put(key, value);
		}
	}

}

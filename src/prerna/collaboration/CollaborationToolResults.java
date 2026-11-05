package prerna.collaboration;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;

// What a collaboration tool's result keeps for the model: the shared Microsoft reactors return
// everything Graph gives (links, member ids, every timestamp), which the chat's context pays for
// on every later round. Only the tools named here are trimmed; anything else passes unchanged.
final class CollaborationToolResults {

	private static final Logger classLogger = LogManager.getLogger(CollaborationToolResults.class);

	// enough of a last message to tell what the chat is about
	private static final int LAST_MESSAGE_CHARS = 300;

	// tools whose results carry UI-only copies, links and defaults the model never
	// uses
	private static final Set<String> CLEANED = Set.of("ReadThread", "ListChatMessages", "SearchTeamsMessages",
			"ListEvents", "ListM365Mail", "SearchFiles");
	// formatted originals and links: the model reads text and acts by id
	private static final Set<String> DROPPED_KEYS = Set.of("displayBody", "webLink", "webUrl", "lastModifiedDateTime",
			"hostedContents", "bodyTruncated", "uniqueBodyTruncated");
	// a value that is just the default says nothing
	private static final Map<String, Object> DEFAULTS = Map.of("importance", "normal", "messageType", "message",
			"showAs", "busy", "fromType", "user");
	// an invite body is mostly join details
	private static final int EVENT_BODY_CHARS = 1000;
	private static final int EVENT_ATTENDEES = 10;
	// a page of mail shares one body budget, so a call for 50 or 100 full mails
	// cannot fill the context
	static final int MAIL_PAGE_BODY_CHARS = 30_000;

	private CollaborationToolResults() {

	}

	static String forModel(String toolName, String output, User user) {
		if (output == null || output.isBlank()) {
			return output;
		}
		try {
			if ("ListChats".equals(toolName)) {
				return listChats(output, user);
			}
			if (CLEANED.contains(toolName)) {
				Object parsed = CollaborationDbUtils.parseMap(output);
				if (parsed == null) {
					return output;
				}
				Object cleaned = clean(toolName, null, parsed);
				if ("ListM365Mail".equals(toolName)) {
					shareMailBodies(cleaned);
				}
				return CollaborationDbUtils.toJson(cleaned);
			}
		} catch (RuntimeException e) {
			// a result we cannot read is passed as it came
			classLogger.warn("Could not trim the {} result", toolName, e);
		}
		return output;
	}

	// id (the chatId other tools take), kind, name, last activity and unread, the
	// last message's
	// sender, time and start, and for a 1:1 the other person; no links, member ids
	// or roles
	@SuppressWarnings("unchecked")
	private static String listChats(String output, User user) {
		Map<String, Object> result = CollaborationDbUtils.parseMap(output);
		if (result == null || !(result.get("chats") instanceof List<?> chats)) {
			return output;
		}
		String me = ownEmail(user);
		List<Map<String, Object>> slim = new ArrayList<>();
		for (Object item : chats) {
			if (!(item instanceof Map<?, ?> map)) {
				continue;
			}
			Map<String, Object> chat = (Map<String, Object>) map;
			Map<String, Object> out = new LinkedHashMap<>();
			put(out, "id", chat.get("id"));
			put(out, "chatType", chat.get("chatType"));
			put(out, "name", chat.get("displayName") != null ? chat.get("displayName") : chat.get("topic"));
			put(out, "hasUnread", chat.get("hasUnread"));
			// lastAt is when the last message came; Graph's lastUpdatedDateTime tracks chat
			// edits, not messages
			if (chat.get("lastMessage") instanceof Map<?, ?> last) {
				put(out, "lastAt", last.get("createdDateTime"));
				Map<String, Object> message = new LinkedHashMap<>();
				put(message, "from", last.get("fromName"));
				if (last.get("body") instanceof String body && !body.isBlank()) {
					message.put("text",
							body.length() > LAST_MESSAGE_CHARS ? body.substring(0, LAST_MESSAGE_CHARS) + "..." : body);
				}
				if (!message.isEmpty()) {
					out.put("lastMessage", message);
				}
			}
			if ("oneOnOne".equals(chat.get("chatType")) && chat.get("members") instanceof List<?> members) {
				for (Object m : members) {
					if (m instanceof Map<?, ?> member
							&& !(member.get("email") instanceof String email
									&& email.toLowerCase(Locale.ROOT).equals(me))
							&& (member.get("name") != null || member.get("email") != null)) {
						Map<String, Object> with = new LinkedHashMap<>();
						put(with, "name", member.get("name"));
						put(with, "email", member.get("email"));
						out.put("with", with);
						break;
					}
				}
			}
			slim.add(out);
		}
		Map<String, Object> trimmed = new LinkedHashMap<>(result);
		trimmed.put("chats", slim);
		return CollaborationDbUtils.toJson(trimmed);
	}

	// cuts every body on the page to an equal share of MAIL_PAGE_BODY_CHARS, and
	// says so
	@SuppressWarnings("unchecked")
	private static void shareMailBodies(Object result) {
		if (!(result instanceof Map<?, ?> page) || !(page.get("messages") instanceof List<?> messages)
				|| messages.isEmpty()) {
			return;
		}
		int share = MAIL_PAGE_BODY_CHARS / messages.size();
		boolean cut = false;
		for (Object item : messages) {
			if (item instanceof Map<?, ?> message && message.get("body") instanceof String body
					&& body.length() > share) {
				((Map<String, Object>) message).put("body", body.substring(0, share) + "...");
				cut = true;
			}
		}
		if (cut) {
			((Map<String, Object>) page).put("note", "Bodies are cut to " + share + " characters each so the page "
					+ "fits. For more of one message, list fewer messages or read its thread with conversationId.");
		}
	}

	// drops UI copies, links, defaults and empty values everywhere in the result
	@SuppressWarnings("unchecked")
	private static Object clean(String toolName, String key, Object value) {
		if (value instanceof Map<?, ?> map) {
			Map<String, Object> in = (Map<String, Object>) map;
			// a mail's uniqueBody is its new text, body repeats the quoted history
			boolean unique = in.get("uniqueBody") instanceof String u && !u.isBlank();
			Map<String, Object> out = new LinkedHashMap<>();
			for (Map.Entry<String, Object> e : in.entrySet()) {
				String k = e.getKey();
				Object v = e.getValue();
				if (DROPPED_KEYS.contains(k) || k.startsWith("@odata") || (unique && "body".equals(k))
						|| (v != null && v.equals(DEFAULTS.get(k)))
						|| (Boolean.FALSE.equals(v) && k.startsWith("is"))) {
					continue;
				}
				if ("uniqueBody".equals(k)) {
					k = "body";
				}
				if ("ListEvents".equals(toolName) && "attendees".equals(k) && v instanceof List<?> list) {
					putAttendees(out, list);
					continue;
				}
				if ("ListEvents".equals(toolName) && "body".equals(k) && v instanceof String text
						&& text.length() > EVENT_BODY_CHARS) {
					v = text.substring(0, EVENT_BODY_CHARS) + "...";
				}
				Object c = clean(toolName, k, v);
				if (c != null && !(c instanceof String t && t.isBlank()) && !(c instanceof List<?> l && l.isEmpty())
						&& !(c instanceof Map<?, ?> m && m.isEmpty())) {
					out.put(k, c);
				}
			}
			return out;
		}
		if (value instanceof List<?> list) {
			List<Object> out = new ArrayList<>();
			for (Object item : list) {
				Object c = clean(toolName, key, item);
				if (c != null) {
					out.add(c);
				}
			}
			return out;
		}
		return value;
	}

	// the first attendees by name and address, the response only when given, and
	// the full count
	private static void putAttendees(Map<String, Object> out, List<?> attendees) {
		List<Map<String, Object>> kept = new ArrayList<>();
		for (Object a : attendees) {
			if (kept.size() >= EVENT_ATTENDEES) {
				break;
			}
			if (a instanceof Map<?, ?> attendee) {
				Map<String, Object> one = new LinkedHashMap<>();
				put(one, "name", attendee.get("name"));
				put(one, "address", attendee.get("address"));
				if ("optional".equals(attendee.get("type"))) {
					one.put("optional", true);
				}
				if (attendee.get("response") instanceof String r && !"none".equals(r)) {
					one.put("response", r);
				}
				kept.add(one);
			}
		}
		if (!kept.isEmpty()) {
			out.put("attendees", kept);
		}
		if (attendees.size() > kept.size()) {
			out.put("attendeeCount", attendees.size());
		}
	}

	private static String ownEmail(User user) {
		try {
			String email = user == null ? null : MicrosoftLoginUtils.getMicrosoftEmail(user);
			return email == null ? "" : email.toLowerCase(Locale.ROOT);
		} catch (RuntimeException e) {
			return "";
		}
	}

	private static void put(Map<String, Object> out, String key, Object value) {
		if (value != null && !(value instanceof String text && text.isBlank())) {
			out.put(key, value);
		}
	}
}

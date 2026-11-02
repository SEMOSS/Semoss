package prerna.collaboration;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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
		} catch (RuntimeException e) {
			// a result we cannot read is passed as it came
			classLogger.warn("Could not trim the {} result", toolName, e);
		}
		return output;
	}

	// id (the chatId other tools take), kind, name, last activity and unread, the last message's
	// sender, time and start, and for a 1:1 the other person; no links, member ids or roles
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
			// lastAt is when the last message came; Graph's lastUpdatedDateTime tracks chat edits, not messages
			if (chat.get("lastMessage") instanceof Map<?, ?> last) {
				put(out, "lastAt", last.get("createdDateTime"));
				Map<String, Object> message = new LinkedHashMap<>();
				put(message, "from", last.get("fromName"));
				if (last.get("body") instanceof String body && !body.isBlank()) {
					message.put("text", body.length() > LAST_MESSAGE_CHARS
							? body.substring(0, LAST_MESSAGE_CHARS) + "..." : body);
				}
				if (!message.isEmpty()) {
					out.put("lastMessage", message);
				}
			}
			if ("oneOnOne".equals(chat.get("chatType")) && chat.get("members") instanceof List<?> members) {
				for (Object m : members) {
					if (m instanceof Map<?, ?> member
							&& !(member.get("email") instanceof String email && email.toLowerCase(Locale.ROOT).equals(me))
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

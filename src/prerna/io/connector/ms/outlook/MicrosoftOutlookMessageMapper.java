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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jsoup.Jsoup;

/**
 * Turns the json Graph returns for a message into the map this codebase answers
 * with.
 *
 * <p>
 * Kept apart from both callers because they are otherwise unrelated: the mail
 * function engines read a mailbox they were configured with using an app only
 * token, and the Outlook reactors read the signed in user's own mailbox using a
 * delegated one. What a message looks like on the way out should not depend on
 * which of those asked, so the mapping lives here and neither owns it.
 *
 * <p>
 * The shape is the one the IMAP and POP3 engines settled on rather than
 * anything Graph suggests, so a caller reading over Graph sees the same keys it
 * would reading over a protocol. The single visible difference is the uid,
 * which is Graph's opaque id rather than a number.
 */
public class MicrosoftOutlookMessageMapper {

	private MicrosoftOutlookMessageMapper() {

	}

	/**
	 * Describe one message.
	 *
	 * <p>
	 * Attachments are left to the caller, since deciding what to do with them -
	 * naming them, saving them, or leaving them alone - is where an app only reader
	 * and a delegated one genuinely differ.
	 *
	 * @param message      the message as Graph returned it
	 * @param includeBody  whether the body text comes back
	 * @param maxBodyChars the longest body to return before truncating it, or 0 to
	 *                     return whatever length it is
	 * @return the message as a map
	 */
	public static Map<String, Object> toMessage(Map<String, Object> message, boolean includeBody, int maxBodyChars) {
		Map<String, Object> output = new LinkedHashMap<>();
		// graph names a message with an opaque string, where a protocol uses a
		// number. it round trips the same way, which is all a caller does with it
		output.put("uid", message.get("id"));
		putIfPresent(output, "messageId", message.get("internetMessageId"));
		putIfPresent(output, "from", addressOf(message.get("from")));
		putIfPresent(output, "to", addressList(message.get("toRecipients")));
		putIfPresent(output, "cc", addressList(message.get("ccRecipients")));
		putIfPresent(output, "subject", message.get("subject"));
		putIfPresent(output, "sentDate", message.get("sentDateTime"));
		putIfPresent(output, "receivedDate", message.get("receivedDateTime"));
		output.put("unread", !Boolean.TRUE.equals(message.get("isRead")));

		if (includeBody) {
			String body = bodyOf(message);
			if (maxBodyChars > 0 && body.length() > maxBodyChars) {
				body = body.substring(0, maxBodyChars) + " ... [truncated]";
				output.put("bodyTruncated", true);
			}
			output.put("body", body);
		}
		return output;
	}

	/**
	 * The readable text of a message, preferring what Graph says is plain over
	 * markup, the same way the protocol engines do.
	 *
	 * @param message the message as Graph returned it
	 * @return the body text, empty when there is none
	 */
	public static String bodyOf(Map<String, Object> message) {
		Object body = message.get("body");
		if (!(body instanceof Map)) {
			Object preview = message.get("bodyPreview");
			return preview == null ? "" : preview.toString().trim();
		}
		Map<?, ?> bodyMap = (Map<?, ?>) body;
		String content = bodyMap.get("content") == null ? "" : bodyMap.get("content").toString();
		if ("html".equalsIgnoreCase(String.valueOf(bodyMap.get("contentType")))) {
			// the markup is noise to whoever asked what the message says
			return Jsoup.parse(content).text().trim();
		}
		return content.trim();
	}

	/**
	 * The address out of a Graph recipient object.
	 *
	 * @param recipient the {@code from} or one entry of a recipient collection
	 * @return the address, or null when there is none
	 */
	public static String addressOf(Object recipient) {
		if (!(recipient instanceof Map)) {
			return null;
		}
		Object emailAddress = ((Map<?, ?>) recipient).get("emailAddress");
		if (!(emailAddress instanceof Map)) {
			return null;
		}
		Object address = ((Map<?, ?>) emailAddress).get("address");
		return address == null ? null : address.toString();
	}

	/**
	 * The addresses of one recipient collection, joined the way the protocol
	 * engines join them.
	 *
	 * @param recipients the collection as Graph returned it
	 * @return the addresses joined, or null when there are none
	 */
	public static String addressList(Object recipients) {
		String[] addresses = addressArray(recipients);
		return addresses == null ? null : String.join(", ", addresses);
	}

	/**
	 * The addresses of one recipient collection, for a caller that wants them
	 * separately rather than joined.
	 *
	 * @param recipients the collection as Graph returned it
	 * @return the addresses, or null when there are none
	 */
	public static String[] addressArray(Object recipients) {
		if (!(recipients instanceof List)) {
			return null;
		}
		List<String> addresses = new ArrayList<>();
		for (Object recipient : (List<?>) recipients) {
			String address = addressOf(recipient);
			if (address != null) {
				addresses.add(address);
			}
		}
		if (addresses.isEmpty()) {
			return null;
		}
		return addresses.toArray(new String[0]);
	}

	/**
	 * Set a key only when there is something to set it to, so a caller reading the
	 * output does not have to tell a null apart from an absent field.
	 *
	 * @param output the map being built
	 * @param key    the key to set
	 * @param value  the value, ignored when null
	 */
	public static void putIfPresent(Map<String, Object> output, String key, Object value) {
		if (value != null) {
			output.put(key, value);
		}
	}

}

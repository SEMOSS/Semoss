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
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import jakarta.mail.internet.AddressException;
import jakarta.mail.internet.InternetAddress;

/**
 * Original reply-all defaults and explicit, verifiable draft envelope
 * overrides.
 */
final class MicrosoftOutlookReplyRecipients {

	private MicrosoftOutlookReplyRecipients() {
	}

	/**
	 * Reply-To replaces From; TO takes precedence over CC and the connected account
	 * is omitted.
	 */
	static Map<String, List<String>> defaults(Map<String, Object> message, String account) {
		if (account == null || account.isBlank()) {
			throw new IllegalArgumentException(
					"Your connected Outlook address could not be verified. Reconnect Microsoft and try again.");
		}
		Set<String> seen = new LinkedHashSet<>();
		seen.add(account.trim().toLowerCase(Locale.ROOT));
		List<String> to = new ArrayList<>();
		List<String> cc = new ArrayList<>();
		String[] replyTo = MicrosoftOutlookMessageMapper.addressArray(message.get("replyTo"));
		String sender = MicrosoftOutlookMessageMapper.addressOf(message.get("from"));
		if (replyTo == null && (sender == null || sender.isBlank())) {
			throw new IllegalArgumentException("The original sender could not be verified.");
		}
		appendUnique(to, seen, replyTo == null ? new String[] { sender } : replyTo);
		appendUnique(to, seen, MicrosoftOutlookMessageMapper.addressArray(message.get("toRecipients")));
		appendUnique(cc, seen, MicrosoftOutlookMessageMapper.addressArray(message.get("ccRecipients")));
		return Map.of("to", to, "cc", cc);
	}

	private static void appendUnique(List<String> target, Set<String> seen, String[] addresses) {
		for (String address : validate(addresses)) {
			if (seen.add(address.toLowerCase(Locale.ROOT))) {
				target.add(address);
			}
		}
	}

	/**
	 * Validate before any draft is created; null is an explicitly empty list in
	 * override mode.
	 */
	static String[] validate(String[] addresses) {
		if (addresses == null) {
			return new String[0];
		}
		return Arrays.stream(addresses).map(address -> {
			if (address == null) {
				throw new IllegalArgumentException("Enter valid email addresses.");
			}
			String value = address.trim();
			try {
				InternetAddress parsed = new InternetAddress(value, true);
				parsed.validate();
				if (!value.equals(parsed.getAddress()) || !value.contains("@") || value.contains("\r")
						|| value.contains("\n")) {
					throw new AddressException("Expected a bare email address");
				}
			} catch (AddressException e) {
				throw new IllegalArgumentException("Enter valid email addresses.", e);
			}
			return value;
		}).toArray(String[]::new);
	}

	/**
	 * Graph must explicitly report each recipient collection, including empty
	 * lists.
	 */
	static boolean matches(Object actual, String[] expected) {
		if (!(actual instanceof List<?> recipients)) {
			return false;
		}
		Set<String> received = new LinkedHashSet<>();
		for (Object recipient : recipients) {
			String address = MicrosoftOutlookMessageMapper.addressOf(recipient);
			if (address == null || address.isBlank()) {
				return false;
			}
			received.add(address.trim().toLowerCase(Locale.ROOT));
		}
		Set<String> requested = new LinkedHashSet<>();
		for (String address : expected) {
			requested.add(address.toLowerCase(Locale.ROOT));
		}
		return received.equals(requested);
	}
}

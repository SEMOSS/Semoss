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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import jakarta.mail.internet.AddressException;
import jakarta.mail.internet.InternetAddress;

/**
 * Who a reply goes to, worked out the same way for every mailbox.
 */
public final class MailRecipientRules {

	private MailRecipientRules() {

	}

	/**
	 * Who a reply to everybody on a message goes to.
	 *
	 * <p>
	 * Reply-To stands in for From, everybody it was sent to goes on To ahead of Cc,
	 * and the user's own address is left off, unless nobody else is on the message,
	 * in which case the reply answers the user the way their mail app would.
	 * </p>
	 *
	 * @param replyTo the Reply-To addresses, or null
	 * @param from    the sender, or null
	 * @param to      the recipients, or null
	 * @param cc      the copied recipients, or null
	 * @param account the user's own address
	 * @return who the reply goes to
	 * @throws IllegalArgumentException when the user's address or the sender cannot
	 *                                  be told
	 */
	public static MailRecipients replyDefaults(List<String> replyTo, String from, List<String> to, List<String> cc,
			String account) {
		if (account == null || account.isBlank()) {
			throw new IllegalArgumentException(
					"Your connected email address could not be verified. Sign in again and try again.");
		}
		boolean hasReplyTo = replyTo != null && !replyTo.isEmpty();
		if (!hasReplyTo && (from == null || from.isBlank())) {
			throw new IllegalArgumentException("The original sender could not be verified.");
		}
		Set<String> seen = new LinkedHashSet<>();
		seen.add(account.trim().toLowerCase(Locale.ROOT));
		List<String> answerTo = new ArrayList<>();
		List<String> answerCc = new ArrayList<>();
		appendUnique(answerTo, seen, hasReplyTo ? replyTo : List.of(from));
		appendUnique(answerTo, seen, to);
		appendUnique(answerCc, seen, cc);
		if (answerTo.isEmpty() && answerCc.isEmpty()) {
			answerTo.add(account.trim());
		}
		return new MailRecipients(answerTo, answerCc);
	}

	/**
	 * Check that every address is a bare email address, before anything is written
	 * to the provider.
	 *
	 * @param addresses the addresses, or null for none
	 * @return the addresses, trimmed
	 * @throws IllegalArgumentException when one is not an email address
	 */
	public static List<String> validate(List<String> addresses) {
		List<String> valid = new ArrayList<>();
		if (addresses == null) {
			return valid;
		}
		for (String address : addresses) {
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
			valid.add(value);
		}
		return valid;
	}

	private static void appendUnique(List<String> target, Set<String> seen, List<String> addresses) {
		if (addresses == null) {
			return;
		}
		for (String address : validate(addresses)) {
			if (seen.add(address.toLowerCase(Locale.ROOT))) {
				target.add(address);
			}
		}
	}
}

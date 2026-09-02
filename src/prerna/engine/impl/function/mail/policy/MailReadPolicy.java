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
package prerna.engine.impl.function.mail.policy;

import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.math.NumberUtils;

import prerna.engine.impl.function.mail.config.MailProperties;

/**
 * How much of a mailbox a reading engine is willing to hand over.
 *
 * <p>
 * Reading is not destructive, so these are about keeping one call proportionate
 * rather than about safety: a caller that asks for everything gets a bounded
 * answer, a long message is truncated rather than returned whole, and
 * attachments are described rather than written to disk until the SMSS says
 * otherwise. The sender allowlist is the exception, and is a real restriction -
 * an engine cataloged against a shared mailbox can be limited to the
 * correspondents it was meant for.
 *
 * <p>
 * Every mailbox applies these the same way, so what an engine returns does not
 * depend on whether it read over a protocol or over Graph.
 *
 * @param maxMessages             the most messages one search may return
 * @param defaultMessages         how many come back when a caller does not say
 * @param maxBodyChars            how much of a body is returned before it is
 *                                cut short
 * @param maxAttachmentSize       the largest attachment that will be written
 *                                into an insight
 * @param allowAttachmentDownload whether attachments may be written at all
 * @param allowedSenderDomains    the domains a message has to have come from,
 *                                lower case and without a leading {@code @}, or
 *                                empty to accept any sender
 */
public record MailReadPolicy(int maxMessages, int defaultMessages, int maxBodyChars, long maxAttachmentSize,
		boolean allowAttachmentDownload, Set<String> allowedSenderDomains) {

	public MailReadPolicy {
		allowedSenderDomains = Set.copyOf(allowedSenderDomains);
	}

	/**
	 * Read the limits out of an engine's SMSS, falling back to defaults chosen to
	 * be usable without any of these being set.
	 *
	 * @param properties the engine's SMSS properties
	 * @return the policy
	 */
	public static MailReadPolicy from(Properties properties) {
		int maximum = Math.max(1, NumberUtils.toInt(properties.getProperty(MailProperties.MAX_MESSAGES), 25));
		int defaults = Math.min(maximum,
				Math.max(1, NumberUtils.toInt(properties.getProperty(MailProperties.DEFAULT_MESSAGES), 10)));
		int bodyChars = Math.max(100, NumberUtils.toInt(properties.getProperty(MailProperties.MAX_BODY_CHARS), 10_000));
		long attachmentSize = Math.max(1024L,
				NumberUtils.toLong(properties.getProperty(MailProperties.MAX_ATTACHMENT_SIZE), 5L * 1024L * 1024L));
		boolean allowDownloads = MailProperties
				.parseBoolean(properties.getProperty(MailProperties.ALLOW_ATTACHMENT_DOWNLOAD), false);
		Set<String> domains = new LinkedHashSet<>();
		for (String domain : MailProperties.splitList(properties.getProperty(MailProperties.ALLOWED_SENDER_DOMAINS))) {
			domains.add(domain.toLowerCase().replaceFirst("^@", ""));
		}
		return new MailReadPolicy(maximum, defaults, bodyChars, attachmentSize, allowDownloads, domains);
	}

	/**
	 * Hold a requested count inside what this engine will return.
	 *
	 * @param requested how many the caller asked for
	 * @return how many they get, which is at least one and at most
	 *         {@link #maxMessages()}
	 */
	public int boundedLimit(int requested) {
		return Math.min(this.maxMessages, Math.max(1, requested));
	}

	/**
	 * Whether a message from this address may be returned.
	 *
	 * <p>
	 * A subdomain of an allowed domain counts, so allowing {@code example.com} also
	 * allows {@code mail.example.com}. An address that cannot be read at all is
	 * refused rather than let through, since a message whose sender is unknown is
	 * exactly the kind the allowlist exists to keep out.
	 *
	 * @param sender the address the message came from, or null when there is none
	 * @return true when the message may be returned
	 */
	public boolean isSenderAllowed(String sender) {
		if (this.allowedSenderDomains.isEmpty()) {
			return true;
		}
		if (sender == null) {
			return false;
		}
		String value = sender.toLowerCase();
		int at = value.lastIndexOf('@');
		if (at < 0) {
			return false;
		}
		String domain = value.substring(at + 1).replaceAll("[>\"\\s]", "");
		return this.allowedSenderDomains.stream()
				.anyMatch(allowed -> domain.equals(allowed) || domain.endsWith("." + allowed));
	}
}

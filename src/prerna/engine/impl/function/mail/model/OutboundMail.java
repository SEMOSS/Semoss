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
package prerna.engine.impl.function.mail.model;

import java.util.List;

/**
 * One message, checked and ready to send.
 *
 * <p>
 * Everything a sender is allowed to refuse has been settled by the time this
 * exists: the sender is pinned or deliberately overridden, the recipients are
 * within the allowed domains and under the count limit, the defaults have been
 * applied, the subject carries its prefix, and the attachments are files of the
 * calling insight. A sender delivers this as it stands.
 *
 * <p>
 * A missing list is empty rather than null, so nothing has to check, and the
 * {@code Array} methods hand back null for an empty one because that is what
 * jakarta.mail and Graph both read as "none".
 *
 * @param to          the recipients
 * @param cc          the copied recipients
 * @param bcc         the blind copied recipients
 * @param from        the address to send as
 * @param subject     the subject line
 * @param body        the body
 * @param html        whether the body is html rather than plain text
 * @param attachments the resolved paths of the files to attach
 */
public record OutboundMail(List<String> to, List<String> cc, List<String> bcc, String from, String subject, String body,
		boolean html, List<String> attachments) {

	public OutboundMail {
		to = immutable(to);
		cc = immutable(cc);
		bcc = immutable(bcc);
		attachments = immutable(attachments);
	}

	/**
	 * @return how many addresses this message reaches in total, which is what a
	 *         recipient cap is counted against
	 */
	public int recipientCount() {
		return this.to.size() + this.cc.size() + this.bcc.size();
	}

	/**
	 * @return the recipients, or null when there are none
	 */
	public String[] toArray() {
		return array(this.to);
	}

	/**
	 * @return the copied recipients, or null when there are none
	 */
	public String[] ccArray() {
		return array(this.cc);
	}

	/**
	 * @return the blind copied recipients, or null when there are none
	 */
	public String[] bccArray() {
		return array(this.bcc);
	}

	/**
	 * @return the files to attach, or null when there are none
	 */
	public String[] attachmentArray() {
		return array(this.attachments);
	}

	/**
	 * @param values the addresses or paths, which may be null
	 * @return them as a list nothing can change afterwards
	 */
	private static List<String> immutable(List<String> values) {
		return values == null ? List.of() : List.copyOf(values);
	}

	/**
	 * @param values the addresses or paths
	 * @return them as an array, or null when there are none
	 */
	private static String[] array(List<String> values) {
		return values.isEmpty() ? null : values.toArray(String[]::new);
	}
}

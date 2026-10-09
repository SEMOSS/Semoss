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

import java.io.File;
import java.util.List;

/**
 * A message a caller wrote, checked and ready to hand to a provider.
 *
 * <p>
 * A list left out is empty rather than null, and the {@code Array} methods hand
 * back null for an empty one, which is what the Graph message builder reads as
 * none.
 * </p>
 *
 * @param to          the recipients
 * @param cc          the copied recipients
 * @param bcc         the blind copied recipients
 * @param subject     the subject line
 * @param body        the body
 * @param html        whether the body is html rather than plain text
 * @param attachments the files to attach, resolved inside the insight folder
 */
public record OutgoingMail(List<String> to, List<String> cc, List<String> bcc, String subject, String body,
		boolean html, List<File> attachments) {

	/**
	 * What the file editor puts in front of an upload's name to keep two uploads
	 * apart.
	 */
	private static final String UPLOAD_PREFIX = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-";

	public OutgoingMail {
		to = to == null ? List.of() : List.copyOf(to);
		cc = cc == null ? List.of() : List.copyOf(cc);
		bcc = bcc == null ? List.of() : List.copyOf(bcc);
		attachments = attachments == null ? List.of() : List.copyOf(attachments);
	}

	/**
	 * @return whether anybody at all is named to receive it
	 */
	public boolean hasRecipients() {
		return !this.to.isEmpty() || !this.cc.isEmpty() || !this.bcc.isEmpty();
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
	 * @return the paths of the files to attach, or null when there are none
	 */
	public String[] attachmentPaths() {
		return this.attachments.isEmpty() ? null
				: this.attachments.stream().map(File::getAbsolutePath).toArray(String[]::new);
	}

	/**
	 * @return the names the attached files carry on the message
	 */
	public List<String> attachmentNames() {
		return this.attachments.stream().map(OutgoingMail::attachmentName).toList();
	}

	/**
	 * The name a file carries on a message: its own name, without the prefix the
	 * file editor gives an upload, and never the path it was read from.
	 *
	 * @param file the file
	 * @return the name
	 */
	public static String attachmentName(File file) {
		return file.getName().replaceFirst(UPLOAD_PREFIX, "");
	}

	private static String[] array(List<String> values) {
		return values.isEmpty() ? null : values.toArray(new String[0]);
	}
}

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
package prerna.io.connector.google.gmail;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Turns a folder named the way the mail reactors name folders into the label
 * Gmail files mail under.
 *
 * <p>
 * The mail reactors take Outlook's well known folder names for every mailbox,
 * so the same request reads the same place in each. Gmail keeps those places as
 * labels it owns, except the archive, which is everything that has left the
 * inbox, and is read with a search instead.
 * </p>
 */
final class GoogleGmailFolders {

	static final String INBOX = "INBOX";
	static final String SENT = "SENT";
	static final String DRAFT = "DRAFT";
	static final String TRASH = "TRASH";
	static final String SPAM = "SPAM";

	/**
	 * What reads the archive: whatever is not in the inbox or another folder Gmail
	 * keeps.
	 */
	static final String ARCHIVE_QUERY = "-in:inbox -in:sent -in:drafts -in:trash -in:spam";

	private GoogleGmailFolders() {

	}

	/**
	 * Where a folder name points.
	 *
	 * @param labelId the label, or null for the archive
	 * @param archive whether it is the archive, which carries no label
	 */
	record Target(String labelId, boolean archive) {

	}

	/**
	 * @param gmail  the helper, for reading the user's own labels when the name is
	 *               not one Gmail keeps
	 * @param folder a well known folder name, a label name, or a label id
	 * @return where it points
	 */
	static Target resolve(GoogleGmailHelper gmail, String folder) {
		String wanted = folder.trim();
		switch (wanted.toLowerCase(Locale.ROOT)) {
		case "inbox":
			return new Target(INBOX, false);
		case "sentitems":
		case "sent":
			return new Target(SENT, false);
		case "drafts":
		case "draft":
			return new Target(DRAFT, false);
		case "deleteditems":
		case "trash":
			return new Target(TRASH, false);
		case "junkemail":
		case "spam":
			return new Target(SPAM, false);
		case "archive":
			return new Target(null, true);
		default:
			break;
		}
		List<Map<String, Object>> labels = gmail.listLabels();
		for (Map<String, Object> label : labels) {
			if (wanted.equalsIgnoreCase(String.valueOf(label.get("id")))
					|| wanted.equalsIgnoreCase(String.valueOf(label.get("name")))) {
				return new Target(String.valueOf(label.get("id")), false);
			}
		}
		// nothing of that name leaves the value as it was given, which is what an id
		// looks like from here
		return new Target(wanted, false);
	}
}

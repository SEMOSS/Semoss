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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import prerna.auth.User;
import prerna.io.connector.ConnectorPage;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.io.connector.mail.AbstractListMailFoldersReactor;
import prerna.io.connector.mail.MailApp;
import prerna.io.connector.mail.MailFolder;

/**
 * Lists the labels of the signed in user's own Gmail mailbox, which are the
 * places it files mail, each label followed by the labels nested under it.
 *
 * <p>
 * Required Google scope, each under {@code https://www.googleapis.com/auth/}:
 * one of {@code gmail.labels}, {@code gmail.readonly} or {@code gmail.modify}.
 * </p>
 */
public class GoogleGmailListMailFoldersReactor extends AbstractListMailFoldersReactor {

	/**
	 * Label names compared one path segment at a time, so a label sorts right
	 * before the labels nested under it.
	 */
	private static final Comparator<String> PATH_ORDER = (left, right) -> {
		String[] leftSegments = left.split("/");
		String[] rightSegments = right.split("/");
		for (int i = 0; i < Math.min(leftSegments.length, rightSegments.length); i++) {
			int compared = String.CASE_INSENSITIVE_ORDER.compare(leftSegments[i], rightSegments[i]);
			if (compared != 0) {
				return compared;
			}
		}
		return Integer.compare(leftSegments.length, rightSegments.length);
	};

	@Override
	protected MailApp getMailApp() {
		return MailApp.GMAIL;
	}

	@Override
	protected ConnectorPage<MailFolder> listFolders(User user, int offset, int limit) throws Exception {
		GoogleGmailHelper gmail = new GoogleGmailHelper(GoogleLoginUtils.getValidAccessToken(user));
		List<Map<String, Object>> labels = gmail.listLabels().stream().filter(GoogleGmailMessageMapper::isFiled)
				.sorted(Comparator.comparingInt(GoogleGmailMessageMapper::sortOrder)
						.thenComparing(label -> String.valueOf(label.get("name")), PATH_ORDER))
				.toList();
		// where each label sits is worked out over every label, before the page is cut
		ConnectorPage<PlacedLabel> page = ConnectorPage.of(place(labels), offset, limit);
		// a listing of labels says nothing of how many messages carry each, so the
		// page's own labels are read one by one for their counts
		List<MailFolder> folders = page.items().stream()
				.map(placed -> GoogleGmailMessageMapper.toMailFolder(
						gmail.getLabel(String.valueOf(placed.label().get("id"))), placed.parentId(),
						placed.parentPath(), placed.depth()))
				.toList();
		return new ConnectorPage<>(folders, page.hasMore());
	}

	/**
	 * Work out which label each user label is nested under: the nearest one its
	 * name continues, such as {@code Projects} for {@code Projects/Alpha}. Gmail
	 * keeps no labels under its own.
	 *
	 * @param labels the labels, each sorted before the ones nested under it
	 * @return the labels in the same order, each with where it sits
	 */
	static List<PlacedLabel> place(List<Map<String, Object>> labels) {
		Map<String, PlacedLabel> byName = new HashMap<>();
		List<PlacedLabel> placed = new ArrayList<>();
		for (Map<String, Object> label : labels) {
			String name = String.valueOf(label.get("name"));
			PlacedLabel parent = null;
			if (!"system".equalsIgnoreCase(String.valueOf(label.get("type")))) {
				for (int slash = name.lastIndexOf('/'); slash > 0
						&& parent == null; slash = name.lastIndexOf('/', slash - 1)) {
					parent = byName.get(name.substring(0, slash).toLowerCase(Locale.ROOT));
				}
			}
			PlacedLabel entry = parent == null ? new PlacedLabel(label, null, null, 0)
					: new PlacedLabel(label, String.valueOf(parent.label().get("id")),
							String.valueOf(parent.label().get("name")), parent.depth() + 1);
			placed.add(entry);
			byName.put(name.toLowerCase(Locale.ROOT), entry);
		}
		return placed;
	}

	/**
	 * A label and where it sits among the others.
	 *
	 * @param label      the label as Gmail listed it
	 * @param parentId   the id of the label it is nested under, or null
	 * @param parentPath the name of the label it is nested under, or null
	 * @param depth      how many labels it is nested under
	 */
	record PlacedLabel(Map<String, Object> label, String parentId, String parentPath, int depth) {
	}
}

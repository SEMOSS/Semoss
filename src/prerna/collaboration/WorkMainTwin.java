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
package prerna.collaboration;

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.auth.AccessToken;
import prerna.auth.User;

/**
 * A person's own Digital Twin: the Collaboration Agent as that person sees it in their own chats, under a name and a
 * picture of their choice (BRAIN_PERSONA.TWIN_NAME and TWIN_IMAGE, one row per owner). It is not a project, carries
 * no Digital Twin mark and cannot be shared: the twins a person shares with others are separate agents
 * (SecurityProjectUtils.isDigitalTwin). Only the signed-in person reads or changes their own here.
 */
public final class WorkMainTwin {

	private static final Logger classLogger = LogManager.getLogger(WorkMainTwin.class);

	/** What the twin is called where no name can be made from its owner's. */
	static final String UNNAMED = "Your Digital Twin";

	/** The longer side of the stored picture, in pixels. */
	static final int IMAGE_SIDE = 192;

	/**
	 * The most base64 characters a picture may arrive as, about 1 MB of picture: what an avatar option from
	 * WorkGenerateTwinAvatar can take (a PNG of WorkTwinAvatar.OPTION_SIDE), so a caller can save one as it is.
	 */
	static final int MAX_IMAGE_CHARS = 1_400_000;

	private WorkMainTwin() {
	}

	/**
	 * The signed-in person's own Digital Twin. Nothing is written by reading it.
	 *
	 * @return {@code name}, what it is called now; and {@code image}, the picture they chose as a base64 PNG with no
	 *         prefix, left out when they chose none
	 */
	public static Map<String, Object> get(User user) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String[] saved = CollaborationDbUtils.queryOne(
				"SELECT TWIN_NAME, TWIN_IMAGE FROM BRAIN_PERSONA WHERE OWNER_ID = ? AND OWNER_TYPE = ?",
				rs -> new String[] { CollaborationDbUtils.getString(rs, "TWIN_NAME"),
						CollaborationDbUtils.getString(rs, "TWIN_IMAGE") },
				owner.getValue0(), owner.getValue1());
		String name = named(user, owner, saved == null ? null : saved[0]);
		Map<String, Object> twin = new LinkedHashMap<>();
		twin.put("name", name == null ? UNNAMED : name);
		if (saved != null && saved[1] != null && !saved[1].isBlank()) {
			twin.put("image", saved[1]);
		}
		return twin;
	}

	/**
	 * What the signed-in person's own Digital Twin is called, for its prompt. The picture is not read. Never fails
	 * a run.
	 *
	 * @return the name, or null when there is none to give: no user, Collaboration is off, no name can be made
	 *         from theirs, or it could not be read
	 */
	public static String name(User user) {
		try {
			if (user == null || !CollaborationDbUtils.isInitalized()) {
				return null;
			}
			Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
			String saved = CollaborationDbUtils.queryOne(
					"SELECT TWIN_NAME FROM BRAIN_PERSONA WHERE OWNER_ID = ? AND OWNER_TYPE = ?",
					rs -> CollaborationDbUtils.getString(rs, "TWIN_NAME"), owner.getValue0(), owner.getValue1());
			return named(user, owner, saved);
		} catch (Exception e) {
			classLogger.warn("Could not read the name of its owner's Digital Twin; it runs without one", e);
			return null;
		}
	}

	/**
	 * Changes the name, the picture or both of the signed-in person's own Digital Twin. A value that is null stays
	 * as it is.
	 *
	 * @param name  what to call it, as one line; blank goes back to the default, which follows their own name
	 * @param image the picture as base64, in any format ImageIO reads and of at most MAX_IMAGE_CHARS characters;
	 *              it is kept as a PNG no larger than IMAGE_SIDE. Blank removes the picture
	 * @return the twin as it now is, as {@link #get} gives it
	 * @throws IllegalArgumentException when both are null, and for a picture that cannot be read or is too large;
	 *                                  nothing is written then
	 */
	public static Map<String, Object> save(User user, String name, String image) {
		if (name == null && image == null) {
			throw new IllegalArgumentException("Give a name or a picture to save.");
		}
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		// checked before anything is written, so a refused picture leaves the name alone too
		String picture = image == null || image.isBlank() ? null : picture(image);
		synchronized (CollaborationDbUtils.ownerLock(WorkTwinPersona.LOCK, ownerId, ownerType)) {
			if (name != null) {
				String label = CollaborationPrompts.oneName(name);
				// the default is never stored, so it keeps following the person's own name
				WorkTwinPersona.write(ownerId, ownerType, "TWIN_NAME",
						label.isEmpty() || label.equals(defaultName(user, owner)) ? null : label);
			}
			if (image != null) {
				WorkTwinPersona.write(ownerId, ownerType, "TWIN_IMAGE", picture);
			}
		}
		return get(user);
	}

	/**
	 * What the signed-in person's own Digital Twin is told about itself in their chat: who it is, then the parts of
	 * their persona it thinks with when they have one. Never fails a run: a name or a persona that cannot be read
	 * is left out.
	 */
	public static String promptBlock(User user) {
		String identity = CollaborationPrompts.mainTwin(name(user));
		String thinking = WorkTwinPersona.forMainTwin(user);
		return thinking == null ? identity : identity + "\n\n" + thinking;
	}

	/**
	 * The name a twin starts with: its owner's first name, which is the first word of what follows the comma in
	 * "Last, First" and else the first word of the name.
	 *
	 * @return the name, or null when the person's name holds no word
	 */
	static String defaultName(String displayName) {
		String name = CollaborationPrompts.oneName(displayName);
		int comma = name.indexOf(',');
		String given = comma < 0 ? name : name.substring(comma + 1).trim();
		int end = given.indexOf(' ');
		String first = end < 0 ? given : given.substring(0, end);
		return first.isEmpty() ? null : CollaborationPrompts.oneName(first + "'s Digital Twin");
	}

	// the saved name, else the default; null when neither can be made
	private static String named(User user, Pair<String, String> owner, String saved) {
		String name = CollaborationPrompts.oneName(saved);
		return name.isEmpty() ? defaultName(user, owner) : name;
	}

	// From the name on their Collaboration profile, else the one they signed in with. The profile row is only
	// read: BrainProfileUtils makes it.
	private static String defaultName(User user, Pair<String, String> owner) {
		String name = defaultName(CollaborationDbUtils.queryOne(
				"SELECT DISPLAY_NAME FROM BRAIN_PROFILE WHERE OWNER_ID = ? AND OWNER_TYPE = ?",
				rs -> CollaborationDbUtils.getString(rs, "DISPLAY_NAME"), owner.getValue0(), owner.getValue1()));
		if (name == null) {
			AccessToken token = user.getAccessToken(user.getPrimaryLogin());
			name = defaultName(token == null ? null : token.getName());
		}
		return name;
	}

	// the picture that was sent, as the base64 of a PNG no larger than IMAGE_SIDE
	private static String picture(String image) {
		if (image.length() > MAX_IMAGE_CHARS) {
			throw new IllegalArgumentException("That picture is too large. Choose a smaller one.");
		}
		byte[] png;
		try {
			png = WorkTwinAvatar.png(Base64.getDecoder().decode(image.trim()), IMAGE_SIDE);
		} catch (IllegalArgumentException e) {
			// not base64
			png = null;
		}
		if (png == null) {
			throw new IllegalArgumentException("That picture could not be read. Choose another one.");
		}
		return Base64.getEncoder().encodeToString(png);
	}
}

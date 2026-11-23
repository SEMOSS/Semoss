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
package prerna.util.openshell;

import prerna.util.Constants;

/**
 * Which space a transported root belongs to, read from its normalized absolute path.
 *
 * <p>
 * Only user spaces -- projects, rooms and user assets -- are written by user code, so
 * only they are synced between the host and a sandbox. Engine and runtime trees are
 * uploaded once and never pulled back.
 */
public final class TransportedRoots {

	private static final String PROJECT_MARKER = "/" + Constants.PROJECT_FOLDER + "/";
	private static final String ROOM_MARKER = "/" + Constants.ROOM_FOLDER + "/";
	private static final String USER_MARKER = "/" + Constants.USER_FOLDER + "/";

	private TransportedRoots() {
		// static utility
	}

	/** A project, room or user-asset root: the spaces user code writes and sync covers. */
	public static boolean isUserSpace(String normalized) {
		return isProject(normalized) || isRoom(normalized) || isUserAsset(normalized);
	}

	public static boolean isProject(String normalized) {
		return normalized.contains(PROJECT_MARKER);
	}

	public static boolean isRoom(String normalized) {
		return normalized.contains(ROOM_MARKER);
	}

	public static boolean isUserAsset(String normalized) {
		return normalized.contains(USER_MARKER);
	}

	/** The project id of a root under {@code project/<name>__<id>/}, or null outside a project. */
	public static String projectIdOf(String normalized) {
		return idOf(folderAfter(normalized, PROJECT_MARKER));
	}

	/** The asset project id of a root under {@code user/Asset__<id>/}, or null outside one. */
	public static String userAssetIdOf(String normalized) {
		return idOf(folderAfter(normalized, USER_MARKER));
	}

	/** {@code <name>__<id>} folders carry the id after the last separator. */
	private static String idOf(String folder) {
		if (folder == null) {
			return null;
		}
		int separator = folder.lastIndexOf("__");
		return separator < 0 ? folder : folder.substring(separator + 2);
	}

	/** The room id of a root under {@code room/<id>/}, or null outside a room. */
	public static String roomIdOf(String normalized) {
		return folderAfter(normalized, ROOM_MARKER);
	}

	private static String folderAfter(String normalized, String marker) {
		int at = normalized.indexOf(marker);
		if (at < 0) {
			return null;
		}
		String rest = normalized.substring(at + marker.length());
		int slash = rest.indexOf('/');
		return slash < 0 ? rest : rest.substring(0, slash);
	}
}

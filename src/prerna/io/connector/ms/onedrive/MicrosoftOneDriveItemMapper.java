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
package prerna.io.connector.ms.onedrive;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Turns the json Graph returns for a drive and its items into the maps the
 * OneDrive reactors answer with.
 *
 * <p>
 * Kept apart from {@link MicrosoftOneDriveHelper} for the same reason the
 * calendar and mail mappers are kept apart from their helpers: what a file
 * looks like on the way out is a decision about this codebase's shape, not
 * about how Graph is called, and a reactor that reads a file back after
 * uploading one should see it described the same way a listing describes it.
 * </p>
 *
 * <p>
 * The one thing every item carries is the pair that addresses it again: a
 * {@code driveId} and an {@code id}. A file somebody else shared is not in the
 * signed in user's drive, so without the drive it came from there is no way to
 * read it a second time. That pair is why a listing of shared files can be
 * handed straight to the get, download and share reactors.
 * </p>
 *
 * <p>
 * An item that stands for something in another drive carries a
 * {@code remoteItem} facet holding the real name, size and location, with only
 * a local id of its own. Every field is therefore read off the remote facet
 * when there is one, so a shortcut to a shared folder describes the folder
 * rather than the shortcut.
 * </p>
 */
public class MicrosoftOneDriveItemMapper {

	private static final String ID = "id";
	private static final String NAME = "name";
	private static final String SIZE = "size";
	private static final String FILE = "file";
	private static final String USER = "user";
	private static final String PATH = "path";
	private static final String FOLDER = "folder";
	private static final String SHARED = "shared";
	private static final String OWNER = "owner";
	private static final String WEB_URL = "webUrl";
	private static final String DRIVE_ID = "driveId";
	private static final String MIME_TYPE = "mimeType";
	private static final String CHILD_COUNT = "childCount";
	private static final String DISPLAY_NAME = "displayName";
	private static final String EMAIL = "email";
	private static final String REMOTE_ITEM = "remoteItem";
	private static final String SHARED_BY = "sharedBy";
	private static final String PARENT_REFERENCE = "parentReference";

	/** The prefix Graph puts in front of a path relative to a drive root. */
	private static final String ROOT_PATH_PREFIX = "/drive/root:";

	private MicrosoftOneDriveItemMapper() {

	}

	/**
	 * Describe one drive item.
	 *
	 * @param item            the item as Graph returned it
	 * @param fallbackDriveId the drive the item was read from, used when the item
	 *                        does not name its own drive, or null when it is not
	 *                        known
	 * @return the item as a map
	 */
	public static Map<String, Object> toDriveItem(Map<String, Object> item, String fallbackDriveId) {
		Map<String, Object> described = effectiveItem(item);

		Map<String, Object> output = new LinkedHashMap<>();
		output.put(ID, described.get(ID));
		putIfPresent(output, NAME, described.get(NAME));
		putIfPresent(output, DRIVE_ID, driveIdOf(item, fallbackDriveId));
		output.put("isFolder", described.get(FOLDER) != null);
		putIfPresent(output, SIZE, described.get(SIZE));
		putIfPresent(output, MIME_TYPE, mimeTypeOf(described));
		putIfPresent(output, CHILD_COUNT, childCountOf(described));
		putIfPresent(output, WEB_URL, described.get(WEB_URL));
		putIfPresent(output, PATH, relativePathOf(described));
		putIfPresent(output, "parentId", parentIdOf(described));
		putIfPresent(output, "createdDateTime", described.get("createdDateTime"));
		putIfPresent(output, "lastModifiedDateTime", described.get("lastModifiedDateTime"));
		putIfPresent(output, "createdBy", displayNameOf(described.get("createdBy")));
		putIfPresent(output, "lastModifiedBy", displayNameOf(described.get("lastModifiedBy")));

		// the shared facet is on the remote item for something shared in from
		// another drive, and on the item itself for something shared out of the
		// signed in user's own drive
		Object shared = described.get(SHARED);
		if (shared == null) {
			shared = item.get(SHARED);
		}
		output.put("isShared", shared != null || item.get(REMOTE_ITEM) != null);
		if (shared instanceof Map) {
			Map<?, ?> sharedMap = (Map<?, ?>) shared;
			putIfPresent(output, "sharedBy", displayNameOf(sharedMap.get(SHARED_BY)));
			putIfPresent(output, "sharedByEmail", emailOf(sharedMap.get(SHARED_BY)));
			putIfPresent(output, "sharedOwner", displayNameOf(sharedMap.get(OWNER)));
			putIfPresent(output, "sharedOwnerEmail", emailOf(sharedMap.get(OWNER)));
			putIfPresent(output, "sharedScope", sharedMap.get("scope"));
			putIfPresent(output, "sharedDateTime", sharedMap.get("sharedDateTime"));
		}
		return output;
	}

	/**
	 * Describe one drive.
	 *
	 * @param drive the drive as Graph returned it
	 * @return the drive as a map
	 */
	public static Map<String, Object> toDrive(Map<String, Object> drive) {
		Map<String, Object> output = new LinkedHashMap<>();
		output.put(ID, drive.get(ID));
		putIfPresent(output, NAME, drive.get(NAME));
		putIfPresent(output, "driveType", drive.get("driveType"));
		putIfPresent(output, OWNER, displayNameOf(drive.get(OWNER)));
		putIfPresent(output, "ownerEmail", emailOf(drive.get(OWNER)));
		putIfPresent(output, WEB_URL, drive.get(WEB_URL));

		Object quota = drive.get("quota");
		if (quota instanceof Map) {
			Map<?, ?> quotaMap = (Map<?, ?>) quota;
			putIfPresent(output, "quotaTotal", quotaMap.get("total"));
			putIfPresent(output, "quotaUsed", quotaMap.get("used"));
			putIfPresent(output, "quotaRemaining", quotaMap.get("remaining"));
		}
		return output;
	}

	/**
	 * Describe one sharing link.
	 *
	 * @param permission the permission as Graph returned it from a create link
	 *                   request
	 * @return the link as a map
	 */
	public static Map<String, Object> toSharingLink(Map<String, Object> permission) {
		Map<String, Object> output = new LinkedHashMap<>();
		output.put(ID, permission.get(ID));
		putIfPresent(output, "roles", permission.get("roles"));
		putIfPresent(output, "expirationDateTime", permission.get("expirationDateTime"));
		putIfPresent(output, "hasPassword", permission.get("hasPassword"));

		Object link = permission.get("link");
		if (link instanceof Map) {
			Map<?, ?> linkMap = (Map<?, ?>) link;
			putIfPresent(output, WEB_URL, linkMap.get(WEB_URL));
			putIfPresent(output, "type", linkMap.get("type"));
			putIfPresent(output, "scope", linkMap.get("scope"));
			putIfPresent(output, "preventsDownload", linkMap.get("preventsDownload"));
		}
		return output;
	}

	/**
	 * The item the fields are read off, which is the remote facet when the item
	 * only stands for something living in another drive.
	 *
	 * @param item the item as Graph returned it
	 * @return the item that carries the real fields
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, Object> effectiveItem(Map<String, Object> item) {
		Object remote = item.get(REMOTE_ITEM);
		if (remote instanceof Map) {
			return (Map<String, Object>) remote;
		}
		return item;
	}

	/**
	 * The drive an item lives in.
	 *
	 * <p>
	 * A shared item names its drive under the remote facet's parent, which is the
	 * whole point of reading it: that drive is not the signed in user's, so a
	 * caller cannot assume {@code /me/drive} and has to be told.
	 * </p>
	 *
	 * @param item     the item as Graph returned it
	 * @param fallback the drive the item was read from, or null when not known
	 * @return the drive id, or the fallback when the item does not name one
	 */
	public static String driveIdOf(Map<String, Object> item, String fallback) {
		Map<String, Object> described = effectiveItem(item);
		String driveId = stringOf(parentField(described, DRIVE_ID));
		if (driveId == null) {
			driveId = stringOf(parentField(item, DRIVE_ID));
		}
		return driveId == null ? fallback : driveId;
	}

	/**
	 * The id that addresses an item inside the drive {@link #driveIdOf} names.
	 *
	 * @param item the item as Graph returned it
	 * @return the item id, or null when there is none
	 */
	public static String itemIdOf(Map<String, Object> item) {
		return stringOf(effectiveItem(item).get(ID));
	}

	/**
	 * @param item a drive item as Graph returned it
	 * @return true when the item is a folder
	 */
	public static boolean isFolder(Map<String, Object> item) {
		return effectiveItem(item).get(FOLDER) != null;
	}

	/**
	 * @param item a drive item as Graph returned it
	 * @return the name the item carries in its drive, or null when it has none
	 */
	public static String nameOf(Map<String, Object> item) {
		return stringOf(effectiveItem(item).get(NAME));
	}

	/**
	 * The path of an item within its drive, with the Graph prefix taken off so it
	 * can be passed back as a {@code path}.
	 *
	 * @param item the item the path is read off
	 * @return the folder holding the item, relative to the drive root, or null when
	 *         Graph did not say
	 */
	private static String relativePathOf(Map<String, Object> item) {
		String path = stringOf(parentField(item, PATH));
		if (path == null) {
			return null;
		}
		int rootAt = path.indexOf(ROOT_PATH_PREFIX);
		if (rootAt >= 0) {
			path = path.substring(rootAt + ROOT_PATH_PREFIX.length());
		}
		while (path.startsWith("/")) {
			path = path.substring(1);
		}
		return path.isEmpty() ? null : path;
	}

	/**
	 * @param item the item the parent is read off
	 * @return the id of the folder holding it, or null when Graph did not say
	 */
	private static String parentIdOf(Map<String, Object> item) {
		return stringOf(parentField(item, ID));
	}

	/**
	 * @param item the item the field is read off
	 * @param key  the field of the parent reference to read
	 * @return the value, or null when there is no parent reference or no such field
	 */
	private static Object parentField(Map<String, Object> item, String key) {
		Object parentReference = item.get(PARENT_REFERENCE);
		if (!(parentReference instanceof Map)) {
			return null;
		}
		return ((Map<?, ?>) parentReference).get(key);
	}

	/**
	 * @param item the item as Graph returned it
	 * @return the mime type reported for a file, or null for a folder
	 */
	private static String mimeTypeOf(Map<String, Object> item) {
		Object file = item.get(FILE);
		if (!(file instanceof Map)) {
			return null;
		}
		return stringOf(((Map<?, ?>) file).get(MIME_TYPE));
	}

	/**
	 * @param item the item as Graph returned it
	 * @return how many items a folder holds, or null for a file
	 */
	private static Object childCountOf(Map<String, Object> item) {
		Object folder = item.get(FOLDER);
		if (!(folder instanceof Map)) {
			return null;
		}
		return ((Map<?, ?>) folder).get(CHILD_COUNT);
	}

	/**
	 * @param identitySet an {@code identitySet} as Graph returned it, such as a
	 *                    {@code lastModifiedBy} or an {@code owner}
	 * @return the person's display name, or null when there is none
	 */
	private static String displayNameOf(Object identitySet) {
		if (!(identitySet instanceof Map)) {
			return null;
		}
		Object user = ((Map<?, ?>) identitySet).get(USER);
		if (!(user instanceof Map)) {
			return null;
		}
		return stringOf(((Map<?, ?>) user).get(DISPLAY_NAME));
	}

	/**
	 * @param identitySet an {@code identitySet} as Graph returned it
	 * @return the person's email address, or null when there is none
	 */
	private static String emailOf(Object identitySet) {
		if (!(identitySet instanceof Map)) {
			return null;
		}
		Object user = ((Map<?, ?>) identitySet).get(USER);
		if (!(user instanceof Map)) {
			return null;
		}
		return stringOf(((Map<?, ?>) user).get(EMAIL));
	}

	/**
	 * @param value the value to read
	 * @return the value as a string, or null when there is nothing to read
	 */
	private static String stringOf(Object value) {
		if (value == null || value.toString().trim().isEmpty()) {
			return null;
		}
		return value.toString();
	}

	/**
	 * Set a value, and only when there is one.
	 *
	 * @param output the map being built
	 * @param key    the key to set
	 * @param value  the value, which is left out when it is null
	 */
	private static void putIfPresent(Map<String, Object> output, String key, Object value) {
		if (value != null) {
			output.put(key, value);
		}
	}

}

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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;

/**
 * Where the OneDrive files copied into a folder came from.
 *
 * <p>
 * A download leaves a small record beside the copy it wrote, at
 * {@code .semoss/onedrive/<file name>.json} in the copy's folder: the drive and
 * item it was read from, the version of the contents it held then, and a hash
 * of the bytes written. Without it a copy is only a file, and nothing can tell
 * that the file it was taken from has moved on.
 * </p>
 *
 * <p>
 * The record is what lets a later download bring the same copy up to date
 * rather than write a second one, lets a chat check its copies against
 * OneDrive, and lets an upload go back over the original only while the
 * original is still the version the copy was taken from. The hash is what keeps
 * all three from throwing work away: a copy somebody changed here is never
 * written over by a newer version from OneDrive.
 * </p>
 *
 * <p>
 * A record is named after its copy, so a copy that is renamed or deleted simply
 * stops being tracked and behaves like any other file.
 * </p>
 */
public final class MicrosoftOneDriveSources {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftOneDriveSources.class);

	private static final Gson GSON = new Gson();

	/** Under the folder SEMOSS keeps its own state in inside a working folder. */
	private static final String FOLDER = ".semoss/onedrive";
	private static final String EXTENSION = ".json";

	/**
	 * Where one copy came from.
	 *
	 * @param driveId              the drive the file lives in
	 * @param itemId               the file's id in that drive
	 * @param version              the version of the contents the copy was taken
	 *                             from, as OneDrive names it
	 * @param lastModifiedDateTime when that version was written
	 * @param lastModifiedBy       who wrote it
	 * @param sha256               hash of the bytes written here
	 * @param told                 what the owner was last told about a copy that
	 *                             could not go back over its original, so they are
	 *                             told once: the version of the original the copy
	 *                             disagrees with, or null
	 */
	public record Source(String driveId, String itemId, String version, String lastModifiedDateTime,
			String lastModifiedBy, String sha256, String told) {
	}

	private MicrosoftOneDriveSources() {

	}

	/**
	 * Records where a copy came from, hashing the copy as it is now.
	 *
	 * <p>
	 * A file whose version OneDrive did not name cannot be checked later, so it is
	 * left untracked, and any record it had is dropped.
	 * </p>
	 *
	 * @param copy                 the file written here
	 * @param driveId              the drive it was read from
	 * @param itemId               its id in that drive
	 * @param version              the version of its contents, or null when not
	 *                             known
	 * @param lastModifiedDateTime when that version was written, or null
	 * @param lastModifiedBy       who wrote it, or null
	 * @throws IOException if the copy cannot be read or the record cannot be
	 *                     written
	 */
	public static void record(File copy, String driveId, String itemId, String version, String lastModifiedDateTime,
			String lastModifiedBy) throws IOException {
		File record = recordOf(copy);
		if (version == null || itemId == null) {
			Files.deleteIfExists(record.toPath());
			return;
		}
		write(record, new Source(driveId, itemId, version, lastModifiedDateTime, lastModifiedBy, sha256(copy), null));
	}

	/**
	 * Notes what the owner was told about a copy that could not go back over its
	 * original, leaving the rest of its record as it is.
	 *
	 * @param copy   the file
	 * @param source its record
	 * @param told   what they were told; see {@link Source#told()}
	 * @throws IOException if the record cannot be written
	 */
	public static void tell(File copy, Source source, String told) throws IOException {
		write(recordOf(copy), new Source(source.driveId(), source.itemId(), source.version(),
				source.lastModifiedDateTime(), source.lastModifiedBy(), source.sha256(), told));
	}

	private static void write(File record, Source source) throws IOException {
		Files.createDirectories(record.getParentFile().toPath());
		// whole or not at all, since a reader may be looking at the same moment
		File temp = new File(record.getParentFile(), ".write-" + UUID.randomUUID());
		try {
			Files.writeString(temp.toPath(), GSON.toJson(source), StandardCharsets.UTF_8);
			Files.move(temp.toPath(), record.toPath(), StandardCopyOption.REPLACE_EXISTING);
		} finally {
			Files.deleteIfExists(temp.toPath());
		}
	}

	/**
	 * Reads where a file came from.
	 *
	 * @param copy the file
	 * @return its source, or null when it is not a tracked copy of a OneDrive file
	 */
	public static Source read(File copy) {
		File record = recordOf(copy);
		if (!record.isFile()) {
			return null;
		}
		try {
			Source source = GSON.fromJson(Files.readString(record.toPath(), StandardCharsets.UTF_8), Source.class);
			if (source == null || source.itemId() == null || source.version() == null || source.sha256() == null) {
				return null;
			}
			return source;
		} catch (IOException | JsonParseException e) {
			classLogger.warn("Could not read the OneDrive source of {}; it is treated as an ordinary file",
					copy.getName(), e);
			return null;
		}
	}

	/**
	 * Lists the tracked copies in a folder. A record whose copy is gone is removed
	 * on the way, so a file that later takes the same name is not mistaken for it.
	 *
	 * @param folder the folder the copies were written into
	 * @return each copy still there with its source, by name
	 */
	public static Map<File, Source> list(File folder) {
		Map<File, Source> copies = new LinkedHashMap<>();
		File[] records = new File(folder, FOLDER).listFiles((dir, name) -> name.endsWith(EXTENSION));
		if (records == null) {
			return copies;
		}
		Arrays.sort(records);
		for (File record : records) {
			String name = record.getName();
			File copy = new File(folder, name.substring(0, name.length() - EXTENSION.length()));
			if (!copy.isFile()) {
				if (!record.delete()) {
					classLogger.warn("Could not remove the OneDrive record left by {}", copy.getName());
				}
				continue;
			}
			Source source = read(copy);
			if (source != null) {
				copies.put(copy, source);
			}
		}
		return copies;
	}

	/**
	 * Finds a copy of one OneDrive file that nobody has changed here.
	 *
	 * @param folder  the folder to look in
	 * @param driveId the drive the file lives in
	 * @param itemId  its id in that drive
	 * @return the copy with its source, or null when there is none or every one
	 *         was changed here
	 */
	public static Map.Entry<File, Source> untouchedCopy(File folder, String driveId, String itemId) {
		for (Map.Entry<File, Source> copy : list(folder).entrySet()) {
			Source source = copy.getValue();
			if (source.itemId().equals(itemId) && Objects.equals(source.driveId(), driveId)
					&& isUntouched(copy.getKey(), source)) {
				return copy;
			}
		}
		return null;
	}

	/**
	 * Whether a copy still holds the bytes that were written when it was taken.
	 *
	 * @param copy   the file
	 * @param source what was recorded for it
	 * @return false when it was changed here, or cannot be read
	 */
	public static boolean isUntouched(File copy, Source source) {
		try {
			return source.sha256().equals(sha256(copy));
		} catch (IOException e) {
			classLogger.warn("Could not read {} to compare it with what was downloaded", copy.getName(), e);
			return false;
		}
	}

	private static File recordOf(File copy) {
		return new File(copy.getAbsoluteFile().getParentFile(), FOLDER + "/" + copy.getName() + EXTENSION);
	}

	private static String sha256(File file) throws IOException {
		MessageDigest digest;
		try {
			digest = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			// every Java runtime has to provide it
			throw new IllegalStateException(e);
		}
		byte[] buffer = new byte[64 * 1024];
		try (InputStream in = Files.newInputStream(file.toPath())) {
			for (int read = in.read(buffer); read != -1; read = in.read(buffer)) {
				digest.update(buffer, 0, read);
			}
		}
		return HexFormat.of().formatHex(digest.digest());
	}
}

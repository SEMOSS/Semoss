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
package prerna.engine.impl.function.mail.attachment;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.om.Insight;

/**
 * The one place a mailbox is allowed to write to disk.
 *
 * <p>
 * An attachment is a file whose name was chosen by whoever sent the mail, which
 * makes it the least trustworthy input any of these engines handle. Every
 * mailbox goes through here so that is dealt with once: the name is reduced to
 * something harmless, the resolved path is checked to still be inside the
 * insight folder, an existing file is never overwritten, and anything over the
 * configured size is refused.
 *
 * <p>
 * A refusal returns null rather than throwing. The message has already been
 * read and is worth returning; the attachment not being saved is reported in
 * the log and the caller carries on describing it.
 */
public final class AttachmentStore {

	private static final Logger classLogger = LogManager.getLogger(AttachmentStore.class);
	private static final int COPY_BUFFER_SIZE = 8192;

	private final long maximumBytes;

	/**
	 * @param maximumBytes the largest attachment that will be written
	 */
	public AttachmentStore(long maximumBytes) {
		this.maximumBytes = maximumBytes;
	}

	/**
	 * Save an attachment that has already been read into memory, which is the shape
	 * Graph hands them over in.
	 *
	 * @param rawName the file name the sender chose
	 * @param bytes   the file
	 * @param insight the insight to write into
	 * @return where it was written, or null when it was too large
	 * @throws IOException when it cannot be written
	 */
	public File save(String rawName, byte[] bytes, Insight insight) throws IOException {
		if (bytes.length > this.maximumBytes) {
			classLogger.warn("The attachment {} is larger than the configured maximum of {} bytes and was not saved",
					rawName, this.maximumBytes);
			return null;
		}
		File target = target(rawName, insight);
		Files.write(target.toPath(), bytes);
		return target;
	}

	/**
	 * Save an attachment as it is read, which is the shape the mail protocols hand
	 * them over in.
	 *
	 * <p>
	 * The size is counted while copying rather than asked for beforehand, because a
	 * protocol will happily stream a part without saying how long it is first. A
	 * file that turns out to be too large is deleted rather than left half written.
	 *
	 * @param rawName the file name the sender chose
	 * @param input   the file, which is closed either way
	 * @param insight the insight to write into
	 * @return where it was written, or null when it turned out to be too large
	 * @throws IOException when it cannot be read or written
	 */
	public File save(String rawName, InputStream input, Insight insight) throws IOException {
		File target = target(rawName, insight);
		long written = 0;
		try (InputStream stream = input; OutputStream output = Files.newOutputStream(target.toPath())) {
			byte[] buffer = new byte[COPY_BUFFER_SIZE];
			int read;
			while ((read = stream.read(buffer)) >= 0) {
				written += read;
				if (written > this.maximumBytes) {
					classLogger.warn(
							"The attachment {} is larger than the configured maximum of {} bytes and was not saved",
							rawName, this.maximumBytes);
					output.close();
					Files.deleteIfExists(target.toPath());
					return null;
				}
				output.write(buffer, 0, read);
			}
		}
		return target;
	}

	/**
	 * Decide where an attachment is going to be written.
	 *
	 * <p>
	 * The containment check is on the canonical path, after the name has been
	 * sanitized, so it holds even if sanitizing were ever loosened.
	 *
	 * @param rawName the file name the sender chose
	 * @param insight the insight to write into
	 * @return the file to write
	 * @throws IOException              when the path would land outside the insight
	 *                                  folder, or the folder cannot be made
	 * @throws IllegalArgumentException when there is no insight to write into
	 */
	private File target(String rawName, Insight insight) throws IOException {
		if (insight == null) {
			throw new IllegalArgumentException("An insight is required to save an attachment");
		}
		File folder = new File(insight.getInsightFolder()).getCanonicalFile();
		Files.createDirectories(folder.toPath());
		File target = uniqueFile(folder, sanitizeName(rawName)).getCanonicalFile();
		if (!target.toPath().startsWith(folder.toPath())) {
			throw new IOException("Refusing to write an attachment outside of the insight folder");
		}
		return target;
	}

	/**
	 * Reduce a sender's file name to something safe to write.
	 *
	 * <p>
	 * Only the last path segment is kept, and everything outside letters, digits
	 * and a few punctuation characters becomes an underscore, so a name carrying
	 * directories, a drive letter or anything a shell would read is left with none
	 * of it. A name that ends up empty, or as a directory reference, is replaced
	 * outright.
	 *
	 * @param rawName the file name the sender chose, which may be null
	 * @return a file name, never null and never a path
	 */
	public static String sanitizeName(String rawName) {
		if (rawName == null) {
			return "attachment";
		}
		String fileName = new File(rawName).getName().replaceAll("[^a-zA-Z0-9._-]", "_");
		return fileName.isEmpty() || ".".equals(fileName) || "..".equals(fileName) ? "attachment" : fileName;
	}

	/**
	 * Find a name nothing is using yet, so saving an attachment never destroys a
	 * file the insight already had.
	 *
	 * <p>
	 * Two messages carrying a file of the same name is ordinary, so a counter is
	 * added before the extension rather than after it, keeping the file openable by
	 * whatever opens that kind.
	 *
	 * @param folder   the folder to write into
	 * @param fileName the sanitized name to start from
	 * @return a file that does not exist
	 * @throws IOException when a thousand names are already taken, which means
	 *                     something is looping rather than that the folder is full
	 */
	public static File uniqueFile(File folder, String fileName) throws IOException {
		File target = new File(folder, fileName);
		if (!target.exists()) {
			return target;
		}
		String base = fileName;
		String extension = "";
		int dot = fileName.lastIndexOf('.');
		if (dot > 0) {
			base = fileName.substring(0, dot);
			extension = fileName.substring(dot);
		}
		for (int i = 1; i < 1000; i++) {
			target = new File(folder, base + "_" + i + extension);
			if (!target.exists()) {
				return target;
			}
		}
		throw new IOException("Could not choose an unused file name for attachment " + fileName);
	}
}

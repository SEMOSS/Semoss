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
package prerna.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

import prerna.util.gson.GsonUtility;
import prerna.util.gson.LocalDateTimeAdapter;
import prerna.util.gson.ZonedDateTimeAdapter;

/**
 * Creates and extracts zip files. Extraction keeps every entry inside the
 * folder it is extracted into and holds each zip to this server's limits
 * ({@link #ZIP_MAX_ENTRIES}, {@link #ZIP_MAX_EXTRACTED_BYTES} and
 * {@link #ZIP_MAX_COMPRESSION_RATIO}), so a zip cannot write outside its folder
 * or fill the disk.
 */
public final class ZipUtils {

	private static final Logger classLogger = LogManager.getLogger(ZipUtils.class);

	private static final int BUFFER_SIZE = 2048;

	/**
	 * RDF_Map key for the most entries a zip may hold to be extracted. Defaults to
	 * 200,000.
	 */
	public static final String ZIP_MAX_ENTRIES = "ZIP_MAX_ENTRIES";

	// how many zips inside zips unzipNested will open
	public static final int MAX_NESTED_ZIP_DEPTH = 5;

	/**
	 * RDF_Map key for the most bytes a zip may expand to when extracted, counted as
	 * they are written rather than from the sizes the zip declares. Defaults to 20
	 * GB.
	 */
	public static final String ZIP_MAX_EXTRACTED_BYTES = "ZIP_MAX_EXTRACTED_BYTES";

	/**
	 * RDF_Map key for the most a zip may expand to as a multiple of its own size,
	 * held once more than 100 MB is extracted. Defaults to 1,000: a zip whose
	 * entries do not share their compressed data cannot pass about 1,032, the most
	 * DEFLATE compresses, so the default refuses bombs that reuse compressed data
	 * and ones built from long runs of one byte.
	 */
	public static final String ZIP_MAX_COMPRESSION_RATIO = "ZIP_MAX_COMPRESSION_RATIO";

	private static final long DEFAULT_MAX_ENTRIES = 200_000;
	private static final long DEFAULT_MAX_EXTRACTED_BYTES = 20L * 1024 * 1024 * 1024;
	private static final long DEFAULT_MAX_COMPRESSION_RATIO = 1_000;

	/**
	 * Extracted bytes below which the compression ratio is not held, so small, very
	 * compressible zips always extract.
	 */
	private static final long RATIO_CHECK_FLOOR = 100L * 1024 * 1024;

	/**
	 * Separator between the parts of a zip entry's name. Always {@code /}: a name
	 * built with the Windows separator {@code \} breaks traversal of the zip.
	 */
	private static final String FILE_SEPARATOR = "/";

	/**
	 * Writes the JSON files {@link #zipObjectToFile} adds to a zip: pretty printed,
	 * without escaping HTML characters, with numbers read back as long or double,
	 * and with {@link LocalDateTime} and {@link ZonedDateTime} values through their
	 * adapters.
	 */
	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.registerTypeAdapter(LocalDateTime.class, new LocalDateTimeAdapter())
			.registerTypeAdapter(ZonedDateTime.class, new ZonedDateTimeAdapter()).setPrettyPrinting().create();

	private ZipUtils() {

	}

	/**
	 * Zips a folder into a new zip file, with the folder itself as the zip's top
	 * level entry, and returns the zip still open so more entries can be added. The
	 * caller closes it. Empty folders add no entry.
	 *
	 * @param folderPath  the folder to zip
	 * @param zipFilePath where the zip file is written, replacing any file there
	 * @return the open zip
	 * @throws IllegalArgumentException if the zip file cannot be created or the
	 *                                  folder cannot be added to it
	 */
	public static ZipOutputStream zipFolder(String folderPath, String zipFilePath)
			throws FileNotFoundException, IOException {
		return zipFolder(folderPath, zipFilePath, null, null);
	}

	/**
	 * Zips a folder into a new zip file, with the folder itself as the zip's top
	 * level entry, leaving out the folders and files whose paths in the zip are
	 * listed to ignore, and returns the zip still open so more entries can be
	 * added. The caller closes it. Empty folders add no entry.
	 *
	 * @param folderPath  the folder to zip
	 * @param zipFilePath where the zip file is written, replacing any file there
	 * @param ignoreDirs  paths in the zip of folders to leave out, starting with
	 *                    the folder's own name, such as
	 *                    {@code MyApp__id/app_root/version/.git}, or null for none
	 * @param ignoreFiles paths in the zip of files to leave out, or null for none
	 * @return the open zip
	 * @throws IllegalArgumentException if the zip file cannot be created or the
	 *                                  folder cannot be added to it
	 */
	public static ZipOutputStream zipFolder(String folderPath, String zipFilePath, List<String> ignoreDirs,
			List<String> ignoreFiles) throws FileNotFoundException {
		FileOutputStream fos;
		try {
			fos = new FileOutputStream(zipFilePath);
		} catch (Exception e) {
			classLogger.error("Failed to create zip output file at '{}'", zipFilePath, e);
			throw new IllegalArgumentException("Could not find file. See logs for details.");
		}
		ZipOutputStream zos = new ZipOutputStream(fos);
		File dir = new File(folderPath);
		try {
			classLogger.info("Creating zip '{}' from folder '{}' (ignored directories: {}, ignored files: {})",
					zipFilePath, folderPath, ignoreDirs, ignoreFiles);
			addAllToZip(dir, zos, null, ignoreDirs, ignoreFiles);
		} catch (Exception e) {
			classLogger.error("Failed to add folder '{}' to zip file '{}'", folderPath, zipFilePath, e);
			throw new IllegalArgumentException("Could not add folder to zip. See logs for details.");
		}
		return zos;
	}

	/**
	 * Adds a file to a zip at its top level, under the file's own name.
	 *
	 * @param file the file to add
	 * @param zos  the open zip
	 * @throws FileNotFoundException if the file does not exist
	 * @throws IOException           if the file cannot be read or written to the
	 *                               zip
	 */
	public static void addToZipFile(File file, ZipOutputStream zos) throws FileNotFoundException, IOException {
		ZipEntry zipEntry = new ZipEntry(file.getName());
		zos.putNextEntry(zipEntry);

		FileInputStream fis = null;
		try {
			byte[] buffer = new byte[BUFFER_SIZE];
			int length;
			fis = new FileInputStream(file);
			while ((length = fis.read(buffer)) >= 0) {
				zos.write(buffer, 0, length);
			}
		} finally {
			if (fis != null) {
				fis.close();
			}
		}
		zos.closeEntry();
	}

	/**
	 * Writes an object as pretty printed JSON to a file, then adds that file to a
	 * zip under a folder. The file stays on disk.
	 *
	 * @param zos             the open zip
	 * @param prefixForZip    the folder in the zip to add the file under, or null
	 *                        or empty for the zip's top level
	 * @param filePathToWrite where the JSON file is written; its name is the
	 *                        entry's name in the zip
	 * @param objToWrite      the object to write
	 * @throws IOException if the file cannot be written or added to the zip
	 */
	public static void zipObjectToFile(ZipOutputStream zos, String prefixForZip, String filePathToWrite,
			Object objToWrite) throws IOException {
		File newFile = new File(filePathToWrite);
		GsonUtility.writeObjectToJsonFile(newFile, GSON, objToWrite);
		ZipUtils.addToZipFile(newFile, zos, prefixForZip);
	}

	/**
	 * Adds a file to a zip under a folder, as {@code prefix/name}, or at the zip's
	 * top level when there is no prefix.
	 *
	 * @param file   the file to add
	 * @param zos    the open zip
	 * @param prefix the folder in the zip to add the file under, or null or empty
	 *               for the zip's top level
	 * @throws FileNotFoundException if the file does not exist
	 * @throws IOException           if the file cannot be read or written to the
	 *                               zip
	 */
	public static void addToZipFile(File file, ZipOutputStream zos, String prefix)
			throws FileNotFoundException, IOException {
		ZipEntry zipEntry = null;
		if (prefix == null || prefix.isEmpty()) {
			zipEntry = new ZipEntry(file.getName());
		} else {
			zipEntry = new ZipEntry(prefix + FILE_SEPARATOR + file.getName());
		}
		zos.putNextEntry(zipEntry);

		FileInputStream fis = null;
		try {
			byte[] buffer = new byte[BUFFER_SIZE];
			int length;
			fis = new FileInputStream(file);
			while ((length = fis.read(buffer)) >= 0) {
				zos.write(buffer, 0, length);
			}
		} finally {
			if (fis != null) {
				fis.close();
			}
		}
		zos.closeEntry();
	}

	/**
	 * Adds a file, or a folder and everything in it, to a zip under a prefix,
	 * leaving out the folders and files whose paths in the zip are listed to
	 * ignore. Empty folders add no entry.
	 *
	 * @param file        the file or folder to add
	 * @param zos         the open zip
	 * @param prefix      the path in the zip of the folder holding the file, or
	 *                    null at the zip's top level
	 * @param ignoreDirs  paths in the zip of folders to leave out, or null for none
	 * @param ignoreFiles paths in the zip of files to leave out, or null for none
	 * @throws FileNotFoundException if a file disappears while it is added
	 * @throws IOException           if a file cannot be read or written to the zip
	 */
	private static void addAllToZip(File file, ZipOutputStream zos, String prefix, List<String> ignoreDirs,
			List<String> ignoreFiles) throws FileNotFoundException, IOException {
		if (file.isDirectory()) {
			String subPrefix = file.getName();
			if (prefix != null) {
				subPrefix = prefix + FILE_SEPARATOR + file.getName();
			}
			// make sure its not in the ignore list of folders
			if (ignoreDirs == null || !ignoreDirs.contains(subPrefix)) {
				File[] files = file.listFiles();
				for (File subF : files) {
					addAllToZip(subF, zos, subPrefix, ignoreDirs, ignoreFiles);
				}
			}
		} else {
			String fileName = file.getName();
			if (prefix != null) {
				fileName = prefix + FILE_SEPARATOR + file.getName();
			}
			// make sure its not in the ignore list if we have one
			if (ignoreFiles == null || !ignoreFiles.contains(fileName)) {
				ZipEntry zipEntry = new ZipEntry(fileName);
				zos.putNextEntry(zipEntry);
				FileInputStream fis = null;
				try {
					byte[] buffer = new byte[BUFFER_SIZE];
					int length;
					fis = new FileInputStream(file);
					while ((length = fis.read(buffer)) >= 0) {
						zos.write(buffer, 0, length);
					}
				} finally {
					if (fis != null) {
						fis.close();
					}
				}
				zos.closeEntry();
			}
		}
	}

	/**
	 * Extracts a zip file into a folder and lists what it held. Every entry must
	 * land inside the folder, and the zip must stay within this server's extraction
	 * limits ({@link #ZIP_MAX_ENTRIES}, {@link #ZIP_MAX_EXTRACTED_BYTES} and
	 * {@link #ZIP_MAX_COMPRESSION_RATIO}). A zip that does not is refused, and the
	 * files and folders its extraction created are removed.
	 *
	 * @param zipFilePath the zip file
	 * @param destination the folder to extract into
	 * @return the zip's directories under {@code DIR} and its files under
	 *         {@code FILE}, as paths relative to the folder
	 * @throws ZipException if an entry would land outside the folder or the zip
	 *                      passes a limit
	 * @throws IOException  if the zip cannot be read or an entry cannot be written
	 */
	public static Map<String, List<String>> unzip(String zipFilePath, String destination) throws IOException {
		Path zip = Paths.get(Utility.normalizePath(zipFilePath));
		try (ZipFile zipIn = new ZipFile(zip.toFile())) {
			Extraction extraction = new Extraction(zip, destination, zipIn.size());
			try {
				Enumeration<? extends ZipEntry> entries = zipIn.entries();
				while (entries.hasMoreElements()) {
					ZipEntry entry = entries.nextElement();
					String entryPath = relativeEntryPath(entry);
					if (!entryPath.isEmpty()) {
						extraction.extract(zipIn, entry, extraction.resolve(entryPath, entry));
					}
				}
			} catch (IOException | RuntimeException e) {
				extraction.undo(e);
				throw e;
			}
		}
		return listFilesInZip(Paths.get(zipFilePath));
	}

	// unzip, then unzip each zip it held beside itself (limited depth); returns the absolute paths of the files that are not zips
	public static List<String> unzipNested(String zipFilePath, String destination) throws IOException {
		return unzipNested(zipFilePath, destination, 0);
	}

	private static List<String> unzipNested(String zipFilePath, String destination, int depth) throws IOException {
		if (depth > MAX_NESTED_ZIP_DEPTH) {
			throw new ZipException("The zip holds more than " + MAX_NESTED_ZIP_DEPTH + " levels of zips");
		}
		Path folder = Paths.get(destination).toAbsolutePath().normalize();
		List<String> files = new ArrayList<>();
		for (String relative : unzip(zipFilePath, destination).get("FILE")) {
			Path extracted = folder.resolve(relative).normalize();
			String name = extracted.getFileName().toString();
			if (name.toLowerCase().endsWith(".zip")) {
				String base = name.substring(0, name.length() - ".zip".length());
				files.addAll(unzipNested(extracted.toString(), extracted.resolveSibling(base).toString(), depth + 1));
			} else {
				files.add(extracted.toString().replace('\\', '/'));
			}
		}
		return files;
	}

	/**
	 * Extracts a zip file into a folder. With {@code override}, the extracted
	 * entries replace what is already there. Without it, each top level file or
	 * directory of the zip whose name is taken in the destination is extracted
	 * beside it under the next copy number, such as {@code data (2)}, so nothing
	 * already in the destination is touched. Every entry must land inside the
	 * folder, and the zip must stay within this server's extraction limits
	 * ({@link #ZIP_MAX_ENTRIES}, {@link #ZIP_MAX_EXTRACTED_BYTES} and
	 * {@link #ZIP_MAX_COMPRESSION_RATIO}). A zip that does not is refused, and the
	 * files and folders its extraction created are removed.
	 *
	 * @param zipFilePath the zip file
	 * @param destination the folder to extract into
	 * @param override    whether extracted entries replace existing ones
	 * @return each top level name in the zip mapped to the name it was extracted
	 *         as, in zip order
	 * @throws ZipException if an entry would land outside the folder or the zip
	 *                      passes a limit
	 * @throws IOException  if the zip cannot be read or an entry cannot be written
	 */
	public static Map<String, String> unzip(String zipFilePath, String destination, boolean override)
			throws IOException {
		Map<String, String> topLevelNames = new LinkedHashMap<>();
		Path zip = Paths.get(Utility.normalizePath(zipFilePath));
		try (ZipFile zipIn = new ZipFile(zip.toFile())) {
			Extraction extraction = new Extraction(zip, destination, zipIn.size());
			try {
				Enumeration<? extends ZipEntry> entries = zipIn.entries();
				while (entries.hasMoreElements()) {
					ZipEntry entry = entries.nextElement();
					String entryPath = relativeEntryPath(entry);
					if (entryPath.isEmpty()) {
						continue;
					}
					int slash = entryPath.indexOf('/');
					String topLevel = slash < 0 ? entryPath : entryPath.substring(0, slash);
					String extractedTopLevel = topLevelNames.get(topLevel);
					if (extractedTopLevel == null) {
						Path requested = extraction.resolve(topLevel, entry);
						if (override) {
							extractedTopLevel = topLevel;
						} else {
							File reserved = FileSystemUtil.reserveAvailableFile(requested.toFile(),
									slash >= 0 || entry.isDirectory());
							extraction.created(reserved.toPath());
							extractedTopLevel = reserved.getName();
						}
						topLevelNames.put(topLevel, extractedTopLevel);
					}
					String extractedPath = extractedTopLevel + (slash < 0 ? "" : entryPath.substring(slash));
					extraction.extract(zipIn, entry, extraction.resolve(extractedPath, entry));
				}
			} catch (IOException | RuntimeException e) {
				extraction.undo(e);
				throw e;
			}
		}
		return topLevelNames;
	}

	/**
	 * The path an entry names, relative to the folder it is extracted into:
	 * normalized, with separators as {@code /} and no leading {@code /}. Empty for
	 * an entry that names the folder itself.
	 *
	 * @param entry the zip entry
	 * @return the entry's relative path
	 * @throws ZipException if the path climbs above the folder
	 */
	private static String relativeEntryPath(ZipEntry entry) throws ZipException {
		String path;
		try {
			path = Utility.normalizePath(entry.getName());
		} catch (IllegalArgumentException e) {
			throw outsideFolder(entry);
		}
		while (path.startsWith("/")) {
			path = path.substring(1);
		}
		return path;
	}

	/**
	 * The refusal for an entry that would be extracted outside its folder.
	 *
	 * @param entry the zip entry
	 * @return the exception to throw
	 */
	private static ZipException outsideFolder(ZipEntry entry) {
		return new ZipException(
				"The zip entry '" + entry.getName() + "' points outside the folder it is extracted into");
	}

	/**
	 * Reads an extraction limit from RDF_Map, or its default when the setting is
	 * missing or not a positive whole number.
	 *
	 * @param key          the RDF_Map key
	 * @param defaultValue the limit when the setting is missing or invalid
	 * @return the limit
	 */
	private static long limit(String key, long defaultValue) {
		String value = Utility.getDIHelperProperty(key);
		if (value == null || value.isBlank()) {
			return defaultValue;
		}
		long parsed = NumberUtils.toLong(value.trim(), 0);
		if (parsed > 0) {
			return parsed;
		}
		classLogger.warn("Ignoring {} '{}', which is not a positive whole number, and using {}", key, value,
				defaultValue);
		return defaultValue;
	}

	/**
	 * Deletes a file or an empty folder, logging rather than throwing when it
	 * cannot.
	 *
	 * @param path the file or folder to delete
	 */
	private static void deleteQuietly(Path path) {
		try {
			Files.deleteIfExists(path);
		} catch (IOException e) {
			classLogger.warn("Could not remove '{}' left by a zip extraction", path, e);
		}
	}

	/**
	 * One zip being extracted into a folder. It keeps every entry inside the
	 * folder, never writing through a symbolic link already there, holds the zip to
	 * the extraction limits, and remembers what it created, so a refused zip leaves
	 * nothing of its own behind.
	 */
	private static final class Extraction {

		private final Path folder;
		private final long zipSize;
		private final long maxExtractedBytes;
		private final long maxCompressionRatio;
		private final List<Path> created = new ArrayList<>();
		private final Set<Path> checkedDirectories = new HashSet<>();
		private long extractedBytes = 0;

		/**
		 * Starts an extraction, refusing a zip with more entries than the limit and
		 * creating the folder when it does not exist.
		 *
		 * @param zip         the zip file
		 * @param destination the folder to extract into
		 * @param entryCount  how many entries the zip holds
		 * @throws IOException if the zip has too many entries or the folder cannot be
		 *                     created
		 */
		private Extraction(Path zip, String destination, int entryCount) throws IOException {
			long maxEntries = limit(ZIP_MAX_ENTRIES, DEFAULT_MAX_ENTRIES);
			if (entryCount > maxEntries) {
				throw new ZipException("The zip has " + entryCount + " entries, more than the " + maxEntries
						+ " this server extracts (" + ZIP_MAX_ENTRIES + ")");
			}
			Path requested = Paths.get(destination);
			Files.createDirectories(requested);
			this.folder = requested.toRealPath();
			this.zipSize = Math.max(Files.size(zip), 1);
			this.maxExtractedBytes = limit(ZIP_MAX_EXTRACTED_BYTES, DEFAULT_MAX_EXTRACTED_BYTES);
			this.maxCompressionRatio = limit(ZIP_MAX_COMPRESSION_RATIO, DEFAULT_MAX_COMPRESSION_RATIO);
			this.checkedDirectories.add(this.folder);
		}

		/**
		 * Where a relative path lands in the folder.
		 *
		 * @param relativePath the path inside the folder
		 * @param entry        the zip entry it comes from
		 * @return the absolute, normalized path
		 * @throws ZipException if the path lands outside the folder
		 */
		private Path resolve(String relativePath, ZipEntry entry) throws ZipException {
			Path target = folder.resolve(relativePath).normalize();
			if (!target.startsWith(folder)) {
				throw outsideFolder(entry);
			}
			return target;
		}

		/**
		 * Records a file or folder created for this extraction outside of
		 * {@link #extract}, such as a reserved top level name.
		 *
		 * @param path the file or folder created
		 */
		private void created(Path path) {
			created.add(path);
		}

		/**
		 * Extracts one entry. A directory is created. A file is written beside its
		 * target and then moved over it, so a file it replaces is never left half
		 * written and a symbolic link at the target is replaced rather than written
		 * through.
		 *
		 * @param zip    the zip file
		 * @param entry  the entry to extract
		 * @param target where the entry lands
		 * @throws IOException if the entry passes a limit or cannot be written
		 */
		private void extract(ZipFile zip, ZipEntry entry, Path target) throws IOException {
			if (entry.isDirectory()) {
				createDirectories(target, entry);
				return;
			}
			createDirectories(target.getParent(), entry);
			boolean existed = Files.exists(target, LinkOption.NOFOLLOW_LINKS);
			Path part = target.resolveSibling("." + UUID.randomUUID() + ".unzip");
			try {
				try (InputStream in = zip.getInputStream(entry);
						OutputStream out = Files.newOutputStream(part, StandardOpenOption.CREATE_NEW,
								StandardOpenOption.WRITE)) {
					copy(in, out);
				}
				Files.move(part, target, StandardCopyOption.REPLACE_EXISTING);
			} finally {
				deleteQuietly(part);
			}
			if (!existed) {
				created.add(target);
			}
		}

		/**
		 * Copies an entry's bytes, holding the whole extraction to the byte and
		 * compression ratio limits as it goes.
		 *
		 * @param in  the entry's bytes
		 * @param out where they are written
		 * @throws IOException if a limit is passed or the bytes cannot be copied
		 */
		private void copy(InputStream in, OutputStream out) throws IOException {
			byte[] buffer = new byte[BUFFER_SIZE];
			int read;
			while ((read = in.read(buffer)) != -1) {
				extractedBytes += read;
				if (extractedBytes > maxExtractedBytes) {
					throw new ZipException(
							"The zip expands to more than " + FileUtils.byteCountToDisplaySize(maxExtractedBytes)
									+ ", the most this server extracts (" + ZIP_MAX_EXTRACTED_BYTES + ")");
				}
				if (extractedBytes > RATIO_CHECK_FLOOR && extractedBytes / zipSize > maxCompressionRatio) {
					throw new ZipException("The zip expands to more than " + maxCompressionRatio
							+ " times its size, the most this server extracts (" + ZIP_MAX_COMPRESSION_RATIO + ")");
				}
				out.write(buffer, 0, read);
			}
		}

		/**
		 * Creates a directory and its missing parents inside the folder, after checking
		 * that the nearest one already there really is inside the folder, so a symbolic
		 * link cannot carry the extraction out of it.
		 *
		 * @param directory the directory to create
		 * @param entry     the zip entry it is created for
		 * @throws IOException if the directory would be outside the folder or cannot be
		 *                     created
		 */
		private void createDirectories(Path directory, ZipEntry entry) throws IOException {
			if (checkedDirectories.contains(directory)) {
				return;
			}
			Deque<Path> missing = new ArrayDeque<>();
			Path existing = directory;
			while (!Files.exists(existing, LinkOption.NOFOLLOW_LINKS)) {
				missing.push(existing);
				existing = existing.getParent();
			}
			Path realExisting;
			try {
				realExisting = existing.toRealPath();
			} catch (NoSuchFileException e) {
				throw outsideFolder(entry);
			}
			if (!realExisting.startsWith(folder)) {
				throw outsideFolder(entry);
			}
			for (Path next : missing) {
				Files.createDirectory(next);
				created.add(next);
			}
			checkedDirectories.add(directory);
		}

		/**
		 * Removes the files and folders this extraction created, newest first, after it
		 * failed.
		 *
		 * @param cause why the extraction failed
		 */
		private void undo(Exception cause) {
			classLogger.warn("Removing what was extracted into '{}' after the extraction failed: {}", folder,
					cause.getMessage());
			for (int i = created.size() - 1; i >= 0; i--) {
				deleteQuietly(created.get(i));
			}
		}
	}

	/**
	 * Lists the folders and files a zip holds, as paths without a leading
	 * {@code /}, the form git takes them in. The zip is read, not extracted.
	 *
	 * @param fromZip the zip file
	 * @return the zip's folders under {@code DIR} and its files under {@code FILE}
	 * @throws IOException if the zip cannot be opened or read
	 */
	public static Map<String, List<String>> listFilesInZip(Path fromZip) throws IOException {
		FileSystem zipFs = null;
		Map<String, List<String>> paths = new HashMap<>();
		List<String> dirs = new ArrayList<>();
		List<String> files = new ArrayList<>();
		try {
			// need to tell that this is a zip (jar)
			zipFs = FileSystems.newFileSystem(URI.create("jar:" + fromZip.toUri().toString()), Map.of());
			for (Path root : zipFs.getRootDirectories()) {
				Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
					@Override
					public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
						// clean file path for git
						String filePath = file.toString();
						if (file.startsWith("/")) {
							filePath = filePath.replaceFirst("/", "");
							if (!filePath.equals("")) {
								files.add(filePath);
							}
						}
						return FileVisitResult.CONTINUE;
					}

					@Override
					public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
						// clean file path for git
						String pathDir = dir.toString().replaceFirst("/", "");
						if (!pathDir.equals("")) {
							dirs.add(pathDir);
						}

						return super.preVisitDirectory(dir, attrs);
					}
				});
			}
		} catch (Exception e) {
			classLogger.error("Failed to list entries in zip file '{}'", fromZip, e);
			throw e;
		} finally {
			try {
				if (zipFs != null) {
					zipFs.close();
				}
			} catch (IOException e) {
				classLogger.error("Failed to close zip file system for '{}'", fromZip, e);
			}
		}
		paths.put("DIR", dirs);
		paths.put("FILE", files);
		return paths;
	}

	/**
	 * Compresses a file with gzip into a new file.
	 *
	 * @param sourceFile the file to compress
	 * @param gzipFile   where the gzip file is written, replacing any file there
	 * @throws IOException if the file cannot be read or the gzip file cannot be
	 *                     written
	 */
	public static void compressGzipFile(String sourceFile, String gzipFile) throws IOException {
		try (FileOutputStream fos = new FileOutputStream(gzipFile);
				GZIPOutputStream gzipOS = new GZIPOutputStream(fos);
				FileInputStream fis = new FileInputStream(sourceFile)) {
			byte[] buffer = new byte[1024];
			int len;
			while ((len = fis.read(buffer)) > 0) {
				gzipOS.write(buffer, 0, len);
			}
		}
	}

}

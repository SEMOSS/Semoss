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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public final class FileSystemUtil {

	private static final Logger classLogger = LogManager.getLogger(FileSystemUtil.class);

	/**
	 * Describes the override key of the reactors that save file contents. They
	 * replace by default because editors save the file they have open again and
	 * again.
	 */
	public static final String OVERRIDE_SAVE_DESCRIPTION = """
			Whether to replace a file that already exists at a path. When false, the existing file is kept \
			and the new one is saved beside it under the next copy number, such as 'report (2).pdf'. \
			Defaults to true.""";

	/**
	 * Describes the override key of the reactors that create, copy or rename a
	 * file or directory.
	 */
	public static final String OVERRIDE_KEEP_DESCRIPTION = """
			Whether to replace a file or directory that already exists at the destination. When false, the \
			existing one is kept and the new one is written beside it under the next copy number, such as \
			'report (2).pdf' or 'data (2)'. Defaults to false.""";

	/**
	 * Describes the override key of the reactors that create a directory.
	 */
	public static final String OVERRIDE_DIRECTORY_DESCRIPTION = """
			What to do when a directory already exists at the path. When true, that directory is used as it is \
			and nothing is created. When false, the new directory is created beside it under the next copy \
			number, such as 'data (2)'. Defaults to false.""";

	/**
	 * Describes the override key of the reactors that unzip a file.
	 */
	public static final String OVERRIDE_UNZIP_DESCRIPTION = """
			Whether the extracted files replace ones that already exist beside the zip file. When false, each \
			top level file or directory of the zip whose name is taken is extracted beside it under the next \
			copy number, such as 'data (2)', and nothing already there is touched. Defaults to false.""";

	/**
	 * Describes the output of the reactors that write assets, one entry per
	 * {@link SavedAsset}.
	 */
	public static final String SAVED_ASSETS_OUTPUT_DESCRIPTION = """
			One entry per file or directory written: requestedPath is the path asked for, savedPath is the path \
			written, and renamed is true when savedPath carries a copy number because requestedPath was taken.""";

	private static final Pattern COPY_NUMBER_SUFFIX = Pattern.compile("^(.+) \\((\\d{1,6})\\)$");
	private static final List<String> COMPOUND_EXTENSIONS = List.of(".tar.gz", ".tar.bz2", ".tar.xz", ".tar.zst");
	private static final int MAX_COPY_NUMBERS = 10_000;

	/**
	 * Where an asset write landed, as paths relative to the asset folder without a
	 * leading slash. The two differ when something already existed at the
	 * requested path and the write was kept beside it under a copy number, such as
	 * {@code notes (2).md}. Its components are the keys of each entry reactors
	 * return (see {@link #savedAssetsAsNounMetadata(List)}).
	 *
	 * @param requestedPath the path asked for
	 * @param savedPath     the path written
	 * @param renamed       whether the saved path carries a copy number because
	 *                      the requested path was taken
	 */
	public record SavedAsset(String requestedPath, String savedPath, boolean renamed) {

		/**
		 * Records a write, renamed when the saved path differs from the requested
		 * path.
		 *
		 * @param requestedPath the path asked for
		 * @param savedPath     the path written
		 */
		public SavedAsset(String requestedPath, String savedPath) {
			this(requestedPath, savedPath, !requestedPath.equals(savedPath));
		}

		/**
		 * The entry a reactor returns for this write.
		 *
		 * @return a map of requestedPath, savedPath and renamed
		 */
		public Map<String, Object> toMap() {
			Map<String, Object> map = new LinkedHashMap<>();
			map.put("requestedPath", requestedPath);
			map.put("savedPath", savedPath);
			map.put("renamed", renamed);
			return map;
		}
	}

	/**
	 * Writes the contents of a file that has already been claimed.
	 */
	@FunctionalInterface
	private interface AssetWriter {
		void write(File file) throws IOException;
	}

	/**
	 * A name split for copy numbering, such as {@code report (2).pdf} into the
	 * stem {@code report} and the extension {@code .pdf}, with the first copy
	 * number to try.
	 *
	 * @param stem        the name without its extension or copy number
	 * @param extension   the extension with its dot, or empty
	 * @param firstNumber the first copy number to try
	 */
	private record CopyName(String stem, String extension, int firstNumber) {

		/**
		 * The sibling of a path that carries a copy number.
		 *
		 * @param requested the path asked for
		 * @param number    the copy number
		 * @return the numbered file beside {@code requested}
		 */
		File sibling(File requested, int number) {
			return new File(requested.getAbsoluteFile().getParentFile(), stem + " (" + number + ")" + extension);
		}
	}

	/**
	 * Lists the immediate (non-recursive) contents of an asset directory. Hidden
	 * assets (see {@link #isHiddenAsset(File)}) are excluded; if the requested
	 * directory itself is hidden, the access is logged and an empty list is
	 * returned. Results are sorted with directories first, then files, each group
	 * ordered case-insensitively by name.
	 *
	 * @param user               the user performing the browse, used for the time
	 *                           zone of the {@code lastModified} field and for
	 *                           logging unauthorized access attempts
	 * @param filePath           the absolute path of the directory to list
	 * @param relativeFilePath   the assets-relative path of the directory, used for
	 *                           error/log messages
	 * @param pathSubstringIndex the index at which to trim each entry's absolute
	 *                           path down to its assets-relative {@code path} value
	 * @return a sorted list of maps, one per visible entry, each containing
	 *         {@code name}, {@code path} (directories end with a trailing
	 *         {@code "/"}), {@code type} ({@code "directory"} or the file
	 *         extension), and {@code lastModified}
	 */
	public static List<Map<String, Object>> browseFileSystem(User user, String filePath, String relativeFilePath,
			int pathSubstringIndex) {
		return browseFileSystem(user, filePath, relativeFilePath, pathSubstringIndex, false);
	}

	/**
	 * Same as {@link #browseFileSystem(User, String, String, int)} but, when
	 * {@code publicFolderOnly} is true, the only entry returned is the public
	 * assets folder (see {@link Constants#PUBLIC_ASSETS_FOLDER}). This is used to
	 * list the assets root for view-only users, who may see the public folder as a
	 * node they can traverse into but may not see any other top-level asset.
	 *
	 * @param publicFolderOnly when true, restrict the listing to only the public
	 *                         assets folder directory entry
	 */
	public static List<Map<String, Object>> browseFileSystem(User user, String filePath, String relativeFilePath,
			int pathSubstringIndex, boolean publicFolderOnly) {
		File directory = new File(filePath);
		if (!directory.exists()) {
			throw new IllegalArgumentException(
					"The directory " + relativeFilePath + " does not exist within the assets folder");
		}
		if (!directory.isDirectory()) {
			throw new IllegalArgumentException(
					"The path " + relativeFilePath + " exists within the assets folder but is not a directory");
		}
		if (isWithinHiddenAsset(directory, pathSubstringIndex)) {
			// user is trying to access from a starting point that is hidden
			AccessToken token = user == null ? null : user.getPrimaryLoginToken();
			classLogger.warn("User id={} provider={} is trying to access hidden asset {}",
					token == null ? null : token.getId(), token == null ? null : token.getProvider(), relativeFilePath);
			return new ArrayList<>();
		}
		DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss")
				.withZone(user.getZoneId());

		List<Map<String, Object>> retObj = new ArrayList<>();
		File[] allFiles = directory.listFiles();
		for (File f : allFiles) {
			if (isHiddenAsset(f)) {
				continue;
			}
			// view-only users listing the assets root may only see the public folder node
			if (publicFolderOnly && !(f.isDirectory() && Constants.PUBLIC_ASSETS_FOLDER.equals(f.getName()))) {
				continue;
			}
			String path = f.getAbsolutePath().substring(pathSubstringIndex).replace("\\", "/");
			retObj.add(createMeta(f, path, f.isDirectory(), dateTimeFormatter));
		}

		// Sort directories first, then files, each group sorted by name
		// case-insensitively
		Collections.sort(retObj, new Comparator<Map<String, Object>>() {
			@Override
			public int compare(Map<String, Object> o1, Map<String, Object> o2) {
				boolean d1 = "directory".equals(o1.get("type"));
				boolean d2 = "directory".equals(o2.get("type"));
				if (d1 != d2) {
					return d1 ? -1 : 1;
				}
				return ((String) o1.get("name")).compareToIgnoreCase((String) o2.get("name"));
			}
		});

		return retObj;
	}

	/**
	 * Recursively searches an asset directory for entries whose name matches the
	 * given pattern and returns a sorted list of results. Hidden assets (see
	 * {@link #isHiddenAsset(File)}) are skipped and not descended into; if the
	 * starting directory itself is hidden, the access is logged and an empty list
	 * is returned. Results are sorted with directories first, then files, each
	 * group ordered case-insensitively by name.
	 *
	 * @param user    the user performing the search, used for the time zone of the
	 *                {@code lastModified} field and for logging unauthorized access
	 *                attempts
	 * @param dir     the directory to start the search from
	 * @param pattern the pattern matched (via
	 *                {@link java.util.regex.Matcher#find()}) against each entry's
	 *                name
	 * @param baseLen the absolute-path prefix length to trim when building each
	 *                entry's relative {@code path} value
	 * @return a sorted list of maps, one per matching entry, each containing
	 *         {@code name}, {@code path} (directories end with a trailing
	 *         {@code "/"}), {@code type} ({@code "directory"} or the file
	 *         extension), and {@code lastModified}
	 */
	public static List<Map<String, Object>> search(User user, File dir, Pattern pattern, int baseLen) {
		if (isWithinHiddenAsset(dir, baseLen)) {
			// user is trying to access from a starting point that is hidden
			AccessToken token = user == null ? null : user.getPrimaryLoginToken();
			classLogger.warn("User id={} provider={} is trying to access hidden asset {}",
					token == null ? null : token.getId(), token == null ? null : token.getProvider(), dir.getName());
			return new ArrayList<>();
		}

		DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss")
				.withZone(user.getZoneId());

		List<Map<String, Object>> results = new ArrayList<>();
		searchRecursive(dir, pattern, baseLen, results, dateTimeFormatter);

		// Sort directories first, then files, each group sorted by name
		// case-insensitively
		Collections.sort(results, new Comparator<Map<String, Object>>() {
			@Override
			public int compare(Map<String, Object> o1, Map<String, Object> o2) {
				boolean d1 = "directory".equals(o1.get("type"));
				boolean d2 = "directory".equals(o2.get("type"));
				if (d1 != d2) {
					return d1 ? -1 : 1;
				}
				return ((String) o1.get("name")).compareToIgnoreCase((String) o2.get("name"));
			}
		});
		return results;
	}

	/**
	 * Walks {@code dir} depth-first, appending a metadata map for every entry whose
	 * name matches {@code pattern} into {@code results}. Hidden assets (see
	 * {@link #isHiddenAsset(File)}) are skipped and not descended into. Unreadable
	 * directories (those for which {@link File#listFiles()} returns {@code null})
	 * are silently ignored. The {@code results} list is mutated in place and is
	 * left unsorted; callers are responsible for any ordering.
	 *
	 * @param dir               the directory to recurse into
	 * @param pattern           the pattern matched against each entry's name
	 * @param baseLen           the absolute-path prefix length to trim when
	 *                          building each entry's relative {@code path} value
	 * @param results           the accumulator that matching entries are added to
	 * @param dateTimeFormatter the formatter used to render each entry's
	 *                          {@code lastModified} value
	 */
	public static void searchRecursive(File dir, Pattern pattern, int baseLen, List<Map<String, Object>> results,
			DateTimeFormatter dateTimeFormatter) {
		File[] entries = dir.listFiles();
		if (entries == null) {
			return;
		}

		for (File f : entries) {
			String name = f.getName();
			// hide .git directory and .admin directory
			if (isHiddenAsset(f)) {
				continue;
			}
			// build relative path
			String rel = f.getAbsolutePath().substring(baseLen).replace('\\', '/');
			// match
			if (pattern.matcher(name).find()) {
				Map<String, Object> meta = createMeta(f, rel, f.isDirectory(), dateTimeFormatter);
				results.add(meta);
			}
			// recurse
			if (f.isDirectory()) {
				searchRecursive(f, pattern, baseLen, results, dateTimeFormatter);
			}
		}
	}

	/**
	 * Builds the metadata map describing a single file or directory entry, as
	 * returned by {@link #browseFileSystem} and {@link #search}.
	 *
	 * @param f                 the file or directory being described
	 * @param relativePath      the entry's assets-relative path; a trailing
	 *                          {@code "/"} is appended when {@code isDir} is true
	 * @param isDir             whether the entry is a directory
	 * @param dateTimeFormatter the formatter used to render the
	 *                          {@code lastModified} value
	 * @return a map containing {@code name}, {@code path} (directories end with a
	 *         trailing {@code "/"}), {@code lastModified}, and {@code type}
	 *         ({@code "directory"} or the file extension)
	 */
	private static Map<String, Object> createMeta(File f, String relativePath, boolean isDir,
			DateTimeFormatter dateTimeFormatter) {
		Map<String, Object> map = new HashMap<>();
		map.put("name", f.getName());
		if (isDir && !relativePath.endsWith("/")) {
			relativePath = relativePath + "/";
		}
		map.put("path", relativePath);
		map.put("lastModified", dateTimeFormatter.format(Instant.ofEpochMilli(f.lastModified())));
		map.put("type", isDir ? "directory" : FilenameUtils.getExtension(f.getName()));
		return map;
	}

	/**
	 * Determine whether an asset should be hidden from file explorer listings.
	 * Hides the ".git" directory and the ".admin" directory. Only the leaf name is
	 * inspected, so this is suitable for filtering the direct children of an
	 * already-validated directory; to also reject entries that live <em>inside</em>
	 * a hidden directory, use {@link #isWithinHiddenAsset(File, int)}.
	 *
	 * @param f the file or directory being considered
	 * @return true if the entry should be excluded from the results
	 */
	private static boolean isHiddenAsset(File f) {
		return f.isDirectory() && isHiddenName(f.getName());
	}

	/**
	 * Determine whether a file or directory sits at or beneath a hidden asset
	 * (".git" or ".admin") by inspecting every segment of its assets-relative path.
	 * Unlike {@link #isHiddenAsset(File)}, which only looks at the leaf name, this
	 * also blocks access when an <em>ancestor</em> segment is hidden — e.g. a
	 * caller targeting ".git/hooks" or ".admin/secrets" as the starting point of a
	 * browse or search.
	 *
	 * @param f       the file or directory being considered
	 * @param baseLen the absolute-path prefix length marking where the
	 *                assets-relative portion of {@code f}'s path begins
	 * @return true if any segment of the entry's relative path is a hidden asset
	 */
	private static boolean isWithinHiddenAsset(File f, int baseLen) {
		String absPath = f.getAbsolutePath();
		String relativePath = (baseLen < absPath.length() ? absPath.substring(baseLen) : "").replace('\\', '/');
		for (String segment : relativePath.split("/")) {
			if (isHiddenName(segment)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * @param name a single path-segment name
	 * @return true if the name is one of the hidden asset directories (".git" or
	 *         ".admin")
	 */
	private static boolean isHiddenName(String name) {
		return name.equals(".git") || name.equals(".admin");
	}

	/**
	 * Strips leading/trailing slashes (and normalizes back-slashes) from an
	 * assets-relative path so it can be compared segment-by-segment. Assumes the
	 * input has already been run through {@link Utility#normalizePath(String)} so
	 * that any ".." segments have been resolved away.
	 *
	 * @param relativeFilePath the assets-relative path (may be null)
	 * @return the path without any leading or trailing slash ("" for the root)
	 */
	private static String stripAssetPathSlashes(String relativeFilePath) {
		if (relativeFilePath == null) {
			return "";
		}
		String p = relativeFilePath.replace('\\', '/').trim();
		while (p.startsWith("/")) {
			p = p.substring(1);
		}
		while (p.endsWith("/")) {
			p = p.substring(0, p.length() - 1);
		}
		return p;
	}

	/**
	 * Determines whether an assets-relative path targets the public assets folder
	 * (see {@link Constants#PUBLIC_ASSETS_FOLDER}) or something inside it. The path
	 * is expected to already be normalized (see {@link Utility#normalizePath}) so
	 * that ".." traversal cannot be used to escape the public folder.
	 *
	 * @param relativeFilePath the assets-relative path to test (may be null)
	 * @return true if the path is the public folder itself or lives beneath it
	 */
	public static boolean isWithinPublicAssetFolder(String relativeFilePath) {
		String p = stripAssetPathSlashes(relativeFilePath);
		return p.equals(Constants.PUBLIC_ASSETS_FOLDER) || p.startsWith(Constants.PUBLIC_ASSETS_FOLDER + "/");
	}

	/**
	 * Resolves the assets-relative path a user is allowed to read, enforcing the
	 * public-folder restriction for view-only users. Callers must have already
	 * verified that the user can at least view the app/engine.
	 *
	 * <p>
	 * Users who can edit have unrestricted access and their path is returned
	 * unchanged. View-only users are confined to the public assets folder: a
	 * request for the assets root is scoped down to the public folder, a request
	 * already within the public folder is allowed, and any other request is
	 * rejected.
	 *
	 * @param canEdit          whether the user can edit the app/engine assets
	 *                         (unrestricted access)
	 * @param relativeFilePath the requested assets-relative path (already
	 *                         normalized); null/empty means the assets root
	 * @return the assets-relative path to operate on: unchanged for editors,
	 *         confined to the public folder (with a leading slash, no trailing
	 *         slash) for view-only users
	 * @throws IllegalArgumentException if a view-only user requests a path outside
	 *                                  the public folder
	 */
	public static String resolveReadableAssetPath(boolean canEdit, String relativeFilePath) {
		if (canEdit) {
			return relativeFilePath;
		}
		String p = stripAssetPathSlashes(relativeFilePath);
		if (p.isEmpty()) {
			// scope the assets root down to the public folder for view-only users
			return "/" + Constants.PUBLIC_ASSETS_FOLDER;
		}
		if (p.equals(Constants.PUBLIC_ASSETS_FOLDER) || p.startsWith(Constants.PUBLIC_ASSETS_FOLDER + "/")) {
			return "/" + p;
		}
		throw new IllegalArgumentException("User only has read access to the '" + Constants.PUBLIC_ASSETS_FOLDER
				+ "' folder within the assets folder.");
	}

	/**
	 * Determines whether a browse of the given assets-relative path should be
	 * restricted to only showing the public folder node. Callers must have already
	 * verified that the user can at least view the app/engine.
	 *
	 * <p>
	 * Editors browse freely (returns false). A view-only user browsing the assets
	 * root sees only the public folder node (returns true), a view-only user
	 * browsing within the public folder sees its contents (returns false), and any
	 * other browse request by a view-only user is rejected.
	 *
	 * @param canEdit          whether the user can edit the app/engine assets
	 * @param relativeFilePath the requested assets-relative path (already
	 *                         normalized); null/empty means the assets root
	 * @return true if the browse listing should be restricted to only the public
	 *         folder node; false for an unrestricted listing
	 * @throws IllegalArgumentException if a view-only user browses a path outside
	 *                                  the public folder
	 */
	public static boolean restrictBrowseToPublicFolder(boolean canEdit, String relativeFilePath) {
		if (canEdit) {
			return false;
		}
		String p = stripAssetPathSlashes(relativeFilePath);
		if (p.isEmpty()) {
			// view-only user at the assets root: only show the public folder node
			return true;
		}
		if (p.equals(Constants.PUBLIC_ASSETS_FOLDER) || p.startsWith(Constants.PUBLIC_ASSETS_FOLDER + "/")) {
			return false;
		}
		throw new IllegalArgumentException("User only has read access to the '" + Constants.PUBLIC_ASSETS_FOLDER
				+ "' folder within the assets folder.");
	}

	/**
	 * Deletes a list of asset files or directories. Missing paths, and paths that
	 * name the asset folder itself, are skipped.
	 *
	 * @param assetFolder          The base folder for the assets.
	 * @param filePaths            A list of relative paths to the files/directories
	 *                             to be deleted.
	 * @param gitRelativeFilePaths A list to be populated with the git-relative
	 *                             paths of the deleted items.
	 * @param deletedFiles         A list to be populated with the File objects of
	 *                             the deleted items.
	 */
	public static void deleteAssetFiles(String assetFolder, List<String> filePaths, List<String> gitRelativeFilePaths,
			List<File> deletedFiles) {
		for (String rawPath : filePaths) {
			String inputFilePath = assetRelativePath(rawPath);
			if (inputFilePath.isEmpty()) {
				continue;
			}

			String realFilePath = assetFolder + "/" + inputFilePath;
			realFilePath = realFilePath.replace("\\", "/");
			File realFile = new File(realFilePath);
			if (!realFile.exists()) {
				classLogger.warn("Cannot find the folder/file at path {}. Skipping.", inputFilePath);
				continue;
			}

			if (realFile.isDirectory()) {
				try {
					FileUtils.deleteDirectory(realFile);
				} catch (IOException e) {
					classLogger.error("Error deleting directory at path {}", inputFilePath, e);
					throw new IllegalArgumentException(
							"Error occurred trying to delete folder at path " + inputFilePath);
				}
			} else {
				try {
					FileUtils.forceDelete(realFile);
				} catch (IOException e) {
					classLogger.error("Error deleting file at path {}", inputFilePath, e);
					throw new IllegalArgumentException("Error occurred trying to delete file at path " + inputFilePath);
				}
			}

			// Collect for Git and cluster sync
			gitRelativeFilePaths.add(Constants.ASSETS_FOLDER + "/" + inputFilePath);
			deletedFiles.add(realFile);
		}
	}

	/**
	 * Prepares an asset for download. If it's a directory, it will be zipped.
	 * 
	 * @param toDownloadF      The file or directory to be downloaded.
	 * @param relativeFilePath The relative path of the asset, used for error
	 *                         messages.
	 * @param insightFolder    The folder of the current insight, used to store the
	 *                         zip file.
	 * @return The absolute path to the file ready for download.
	 */
	public static String prepareAssetForDownload(File toDownloadF, String relativeFilePath, String insightFolder) {
		if (!toDownloadF.exists()) {
			throw new IllegalArgumentException(
					"The file/directory " + relativeFilePath + " does not exist within the assets folder");
		}

		String downloadFileLocation = null;
		if (toDownloadF.isDirectory()) {
			String zipFileLocation = Utility.getUniqueFilePath(insightFolder, toDownloadF.getName() + ".zip");
			zipFolder(toDownloadF.getAbsolutePath(), zipFileLocation);
			downloadFileLocation = zipFileLocation;
		} else {
			downloadFileLocation = toDownloadF.getAbsolutePath();
		}
		return downloadFileLocation;
	}

	/**
	 * Zips a folder.
	 * 
	 * @param folder       The absolute path to the folder to zip.
	 * @param downloadPath The path where the output zip file will be saved.
	 */
	private static void zipFolder(String folder, String downloadPath) {
		ZipOutputStream zos = null;
		try {
			zos = ZipUtils.zipFolder(folder, downloadPath);
		} catch (IOException e) {
			classLogger.error("Error zipping folder {} to {}", folder, downloadPath, e);
			throw new IllegalArgumentException("Unable to zip and download directory");
		} finally {
			try {
				if (zos != null) {
					zos.flush();
					zos.close();
				}
			} catch (IOException e) {
				classLogger.error("Could not flush or close Zip Output Stream for {}", downloadPath, e);
				throw new IllegalArgumentException("Could not flush or close Zip Output Stream.");
			}
		}
	}

	/**
	 * Reads an asset file and returns its content as a Base64 encoded string.
	 * 
	 * @param assetFolder The base folder for the assets.
	 * @param filePath    The relative path to the file.
	 * @return The Base64 encoded content of the file.
	 */
	public static String getAssetAsBase64(String assetFolder, String filePath) {
		String assetFilePath = assetFolder + filePath;
		File assetFile = new File(assetFilePath);
		if (!assetFile.exists()) {
			throw new IllegalArgumentException("The filePath " + filePath + " does not exist");
		}
		if (!assetFile.isFile()) {
			throw new IllegalArgumentException("The filePath " + filePath + " exists but is not a file");
		}
		try {
			byte[] bytes = Files.readAllBytes(Paths.get(assetFilePath));
			return Base64.getEncoder().encodeToString(bytes);
		} catch (IOException e) {
			classLogger.error("Error reading file {} for Base64 encoding", assetFilePath, e);
			throw new IllegalArgumentException("Unable to read file " + filePath);
		}
	}

	/**
	 * Reads an asset file and returns its content as a string.
	 * 
	 * @param assetFolder The base folder for the assets.
	 * @param filePath    The relative path to the file.
	 * @return The content of the file as a string.
	 */
	public static String getAssetAsString(String assetFolder, String filePath) {
		String assetFilePath = assetFolder + filePath;
		File assetFile = new File(assetFilePath);
		if (!assetFile.exists()) {
			throw new IllegalArgumentException("The filePath " + filePath + " does not exist");
		}
		if (!assetFile.isFile()) {
			throw new IllegalArgumentException("The filePath " + filePath + " exists but is not a file");
		}
		try {
			return FileUtils.readFileToString(new File(assetFilePath), StandardCharsets.UTF_8);
		} catch (IOException e) {
			classLogger.error("Error reading file {} as string", assetFilePath, e);
			throw new IllegalArgumentException("Unable to read file " + filePath);
		}
	}

	/**
	 * Creates a new directory in the asset folder, including a placeholder file.
	 * When a directory already exists at the path, {@code override} decides what
	 * happens: true uses that directory as it is, false creates the new one beside
	 * it under the next copy number, such as {@code data (2)}.
	 *
	 * @param assetFolder The base folder for the assets.
	 * @param filePath    The relative path for the new directory.
	 * @param override    Whether an existing directory at the path is used as it
	 *                    is.
	 * @return where the directory is
	 * @throws IllegalArgumentException if the path is empty, or a file is at the
	 *                                  path and {@code override} is true
	 */
	public static SavedAsset createNewAssetDirectory(String assetFolder, String filePath, boolean override) {
		String directoryName = assetRelativePath(filePath);
		if (directoryName.isEmpty()) {
			throw new IllegalArgumentException("Must provide a valid filePath");
		}
		File requested = new File(assetFolder + "/" + directoryName);
		if (override && requested.isDirectory()) {
			return new SavedAsset(directoryName, directoryName);
		}
		if (override && requested.exists()) {
			throw new IllegalArgumentException("A file already exists at the path " + directoryName);
		}

		try {
			File directory = override ? requested : reserveAvailableFile(requested, true);
			FileUtils.forceMkdir(directory);
			File placeholder = new File(directory, "placeholder.txt");
			FileUtils.writeStringToFile(placeholder, "placeholder", StandardCharsets.UTF_8);
			return new SavedAsset(directoryName, siblingPath(directoryName, directory.getName()));
		} catch (IOException e) {
			classLogger.error("Error creating new asset directory {}", directoryName, e);
			NounMetadata error = NounMetadata.getErrorNounMessage("Unable to create directory: " + directoryName);
			SemossPixelException exception = new SemossPixelException(error);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
	}

	/**
	 * Creates a new file in the asset folder with starter content for its type,
	 * such as an empty notebook for {@code .ipynb}. When something already exists
	 * at the path, {@code override} decides what happens: true replaces it, false
	 * keeps it and creates the new file beside it under the next copy number.
	 *
	 * @param assetFolder The base folder for the assets.
	 * @param filePath    The relative path for the new file.
	 * @param override    Whether an existing file at the path is replaced.
	 * @return where the file was created
	 * @throws IllegalArgumentException if the path is empty
	 */
	public static SavedAsset createNewAssetFile(String assetFolder, String filePath, boolean override) {
		String fileName = assetRelativePath(filePath);
		if (fileName.isEmpty()) {
			throw new IllegalArgumentException("Must provide a valid filePath");
		}
		String content = getDefaultAssetFileContent(fileName);
		return writeAssetFile(assetFolder, fileName, override,
				file -> FileUtils.writeStringToFile(file, content, StandardCharsets.UTF_8));
	}

	private static String getDefaultAssetFileContent(String filePath) {
		String normalizedPath = filePath == null ? "" : filePath.replace('\\', '/').toLowerCase();

		if (normalizedPath.endsWith("/pipeline.json") || "pipeline.json".equals(normalizedPath)) {
			return "{\n  \"pipelines\": {}\n}";
		}

		if (normalizedPath.endsWith("_mcp.json")) {
			return """
					{
					  "tools": [],
					  "resources": [],
					  "resourceTemplates": [],
					  "prompts": []
					}""";
		}

		if (normalizedPath.endsWith(".json")) {
			return "{}";
		}

		if (normalizedPath.endsWith(".ipynb")) {
			return """
					{
					  "nbformat": 4,
					  "nbformat_minor": 5,
					  "metadata": {
					    "kernelspec": {
					      "display_name": "Python 3",
					      "language": "python",
					      "name": "python3"
					    },
					    "language_info": {
					      "name": "python"
					    }
					  },
					  "cells": [
					    {
					      "id": "%s",
					      "cell_type": "markdown",
					      "metadata": {},
					      "source": [
					        "# New Notebook"
					      ]
					    }
					  ]
					}""".formatted(newNotebookCellId());
		}

		return "new file";
	}

	/**
	 * Id for the cell in a newly created notebook.
	 *
	 * nbformat 4.5 onward wants an id on every cell, unique within the notebook,
	 * matching [a-zA-Z0-9-_] and no longer than 64 characters. A fixed value would
	 * give every notebook the same one, which Jupyter tolerates but which makes
	 * cells indistinguishable once notebooks are merged or diffed.
	 *
	 * @return a random id built from characters the format allows
	 */
	private static String newNotebookCellId() {
		// getRandomString prefixes an "a" and adds this many more characters
		return Utility.getRandomString(8);
	}

	/**
	 * Renames or moves a file or directory within the asset folder. When something
	 * already exists at the new path, {@code override} decides what happens: true
	 * replaces it, false keeps it and gives the renamed entry the next copy number,
	 * such as {@code report (2).pdf}.
	 *
	 * @param assetFolder     The base folder for the assets.
	 * @param currentFileName The current relative path of the file/directory.
	 * @param newFileName     The new relative path for the file/directory.
	 * @param override        Whether an existing entry at the new path is
	 *                        replaced.
	 * @return where the entry was moved to
	 * @throws IllegalArgumentException if the entry is missing, the new path is the
	 *                                  entry itself, or the move would put a
	 *                                  directory inside itself or replace a
	 *                                  directory holding the entry
	 */
	public static SavedAsset renameAsset(String assetFolder, String currentFileName, String newFileName,
			boolean override) {
		String currentName = assetRelativePath(currentFileName);
		String newName = assetRelativePath(newFileName);
		File oldFile = new File(assetFolder + "/" + currentName);
		File newFile = new File(assetFolder + "/" + newName);

		if (!oldFile.exists()) {
			throw new IllegalArgumentException("Cannot find file/folder to rename: " + currentName);
		}
		Path oldPath = oldFile.toPath().toAbsolutePath().normalize();
		Path newPath = newFile.toPath().toAbsolutePath().normalize();
		if (oldFile.isDirectory() && newPath.startsWith(oldPath)) {
			throw new IllegalArgumentException("Cannot move the directory " + currentName + " inside itself");
		}

		File target = newFile;
		if (newFile.exists()) {
			if (isSameFile(oldPath, newPath)) {
				throw new IllegalArgumentException("A file or directory exists with the new name: " + newName);
			}
			if (override) {
				if (oldPath.startsWith(newPath)) {
					throw new IllegalArgumentException(
							"Cannot replace " + newName + " because it holds " + currentName);
				}
				try {
					FileUtils.forceDelete(newFile);
				} catch (IOException e) {
					classLogger.error("Error deleting existing rename destination {}", newName, e);
					throw new SemossPixelException(
							NounMetadata.getErrorNounMessage("Unable to replace existing destination " + newName));
				}
			} else {
				target = nextAvailableFile(newFile, oldFile.isDirectory());
			}
		}

		try {
			FileUtils.forceMkdirParent(target);
		} catch (IOException e) {
			classLogger.error("Error creating parent directory for new asset name {}", newName, e);
			throw new SemossPixelException(
					NounMetadata.getErrorNounMessage("Unable to create parent directory for " + newName));
		}

		try {
			if (oldFile.isDirectory()) {
				FileUtils.moveDirectory(oldFile, target);
			} else {
				FileUtils.moveFile(oldFile, target);
			}
		} catch (IOException e) {
			classLogger.error("Error renaming asset from {} to {}", currentName, newName, e);
			SemossPixelException ex = new SemossPixelException(
					NounMetadata.getErrorNounMessage("Failed to rename " + currentName));
			ex.setContinueThreadOfExecution(false);
			throw ex;
		}
		return new SavedAsset(newName, siblingPath(newName, target.getName()));
	}

	/**
	 * Copies a single file between two already-resolved absolute paths, typically
	 * in different asset spaces. Directories are rejected. When a file already
	 * exists at the destination, {@code override} decides what happens: true
	 * replaces it, false keeps it and writes the copy beside it under the next copy
	 * number, such as {@code deck (2).pptx}. Error messages only mention file
	 * names, never absolute paths.
	 *
	 * @param sourceAbsolutePath the file to copy
	 * @param targetAbsolutePath where to copy it
	 * @param override           replace an existing destination file
	 * @return the file written, which is {@code targetAbsolutePath} unless that
	 *         path was taken and {@code override} is false
	 */
	public static File copyResolvedFile(String sourceAbsolutePath, String targetAbsolutePath, boolean override) {
		File source = new File(sourceAbsolutePath);
		File requested = new File(targetAbsolutePath);
		if (!source.exists()) {
			throw new IllegalArgumentException("Cannot find file to copy: " + source.getName());
		}
		if (source.isDirectory()) {
			throw new IllegalArgumentException("Only files can be copied. '" + source.getName() + "' is a directory");
		}
		Path sourcePath = source.toPath().toAbsolutePath().normalize();
		Path targetPath = requested.toPath().toAbsolutePath().normalize();
		if (override && sourcePath.equals(targetPath)) {
			throw new IllegalArgumentException("Source and destination are the same file");
		}
		if (override && requested.isDirectory()) {
			throw new IllegalArgumentException(
					"The destination '" + requested.getName() + "' is an existing directory");
		}

		File target = requested;
		boolean claimed = false;
		try {
			if (override) {
				FileUtils.forceMkdirParent(target);
			} else {
				target = reserveAvailableFile(requested, false);
				claimed = true;
			}
			FileUtils.copyFile(source, target);
		} catch (IOException e) {
			if (claimed) {
				FileUtils.deleteQuietly(target);
			}
			classLogger.error("Error copying file {} to {}", sourceAbsolutePath, target.getAbsolutePath(), e);
			SemossPixelException ex = new SemossPixelException(
					NounMetadata.getErrorNounMessage("Failed to copy " + source.getName()));
			ex.setContinueThreadOfExecution(false);
			throw ex;
		}
		return target;
	}

	/**
	 * Copies a file or directory within the asset folder. When something already
	 * exists at the destination, {@code override} decides what happens: true
	 * replaces it, false keeps it and writes the copy beside it under the next copy
	 * number. Copying an entry onto its own path with {@code override} false makes
	 * a numbered duplicate beside it, such as {@code report (2).pdf}.
	 *
	 * @param assetFolder    The base folder for the assets.
	 * @param sourceFileName The current relative path of the file/directory to
	 *                       copy.
	 * @param destFileName   The destination relative path for the copy.
	 * @param override       Whether an existing entry at the destination is
	 *                       replaced.
	 * @return where the copy was written
	 * @throws IllegalArgumentException if the source is missing or the source and
	 *                                  destination contain one another
	 */
	public static SavedAsset copyAsset(String assetFolder, String sourceFileName, String destFileName,
			boolean override) {
		String sourceName = assetRelativePath(sourceFileName);
		String destName = assetRelativePath(destFileName);
		File sourceFile = new File(assetFolder + "/" + sourceName);
		File destFile = new File(assetFolder + "/" + destName);

		if (!sourceFile.exists()) {
			throw new IllegalArgumentException("Cannot find file/folder to copy: " + sourceName);
		}
		boolean directory = sourceFile.isDirectory();

		File checkedTarget = override ? destFile : nextAvailableFile(destFile, directory);
		Path sourcePath = sourceFile.toPath().toAbsolutePath().normalize();
		Path destPath = checkedTarget.toPath().toAbsolutePath().normalize();
		if (sourcePath.equals(destPath) || sourcePath.startsWith(destPath)
				|| (directory && destPath.startsWith(sourcePath))) {
			throw new IllegalArgumentException("Source and destination paths cannot contain one another");
		}

		if (override && destFile.exists()) {
			try {
				FileUtils.forceDelete(destFile);
			} catch (IOException e) {
				classLogger.error("Error deleting existing copy destination {}", destName, e);
				throw new SemossPixelException(
						NounMetadata.getErrorNounMessage("Unable to replace existing destination " + destName));
			}
		}

		File target = destFile;
		boolean claimed = false;
		try {
			if (override) {
				FileUtils.forceMkdirParent(destFile);
			} else {
				target = reserveAvailableFile(destFile, directory);
				claimed = true;
			}
		} catch (IOException e) {
			classLogger.error("Error creating parent directory for copy destination {}", destName, e);
			throw new SemossPixelException(
					NounMetadata.getErrorNounMessage("Unable to create parent directory for " + destName));
		}

		try {
			if (directory) {
				FileUtils.copyDirectory(sourceFile, target);
			} else {
				FileUtils.copyFile(sourceFile, target);
			}
		} catch (IOException e) {
			if (claimed) {
				FileUtils.deleteQuietly(target);
			}
			classLogger.error("Error copying asset from {} to {}", sourceName, destName, e);
			SemossPixelException ex = new SemossPixelException(
					NounMetadata.getErrorNounMessage("Failed to copy " + sourceName));
			ex.setContinueThreadOfExecution(false);
			throw ex;
		}
		return new SavedAsset(destName, siblingPath(destName, target.getName()));
	}

	/**
	 * Validates a list of file paths against a set of rules.
	 * 
	 * @param filePaths          A list of file paths to validate.
	 * @param strictScriptSource If true, disallows saving of .py and .R files.
	 */
	public static void validateAssetFiles(List<String> filePaths, boolean strictScriptSource) {
		for (String rawFileName : filePaths) {
			String fileName = Utility.normalizePath(rawFileName.trim());
			if (strictScriptSource) {
				String extension = FilenameUtils.getExtension(fileName);
				if ("py".equalsIgnoreCase(extension) || "R".equalsIgnoreCase(extension)) {
					throw new IllegalArgumentException("User is not allowed to create or save R or Py scripts");
				}
			}
		}
	}

	/**
	 * Saves a list of files with their corresponding content to the asset folder.
	 * When a file already exists at a path, {@code override} decides what happens:
	 * true replaces it, false keeps it and saves the new file beside it under the
	 * next copy number, such as {@code notes (2).md}. Empty paths are skipped.
	 *
	 * @param assetFolder The base folder for the assets.
	 * @param filePaths   A list of relative file paths.
	 * @param contents    A list of file contents corresponding to the filePaths.
	 * @param override    Whether an existing file at a path is replaced.
	 * @return where each file was saved, in the order of {@code filePaths}
	 */
	public static List<SavedAsset> saveAssetFiles(String assetFolder, List<String> filePaths, List<String> contents,
			boolean override) {
		List<SavedAsset> saved = new ArrayList<>();
		for (int i = 0; i < filePaths.size(); i++) {
			String fileName = assetRelativePath(filePaths.get(i));
			if (fileName.isEmpty()) {
				continue;
			}
			// content is written as-is: the Pixel translation layer already decodes
			// <encode> blocks (PR #2510); decoding again corrupts literal "%xx" (e.g. %02x)
			String content = contents.get(i);
			saved.add(writeAssetFile(assetFolder, fileName, override,
					file -> FileUtils.writeStringToFile(file, content, StandardCharsets.UTF_8)));
		}
		return saved;
	}

	/**
	 * Saves a list of files with their corresponding base64 content to the asset
	 * folder. When a file already exists at a path, {@code override} decides what
	 * happens: true replaces it, false keeps it and saves the new file beside it
	 * under the next copy number, such as {@code deck (2).pptx}. Empty paths are
	 * skipped.
	 *
	 * @param assetFolder  The base folder for the assets.
	 * @param filePaths    A list of relative file paths.
	 * @param contents     A list of file contents corresponding to the filePaths.
	 * @param decodeBase64 Boolean if we should decode the base64 string before
	 *                     writing to the filePath
	 * @param override     Whether an existing file at a path is replaced.
	 * @return where each file was saved, in the order of {@code filePaths}
	 */
	public static List<SavedAsset> saveAssetFilesBase64(String assetFolder, List<String> filePaths,
			List<String> contents, boolean decodeBase64, boolean override) {
		List<SavedAsset> saved = new ArrayList<>();
		for (int i = 0; i < filePaths.size(); i++) {
			String fileName = assetRelativePath(filePaths.get(i));
			if (fileName.isEmpty()) {
				continue;
			}
			String content = contents.get(i);
			if (decodeBase64) {
				byte[] decodedBytes = decodeBase64Content(content);
				// write the decoded bytes directly - routing them through a
				// String corrupts binary content (pptx, images, pdf, etc.)
				saved.add(writeAssetFile(assetFolder, fileName, override,
						file -> FileUtils.writeByteArrayToFile(file, decodedBytes)));
			} else {
				saved.add(writeAssetFile(assetFolder, fileName, override,
						file -> FileUtils.writeStringToFile(file, content, StandardCharsets.UTF_8)));
			}
		}
		return saved;
	}

	/**
	 * Decodes the base64 content of a file being saved.
	 *
	 * @param content the base64 content
	 * @return the decoded bytes
	 * @throws IllegalArgumentException if the content is not base64
	 */
	private static byte[] decodeBase64Content(String content) {
		try {
			return Base64.getDecoder().decode(content);
		} catch (Exception e) {
			throw new IllegalArgumentException(
					"Failed to decode string input: input is not base64-encoded utf-8 string", e);
		}
	}

	/**
	 * Writes one asset file. With {@code override} the file at the path is
	 * written, replacing what is there. Without it the path, or the first free
	 * numbered sibling, is claimed first (see
	 * {@link #reserveAvailableFile(File, boolean)}) and removed again when the
	 * write fails.
	 *
	 * @param assetFolder the base folder for the assets
	 * @param fileName    the file's path relative to the asset folder
	 * @param override    whether an existing file at the path is replaced
	 * @param writer      writes the content into the file
	 * @return where the file was saved
	 */
	private static SavedAsset writeAssetFile(String assetFolder, String fileName, boolean override,
			AssetWriter writer) {
		File target = new File(assetFolder + "/" + fileName);
		boolean claimed = false;
		try {
			if (!override) {
				target = reserveAvailableFile(target, false);
				claimed = true;
			}
			writer.write(target);
		} catch (IOException e) {
			if (claimed) {
				FileUtils.deleteQuietly(target);
			}
			classLogger.error("Error saving asset file {}", fileName, e);
			NounMetadata error = NounMetadata.getErrorNounMessage("Unable to save file: " + fileName);
			SemossPixelException exception = new SemossPixelException(error);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		return new SavedAsset(fileName, siblingPath(fileName, target.getName()));
	}

	/**
	 * The result a reactor returns after writing assets: one map per asset, as
	 * {@link SavedAsset#toMap()} builds it, so callers read where each asset
	 * landed. Each asset saved under a copy number also carries a success message
	 * naming its new path, which clients show beside the result.
	 *
	 * @param savedAssets where each asset was written
	 * @return the noun holding the list of maps
	 */
	public static NounMetadata savedAssetsAsNounMetadata(List<SavedAsset> savedAssets) {
		List<Map<String, Object>> output = new ArrayList<>();
		for (SavedAsset savedAsset : savedAssets) {
			output.add(savedAsset.toMap());
		}
		NounMetadata noun = new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE,
				PixelOperationType.OPERATION);
		for (SavedAsset savedAsset : savedAssets) {
			if (savedAsset.renamed()) {
				noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("'" + savedAsset.requestedPath()
						+ "' already exists, so it was saved as '" + savedAsset.savedPath() + "'"));
			}
		}
		return noun;
	}

	/**
	 * Where the top level entries of an unzipped file landed, relative to the
	 * asset folder.
	 *
	 * @param zipRelativePath the zip file's path relative to the asset folder
	 * @param extractedNames  each top level name in the zip mapped to the name it
	 *                        was extracted as, as
	 *                        {@link ZipUtils#unzip(String, String, boolean)}
	 *                        returns them
	 * @return one saved asset per top level entry
	 */
	public static List<SavedAsset> unzippedAssets(String zipRelativePath, Map<String, String> extractedNames) {
		List<SavedAsset> savedAssets = new ArrayList<>();
		for (Map.Entry<String, String> extracted : extractedNames.entrySet()) {
			savedAssets.add(new SavedAsset(siblingPath(zipRelativePath, extracted.getKey()),
					siblingPath(zipRelativePath, extracted.getValue())));
		}
		return savedAssets;
	}

	/**
	 * The path to write for a request whose path may be taken: the requested path
	 * when nothing is there, otherwise the first free sibling numbered the way
	 * desktop file managers number a second download. {@code report.pdf} becomes
	 * {@code report (2).pdf}, then {@code report (3).pdf}. A name that already
	 * carries a copy number counts on from it, so a taken {@code report (2).pdf}
	 * becomes {@code report (3).pdf}. The extension stays last, compound ones such
	 * as {@code .tar.gz} included, while a directory or a dotfile such as
	 * {@code .env} is numbered at the end of its whole name. Nothing is created;
	 * use {@link #reserveAvailableFile(File, boolean)} to claim the path.
	 *
	 * @param requested the path asked for
	 * @param directory whether a directory is being written
	 * @return {@code requested} or its first free numbered sibling
	 * @throws IllegalArgumentException if every copy number tried is taken
	 */
	static File nextAvailableFile(File requested, boolean directory) {
		if (!requested.exists()) {
			return requested;
		}
		CopyName name = copyName(requested.getName(), directory);
		for (int number = name.firstNumber(); number < name.firstNumber() + MAX_COPY_NUMBERS; number++) {
			File candidate = name.sibling(requested, number);
			if (!candidate.exists()) {
				return candidate;
			}
		}
		throw noFreeName(requested);
	}

	/**
	 * Claims the path to write: the requested path when it is free, otherwise its
	 * first free numbered sibling, named as {@link #nextAvailableFile(File, boolean)}
	 * describes. Claiming creates the entry, an empty file or directory, in one
	 * step that fails when the name is taken, so two writes at the same time never
	 * get the same name. Missing parent directories are created.
	 *
	 * @param requested the path asked for
	 * @param directory whether to claim a directory instead of a file
	 * @return the claimed file or directory, now existing and empty
	 * @throws IOException              if the parent or the entry cannot be
	 *                                  created for a reason other than a taken name
	 * @throws IllegalArgumentException if every copy number tried is taken
	 */
	static File reserveAvailableFile(File requested, boolean directory) throws IOException {
		FileUtils.forceMkdir(requested.getAbsoluteFile().getParentFile());
		if (claim(requested, directory)) {
			return requested;
		}
		CopyName name = copyName(requested.getName(), directory);
		for (int number = name.firstNumber(); number < name.firstNumber() + MAX_COPY_NUMBERS; number++) {
			File candidate = name.sibling(requested, number);
			if (claim(candidate, directory)) {
				return candidate;
			}
		}
		throw noFreeName(requested);
	}

	/**
	 * Creates a file or directory only when nothing is at its path.
	 *
	 * @param file      the path to create
	 * @param directory whether to create a directory instead of a file
	 * @return true when it was created, false when the path was taken
	 * @throws IOException if it cannot be created and nothing is at its path
	 */
	private static boolean claim(File file, boolean directory) throws IOException {
		boolean created = directory ? file.mkdir() : file.createNewFile();
		if (!created && !file.exists()) {
			throw new IOException("Unable to create " + file.getName());
		}
		return created;
	}

	/**
	 * Splits a name for copy numbering.
	 *
	 * @param name      the file or directory name
	 * @param directory whether the name is a directory's, which has no extension
	 * @return the stem, the extension and the first copy number to try
	 */
	private static CopyName copyName(String name, boolean directory) {
		String extension = "";
		if (!directory) {
			String lowerName = name.toLowerCase(Locale.ROOT);
			for (String compound : COMPOUND_EXTENSIONS) {
				if (lowerName.endsWith(compound) && lowerName.length() > compound.length()) {
					extension = name.substring(name.length() - compound.length());
					break;
				}
			}
			int dot = name.lastIndexOf('.');
			if (extension.isEmpty() && dot > 0) {
				extension = name.substring(dot);
			}
		}
		String stem = name.substring(0, name.length() - extension.length());
		int firstNumber = 2;
		Matcher numbered = COPY_NUMBER_SUFFIX.matcher(stem);
		if (numbered.matches()) {
			stem = numbered.group(1);
			firstNumber = Math.max(2, Integer.parseInt(numbered.group(2)) + 1);
		}
		return new CopyName(stem, extension, firstNumber);
	}

	private static IllegalArgumentException noFreeName(File requested) {
		return new IllegalArgumentException("Unable to find a free name for " + requested.getName());
	}

	/**
	 * The path of an entry that sits beside another, both relative to the asset
	 * folder.
	 *
	 * @param relativePath the path of the entry asked for, without a trailing slash
	 * @param name         the name of the entry beside it
	 * @return {@code relativePath} with its last segment replaced by {@code name}
	 */
	public static String siblingPath(String relativePath, String name) {
		int slash = relativePath.lastIndexOf('/');
		return slash < 0 ? name : relativePath.substring(0, slash + 1) + name;
	}

	/**
	 * Normalizes a requested asset path for writing: back-slashes become slashes,
	 * ".." segments are resolved (see {@link Utility#normalizePath(String)}), and
	 * leading and trailing slashes are removed.
	 *
	 * @param rawPath the path from the request, may be null
	 * @return the relative path, or an empty string for none
	 */
	private static String assetRelativePath(String rawPath) {
		if (rawPath == null) {
			return "";
		}
		return stripAssetPathSlashes(Utility.normalizePath(rawPath.trim()));
	}

	/**
	 * Whether two existing paths are the same file, such as names that differ only
	 * in case on a case-insensitive file system.
	 *
	 * @param first  one path
	 * @param second the other path
	 * @return true when both name the same file
	 */
	private static boolean isSameFile(Path first, Path second) {
		try {
			return Files.isSameFile(first, second);
		} catch (IOException e) {
			return first.equals(second);
		}
	}

	/**
	 * Deletes a folder and all its sub-directories
	 * 
	 * @param folderLocation The location of the folder
	 */
	public static void deleteFolderIfExists(String folderLocation) {
		deleteFolderIfExists(new File(folderLocation));
	}

	/**
	 * Deletes a folder and all its sub-directories
	 * 
	 * @param folder The folder to delete
	 */
	public static void deleteFolderIfExists(File folder) {
		if (folder.exists() && folder.isDirectory()) {
			try {
				FileUtils.forceDelete(folder);
			} catch (IOException e) {
				classLogger.error("Error deleting folder {}", folder.getAbsolutePath(), e);
			}
		}
	}

	/**
	 * Deletes a file
	 * 
	 * @param fileLocation The location of the file
	 */
	public static void deleteFileIfExists(String fileLocation) {
		deleteFileIfExists(new File(fileLocation));
	}

	/**
	 * Deletes a file
	 * 
	 * @param file The File object to delete
	 */
	public static void deleteFileIfExists(File file) {
		if (file.exists() && file.isFile()) {
			try {
				FileUtils.forceDelete(file);
			} catch (IOException e) {
				classLogger.error("Error deleting file {}", file.getAbsolutePath(), e);
			}
		}
	}
}

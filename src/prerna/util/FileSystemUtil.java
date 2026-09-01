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
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public final class FileSystemUtil {

	private static final Logger classLogger = LogManager.getLogger(FileSystemUtil.class);

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
	 * Deletes a list of asset files or directories.
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
			String inputFilePath = Utility.normalizePath(rawPath.trim());
			if (inputFilePath == null || inputFilePath.isEmpty()) {
				continue;
			}
			while (inputFilePath.startsWith("/")) {
				inputFilePath = inputFilePath.substring(1);
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
	 * 
	 * @param assetFolder The base folder for the assets.
	 * @param filePath    The relative path for the new directory.
	 */
	public static void createNewAssetDirectory(String assetFolder, String filePath) {
		while (filePath.startsWith("/")) {
			filePath = filePath.substring(1);
		}
		File directory = new File(assetFolder + "/" + filePath);

		if (directory.exists() && directory.isDirectory()) {
			classLogger.warn("Folder already exists: {}. Skipping creation.", filePath);
			return;
		}

		try {
			directory.mkdirs();
			File placeholder = new File(directory.getAbsolutePath(), "placeholder.txt");
			FileUtils.writeStringToFile(placeholder, "placeholder", StandardCharsets.UTF_8);
		} catch (IOException e) {
			classLogger.error("Error creating new asset directory {}", filePath, e);
			NounMetadata error = NounMetadata.getErrorNounMessage("Unable to create directory: " + filePath);
			SemossPixelException exception = new SemossPixelException(error);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
	}

	/**
	 * Creates a new empty file in the asset folder.
	 * 
	 * @param assetFolder The base folder for the assets.
	 * @param filePath    The relative path for the new file.
	 */
	public static void createNewAssetFile(String assetFolder, String filePath) {
		while (filePath.startsWith("/")) {
			filePath = filePath.substring(1);
		}
		File file = new File(assetFolder + "/" + filePath);
		try {
			FileUtils.writeStringToFile(file, getDefaultAssetFileContent(filePath), StandardCharsets.UTF_8);
		} catch (IOException e) {
			classLogger.error("Error creating new asset file {}", filePath, e);
			NounMetadata error = NounMetadata.getErrorNounMessage("Unable to save file: " + filePath);
			SemossPixelException exception = new SemossPixelException(error);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
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
	 * Renames a file or directory within the asset folder.
	 * 
	 * @param assetFolder     The base folder for the assets.
	 * @param currentFileName The current relative path of the file/directory.
	 * @param newFileName     The new relative path for the file/directory.
	 */
	public static void renameAsset(String assetFolder, String currentFileName, String newFileName) {
		while (currentFileName.startsWith("/")) {
			currentFileName = currentFileName.substring(1);
		}
		while (newFileName.startsWith("/")) {
			newFileName = newFileName.substring(1);
		}
		String oldAbs = (assetFolder + "/" + currentFileName).replace("\\", "/");
		String newAbs = (assetFolder + "/" + newFileName).replace("\\", "/");
		File oldFile = new File(oldAbs);
		File newFile = new File(newAbs);

		// validation checks
		if (!oldFile.exists()) {
			throw new IllegalArgumentException("Cannot find file/folder to rename: " + currentFileName);
		}
		if (newFile.exists()) {
			throw new IllegalArgumentException("A file or directory exists with the new name: " + newFileName);
		}

		try {
			FileUtils.forceMkdirParent(newFile);
		} catch (IOException e) {
			classLogger.error("Error creating parent directory for new asset name {}", newFileName, e);
			throw new SemossPixelException(
					NounMetadata.getErrorNounMessage("Unable to create parent directory for " + newFileName));
		}

		// rename the file/folder
		try {
			if (oldFile.isDirectory()) {
				FileUtils.moveDirectory(oldFile, newFile);
			} else {
				FileUtils.moveFile(oldFile, newFile);
			}
		} catch (IOException e) {
			classLogger.error("Error renaming asset from {} to {}", currentFileName, newFileName, e);
			SemossPixelException ex = new SemossPixelException(
					NounMetadata.getErrorNounMessage("Failed to rename " + currentFileName));
			ex.setContinueThreadOfExecution(false);
			throw ex;
		}
	}

	/**
	 * Copies a file or directory within the asset folder.
	 * 
	 * @param assetFolder    The base folder for the assets.
	 * @param sourceFileName The current relative path of the file/directory to
	 *                       copy.
	 * @param destFileName   The destination relative path for the copy.
	 */
	public static void copyAsset(String assetFolder, String sourceFileName, String destFileName) {
		copyAsset(assetFolder, sourceFileName, destFileName, false);
	}

	/**
	 * Copies a file or directory within the asset folder, optionally replacing an
	 * existing destination.
	 *
	 * @param assetFolder    The base folder for the assets.
	 * @param sourceFileName The current relative path of the file/directory to
	 *                       copy.
	 * @param destFileName   The destination relative path for the copy.
	 * @param override       If true, delete an existing destination before copying.
	 */
	public static void copyAsset(String assetFolder, String sourceFileName, String destFileName, boolean override) {
		while (sourceFileName.startsWith("/")) {
			sourceFileName = sourceFileName.substring(1);
		}
		while (destFileName.startsWith("/")) {
			destFileName = destFileName.substring(1);
		}
		String sourceAbs = (assetFolder + "/" + sourceFileName).replace("\\", "/");
		String destAbs = (assetFolder + "/" + destFileName).replace("\\", "/");
		File sourceFile = new File(sourceAbs);
		File destFile = new File(destAbs);

		if (!sourceFile.exists()) {
			throw new IllegalArgumentException("Cannot find file/folder to copy: " + sourceFileName);
		}

		Path sourcePath = sourceFile.toPath().toAbsolutePath().normalize();
		Path destPath = destFile.toPath().toAbsolutePath().normalize();
		if (sourcePath.equals(destPath) || sourcePath.startsWith(destPath)
				|| (sourceFile.isDirectory() && destPath.startsWith(sourcePath))) {
			throw new IllegalArgumentException("Source and destination paths cannot contain one another");
		}

		if (destFile.exists()) {
			if (!override) {
				throw new IllegalArgumentException(
						"A file or directory already exists at the destination: " + destFileName);
			}
			try {
				FileUtils.forceDelete(destFile);
			} catch (IOException e) {
				classLogger.error("Error deleting existing copy destination {}", destFileName, e);
				throw new SemossPixelException(
						NounMetadata.getErrorNounMessage("Unable to replace existing destination " + destFileName));
			}
		}

		try {
			FileUtils.forceMkdirParent(destFile);
		} catch (IOException e) {
			classLogger.error("Error creating parent directory for copy destination {}", destFileName, e);
			throw new SemossPixelException(
					NounMetadata.getErrorNounMessage("Unable to create parent directory for " + destFileName));
		}

		try {
			if (sourceFile.isDirectory()) {
				FileUtils.copyDirectory(sourceFile, destFile);
			} else {
				FileUtils.copyFile(sourceFile, destFile);
			}
		} catch (IOException e) {
			classLogger.error("Error copying asset from {} to {}", sourceFileName, destFileName, e);
			SemossPixelException ex = new SemossPixelException(
					NounMetadata.getErrorNounMessage("Failed to copy " + sourceFileName));
			ex.setContinueThreadOfExecution(false);
			throw ex;
		}
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
	 * 
	 * @param assetFolder The base folder for the assets.
	 * @param filePaths   A list of relative file paths.
	 * @param contents    A list of file contents corresponding to the filePaths.
	 */
	public static void saveAssetFiles(String assetFolder, List<String> filePaths, List<String> contents) {
		// iterate each fileName/content pair
		for (int i = 0; i < filePaths.size(); i++) {
			String rawFileName = filePaths.get(i).trim();
			String fileName = Utility.normalizePath(rawFileName);
			if (fileName == null || fileName.isEmpty()) {
				continue;
			}
			while (fileName.startsWith("/")) {
				fileName = fileName.substring(1);
			}

			String filePath = assetFolder + "/" + fileName;
			// content is written as-is: the Pixel translation layer already decodes
			// <encode> blocks (PR #2510); decoding again corrupts literal "%xx" (e.g. %02x)
			String content = contents.get(i);

			File file = new File(filePath);
			try {
				FileUtils.writeStringToFile(file, content, StandardCharsets.UTF_8);
			} catch (IOException e) {
				classLogger.error("Error saving asset file {}", fileName, e);
				NounMetadata error = NounMetadata.getErrorNounMessage("Unable to save file: " + fileName);
				SemossPixelException exception = new SemossPixelException(error);
				exception.setContinueThreadOfExecution(false);
				throw exception;
			}
		}
	}

	/**
	 * Saves a list of files with their corresponding base64 content to the asset
	 * folder.
	 * 
	 * @param assetFolder  The base folder for the assets.
	 * @param filePaths    A list of relative file paths.
	 * @param contents     A list of file contents corresponding to the filePaths.
	 * @param decodeBase64 Boolean if we should decode the base64 string before
	 *                     writing to the filePath
	 */
	public static void saveAssetFilesBase64(String assetFolder, List<String> filePaths, List<String> contents,
			boolean decodeBase64) {
		// iterate each fileName/content pair
		for (int i = 0; i < filePaths.size(); i++) {
			String rawFileName = filePaths.get(i).trim();
			String fileName = Utility.normalizePath(rawFileName);
			if (fileName == null || fileName.isEmpty()) {
				continue;
			}
			while (fileName.startsWith("/")) {
				fileName = fileName.substring(1);
			}

			String filePath = assetFolder + "/" + fileName;
			String content = contents.get(i);
			byte[] decodedBytes = null;
			if (decodeBase64) {
				try {
					decodedBytes = Base64.getDecoder().decode(content);
				} catch (Exception e) {
					throw new IllegalArgumentException(
							"Failed to decode string input: input is not base64-encoded utf-8 string", e);
				}
			}

			File file = new File(filePath);
			try {
				if (decodedBytes != null) {
					// write the decoded bytes directly - routing them through a
					// String corrupts binary content (pptx, images, pdf, etc.)
					FileUtils.writeByteArrayToFile(file, decodedBytes);
				} else {
					FileUtils.writeStringToFile(file, content, StandardCharsets.UTF_8);
				}
			} catch (IOException e) {
				classLogger.error("Error saving asset file {}", fileName, e);
				NounMetadata error = NounMetadata.getErrorNounMessage("Unable to save file: " + fileName);
				SemossPixelException exception = new SemossPixelException(error);
				exception.setContinueThreadOfExecution(false);
				throw exception;
			}
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

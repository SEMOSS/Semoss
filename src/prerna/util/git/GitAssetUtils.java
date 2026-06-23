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
package prerna.util.git;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.Utility;

/**
 * Static utility methods for browsing and describing the files and directories
 * of a project's asset/Git folder. Provides shallow directory listings (
 * {@link #browse(String, String, String)}), flat per-file metadata (
 * {@link #getAssetMetadata(String, String, String, boolean)}), recursive
 * metadata collection with wildcard filtering (
 * {@link #listAssetMetadata(String, String, String, java.util.List, java.util.List)})
 * and supporting helpers for date formatting and glob-style matching.
 */
public class GitAssetUtils {

	private static final Logger classLogger = LogManager.getLogger(GitAssetUtils.class);

	private GitAssetUtils() {

	}

	/**
	 * Lists the immediate files and directories of the given folder, splitting the
	 * results into file and directory name/date lists. Delegates to
	 * {@link #browse(String, String, String)} with an empty replacement string.
	 *
	 * @param gitFolder absolute path of the folder to browse
	 * @param replacer  substring removed from directory names in the results; may
	 *                  be {@code null}
	 * @return a map keyed by {@code FILE_LIST}, {@code DIR_LIST}, {@code FILE_DATE}
	 *         and {@code DIR_DATE} holding the corresponding names and formatted
	 *         last-modified dates
	 */
	public static Map<String, List<String>> browse(String gitFolder, String replacer) {
		return browse(gitFolder, replacer, "");
	}

	/**
	 * Formats an epoch-millisecond timestamp as a {@code MM/dd/yyyy HH:mm:ss}
	 * string using the default time zone.
	 *
	 * @param time timestamp in milliseconds since the epoch
	 * @return the formatted date-time string
	 */
	public static String getDate(long time) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
		return dateFormat.format(time);
	}

	/**
	 * Builds flat metadata for the immediate contents of the given folder. For each
	 * file and directory a map is produced containing its {@code name}, formatted
	 * {@code lastModified} date, a {@code path} relativized against
	 * {@code replacer} and prefixed with {@code prefix}, and a {@code type} (the
	 * file extension for files, or {@code directory} for directories). This listing
	 * is not recursive. If the folder cannot be listed (i.e.
	 * {@link File#listFiles()} returns {@code null}) an empty list is returned.
	 * When {@code addApp} is {@code true} an additional synthetic
	 * {@code app_assets} directory entry is appended.
	 *
	 * @param gitFolder absolute path of the folder whose contents are described
	 * @param replacer  base path used both to relativize each entry's {@code path}
	 *                  and, for files/directories, as a substring stripped from the
	 *                  computed path; may be {@code null}
	 * @param prefix    string prepended to each relativized {@code path}
	 * @param addApp    when {@code true}, append a synthetic {@code app_assets}
	 *                  directory entry to the result
	 * @return a list of per-entry metadata maps; empty if the folder has no
	 *         listable contents
	 */
	public static List<Map<String, Object>> getAssetMetadata(String gitFolder, String replacer, String prefix,
			boolean addApp) {
		List<Map<String, Object>> retList = new Vector<>();
		File folder = new File(gitFolder);
		File[] listOfFiles = folder.listFiles();
		if (listOfFiles == null) {
			return retList;
		}
		String repString = ""; // can be $IF

		for (File f : listOfFiles) {
			Map<String, Object> fileMap = new HashMap<>();
			fileMap.put("name", f.getName());
			fileMap.put("lastModified", getDate(f.lastModified()));
			String relative = new File(replacer).toURI().relativize(new File(f.getAbsolutePath()).toURI()).getPath();
			relative = prefix + relative;
			fileMap.put("path", relative);
			if (f.isFile()) {
				String path = f.getAbsolutePath().replaceAll("\\\\", "/");
				if (replacer != null) {
					path = path.replaceAll(replacer, repString);
				}
				path = path.replaceFirst("/", "");
				fileMap.put("type", FilenameUtils.getExtension(path));
			} else if (f.isDirectory()) {
				String path = f.getName().replaceAll("\\\\", "/");
				// no hidden files
				if (!path.startsWith(".")) {
					if (replacer != null) {
						path = path.replaceAll(replacer, repString);
					}
				}
				fileMap.put("type", "directory");
			}
			retList.add(fileMap);
		}
		if (addApp) {
			Map<String, Object> appFolder = new HashMap<>();

			appFolder.put("name", "app_assets");
			appFolder.put("lastModified", "");
			appFolder.put("path", "app_assets/");
			appFolder.put("type", "directory");
			retList.add(appFolder);
		}

		return retList;
	}

	/**
	 * Recursively collects metadata for files (and directories) under
	 * {@code gitFolder} whose names match the wildcard pattern {@code extn} (via
	 * {@link #isMatch(String, String)}). Matching files contribute their
	 * {@code name}, relativized {@code path}, {@code lastModified} date and
	 * {@code type} (file extension); matching non-hidden directories are also added
	 * as {@code directory} entries. Non-hidden subdirectories (those not starting
	 * with {@code .}) are queued in {@code dirList} and processed by recursing on
	 * the first queued directory until the queue is empty. The {@code dirList} and
	 * {@code retList} arguments are lazily initialized when {@code null} and are
	 * mutated in place across the recursion.
	 *
	 * @param gitFolder absolute path of the folder to scan on this invocation
	 * @param extn      glob-style pattern (supporting {@code *} and {@code ?}) that
	 *                  entry names are matched against
	 * @param replacer  base path used to relativize each matching file's
	 *                  {@code path} and as a substring stripped from file paths;
	 *                  may be {@code null}
	 * @param dirList   accumulator of pending subdirectory paths still to be
	 *                  scanned; initialized to an empty list when {@code null}
	 * @param retList   accumulator of result metadata maps; initialized to an empty
	 *                  list when {@code null}
	 * @return the accumulated list of metadata maps for all matching entries found
	 *         across the recursion
	 */
	public static List<Map<String, Object>> listAssetMetadata(String gitFolder, String extn, String replacer,
			List<String> dirList, List<Map<String, Object>> retList) {
		if (dirList == null) {
			dirList = new ArrayList<String>();
		}
		if (retList == null) {
			retList = new ArrayList<>();
		}

		File folder = new File(gitFolder);
		File[] listOfFiles = folder.listFiles();

		for (File f : listOfFiles) {
			Map<String, Object> fileMap = new HashMap<>();
			fileMap.put("name", f.getName());
			String relative = new File(replacer).toURI().relativize(new File(f.getAbsolutePath()).toURI()).getPath();
			fileMap.put("path", relative);
			fileMap.put("lastModified", getDate(f.lastModified()));
			if (f.isFile()) {
				String path = f.getAbsolutePath();
				path = path.replaceAll("\\\\", "/");
				if (isMatch(Utility.getInstanceName(path), extn)) {
					if (replacer != null) {
						path = path.replaceAll(replacer, "");
					}

					path.replaceFirst("/", "");
					fileMap.put("type", FilenameUtils.getExtension(path));
					retList.add(fileMap);
				}
			} else if (f.isDirectory()) {
				String path = f.getName();
				if (!path.startsWith(".")) { // no hidden files
					dirList.add(f.getAbsolutePath());
					if (isMatch(path, extn)) {
						fileMap.put("path", path);
						fileMap.put("type", "directory");
						retList.add(fileMap);
					}
				}
			}
		}

		if (!dirList.isEmpty()) {
			return listAssetMetadata(dirList.remove(0), extn, replacer, dirList, retList);
		}

		return retList;
	}

	/**
	 * Performs glob-style wildcard matching of a string against a pattern, where
	 * {@code ?} matches any single character and {@code *} matches any (possibly
	 * empty) sequence of characters. The entire string must be consumed for a
	 * match.
	 *
	 * @param s the string to test
	 * @param p the pattern, which may contain {@code ?} and {@code *} wildcards
	 * @return {@code true} if {@code s} matches {@code p} in full, {@code false}
	 *         otherwise
	 */
	public static boolean isMatch(String s, String p) {
		int i = 0;
		int j = 0;
		int starIndex = -1;
		int iIndex = -1;

		while (i < s.length()) {
			if (j < p.length() && (p.charAt(j) == '?' || p.charAt(j) == s.charAt(i))) {
				++i;
				++j;
			} else if (j < p.length() && p.charAt(j) == '*') {
				starIndex = j;
				iIndex = i;
				j++;
			} else if (starIndex != -1) {
				j = starIndex + 1;
				i = iIndex + 1;
				iIndex++;
			} else {
				return false;
			}
		}

		while (j < p.length() && p.charAt(j) == '*') {
			++j;
		}

		return j == p.length();
	}

	/**
	 * Lists the immediate (non-recursive) contents of the given folder, separating
	 * files from directories. File entries record the raw file name; directory
	 * entries are limited to non-hidden directories (those not starting with
	 * {@code .}) and have each occurrence of {@code replacer} replaced with
	 * {@code replaceWith}. Last-modified timestamps are formatted via
	 * {@link #getDate(long)} and returned in parallel lists.
	 *
	 * @param gitFolder   absolute path of the folder to browse
	 * @param replacer    substring replaced within directory names; applied only
	 *                    when non-{@code null}
	 * @param replaceWith replacement text substituted for {@code replacer} in
	 *                    directory names
	 * @return a map keyed by {@code FILE_LIST} (file names), {@code DIR_LIST}
	 *         (visible directory names), {@code FILE_DATE} (file last-modified
	 *         dates) and {@code DIR_DATE} (directory last-modified dates)
	 */
	public static Map<String, List<String>> browse(String gitFolder, String replacer, String replaceWith) {
		Map<String, List<String>> retHash = new Hashtable<String, List<String>>();

		List<String> files = new ArrayList<String>();
		List<String> directories = new ArrayList<String>();
		List<String> fileDates = new ArrayList<String>();
		List<String> dirDates = new ArrayList<String>();

		File folder = new File(gitFolder);
		File[] listOfFiles = folder.listFiles();
		String repString = replaceWith; // can be $IF

		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				String path = listOfFiles[i].getName();
				// we probably dont need this anymore
				/*
				 * if(replacer != null) { path = path.replaceAll(replacer, repString); }
				 */
				files.add(path.replaceFirst("/", ""));
				String time = getDate(listOfFiles[i].lastModified());
				fileDates.add(time);
			} else if (listOfFiles[i].isDirectory()) {
				String path = listOfFiles[i].getName().replaceAll("\\\\", "/");
				// no hidden files
				if (!path.startsWith(".")) {
					if (replacer != null) {
						path = path.replaceAll(replacer, repString);
					}
					directories.add(path);
				}
				String time = getDate(listOfFiles[i].lastModified());
				dirDates.add(time);
			}
		}
		retHash.put("FILE_LIST", files);
		retHash.put("DIR_LIST", directories);
		retHash.put("FILE_DATE", fileDates);
		retHash.put("DIR_DATE", dirDates);

		return retHash;
	}

}

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

import prerna.auth.User;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public final class FileSystemUtil {

	private static final Logger classLogger = LogManager.getLogger(FileSystemUtil.class);

	/**
	 * 
	 * @param user
	 * @param filePath
	 * @param relativeFilePath
	 * @param pathSubstringIndex
	 * @return
	 */
	public static List<Map<String, Object>> browseFileSystem(User user, String filePath, String relativeFilePath,
			int pathSubstringIndex) {
		File directory = new File(filePath);
		if (!directory.exists()) {
			throw new IllegalArgumentException(
					"The directory " + relativeFilePath + " does not exist within the assets folder");
		}
		if (!directory.isDirectory()) {
			throw new IllegalArgumentException(
					"The path " + relativeFilePath + " exists within the assets folder but is not a directory");
		}

		DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss")
				.withZone(user.getZoneId());

		List<Map<String, Object>> retObj = new ArrayList<>();
		File[] allFiles = directory.listFiles();
		for (File f : allFiles) {
			if (f.getName().startsWith(".") && f.isDirectory()) {
				// we dont want to show this
				continue;
			}
			Map<String, Object> fileMap = new HashMap<>();
			fileMap.put("name", f.getName());
			String path = f.getAbsolutePath().substring(pathSubstringIndex).replace("\\", "/");
			if (f.isDirectory()) {
				fileMap.put("type", "directory");
				path = path + "/";
			} else {
				fileMap.put("type", FilenameUtils.getExtension(f.getName()));
			}
			fileMap.put("path", path);
			fileMap.put("lastModified", dateTimeFormatter.format(Instant.ofEpochMilli(f.lastModified())));
			retObj.add(fileMap);
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
	 * Searches for files and directories recursively and returns a sorted list of
	 * results.
	 * 
	 * @param dir               The directory to start the search from.
	 * @param pattern           The pattern to match file/directory names against.
	 * @param baseLen           The base length for calculating relative paths.
	 * @param dateTimeFormatter The date time formatter for last modified dates.
	 * @return A sorted list of maps, where each map represents a file or directory.
	 */
	public static List<Map<String, Object>> search(File dir, Pattern pattern, int baseLen,
			DateTimeFormatter dateTimeFormatter) {
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
	 * 
	 * @param dir
	 * @param pattern
	 * @param baseLen
	 * @param results
	 * @param dateTimeFormatter
	 */
	public static void searchRecursive(File dir, Pattern pattern, int baseLen, List<Map<String, Object>> results,
			DateTimeFormatter dateTimeFormatter) {
		File[] entries = dir.listFiles();
		if (entries == null) {
			return;
		}

		for (File f : entries) {
			String name = f.getName();
			// skip hidden directory
			if (f.isDirectory() && name.startsWith(".")) {
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
	 * 
	 * @param f
	 * @param relativePath
	 * @param isDir
	 * @param dateTimeFormatter
	 * @return
	 */
	private static Map<String, Object> createMeta(File f, String relativePath, boolean isDir,
			DateTimeFormatter dateTimeFormatter) {
		Map<String, Object> map = new HashMap<>();
		map.put("name", f.getName());
		map.put("path", relativePath);
		map.put("lastModified", dateTimeFormatter.format(Instant.ofEpochMilli(f.lastModified())));
		map.put("type", isDir ? "directory" : FilenameUtils.getExtension(f.getName()));
		return map;
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
			FileUtils.writeStringToFile(file, "new file", StandardCharsets.UTF_8);
		} catch (IOException e) {
			classLogger.error("Error creating new asset file {}", filePath, e);
			NounMetadata error = NounMetadata.getErrorNounMessage("Unable to save file: " + filePath);
			SemossPixelException exception = new SemossPixelException(error);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
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
		if (destFile.exists()) {
			throw new IllegalArgumentException(
					"A file or directory already exists at the destination: " + destFileName);
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
			if (decodeBase64) {
				try {
					content = new String(Base64.getDecoder().decode(content), StandardCharsets.UTF_8);
				} catch (Exception e) {
					throw new IllegalArgumentException(
							"Failed to decode string input: input is not base64-encoded utf-8 string", e);
				}
			}

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
	 * @param fileLocation The location fo the file
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

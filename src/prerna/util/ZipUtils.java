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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.GsonBuilder;

import prerna.util.gson.GsonUtility;

public final class ZipUtils {

	private static final Logger classLogger = LogManager.getLogger(ZipUtils.class);

	// buffer for read and write data to file
	private static byte[] buffer = new byte[2048];

	// always need to use this for zipping up and unzipping
	// traversing will break if the file separator is a "\"
	// which is generated on windows
	public static final String FILE_SEPARATOR = "/";

	private static final long MAX_TOTAL_UNCOMPRESSED_BYTES = 10L * 1024 * 1024 * 1024;
	private static final int MAX_ENTRIES = 100_000;

	private ZipUtils() {

	}

	/**
	 * Zip files within a dir
	 * 
	 * @param folderPath
	 * @param zipFilePath
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static ZipOutputStream zipFolder(String folderPath, String zipFilePath)
			throws FileNotFoundException, IOException {
		return zipFolder(folderPath, zipFilePath, null, null);
	}

	/**
	 * Zip files within a dir
	 * 
	 * @param folderPath
	 * @param zipFilePath
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
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
	 * Add file to ZipOutputStream
	 * 
	 * @param file
	 * @param zos
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void addToZipFile(File file, ZipOutputStream zos) throws FileNotFoundException, IOException {
		ZipEntry zipEntry = new ZipEntry(file.getName());
		zos.putNextEntry(zipEntry);

		FileInputStream fis = null;
		try {
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
	 * 
	 * @param logger
	 * @param zos
	 * @param prefixForZip
	 * @param filePathToWrite
	 * @param objToWrite
	 * @throws IOException
	 */
	public static void zipObjectToFile(ZipOutputStream zos, String prefixForZip, String filePathToWrite,
			Object objToWrite) throws IOException {
		File newFile = new File(filePathToWrite);
		GsonUtility.writeObjectToJsonFile(newFile, new GsonBuilder().setPrettyPrinting().create(), objToWrite);
		ZipUtils.addToZipFile(newFile, zos, prefixForZip);
	}

	/**
	 * Add file to ZipOutputStream
	 * 
	 * @param file
	 * @param zos
	 * @throws FileNotFoundException
	 * @throws IOException
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
	 * 
	 * @param file
	 * @param zos
	 * @param prefix
	 * @param ignoreFiles
	 * @throws FileNotFoundException
	 * @throws IOException
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
	 * Unzip files to a folder and track files that have been added
	 * 
	 * @param zipFilePath
	 * @param destDirectory
	 * @return Map of list of files depending on if it is a DIR or FILE
	 * @throws IOException
	 */
	public static Map<String, List<String>> unzip(String zipFilePath, String destination) throws IOException {
		// grab list of files that are being unzipped
		Map<String, List<String>> files = listFilesInZip(Paths.get(zipFilePath));
		// unzip files
		ZipFile zipIn = null;
		try {
			zipIn = new ZipFile(Utility.normalizePath(zipFilePath));
			Path destinationRoot = Paths.get(Utility.normalizePath(destination));
			Files.createDirectories(destinationRoot);
			destinationRoot = destinationRoot.toRealPath();

			Enumeration<? extends ZipEntry> entries = zipIn.entries();
			int entryCount = 0;
			long remainingBytes = MAX_TOTAL_UNCOMPRESSED_BYTES;
			while (entries.hasMoreElements()) {
				ZipEntry entry = entries.nextElement();
				if (++entryCount > MAX_ENTRIES) {
					throw new IOException("Archive contains more than " + MAX_ENTRIES + " entries");
				}
				Path target;
				try {
					target = Utility.resolveWithin(destinationRoot, entry.getName());
				} catch (IllegalArgumentException | SecurityException e) {
					throw new IOException(
							"Archive entry escapes the destination directory: " + Utility.cleanLogString(entry.getName()),
							e);
				}
				if (entry.isDirectory()) {
					Files.createDirectories(target);
				} else {
					Path parent = target.getParent();
					if (parent != null) {
						Files.createDirectories(parent);
					}
					try (InputStream is = zipIn.getInputStream(entry)) {
						remainingBytes -= extractFile(is, target, remainingBytes);
					}
				}
			}
		} finally {
			if (zipIn != null) {
				zipIn.close();
			}
		}

		return files;
	}

	/**
	 * Copy file to path
	 * 
	 * @param zipIn
	 * @param target
	 * @param remainingBytes Uncompressed budget left for the whole archive
	 * @return Number of bytes written
	 * @throws IOException
	 */
	private static long extractFile(InputStream zipIn, Path target, long remainingBytes) throws IOException {
		BufferedOutputStream bos = null;
		long written = 0;
		try {
			bos = new BufferedOutputStream(Files.newOutputStream(target));
			byte[] bytesIn = buffer;
			int read = 0;
			while ((read = zipIn.read(bytesIn)) != -1) {
				written += read;
				if (written > remainingBytes) {
					throw new IOException("Archive exceeds the maximum uncompressed size of "
							+ MAX_TOTAL_UNCOMPRESSED_BYTES + " bytes");
				}
				bos.write(bytesIn, 0, read);
			}
		} finally {
			try {
				if (bos != null) {
					bos.close();
				}
			} catch (IOException e) {
				classLogger.error("Failed to close output stream after extracting zip entry to '{}'", target, e);
			}
		}
		return written;
	}

	/**
	 * https://stackoverflow.com/questions/15667125/read-content-from-files-which-are-inside-zip-file
	 * NOTE ::: Cleaning up paths to remove initial / to push files to git
	 * 
	 * @param fromZip
	 * @throws IOException
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
	 * 
	 * @param sourceFile
	 * @param gzipFile
	 * @throws IOException
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

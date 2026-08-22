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
package prerna.engine.impl.storage;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.StorageTypeEnum;

/**
 * A true local-filesystem storage engine that performs all I/O via
 * {@link java.nio.file} APIs.
 *
 * Unlike {@link LocalFileSystemStorageEngine}, this engine does NOT shell out
 * to the rclone binary. It is intended only for developers running SEMOSS in
 * local environments and is gated to administrators at registration time.
 *
 * All storage paths are resolved against {@code PATH_PREFIX} and are sandboxed
 * to prevent traversal outside that root.
 */
public class DeveloperLocalFileSystemStorageEngine extends AbstractStorageEngine {

	private static final Logger classLogger = LogManager.getLogger(DeveloperLocalFileSystemStorageEngine.class);

	public static final String PATH_PREFIX = "PATH_PREFIX";

	protected String pathPrefix;
	protected Path pathPrefixRoot;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		String configured = smssProp.getProperty(PATH_PREFIX);
		if (configured == null || configured.trim().isEmpty()) {
			throw new IllegalArgumentException(
					"Must provide the " + PATH_PREFIX + " for the developer local file system");
		}

		configured = configured.replace("\\", "/").trim();
		while (configured.length() > 1 && configured.endsWith("/")) {
			configured = configured.substring(0, configured.length() - 1);
		}
		this.pathPrefix = configured;
		this.pathPrefixRoot = Paths.get(this.pathPrefix).toAbsolutePath().normalize();

		if (!Files.exists(this.pathPrefixRoot)) {
			Files.createDirectories(this.pathPrefixRoot);
		} else if (!Files.isDirectory(this.pathPrefixRoot)) {
			throw new IllegalArgumentException(PATH_PREFIX + " must point to a directory: " + this.pathPrefixRoot);
		}
	}

	@Override
	public StorageTypeEnum getStorageType() {
		return StorageTypeEnum.DEVELOPER_LOCAL_FILE_SYSTEM;
	}

	/**
	 * Resolve a caller-supplied storage path against the configured prefix and
	 * verify it does not escape the sandbox.
	 */
	protected Path resolveStoragePath(String storagePath) {
		String relative = storagePath == null ? "" : storagePath.replace("\\", "/").trim();
		while (relative.startsWith("/")) {
			relative = relative.substring(1);
		}
		Path resolved = this.pathPrefixRoot.resolve(relative).normalize();
		if (!resolved.startsWith(this.pathPrefixRoot)) {
			throw new IllegalArgumentException(
					"Storage path resolves outside of configured " + PATH_PREFIX + ": " + storagePath);
		}
		return resolved;
	}

	@Override
	public List<String> list(String path) throws Exception {
		List<String> result = new ArrayList<>();
		Path target = resolveStoragePath(path);
		if (!Files.exists(target)) {
			return result;
		}
		if (!Files.isDirectory(target)) {
			result.add(target.getFileName().toString());
			return result;
		}
		try (Stream<Path> children = Files.list(target)) {
			children.forEach(child -> {
				String name = child.getFileName().toString();
				if (Files.isDirectory(child)) {
					name = name + "/";
				}
				result.add(name);
			});
		}
		return result;
	}

	@Override
	public List<Map<String, Object>> listDetails(String path) throws Exception {
		List<Map<String, Object>> result = new ArrayList<>();
		Path target = resolveStoragePath(path);
		if (!Files.exists(target)) {
			return result;
		}
		if (!Files.isDirectory(target)) {
			result.add(toDetailMap(target));
			return result;
		}
		try (Stream<Path> children = Files.list(target)) {
			List<Path> sorted = new ArrayList<>();
			children.forEach(sorted::add);
			sorted.sort(Comparator.comparing(p -> p.getFileName().toString()));
			for (Path child : sorted) {
				result.add(toDetailMap(child));
			}
		}
		return result;
	}

	private Map<String, Object> toDetailMap(Path entry) throws IOException {
		Map<String, Object> details = new HashMap<>();
		boolean isDir = Files.isDirectory(entry);
		BasicFileAttributes attrs = Files.readAttributes(entry, BasicFileAttributes.class);

		String relative = this.pathPrefixRoot.relativize(entry).toString().replace("\\", "/");

		details.put("Name", entry.getFileName().toString());
		details.put("Size", isDir ? 0L : attrs.size());
		String mime = null;
		if (!isDir) {
			try {
				mime = Files.probeContentType(entry);
			} catch (IOException e) {
				classLogger.debug("Unable to probe content type for {}", entry, e);
			}
		}
		details.put("MimeType", mime);
		details.put("ModTime", attrs.lastModifiedTime().toInstant().toString());
		details.put("IsDir", isDir);
		details.put("Path", relative);
		return details;
	}

	@Override
	public StorageSyncStatus syncLocalToStorage(String localPath, String storagePath, Map<String, Object> metadata)
			throws Exception {
		List<Path> sources = parseLocalPaths(localPath);
		Path destinationRoot = resolveStoragePath(storagePath);
		Files.createDirectories(destinationRoot);

		for (Path source : sources) {
			if (!Files.exists(source)) {
				throw new IOException("Local path does not exist: " + source);
			}
			Path destination = Files.isDirectory(source) ? destinationRoot
					: destinationRoot.resolve(source.getFileName().toString());
			mirror(source, destination);
		}

		// mirror copies recursively without reporting names, and a failure throws, so
		// reaching this point is success
		return StorageSyncStatus.of(storagePath, null, null, null);
	}

	@Override
	public void syncStorageToLocal(String storagePath, String localPath) throws Exception {
		Path source = resolveStoragePath(storagePath);
		if (!Files.exists(source)) {
			throw new IOException("Storage path does not exist: " + storagePath);
		}
		Path destinationRoot = Paths.get(localPath).toAbsolutePath().normalize();
		Files.createDirectories(destinationRoot);
		Path destination = Files.isDirectory(source) ? destinationRoot
				: destinationRoot.resolve(source.getFileName().toString());
		mirror(source, destination);
	}

	/**
	 * Make destination an exact mirror of source. Files are copied when missing or
	 * when size / lastModifiedTime differ; files present in destination but absent
	 * from source are removed.
	 */
	private void mirror(Path source, Path destination) throws IOException {
		if (Files.isRegularFile(source)) {
			// When destination is an existing directory, the actual target file is
			// <destination>/<source-filename>. Use a new local so the parameter stays
			// effectively final - the lambdas below capture `destination` and require it.
			Path target = Files.isDirectory(destination) ? destination.resolve(source.getFileName().toString())
					: destination;
			Files.createDirectories(target.getParent());
			copyIfChanged(source, target);
			return;
		}

		Files.createDirectories(destination);
		try (Stream<Path> walk = Files.walk(source)) {
			walk.forEach(srcEntry -> {
				try {
					Path rel = source.relativize(srcEntry);
					Path dstEntry = destination.resolve(rel.toString());
					if (Files.isDirectory(srcEntry)) {
						Files.createDirectories(dstEntry);
					} else {
						copyIfChanged(srcEntry, dstEntry);
					}
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
		}

		try (Stream<Path> walk = Files.walk(destination)) {
			List<Path> extras = new ArrayList<>();
			walk.forEach(dstEntry -> {
				if (dstEntry.equals(destination)) {
					return;
				}
				Path rel = destination.relativize(dstEntry);
				Path srcEntry = source.resolve(rel.toString());
				if (!Files.exists(srcEntry)) {
					extras.add(dstEntry);
				}
			});
			extras.sort(Comparator.comparingInt(p -> -p.getNameCount()));
			for (Path extra : extras) {
				Files.deleteIfExists(extra);
			}
		}
	}

	private void copyIfChanged(Path source, Path destination) throws IOException {
		if (Files.exists(destination)) {
			long srcSize = Files.size(source);
			long dstSize = Files.size(destination);
			FileTime srcMod = Files.getLastModifiedTime(source);
			FileTime dstMod = Files.getLastModifiedTime(destination);
			if (srcSize == dstSize && srcMod.equals(dstMod)) {
				return;
			}
		} else {
			Files.createDirectories(destination.getParent());
		}
		Files.copy(source, destination, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES);
	}

	@Override
	public String copyToStorage(String localFilePath, String storageFolderPath, Map<String, Object> metadata)
			throws Exception {
		List<Path> sources = parseLocalPaths(localFilePath);
		Path destinationFolder = resolveStoragePath(storageFolderPath);
		Files.createDirectories(destinationFolder);

		for (Path source : sources) {
			if (!Files.exists(source)) {
				throw new IOException("Local path does not exist: " + source);
			}
			Path destination = destinationFolder.resolve(source.getFileName().toString());
			if (Files.isDirectory(source)) {
				copyDirectory(source, destination);
			} else {
				Files.copy(source, destination, StandardCopyOption.REPLACE_EXISTING,
						StandardCopyOption.COPY_ATTRIBUTES);
			}
		}
		return null;
	}

	@Override
	public void copyToLocal(String storageFilePath, String localFolderPath, String versionId) throws Exception {
		// version id is not supported and ignored
		Path source = resolveStoragePath(storageFilePath);
		if (!Files.exists(source)) {
			throw new IOException("Storage path does not exist: " + storageFilePath);
		}
		Path destinationFolder = Paths.get(localFolderPath).toAbsolutePath().normalize();
		Files.createDirectories(destinationFolder);
		Path destination = destinationFolder.resolve(source.getFileName().toString());
		if (Files.isDirectory(source)) {
			copyDirectory(source, destination);
		} else {
			Files.copy(source, destination, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES);
		}
	}

	private void copyDirectory(Path source, Path destination) throws IOException {
		Files.createDirectories(destination);
		try (Stream<Path> walk = Files.walk(source)) {
			walk.forEach(srcEntry -> {
				try {
					Path rel = source.relativize(srcEntry);
					Path dstEntry = destination.resolve(rel.toString());
					if (Files.isDirectory(srcEntry)) {
						Files.createDirectories(dstEntry);
					} else {
						Files.createDirectories(dstEntry.getParent());
						Files.copy(srcEntry, dstEntry, StandardCopyOption.REPLACE_EXISTING,
								StandardCopyOption.COPY_ATTRIBUTES);
					}
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
		}
	}

	@Override
	public void deleteFromStorage(String storagePath) throws Exception {
		deleteFromStorage(storagePath, false);
	}

	@Override
	public void deleteFromStorage(String storagePath, boolean leaveFolderStructure) throws Exception {
		Path target = resolveStoragePath(storagePath);
		if (!Files.exists(target)) {
			return;
		}
		if (!Files.isDirectory(target)) {
			Files.deleteIfExists(target);
			return;
		}
		deleteDirectory(target, leaveFolderStructure);
	}

	@Override
	public void deleteFolderFromStorage(String storageFolderPath) throws Exception {
		Path target = resolveStoragePath(storageFolderPath);
		if (!Files.exists(target)) {
			return;
		}
		if (!Files.isDirectory(target)) {
			Files.deleteIfExists(target);
			return;
		}
		deleteDirectory(target, false);
	}

	private void deleteDirectory(Path root, boolean keepRoot) throws IOException {
		Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				Files.deleteIfExists(file);
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
				if (exc != null) {
					throw exc;
				}
				if (!(keepRoot && dir.equals(root))) {
					Files.deleteIfExists(dir);
				}
				return FileVisitResult.CONTINUE;
			}
		});
	}

	@Override
	public byte[] readBlobToMemory(String storagePath) throws Exception {
		Path target = resolveStoragePath(storagePath);
		if (!Files.exists(target) || Files.isDirectory(target)) {
			throw new IOException("Storage file does not exist: " + storagePath);
		}
		return Files.readAllBytes(target);
	}

	/**
	 * No-op. This engine is stateless - there are no connections, streams, or
	 * background processes to release.
	 */
	@Override
	public void close() {
		// nothing to do
	}

}

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
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

import prerna.engine.api.IEngine;
import prerna.engine.api.IStorageEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.util.Utility;

public abstract class AbstractStorageEngine extends AbstractEngine implements IStorageEngine {

	private static final Logger classLogger = LogManager.getLogger(AbstractStorageEngine.class);

	protected static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

	// how many files an engine transfers at once
	public static final String TRANSFER_LIMIT_KEY = "TRANSFER_LIMIT";
	protected static final int DEFAULT_TRANSFER_LIMIT = 8;

	protected int transferLimit = DEFAULT_TRANSFER_LIMIT;

	/**
	 * What storage already holds for one object, as reported by a listing.
	 *
	 * Listings return this for free, which is the point: comparing against a
	 * listing avoids a HEAD request per file.
	 *
	 * @param size               object size in bytes
	 * @param lastModifiedMillis when storage last wrote it, epoch millis
	 */
	protected record StoredObjectStat(long size, long lastModifiedMillis) {
	}

	/**
	 * What happened to one file in a parallel transfer.
	 *
	 * Failures are carried back rather than thrown so that one bad file does not
	 * abandon the rest of the batch, and so the caller can still report which files
	 * did not make it.
	 *
	 * @param fileKey the object key that was being written
	 * @param failure what went wrong, or null when the transfer succeeded
	 */
	protected record TransferOutcome(String fileKey, Exception failure) {
	}

	/**
	 * Init the general storage values
	 *
	 * @param builder
	 * @throws Exception
	 */
	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		String transferLimitStr = smssProp.getProperty(TRANSFER_LIMIT_KEY);
		if (transferLimitStr != null && !(transferLimitStr = transferLimitStr.trim()).isEmpty()) {
			try {
				int parsed = Integer.parseInt(transferLimitStr);
				if (parsed > 0) {
					this.transferLimit = parsed;
				} else {
					classLogger.warn("{} must be greater than zero, ignoring '{}' and using {}", TRANSFER_LIMIT_KEY,
							transferLimitStr, this.transferLimit);
				}
			} catch (NumberFormatException e) {
				classLogger.warn("Unable to parse {}='{}', using {}", TRANSFER_LIMIT_KEY, transferLimitStr,
						this.transferLimit, e);
			}
		}
	}

	/**
	 * Runs file transfers concurrently, at most transferLimit of them at a time.
	 *
	 * Virtual threads are not pooled, so the cap comes from a semaphore rather than
	 * from the executor. A transfer spends nearly all of its time blocked on a
	 * socket, which is exactly what a virtual thread is cheap at.
	 *
	 * Each task returns its own outcome and nothing is shared, so callers do not
	 * need thread safe collections. Results come back in submission order.
	 *
	 * @param <T>       whatever a single transfer reports back
	 * @param transfers one task per file
	 * @return each task's result, in the order the tasks were given
	 * @throws Exception the first failure, unwrapped from ExecutionException. A
	 *                   failure here is not swallowed the way a per file catch
	 *                   inside a walk would be
	 */
	protected <T> List<T> runTransfersInParallel(List<Callable<T>> transfers) throws Exception {
		return runTransfersInParallel(transfers, this.transferLimit);
	}

	/**
	 * Same as runTransfersInParallel but with the concurrency stated explicitly,
	 * for work that is nested inside a transfer and so must not draw from the same
	 * budget. Uploading the parts of one file is the case that needs this: the file
	 * itself already counts against transferLimit.
	 *
	 * Every call gets its own semaphore, so a nested call cannot be starved by the
	 * outer one. What it can do is multiply the total requests in flight, which is
	 * why the caller states the limit rather than inheriting it.
	 *
	 * @param <T>       whatever a single transfer reports back
	 * @param transfers one task per unit of work
	 * @param limit     how many may run at once
	 * @return each task's result, in the order the tasks were given
	 * @throws Exception the first failure, unwrapped from ExecutionException
	 */
	protected <T> List<T> runTransfersInParallel(List<Callable<T>> transfers, int limit) throws Exception {
		if (transfers == null || transfers.isEmpty()) {
			return Collections.emptyList();
		}

		Semaphore permits = new Semaphore(Math.max(1, limit));
		List<Future<T>> futures = new ArrayList<>(transfers.size());
		// close() waits for every submitted task before returning
		try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
			for (Callable<T> transfer : transfers) {
				futures.add(executor.submit(() -> {
					permits.acquire();
					try {
						return transfer.call();
					} finally {
						permits.release();
					}
				}));
			}
		}

		List<T> results = new ArrayList<>(futures.size());
		for (Future<T> future : futures) {
			try {
				results.add(future.get());
			} catch (ExecutionException e) {
				// the wrapper adds nothing, hand back what actually went wrong
				Throwable cause = e.getCause();
				if (cause instanceof Exception) {
					throw (Exception) cause;
				}
				throw e;
			}
		}
		return results;
	}

	/**
	 * Decides whether a local file still needs to be written to storage, comparing
	 * against what a listing said is already there.
	 *
	 * @param localFile the file about to be uploaded
	 * @param stored    what storage holds for that key, or null when it holds
	 *                  nothing
	 * @return true when the file is missing from storage, a different size, or
	 *         newer locally
	 * @throws IOException if the local file cannot be read
	 */
	protected boolean needsUpload(Path localFile, StoredObjectStat stored) throws IOException {
		return needsUpload(localFile, stored, 0L);
	}

	/**
	 * Same comparison, but ignoring modified time differences smaller than the
	 * given tolerance.
	 *
	 * Some protocols keep a coarser timestamp than the local file system does. SFTP
	 * for instance reports whole seconds, so a file written at .750 comes back as
	 * .000 and looks older than the local copy on every pass, which would re-upload
	 * an unchanged folder forever. The tolerance is the storage side's resolution.
	 *
	 * @param localFile       the file about to be uploaded
	 * @param stored          what storage holds for that key, or null when it holds
	 *                        nothing
	 * @param toleranceMillis how much newer the local file has to be before it
	 *                        counts as changed
	 * @return true when the file is missing from storage, a different size, or
	 *         newer locally by more than the tolerance
	 * @throws IOException if the local file cannot be read
	 */
	protected boolean needsUpload(Path localFile, StoredObjectStat stored, long toleranceMillis) throws IOException {
		if (stored == null) {
			return true;
		}
		return Files.size(localFile) != stored.size()
				|| Files.getLastModifiedTime(localFile).toMillis() > stored.lastModifiedMillis() + toleranceMillis;
	}

	/**
	 * Whether a stored object still has to be downloaded over the local copy.
	 *
	 * A local file that is at least as new as the stored object is left where it
	 * is. That skips the copy that is already identical, which is what rclone copy
	 * does by default, and it also leaves alone a local file holding work that
	 * storage has not seen yet. A room folder pulled part way through a turn is why
	 * the second half matters - overwriting there drops the live session state.
	 *
	 * @param localFile                the file the download would write
	 * @param storedLastModifiedMillis when the stored object last changed, or null
	 *                                 when storage did not report it
	 * @return true when the download should go ahead
	 */
	/**
	 * Removes every empty directory under a local path, the deepest ones first.
	 *
	 * This is a local file system operation and touches nothing in storage. These
	 * stores have no real directories, so a folder that only ever held files which
	 * are now gone leaves an empty directory behind on the local side with nothing
	 * in storage to correspond to it.
	 *
	 * Deepest first matters: emptying a child makes its parent empty, and the
	 * parent is visited afterwards, so a whole emptied branch collapses in one
	 * pass. That includes rootPath itself once everything beneath it has gone.
	 *
	 * A directory that cannot be removed is logged and skipped rather than failing
	 * the transfer it is tidying up after.
	 *
	 * @param rootPath the local folder to clean out
	 */
	protected void deleteLocalEmptyDirectories(Path rootPath) {
		try (Stream<Path> stream = Files.walk(rootPath)) {
			List<Path> directories = stream.sorted(Comparator.reverseOrder()) // Delete children first
					.filter(Files::isDirectory).collect(Collectors.toList());

			for (Path dir : directories) {
				try (DirectoryStream<Path> entries = Files.newDirectoryStream(dir)) {
					if (!entries.iterator().hasNext()) { // Directory is empty
						Files.delete(dir);
						classLogger.info("Deleted empty local folder: {}", dir);
					}
				} catch (IOException e) {
					classLogger.error("Failed to delete empty folder: {}", dir, e);
				}
			}
		} catch (IOException e) {
			classLogger.error("Error while deleting empty directories", e);
		}
	}

	protected boolean needsDownload(Path localFile, Long storedLastModifiedMillis) {
		if (!Files.exists(localFile)) {
			return true;
		}
		if (storedLastModifiedMillis == null) {
			// nothing to compare against, so fetch it rather than keep a copy that
			// might be stale
			return true;
		}

		try {
			return storedLastModifiedMillis > Files.getLastModifiedTime(localFile).toMillis();
		} catch (IOException e) {
			classLogger.warn("Unable to read the timestamp of {}, downloading it again", localFile, e);
			return true;
		}
	}

	/**
	 * Whether a stored object is one of the placeholders written to keep an emptied
	 * folder visible, as opposed to a file the user actually uploaded.
	 *
	 * These stores have no real directories - a folder exists only because some key
	 * is prefixed with it. To keep a folder around after its contents are deleted,
	 * preserveFolderStructure writes a zero byte object whose key ends in a slash.
	 * That trailing slash is the whole difference between a placeholder and a
	 * legitimately empty file: normalizeStoragePrefixPath strips trailing slashes
	 * off anything a user asks to write, so a real upload can never produce one.
	 *
	 * Size alone is not enough. Treating every zero byte object as a placeholder
	 * silently deletes the user's own empty files on the next upload.
	 *
	 * @param objectKey the full key of the stored object
	 * @param size      its size in bytes
	 * @return true when the object is a folder placeholder and safe to clean up
	 */
	protected boolean isFolderPlaceholder(String objectKey, long size) {
		return size == 0 && objectKey != null && objectKey.endsWith("/");
	}

	/**
	 * Converts comma-separated local file/folder paths to List<Path>
	 * 
	 * @param commaSeparatedPaths
	 * @return
	 * @throws Exception
	 */
	protected List<Path> parseLocalPaths(String commaSeparatedPaths) throws Exception {
		List<Path> result = new ArrayList<>();
		String[] parts = commaSeparatedPaths.split(",");

		for (String part : parts) {
			String trimmed = part.trim();
			if (!trimmed.isEmpty()) {
				result.add(Paths.get(trimmed));
			}
		}

		return result;
	}

	/**
	 * Converts comma-separated cloud storage object paths to normalized String list
	 * 
	 * @param commaSeparatedPaths
	 * @return
	 */
	protected List<String> parseStorageObjectPaths(String commaSeparatedPaths) {
		List<String> result = new ArrayList<>();
		String[] parts = commaSeparatedPaths.split(",");

		for (String part : parts) {
			String trimmed = part.trim();
			if (!trimmed.isEmpty()) {
				// Normalize the path using the utility method
				String normalized = Utility.normalizePath(trimmed);
				// Remove the leading slash if present
				if (normalized.startsWith("/")) {
					normalized = normalized.substring(1);
				}
				result.add(normalized);
			}
		}

		return result;
	}

	/**
	 * Normalizes a user supplied storage path so that every engine builds the same
	 * object key from the same input.
	 *
	 * Object stores have no directories and no root - the key is just an opaque
	 * string, and "a/b.txt" and "/a/b.txt" are two unrelated objects that can both
	 * exist in the same bucket. That means whatever the user types has to be
	 * cleaned up in exactly one way everywhere, or a file pushed by one method is
	 * invisible to another. The assumptions made about what the user types are:
	 *
	 * <ul>
	 * <li>Surrounding whitespace is accidental and is trimmed, so " myfolder " and
	 * "myfolder" are the same folder. The trim has to happen before the slash
	 * checks below, otherwise " /myfolder" does not look like it starts with a
	 * slash and the leading slash survives into the key.</li>
	 * <li>A leading slash means "from the root of the bucket" rather than being
	 * part of the key, so it is removed. A key that really does start with a slash
	 * shows up as a blank named folder in the cloud console, is skipped by a prefix
	 * listing of "a/", and is not matched by prefix scoped IAM or lifecycle rules.
	 * So "myfolder" and "/myfolder" have to mean the same folder, or a file written
	 * by one call is invisible to the next.</li>
	 * <li>A trailing slash is folder notation rather than part of the key, so it is
	 * removed. Callers that need a listing prefix append their own single "/",
	 * which is what keeps a request for "dir" from also matching "dirty/...".
	 * Callers that delete everything a bare prefix returns therefore must not use
	 * this method, since they rely on the trailing slash to bound the match.</li>
	 * <li>A null or empty path means the root of the bucket and returns "".</li>
	 * </ul>
	 *
	 * This covers the storage path only. Whitespace around an uploaded file name is
	 * trimmed separately, where the upload methods turn a local file into a
	 * relative path.
	 *
	 * @param storagePath path provided by the caller, may be null
	 * @return normalized path with no surrounding whitespace and no leading or
	 *         trailing slash, or "" for the root of the bucket
	 */
	protected String normalizeStoragePrefixPath(String storagePath) {
		if (storagePath == null) {
			return "";
		}

		String normalized = Utility.normalizePath(storagePath).trim().replace("\\", "/");
		while (normalized.startsWith("/")) {
			normalized = normalized.substring(1);
		}
		while (!normalized.isEmpty() && normalized.endsWith("/")) {
			normalized = normalized.substring(0, normalized.length() - 1);
		}
		return normalized;
	}

	/**
	 * Resolves a storage object key to a path relative to the requested storage
	 * path.
	 *
	 * @param storageObjectKey full object key from cloud provider
	 * @param requestedPath    user-requested path (file or folder)
	 * @return relative path, or null when key does not belong to the requested
	 *         scope
	 */
	protected String resolveRelativeStoragePath(String storageObjectKey, String requestedPath) {
		if (storageObjectKey == null || storageObjectKey.trim().isEmpty()) {
			return null;
		}

		String normalizedObjectKey = storageObjectKey.trim().replace("\\", "/");
		while (normalizedObjectKey.startsWith("/")) {
			normalizedObjectKey = normalizedObjectKey.substring(1);
		}
		if (normalizedObjectKey.isEmpty()) {
			return null;
		}

		String normalizedRequestedPath = normalizeStoragePrefixPath(requestedPath);
		String relativePath;
		if (normalizedRequestedPath.isEmpty()) {
			relativePath = normalizedObjectKey;
		} else if (normalizedObjectKey.equals(normalizedRequestedPath)) {
			int slashIdx = normalizedObjectKey.lastIndexOf('/');
			relativePath = slashIdx >= 0 ? normalizedObjectKey.substring(slashIdx + 1) : normalizedObjectKey;
		} else {
			String folderPrefix = normalizedRequestedPath + "/";
			if (!normalizedObjectKey.startsWith(folderPrefix)) {
				return null;
			}
			relativePath = normalizedObjectKey.substring(folderPrefix.length());
		}

		while (relativePath.startsWith("/")) {
			relativePath = relativePath.substring(1);
		}
		if (relativePath.isEmpty() || relativePath.endsWith("/")) {
			return null;
		}
		return relativePath;
	}

	/**
	 * Copies a legacy smss property forward onto the key this engine actually
	 * reads.
	 *
	 * The engines behind a given storage type have changed implementation, and the
	 * old implementation read different property names. Rather than requiring every
	 * deployed smss file to be hand edited, each engine declares its old key names
	 * here and the value is carried over on open.
	 *
	 * The current key always wins. A legacy key is only consulted when the current
	 * one is missing or blank, and the first legacy key that has a value is the one
	 * used, so callers should pass them most specific first.
	 *
	 * @param smssProp   the properties being opened, updated in place
	 * @param currentKey the key this engine reads
	 * @param legacyKeys older key names to fall back on, in priority order
	 * @return true when a legacy value was carried over
	 */
	protected boolean migrateLegacyProperty(Properties smssProp, String currentKey, String... legacyKeys) {
		String current = smssProp.getProperty(currentKey);
		if (current != null && !current.trim().isEmpty()) {
			return false;
		}

		for (String legacyKey : legacyKeys) {
			String legacyValue = smssProp.getProperty(legacyKey);
			if (legacyValue != null && !legacyValue.trim().isEmpty()) {
				smssProp.put(currentKey, legacyValue);
				classLogger.warn(
						"Storage engine {} is still configured with the older property {}. Reading it as {} for now, "
								+ "but the smss should be updated.",
						SmssUtilities.getUniqueName(this.engineName, this.engineId), legacyKey, currentKey);
				return true;
			}
		}
		return false;
	}

	/**
	 * Flattens a caller supplied metadata map into the string to string map every
	 * provider requires. S3 user metadata, Azure blob metadata and GCS custom
	 * metadata are all string valued, so anything else has to be rendered on the
	 * way out.
	 *
	 * Strings pass through untouched. Everything else is written as JSON, so a List
	 * or a Map survives as something parseable rather than as the output of
	 * toString, which turns ["a","b"] into the unparseable "[a, b]".
	 *
	 * Null values are dropped rather than throwing. Collectors.toMap, which the
	 * engines used to use here, rejects null values, and calling toString on one
	 * NPEs before it even gets that far.
	 *
	 * @param metadata caller supplied metadata, may be null
	 * @return never null, empty when there is nothing to apply
	 */
	protected Map<String, String> flattenMetadata(Map<String, Object> metadata) {
		if (metadata == null || metadata.isEmpty()) {
			return Collections.emptyMap();
		}

		Map<String, String> flatMetadata = new HashMap<>(metadata.size());
		for (Map.Entry<String, Object> entry : metadata.entrySet()) {
			Object value = entry.getValue();
			if (value == null) {
				continue;
			}
			flatMetadata.put(entry.getKey(), value instanceof String ? (String) value : GSON.toJson(value));
		}
		return flatMetadata;
	}

	@Override
	public IEngine.CATALOG_TYPE getCatalogType() {
		return IEngine.CATALOG_TYPE.STORAGE;
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return this.getStorageType().toString();
	}

	@Override
	public boolean holdsFileLocks() {
		return false;
	}

}

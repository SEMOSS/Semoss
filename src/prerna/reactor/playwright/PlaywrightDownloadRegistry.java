/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.reactor.playwright;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.microsoft.playwright.Download;
import com.microsoft.playwright.Page;

import prerna.util.Utility;
import prerna.io.connector.antivirus.VirusScannerUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.User;
import prerna.om.Insight;

/**
 * Captures browser-native downloads for one Playwright session.
 *
 * <p>
 * Playwright stores downloads in a context-owned temporary directory. This
 * registry copies each download into a session-owned directory before it is
 * exposed to the remote viewer. The staged file can then be copied into an
 * Insight by the authenticated browser-session API without sending binary
 * content through WebSocket or MCP JSON.
 */
public final class PlaywrightDownloadRegistry {

	private static final Logger classLogger = LogManager.getLogger(PlaywrightDownloadRegistry.class);

	/** Maximum number of downloads retained for one run. */
	private static final int MAX_FILES_PER_RUN = positiveInt("PLAYWRIGHT_DOWNLOAD_MAX_FILES_PER_RUN", 50);
	/** Maximum size of one staged download. */
	private static final long MAX_BYTES_PER_FILE = positiveLong("PLAYWRIGHT_DOWNLOAD_MAX_BYTES_PER_FILE",
			100L * 1024L * 1024L);
	/** Maximum aggregate staged size for one run. */
	private static final long MAX_BYTES_PER_RUN = positiveLong("PLAYWRIGHT_DOWNLOAD_MAX_BYTES_PER_RUN",
			500L * 1024L * 1024L);

	private final Map<String, DownloadRecord> records = new ConcurrentHashMap<>();
	private final Map<String, Long> stagedBytesByRun = new ConcurrentHashMap<>();
	private final Map<String, AtomicInteger> fileCountsByRun = new ConcurrentHashMap<>();
	private final List<String> orderedIds = Collections.synchronizedList(new ArrayList<>());
	private final Set<Page> attachedPages = Collections.newSetFromMap(new ConcurrentHashMap<>());
	private final Set<Path> reservedStagedPaths = ConcurrentHashMap.newKeySet();
	private final List<Consumer<DownloadRecord>> additionalRecordListeners = new CopyOnWriteArrayList<>();
	private final Object recordMonitor = new Object();
	private final AtomicLong sequence = new AtomicLong(0);
	private final AtomicInteger activeCaptures = new AtomicInteger(0);

	private volatile Consumer<DownloadRecord> recordListener;
	private volatile DownloadTrigger activeTrigger;
	private volatile String activeRunId = UUID.randomUUID().toString();
	private volatile Path stagingRoot;

	/**
	 * Begins a fresh run. Existing records remain available for cleanup and
	 * diagnostics, but are not returned in the new run's result.
	 *
	 * @return the new run identifier
	 */
	public synchronized String beginRun() {
		activeRunId = UUID.randomUUID().toString();
		activeTrigger = null;
		stagedBytesByRun.put(activeRunId, 0L);
		fileCountsByRun.put(activeRunId, new AtomicInteger(0));
		return activeRunId;
	}

	public String getActiveRunId() {
		return activeRunId;
	}

	/** Registers a callback invoked after a download is fully staged or fails. */
	public void setRecordListener(Consumer<DownloadRecord> listener) {
		this.recordListener = listener;
	}

	/** Adds a listener without replacing the transport listener installed by a remote viewer. */
	public void addRecordListener(Consumer<DownloadRecord> listener) {
		if (listener != null) {
			additionalRecordListeners.add(listener);
		}
	}

	/**
	 * Associates downloads triggered by the current Playwright action with the
	 * action's request/step metadata.
	 */
	public void setActiveTrigger(DownloadTrigger trigger) {
		this.activeTrigger = trigger;
	}

	public void clearActiveTrigger() {
		this.activeTrigger = null;
	}

	/** Attaches a download listener once to a page. */
	public void attach(Page page, String tabId) {
		if (page == null || !attachedPages.add(page)) {
			return;
		}
		page.onDownload(download -> capture(download, page, tabId));
	}

	/**
	 * Returns records for the active run in download-event order.
	 */
	public List<DownloadRecord> getActiveRunRecords() {
		return getRecords(activeRunId);
	}

	/**
	 * Returns records for a run in deterministic download-event order.
	 */
	public List<DownloadRecord> getRecords(String runId) {
		String requestedRun = runId == null || runId.isBlank() ? activeRunId : runId;
		List<DownloadRecord> result = new ArrayList<>();
		synchronized (orderedIds) {
			for (String id : orderedIds) {
				DownloadRecord record = records.get(id);
				if (record != null && requestedRun.equals(record.getRunId())) {
					result.add(record);
				}
			}
		}
		return result;
	}

	/**
	 * Waits for download callbacks that have started but have not finished
	 * copying. This is used before a replay response is sent.
	 */
	public boolean awaitIdle(long timeoutMs) {
		long deadline = System.currentTimeMillis() + Math.max(0, timeoutMs);
		while (activeCaptures.get() > 0 && System.currentTimeMillis() < deadline) {
			try {
				Thread.sleep(Math.min(50, Math.max(1, deadline - System.currentTimeMillis())));
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return false;
			}
		}
		return activeCaptures.get() == 0;
	}

	/**
	 * Waits for a download emitted by a particular replay action. Playwright Java
	 * dispatches page events while its API is being pumped, so this method uses
	 * {@link Page#waitForTimeout(double)} rather than sleeping blindly when a page
	 * is available.
	 */
	public boolean awaitDownload(Page page, String runId, DownloadTrigger trigger, long timeoutMs) {
		String requestedRun = runId == null || runId.isBlank() ? activeRunId : runId;
		long deadline = System.currentTimeMillis() + Math.max(0, timeoutMs);
		while (System.currentTimeMillis() <= deadline) {
			if (hasMatchingRecord(requestedRun, trigger)) {
				awaitIdle(Math.max(1, deadline - System.currentTimeMillis()));
				return true;
			}
			long remaining = deadline - System.currentTimeMillis();
			if (remaining <= 0) {
				break;
			}
			long slice = Math.min(100, remaining);
			try {
				if (page != null && !page.isClosed()) {
					page.waitForTimeout(slice);
				} else {
					synchronized (recordMonitor) {
						recordMonitor.wait(slice);
					}
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return false;
			} catch (RuntimeException e) {
				// A navigation can destroy the page execution context while the wait is
				// pumping events. Continue with a condition wait so the replay can still
				// observe a context-level download callback.
				try {
					synchronized (recordMonitor) {
						recordMonitor.wait(slice);
					}
				} catch (InterruptedException interrupted) {
					Thread.currentThread().interrupt();
					return false;
				}
			}
		}
		return hasMatchingRecord(requestedRun, trigger);
	}

	private boolean hasMatchingRecord(String runId, DownloadTrigger trigger) {
		for (DownloadRecord record : getRecords(runId)) {
			if (trigger == null) {
				return true;
			}
			if (trigger.stepId() != null && !trigger.stepId().equals(record.getTriggerStepId())) {
				continue;
			}
			if (trigger.requestId() != null && !trigger.requestId().equals(record.getTriggerRequestId())) {
				continue;
			}
			return true;
		}
		return false;
	}

	/** Timeout used for explicitly download-producing replay steps. */
	public static long replayWaitTimeoutMs() {
		return positiveLong("PLAYWRIGHT_DOWNLOAD_REPLAY_WAIT_MS", 15_000L);
	}

	/** Returns the staged file for a captured record, or {@code null}. */
	public Path getStagedFile(String downloadId) {
		DownloadRecord record = records.get(downloadId);
		if (record == null) {
			return null;
		}
		Path path = record.getStagedPath();
		return path != null && Files.isRegularFile(path) ? path : null;
	}

	/** Returns a session-scoped record by id, including records from prior runs. */
	public DownloadRecord getRecord(String downloadId) {
		return downloadId == null ? null : records.get(downloadId);
	}

	/**
	 * Marks a record persisted to an Insight and removes its temporary copy.
	 */
	public synchronized void markSaved(String downloadId, String insightId, String insightPath) {
		DownloadRecord record = records.get(downloadId);
		if (record == null) {
			return;
		}
		record.markSaved(insightId, insightPath);
		deleteStagedFile(record);
	}

	public void markSaveFailed(String downloadId, String error) {
		DownloadRecord record = records.get(downloadId);
		if (record != null) {
			record.markSaveFailed(error);
		}
	}

	/**
	 * Persists one ready download directly into an authenticated Insight. Classic
	 * Playwright reactors already execute with the Insight's authenticated user, so
	 * they can use this same binary-safe persistence path without routing bytes
	 * through a WebSocket or MCP response.
	 */
	public Map<String, Object> persistRecord(DownloadRecord record, Insight insight) throws IOException {
		return persistRecord(record, insight, insight == null ? null : insight.getUser());
	}

	/**
	 * Persists one record while checking edit access for the authenticated actor.
	 * The actor is separate from the Insight owner because a room/project Insight
	 * may be editable by a delegated user.
	 */
	public Map<String, Object> persistRecord(DownloadRecord record, Insight insight, User actor) throws IOException {
		if (record == null || insight == null) {
			throw new IOException("Browser download or insight is missing");
		}
		if (insight.isSavedInsight() && (actor == null || !SecurityInsightUtils.userCanEditInsight(actor,
				insight.getProjectId(), insight.getRdbmsId()))) {
			throw new IOException("User does not have edit access to this insight");
		}
		synchronized (record) {
			if ("saved".equals(record.getStatus())) {
				return record.toMap();
			}
			if (!"ready".equals(record.getStatus()) && !"save-failed".equals(record.getStatus())) {
				return record.toMap();
			}
			Path staged = getStagedFile(record.getDownloadId());
			if (staged == null) {
				throw new IOException("Staged browser download is no longer available");
			}
			if (Files.size(staged) > MAX_BYTES_PER_FILE) {
				throw new IOException("Download exceeds the per-file limit of " + MAX_BYTES_PER_FILE + " bytes");
			}
			try {
				UUID.fromString(record.getRunId());
			} catch (IllegalArgumentException e) {
				throw new IOException("Invalid browser download run id", e);
			}
			String fileName = sanitizeFilename(record.getFileName());
			Path insightRoot = Path.of(Utility.normalizePath(insight.getInsightFolder())).toAbsolutePath().normalize();
			Files.createDirectories(insightRoot);
			Path realInsightRoot = insightRoot.toRealPath();
			Path targetDir = realInsightRoot.resolve("browser-downloads").resolve(record.getRunId()).normalize();
			if (!targetDir.startsWith(realInsightRoot)) {
				throw new IOException("Invalid browser download destination");
			}
			Files.createDirectories(targetDir);
			if (!targetDir.toRealPath().startsWith(realInsightRoot)) {
				throw new IOException("Invalid browser download destination");
			}
			Path target = targetDir.resolve(uniqueName(targetDir, fileName)).normalize();
			if (!target.startsWith(realInsightRoot)) {
				throw new IOException("Invalid browser download destination");
			}
			try (InputStream scanStream = Files.newInputStream(staged)) {
				Map<String, Collection<String>> viruses = VirusScannerUtils.getViruses(fileName, scanStream);
				if (viruses != null && !viruses.isEmpty()) {
					throw new IOException("Virus scan rejected browser download");
				}
			}
			Files.copy(staged, target);
			String insightPath = "/browser-downloads/" + record.getRunId() + "/" + target.getFileName();
			markSaved(record.getDownloadId(), insight.getInsightId(), insightPath);
			return record.toMap();
		}
	}

	/** Deletes all staged files belonging to the session. */
	public synchronized void cleanup() {
		Path root = stagingRoot;
		stagingRoot = null;
		stagedBytesByRun.clear();
		fileCountsByRun.clear();
		reservedStagedPaths.clear();
		if (root == null) {
			return;
		}
		try {
			if (Files.exists(root)) {
				try (var paths = Files.walk(root)) {
					paths.sorted((left, right) -> right.getNameCount() - left.getNameCount()).forEach(path -> {
						try {
							Files.deleteIfExists(path);
						} catch (IOException e) {
							classLogger.debug("Could not remove staged browser download {}", path, e);
						}
					});
				}
			}
		} catch (IOException e) {
			classLogger.debug("Could not clean staged browser downloads at {}", root, e);
		}
	}

	private void capture(Download download, Page page, String tabId) {
		activeCaptures.incrementAndGet();
		long order = sequence.incrementAndGet();
		String downloadId = "download-" + UUID.randomUUID();
		String runId = activeRunId;
		DownloadTrigger trigger = activeTrigger;
		String originalFileName = safeSuggestedFilename(download, order);
		DownloadRecord record = new DownloadRecord(downloadId, runId, order,
				sanitizeFilename(originalFileName), originalFileName,
				redactUrl(safeDownloadUrl(download)), redactUrl(safePageUrl(page)), tabId,
				trigger == null ? null : trigger.requestId(), trigger == null ? null : trigger.stepId(), Instant.now());
		records.put(downloadId, record);
		orderedIds.add(downloadId);
		try {
			Path destination = stagePath(record);
			if (destination == null) {
				return;
			}
			download.saveAs(destination);
			long size = Files.size(destination);
			if (size > MAX_BYTES_PER_FILE) {
				throw new IOException("Download exceeds the per-file limit of " + MAX_BYTES_PER_FILE + " bytes");
			}
			synchronized (this) {
				long runBytes = stagedBytesByRun.getOrDefault(record.getRunId(), 0L);
				if (runBytes + size > MAX_BYTES_PER_RUN) {
					throw new IOException("Download exceeds the per-run aggregate limit of " + MAX_BYTES_PER_RUN
							+ " bytes");
				}
				stagedBytesByRun.put(record.getRunId(), runBytes + size);
			}
			record.markCaptured(size, sha256(destination), detectMimeType(destination, record.getFileName()));
		} catch (Exception e) {
			String failure = safeDownloadFailure(download);
			record.markFailed(failure == null || failure.isBlank() ? safeMessage(e) : failure);
			deleteStagedFile(record);
		} finally {
			activeCaptures.decrementAndGet();
			notifyListener(record);
		}
	}

	private Path stagePath(DownloadRecord record) throws IOException {
		synchronized (this) {
			AtomicInteger fileCount = fileCountsByRun.computeIfAbsent(record.getRunId(), ignored -> new AtomicInteger());
			if (fileCount.incrementAndGet() > MAX_FILES_PER_RUN) {
				record.markFailed("Download count exceeds the per-run limit of " + MAX_FILES_PER_RUN);
				return null;
			}
			Path root = stagingRoot;
			if (root == null) {
				root = Files.createTempDirectory("semoss-playwright-downloads-");
				stagingRoot = root;
			}
			Path runDir = root.resolve(record.getRunId()).normalize();
			Files.createDirectories(runDir);
			String uniqueName = uniqueName(runDir, record.getFileName());
			Path destination = runDir.resolve(uniqueName).normalize();
			if (!destination.startsWith(runDir) || !reservedStagedPaths.add(destination)) {
				throw new IOException("Invalid or duplicate download filename");
			}
			record.setFileName(uniqueName);
			record.setStagedPath(destination);
			return destination;
		}
	}

	private String uniqueName(Path directory, String requestedName) {
		String base = FilenameUtils.getBaseName(requestedName);
		String extension = FilenameUtils.getExtension(requestedName);
		String candidate = requestedName;
		int counter = 2;
		while (Files.exists(directory.resolve(candidate)) || reservedStagedPaths.contains(directory.resolve(candidate))) {
			candidate = base + " (" + counter++ + ")" + (extension.isBlank() ? "" : "." + extension);
		}
		return candidate;
	}

	private void notifyListener(DownloadRecord record) {
		synchronized (recordMonitor) {
			recordMonitor.notifyAll();
		}
		Consumer<DownloadRecord> listener = recordListener;
		if (listener != null) {
			try {
				listener.accept(record);
			} catch (RuntimeException e) {
				classLogger.debug("Browser download listener failed for {}", record.getDownloadId(), e);
			}
		}
		for (Consumer<DownloadRecord> additionalListener : additionalRecordListeners) {
			try {
				additionalListener.accept(record);
			} catch (RuntimeException e) {
				classLogger.debug("Additional browser download listener failed for {}", record.getDownloadId(), e);
			}
		}
	}

	private void deleteStagedFile(DownloadRecord record) {
		Path path = record.getStagedPath();
		if (path == null) {
			return;
		}
		try {
			Files.deleteIfExists(path);
		} catch (IOException e) {
			classLogger.debug("Could not remove staged browser download {}", path, e);
		}
		reservedStagedPaths.remove(path);
		record.setStagedPath(null);
	}

	private static String safeSuggestedFilename(Download download, long order) {
		try {
			String value = download.suggestedFilename();
			return value == null || value.isBlank() ? "download-" + order + ".bin" : value;
		} catch (Exception e) {
			return "download-" + order + ".bin";
		}
	}

	private static String sanitizeFilename(String filename) {
		String value = filename == null ? "" : filename.trim();
		if (value.isBlank()) {
			return "download.bin";
		}
		try {
			String sanitized = Utility.normalizePath(value);
			String base = FilenameUtils.getName(sanitized);
			return base == null || base.isBlank() ? "download.bin" : base.replaceAll("[^a-zA-Z0-9._() -]", "_");
		} catch (Exception e) {
			return value.replaceAll("[^a-zA-Z0-9._() -]", "_");
		}
	}

	private static String safeDownloadUrl(Download download) {
		try {
			return download.url();
		} catch (Exception e) {
			return "";
		}
	}

	private static String safePageUrl(Page page) {
		try {
			return page == null ? "" : page.url();
		} catch (Exception e) {
			return "";
		}
	}

	private static String safeDownloadFailure(Download download) {
		try {
			return download.failure();
		} catch (Exception e) {
			return null;
		}
	}

	private static String safeMessage(Exception error) {
		String message = error.getMessage();
		return message == null || message.isBlank() ? error.getClass().getSimpleName() : message;
	}

	private static int positiveInt(String key, int fallback) {
		try {
			String value = System.getProperty(key);
			if (value == null || value.isBlank()) {
				value = System.getenv(key);
			}
			int parsed = Integer.parseInt(value);
			return parsed > 0 ? parsed : fallback;
		} catch (Exception e) {
			return fallback;
		}
	}

	private static long positiveLong(String key, long fallback) {
		try {
			String value = System.getProperty(key);
			if (value == null || value.isBlank()) {
				value = System.getenv(key);
			}
			long parsed = Long.parseLong(value);
			return parsed > 0 ? parsed : fallback;
		} catch (Exception e) {
			return fallback;
		}
	}

	private static String redactUrl(String value) {
		if (value == null || value.isBlank()) {
			return "";
		}
		int schemeEnd = value.indexOf(':');
		if (schemeEnd > 0) {
			String scheme = value.substring(0, schemeEnd);
			if ("data".equalsIgnoreCase(scheme) || "blob".equalsIgnoreCase(scheme)) {
				return scheme + ":";
			}
		}
		try {
			URI uri = new URI(value);
			String path = uri.getPath() == null ? "" : uri.getPath();
			String authority = uri.getRawAuthority();
			if (authority != null) {
				int userInfoEnd = authority.indexOf('@');
				if (userInfoEnd >= 0) {
					authority = authority.substring(userInfoEnd + 1);
				}
			}
			return new URI(uri.getScheme(), authority, path, null, null).toString();
		} catch (URISyntaxException e) {
			int query = value.indexOf('?');
			return query >= 0 ? value.substring(0, query) : value;
		}
	}

	private static String detectMimeType(Path path, String filename) {
		try {
			String detected = Files.probeContentType(path);
			if (detected != null && !detected.isBlank()) {
				return detected;
			}
		} catch (IOException ignored) {
		}
		try {
			String detected = java.net.URLConnection.guessContentTypeFromName(filename);
			return detected == null ? "application/octet-stream" : detected;
		} catch (Exception e) {
			return "application/octet-stream";
		}
	}

	private static String sha256(Path path) throws IOException {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			try (InputStream input = Files.newInputStream(path)) {
				byte[] buffer = new byte[8192];
				int read;
				while ((read = input.read(buffer)) >= 0) {
					if (read > 0) {
						digest.update(buffer, 0, read);
					}
				}
			}
			StringBuilder result = new StringBuilder(64);
			for (byte value : digest.digest()) {
				result.append(String.format("%02x", value));
			}
			return result.toString();
		} catch (NoSuchAlgorithmException e) {
			throw new IOException("SHA-256 is unavailable", e);
		}
	}

	public record DownloadTrigger(String requestId, Integer stepId) {
	}

	/** A serializable description of a captured browser download. */
	public static final class DownloadRecord {
		private final String downloadId;
		private final String runId;
		private final long order;
		private volatile String fileName;
		private final String originalFileName;
		private final String sourceUrl;
		private final String pageUrl;
		private final String tabId;
		private final String triggerRequestId;
		private final Integer triggerStepId;
		private final Instant startedAt;
		private volatile Instant completedAt;
		private volatile Instant savedAt;
		private volatile String status = "downloading";
		private volatile String error;
		private volatile long sizeBytes;
		private volatile String sha256;
		private volatile String mimeType = "application/octet-stream";
		private volatile String insightId;
		private volatile String insightPath;
		private volatile Path stagedPath;

		private DownloadRecord(String downloadId, String runId, long order, String fileName, String originalFileName,
				String sourceUrl, String pageUrl, String tabId, String triggerRequestId, Integer triggerStepId,
				Instant startedAt) {
			this.downloadId = downloadId;
			this.runId = runId;
			this.order = order;
			this.fileName = fileName;
			this.originalFileName = originalFileName;
			this.sourceUrl = sourceUrl;
			this.pageUrl = pageUrl;
			this.tabId = tabId;
			this.triggerRequestId = triggerRequestId;
			this.triggerStepId = triggerStepId;
			this.startedAt = startedAt;
		}

		private void markCaptured(long size, String hash, String detectedMime) {
			this.sizeBytes = size;
			this.sha256 = hash;
			this.mimeType = detectedMime;
			this.completedAt = Instant.now();
			this.status = "ready";
		}

		private void markFailed(String reason) {
			this.completedAt = Instant.now();
			this.status = "failed";
			this.error = reason;
		}

		private void markSaved(String targetInsightId, String targetInsightPath) {
			this.insightId = targetInsightId;
			this.insightPath = targetInsightPath;
			this.savedAt = Instant.now();
			this.status = "saved";
			this.error = null;
		}

		private void markSaveFailed(String reason) {
			this.status = "save-failed";
			this.error = reason;
		}

		private void setFileName(String value) {
			this.fileName = value;
		}

		private void setStagedPath(Path value) {
			this.stagedPath = value;
		}

		public String getDownloadId() {
			return downloadId;
		}

		public String getRunId() {
			return runId;
		}

		public long getOrder() {
			return order;
		}

		public String getFileName() {
			return fileName;
		}

		public String getOriginalFileName() {
			return originalFileName;
		}

		public String getSourceUrl() {
			return sourceUrl;
		}

		public String getPageUrl() {
			return pageUrl;
		}

		public String getTabId() {
			return tabId;
		}

		public String getTriggerRequestId() {
			return triggerRequestId;
		}

		public Integer getTriggerStepId() {
			return triggerStepId;
		}

		public Instant getStartedAt() {
			return startedAt;
		}

		public Instant getCompletedAt() {
			return completedAt;
		}

		public Instant getSavedAt() {
			return savedAt;
		}

		public String getStatus() {
			return status;
		}

		public String getError() {
			return error;
		}

		public long getSizeBytes() {
			return sizeBytes;
		}

		public String getSha256() {
			return sha256;
		}

		public String getMimeType() {
			return mimeType;
		}

		public String getInsightId() {
			return insightId;
		}

		public String getInsightPath() {
			return insightPath;
		}

		public Path getStagedPath() {
			return stagedPath;
		}

		/** Converts the record to a JSON-friendly map without the local temp path. */
		public Map<String, Object> toMap() {
			Map<String, Object> output = new LinkedHashMap<>();
			output.put("downloadId", downloadId);
			output.put("runId", runId);
			output.put("order", order);
			output.put("fileName", fileName);
			output.put("originalFileName", originalFileName);
			output.put("status", status);
			output.put("sourceUrl", sourceUrl);
			output.put("pageUrl", pageUrl);
			output.put("tabId", tabId);
			if (triggerRequestId != null) {
				output.put("triggerRequestId", triggerRequestId);
			}
			if (triggerStepId != null) {
				output.put("triggerStepId", triggerStepId);
			}
			output.put("startedAt", startedAt == null ? null : startedAt.toString());
			output.put("completedAt", completedAt == null ? null : completedAt.toString());
			output.put("downloadedAt", completedAt == null ? null : completedAt.toString());
			output.put("savedAt", savedAt == null ? null : savedAt.toString());
			output.put("sizeBytes", sizeBytes);
			output.put("sha256", sha256);
			output.put("mimeType", mimeType);
			if (insightPath != null) {
				output.put("insightPath", insightPath);
			}
			if (error != null) {
				output.put("error", error);
			}
			return output;
		}
	}
}

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
package prerna.reactor.shortcuts.fileupload.job;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.client.http.MetadataClient;
import com.netflix.conductor.client.http.WorkflowClient;

import prerna.reactor.shortcuts.conductor.oss.WorkflowService;
import prerna.util.Utility;

public class FileUploadWatcher implements Runnable {
	private WatchService watchService;

// WatchKey -> Directory
	private final Map<WatchKey, Path> keyToDir = new ConcurrentHashMap<>();

	private final ObjectMapper mapper = new ObjectMapper();

	private final MetadataClient metadataClient = new MetadataClient();
	private final WorkflowClient workflowClient = new WorkflowClient();

// Paused directories
	private final Set<Path> pausedDirs = ConcurrentHashMap.newKeySet();

// Deduplicate file processing
	private final Set<Path> inProgressFiles = ConcurrentHashMap.newKeySet();

	private volatile boolean running = true;

	public FileUploadWatcher() throws IOException {
		this.watchService = FileSystems.getDefault().newWatchService();
	}

// =====================================================
// REGISTER DIRECTORY RECURSIVELY
// =====================================================
	public synchronized void watch(Path rootDir) throws IOException {

		if (!Files.exists(rootDir) || !Files.isDirectory(rootDir)) {
			throw new IllegalArgumentException("Invalid directory: " + rootDir);
		}

		Files.walkFileTree(rootDir, new SimpleFileVisitor<>() {
			@Override
			public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {

				WatchKey key = dir.register(watchService, ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE);

				keyToDir.put(key, dir);
				System.out.println(" Watching: " + dir);
				return FileVisitResult.CONTINUE;
			}
		});
	}

// =====================================================
// REMOVE DIRECTORY (RECURSIVE)
// =====================================================
	public void removeDirectory(Path rootDir) {

		keyToDir.entrySet().removeIf(entry -> {
			WatchKey key = entry.getKey();
			Path watchedDir = entry.getValue();

			if (watchedDir.startsWith(rootDir)) {
				key.cancel();
				pausedDirs.remove(watchedDir);
				System.out.println(" Stopped watching: " + watchedDir);
				return true;
			}
			return false;
		});
	}

// =====================================================
// PAUSE / RESUME PER DIRECTORY
// =====================================================
	public void pauseDirectory(Path dir) {
		pausedDirs.add(dir);
		System.out.println(" Paused: " + dir);
	}

	public void resumeDirectory(Path dir) {
		pausedDirs.remove(dir);
		System.out.println(" Resumed: " + dir);
	}

	private boolean isPaused(Path path) {
		return pausedDirs.stream().anyMatch(path::startsWith);
	}

// =====================================================
// WATCH LOOP (SINGLE THREAD)
// =====================================================
	@Override
	public void run() {

		System.out.println(" WatchServiceWatcher started");

		while (running) {
			WatchKey key;

			try {
				key = watchService.take(); // BLOCKING
			} catch (ClosedWatchServiceException e) {
				break;
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			}

			Path parentDir = keyToDir.get(key);
			if (parentDir == null) {
				key.reset();
				continue;
			}

			for (WatchEvent<?> event : key.pollEvents()) {

				if (event.kind() == OVERFLOW) {
					continue;
				}

				@SuppressWarnings("unchecked")
				WatchEvent<Path> ev = (WatchEvent<Path>) event;
				Path path = parentDir.resolve(ev.context());

				String fileName = event.context().toString();
				String fullPath = path.toString();

				if (isPaused(path)) {
					continue;
				}

				// =========================
				// FILE CREATED (UPLOAD)
				// =========================
				if (event.kind() == ENTRY_CREATE && Files.isRegularFile(path)) {

					System.out.println(" FILE UPLOADED : " + path);

					// Deduplicate CREATE/MODIFY noise
					// if (inProgressFiles.add(path)) {
					/*
					 * 
					 * FileProcessingCallable task = new FileProcessingCallable(fullPath);
					 * 
					 * FileProcessingExecutor.submit(() -> { try { FileProcessResult result =
					 * task.call();
					 * 
					 * if (result.isSuccess()) { System.out.println(" SUCCESS : " +
					 * result.getMessage()); } else { System.err.println(" FAILURE : " +
					 * result.getMessage()); }
					 * 
					 * return result; } catch (Exception e) { System.out.println(e.getMessage()); }
					 * finally { inProgressFiles.remove(fullPath); return null; } });
					 */

					// Trigger workflow

					// Temporal
					/*
					 * WorkflowEntity workflowEntity = null; try { workflowEntity =
					 * SchedulerDatabaseUtility.findWorkflowActiveByDirectory(Utility.normalizePath(
					 * parentDir.toString())); // String workflowJson =
					 * workflowEntity.getWorkflowJson(); // workflowJson =
					 * workflowJson.replace("{filename}", fileName); } catch (Exception e) { // TODO
					 * Auto-generated catch block e.printStackTrace(); } if (workflowEntity == null)
					 * { System.out.println("No active file watcher workflow found for " +
					 * parentDir.toString()); continue; }
					 * 
					 * 
					 * if (workflow.filePattern != null && workflow.filePattern.endsWith(".csv")) {
					 * if (!fileName.endsWith(".csv")) { continue; } }
					 * 
					 * 
					 * try {
					 * 
					 * new WorkflowExecutionService().executeFileWatcherWorkflow(workflowEntity,
					 * fullPath); } catch (Exception e) { // TODO Auto-generated catch block
					 * inProgressFiles.remove(path); e.printStackTrace(); }
					 */

					// }

					// Create workers dynamically
					// WorkflowEntity workflowEntity = null;
					try {
						/*
						 * workflowEntity = SchedulerDatabaseUtility
						 * .findWorkflowActiveByDirectory(Utility.normalizePath(parentDir.toString()));
						 * // String workflowJson = workflowEntity.getWorkflowJson(); // workflowJson =
						 * workflowJson.replace("{filename}", fileName); List<Worker> workers =
						 * WorkerFactory.createWorkers(workflowJson);
						 * 
						 * // Start workers WorkerInitializer.startWorkers(workers);
						 */
						// ===== 1. Find workflow =====
						WorkflowService workflowService = new WorkflowService();
						workflowService.startWorkflow(Utility.normalizePath(fullPath));
						/*
						 * String workflowName = SchedulerDatabaseUtility
						 * .getWorkflowJsonByDirectory(Utility.normalizePath(parentDir.toString()));
						 * 
						 * if (workflowName == null) { throw new RuntimeException(
						 * "No workflow mapped for directory: " +
						 * Utility.normalizePath(parentDir.toString())); }
						 * 
						 * // ===== 2. Get active version ===== String json =
						 * SchedulerDatabaseUtility.getActiveWorkflowJson(workflowName); int version =
						 * SchedulerDatabaseUtility.getActiveWorkflowVersion(workflowName);
						 * 
						 * if (json == null) { throw new
						 * RuntimeException("No active workflow found for: " + workflowName); }
						 * 
						 * // ===== 3. Convert JSON → WorkflowDef ===== WorkflowDef def =
						 * mapper.readValue(json, WorkflowDef.class);
						 * 
						 * // ===== SAFE REGISTER ===== try {
						 * metadataClient.getWorkflowDef(def.getName(), def.getVersion()); // already
						 * exists → do nothing } catch (Exception e) { // not found → register
						 * metadataClient.registerWorkflowDef(def); }
						 * 
						 * // ===== 5. Prepare input ===== Map<String, Object> input = new HashMap<>();
						 * input.put("watchDirectory", Utility.normalizePath(parentDir.toString()));
						 * input.put("filename", fileName); input.put("filePattern", "*.txt");
						 * 
						 * // ✅ FIXED HERE StartWorkflowRequest request = new StartWorkflowRequest();
						 * request.setName(workflowName); request.setVersion(version);
						 * request.setInput(input); request.setCorrelationId("file-" + fileName);
						 * 
						 * String workflowId = workflowClient.startWorkflow(request);
						 * 
						 * SchedulerDatabaseUtility.startWorkflowExecution(workflowId, workflowName,
						 * version);
						 */
					} catch (Exception e) {
						e.printStackTrace();
					}

					/*
					 * if (workflowEntity == null) {
					 * System.out.println("No active file watcher workflow found for " +
					 * parentDir.toString()); continue; }
					 */

				}

				// =========================
				// DIRECTORY CREATED
				// =========================
				else if (event.kind() == ENTRY_CREATE && Files.isDirectory(path)) {
					try {
						watch(path);
					} catch (IOException ignored) {
					}
				}

				// =========================
				// MODIFY
				// =========================
				else if (event.kind() == ENTRY_MODIFY) {
					System.out.println(" ENTRY_MODIFY : " + fullPath);
				}

				// =========================
				// DELETE
				// =========================
				else if (event.kind() == ENTRY_DELETE) {
					System.out.println(" ENTRY_DELETE : " + fullPath);
					removeDirectory(path);
				}
			}

			boolean valid = key.reset();
			if (!valid) {
				keyToDir.remove(key);
			}
		}

		System.out.println(" WatchServiceWatcher stopped");
	}

// =====================================================
// SHUTDOWN
// =====================================================
	public synchronized void shutdown() throws IOException {

		running = false;

		for (WatchKey key : keyToDir.keySet()) {
			key.cancel();
		}

		keyToDir.clear();
		pausedDirs.clear();
		inProgressFiles.clear();

		watchService.close();
		FileProcessingExecutor.shutdown();

		System.out.println(" WatchServiceWatcher shutdown");
	}

// =====================================================
// RESTART (SAFE)
// =====================================================
	public synchronized void restart() throws IOException {

		System.out.println(" Restarting watcher");

		shutdown();

		this.watchService = FileSystems.getDefault().newWatchService();
		this.running = true;

		System.out.println(" WatchServiceWatcher restarted");
	}

}

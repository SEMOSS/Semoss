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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FileWatchServiceFactory {
	private static volatile FileWatchServiceFactory instance;

	private ExecutorService executor;
	private FileUploadWatcher watcher;
	private FileWatcherManager manager;

	private FileWatchServiceFactory() {
	}

	public static FileWatchServiceFactory getInstance() {
		if (instance == null) {
			synchronized (FileWatchServiceFactory.class) {
				if (instance == null) {
					instance = new FileWatchServiceFactory();
				}
			}
		}
		return instance;
	}

	// =====================================================
	// START (IDEMPOTENT)
	// =====================================================
	public synchronized void start() throws Exception {

		if (executor != null) {
			System.out.println(" WatchService already started");
			return;
		}

		watcher = new FileUploadWatcher();
		manager = new FileWatcherManager(watcher);

		executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "watchservice-thread"));

		executor.submit(watcher);

		System.out.println(" WatchService started (singleton)");
	}

	// =====================================================
	// ACCESS MANAGER
	// =====================================================
	public FileWatcherManager getManager() {
		if (manager == null) {
			throw new IllegalStateException("WatchService not started");
		}
		return manager;
	}

	// =====================================================
	// SHUTDOWN
	// =====================================================
	public synchronized void shutdown() throws Exception {

		if (executor == null) {
			return;
		}

		watcher.shutdown();
		executor.shutdownNow();

		executor = null;
		watcher = null;
		manager = null;

		System.out.println(" WatchService shutdown complete");
	}

	// =====================================================
	// RESTART
	// =====================================================
	public synchronized void restart() throws Exception {
		shutdown();
		start();
	}
}

/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.cluster.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.util.Constants;

public class LocalFileWatcher implements Runnable {
	WatchService ws = null;
	DBSynchronizer dbs = null;
	String db = null;
	String fileExtensions = "py;python;r;js;css;mosfet;json;"; // thse are the only file extensions to track
	private static final Logger classLogger = LogManager.getLogger(LocalFileWatcher.class);

	public LocalFileWatcher(DBSynchronizer dbs, String db) {
		this.dbs = dbs;
		this.db = db;
		makeWatcherService();
	}

	// main class for watching local file changes
	// an updating the zookeeper
	public void watchPath(String path) {
		try {
			System.out.println("Watching path " + path);
			Path thisPath = Paths.get(path);
			thisPath.register(ws, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY,
					StandardWatchEventKinds.ENTRY_DELETE);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

	// main class for watching local file changes
	// an updating the zookeeper
	public void watchFilePath(String path) {
		try {
			System.out.println("Watching path " + path);
			Path thisPath = Paths.get(path);
			thisPath.register(ws, StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.ENTRY_DELETE);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

	private void makeWatcherService() {
		if (ws == null) {
			try {
				ws = FileSystems.getDefault().newWatchService();
			} catch (Exception ex) {
				System.err.println("Unable to create a watcher service " + ex);
			}
		}
	}

	public void run() {
		// loop and watch
		try {
			WatchKey key;
			while ((key = ws.take()) != null) {
				for (WatchEvent<?> event : key.pollEvents()) {
					System.out.println("Event kind:" + event.kind() + ". File affected: " + event.context() + ".");

					Path parent = (Path) key.watchable();

					// control here for specific file types
					// .mosfet
					// .py
					// .r etc.
					String fileName = event.context() + "";
					String extn = fileName.substring(fileName.lastIndexOf(".") + 1);

					if (event.kind() == StandardWatchEventKinds.ENTRY_MODIFY && fileExtensions.indexOf(extn) >= 0) // do
																													// it
																													// only
																													// if
																													// it
																													// is
																													// a
																													// valid
																													// file
																													// extension
						dbs.modifyNode(db, parent + "/" + event.context());
					else if (event.kind() == StandardWatchEventKinds.ENTRY_CREATE) {
						// need to see if the create is a new folder and then register
						File file = new File(parent + "/" + event.context());
						if (file.isDirectory())
							watchFilePath(parent + "/" + event.context());
						else if (fileExtensions.indexOf(extn) >= 0)
							dbs.createNode(db, parent + "/" + event.context());
					} else if (event.kind() == StandardWatchEventKinds.ENTRY_DELETE
							&& fileExtensions.indexOf(extn) >= 0)
						dbs.deleteNode(db, parent + "/" + event.context());
				}
				key.reset();
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
}

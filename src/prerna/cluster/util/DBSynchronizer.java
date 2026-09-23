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
package prerna.cluster.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.retry.RetryOneTime;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;

// this is also a singleton
public class DBSynchronizer extends ZKClient {

	// map of db to the version
	Map<String, String> dbVersion = new HashMap<String, String>();
	Map<String, InterProcessLock> dbLock = new HashMap<String, InterProcessLock>();
	Map<String, LocalFileWatcher> dbWatcher = new HashMap<String, LocalFileWatcher>();
	Map<String, CuratorCacheListener> dbListener = new HashMap<String, CuratorCacheListener>();

	Map<String, Object> pathThreadLock = new HashMap<String, Object>(); // this is the main place to claim the monitor
	String namespace = "";
	static DBSynchronizer singleton = null;

	// thse are the only file extensions to track //
	// should we include the .db too ? or .smss ?
	String fileExtensions = "py;python;r;js;css;mosfet;json;java;";

	public String semossHome = null;
	protected static final Logger classLogger = LogManager.getLogger(DBSynchronizer.class);

	private DBSynchronizer() {

	}

	@Override
	public void initCurator() {
		// make the curator here also
		try {
			client = CuratorFrameworkFactory.newClient(zkServer, new RetryOneTime(1));
			client.start();
		} catch (Exception e) {
			classLogger.error("Failed to initialize and start Curator client for ZooKeeper server {}", zkServer, e);
		}
	}

	@Override
	public void waitHere() {
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			br.readLine();
		} catch (Exception ignored) {

		}
	}

	@Override
	public void init() {
		// initiates connection to zk and makes the connection
		try {
			env = System.getenv();
			if (env.containsKey(ZK_SERVER)) {
				zkServer = env.get(ZK_SERVER);
			}

			if (env.containsKey(ZK_SERVER.toUpperCase())) {
				zkServer = env.get(ZK_SERVER.toUpperCase());
			}

			if (env.containsKey(HOST)) {
				host = env.get(HOST);
			}

			if (env.containsKey(HOST.toUpperCase())) {
				host = env.get(HOST.toUpperCase());
			}

			int timeout = (30 * 60 * 1000);

			if (env.containsKey(TIMEOUT)) {
				host = env.get(TIMEOUT);
			}

			if (env.containsKey(TIMEOUT.toUpperCase())) {
				host = env.get(TIMEOUT.toUpperCase());
			}

			if (env.containsKey(BOOTUSER)) {
				user = env.get(BOOTUSER);
			}

			if (env.containsKey(BOOTUSER.toUpperCase())) {
				user = env.get(BOOTUSER.toUpperCase());
			}

			if (env.containsKey(HOME)) {
				home = env.get(HOME);
			}

			if (env.containsKey(HOME.toUpperCase())) {
				home = env.get(HOME.toUpperCase());
			}

			if (env.containsKey(APP_HOME)) {
				app = env.get(APP_HOME);
			}

			if (env.containsKey(APP_HOME.toUpperCase())) {
				app = env.get(APP_HOME.toUpperCase());
			}

			if (env.containsKey(SEMOSS_HOME)) {
				semossHome = env.get(SEMOSS_HOME);
			}

			if (env.containsKey(SEMOSS_HOME.toUpperCase())) {
				semossHome = env.get(SEMOSS_HOME.toUpperCase());
			}

			// TODO >>>timb:not sure if the host is needed for both the engine and user
			// containers
			if (zkServer != null && host != null) {
				// open zk
				// default time is 30 min
				zk = new ZooKeeper(zkServer, timeout, this);

				connected = true;
			}
//			if(ClusterUtil.IS_CLUSTERED_SCHEDULER) {
//				SchedulerListener.getListener();
//			}
//			

		} catch (IOException e) {
			classLogger.error("Failed to connect to ZooKeeper server {}", zkServer, e);
		}
	}

	public static DBSynchronizer getInstance(String semossHome) {
		if (singleton == null) {
			singleton = new DBSynchronizer();
		}
		singleton.semossHome = semossHome;
		singleton.init();
		singleton.initCurator();
		return singleton;
	}

	public void registerDB(String db) {
		// loads the semoss base folder
		// loads the db
		// wonder if the version should just go to time
		if (zk != null) {

			try {
				// goes through each directory and registers
				String dbFolderName = semossHome + "/db/" + db;
				String version = getLastModifiedTime(dbFolderName);
				createIfNotExist(namespace, db + "_lock", version); // , version, null); // create it with the same
																	// version as db

				// create the local file watcher
				LocalFileWatcher dbFolderWatcher = null;
				if (dbWatcher.containsKey(dbFolderName)) {
					dbFolderWatcher = dbWatcher.get(dbFolderName);
				} else {
					dbFolderWatcher = new LocalFileWatcher(this, db);
				}
				dbWatcher.put(db, dbFolderWatcher);

				List<String> files = new ArrayList<String>();
				files.add(dbFolderName);
				registerRecurse(files, db);

				// create a lock file
				InterProcessLock lock = getLockOnPath(namespace + "/" + db + "_lock");
				dbLock.put(db, lock);

				// create the cachelistener
				CuratorCacheListener dbcl = new GlobalFileWatcher(semossHome, this);
				CuratorCache tc = CuratorCache.build(client, namespace + "/" + db);
				tc.listenable().addListener(dbcl);
				tc.start();
				dbListener.put(db, dbcl);

				// start the thread
				Thread t = new Thread(dbFolderWatcher);
				t.start();

			} catch (Exception e) {
				classLogger.error("Failed to register database {}", db, e);
			}
		}
		// else - this is a local deployment nothing to do move on
	}

	public void registerRecurse(List<String> files, String db) {
		List<String> nextLevel = new ArrayList<String>();

		for (int fileIndex = 0; fileIndex < files.size(); fileIndex++) {
			String curFile = files.get(fileIndex);

			// register it
			// remove the semoss home /db
			// register it
			String nodeName = curFile.replace(semossHome + "/db/", "");

			dbWatcher.get(db).watchPath(curFile);

			String version = getLastModifiedTime(curFile);
			createIfNotExist(namespace, nodeName, version); // , version, null);
			classLogger.debug("Adding node.. {}", nodeName);

			// process the child elements
			// navigate this folder to identify the next level
			classLogger.debug("Trying for next level on {}", curFile);
			File curDir = new File(curFile);
			if (curDir.isDirectory()) {
				File[] curDirFiles = curDir.listFiles();
				for (int curDirFileIndex = 0; curDirFileIndex < curDirFiles.length; curDirFileIndex++) {
					File curDirFile = curDirFiles[curDirFileIndex];
					String path = curDirFile.getAbsolutePath().replace("\\\\", "/");

					classLogger.debug("Adding path for next level  {}", path);
					if (curDirFile.isDirectory()) {
						// dont add temp
						// or directories that start with .
						if (isDirectoryAllowed(curDirFile)) {
							nextLevel.add(path);
						}
					} else {
						String extn = path.substring(path.lastIndexOf(".") + 1);
						// register file only if the extension is valid
						if (fileExtensions.indexOf(extn + ";") >= 0) {
							nodeName = path.replace(semossHome + "/db/", "");
							classLogger.debug("Adding node.. {}", nodeName);
							version = getLastModifiedTime(path);
							createIfNotExist(namespace, nodeName, version); // , version, null);
						}
						// dbWatcher.get(db).watchFilePath(path);
					}
				}
			}
		}
		if (nextLevel.size() > 0) {
			registerRecurse(nextLevel, db);
		}
	}

	private boolean isDirectoryAllowed(File dir) {
		return !dir.getName().equalsIgnoreCase("Temp") && !dir.getName().startsWith(".git")
				&& !dir.getName().startsWith("classes");
	}

	private String getLastModifiedTime(String path) {
		try {
			BasicFileAttributes attr = Files.readAttributes(Paths.get(path), BasicFileAttributes.class);
			FileTime ft = attr.lastAccessTime();

			return ft.toString();
		} catch (IOException e) {
			classLogger.error("Failed to read last modified time for path {}", path, e);
		}

		return null;
	}

	private String getNodeName(String input) {
		input = input.replace("\\\\", "/");
		input = input.replace(semossHome + "/db", "");
		return input;
	}

	public void modifyNode(String db, String fileName) {
		// need to get the lock here
		// need to check the extensions here again ?

		try {
			dbLock.get(db).acquire();
			// update file on dbwatcher
			classLogger.info("Following node is being modified {}", fileName);
			String payload = getLastModifiedTime(fileName);
			String nodeName = getNodeName(fileName);
			zk.setData(nodeName, payload.getBytes(), -1);
			dbLock.get(db).release();
		} catch (KeeperException e) {
			classLogger.error("ZooKeeper error modifying node for file {}", fileName, e);
		} catch (InterruptedException e) {
			classLogger.error("Interrupted while modifying node for file {}", fileName, e);
		} catch (Exception e) {
			classLogger.error("Failed to modify node for file {}", fileName, e);
		}

	}

	public void createNode(String db, String fileName) {
		// need to get the lock here

		// create the file on dbwatcher
		classLogger.info("Following node is being added {}", fileName);
		try {
			dbLock.get(db).acquire();
			// update file on dbwatcher
			String payload = getLastModifiedTime(fileName);
			String nodeName = getNodeName(fileName);
			createIfNotExist(namespace, nodeName, payload);
			dbLock.get(db).release();
		} catch (Exception e) {
			classLogger.error("Failed to create node for file {}", fileName, e);
		}
	}

	public void deleteNode(String db, String fileName) {
		// when I see a directory.. does it only get one event or all the event for each
		// file ?
		// dont kow
		classLogger.info("Following node is being deleted {}", fileName);
		try {
			dbLock.get(db).acquire();
			// update file on dbwatcher
			String nodeName = getNodeName(fileName);
			ZKUtil.deleteRecursive(zk, nodeName);
			dbLock.get(db).release();
		} catch (KeeperException e) {
			classLogger.error("ZooKeeper error deleting node for file {}", fileName, e);
		} catch (InterruptedException e) {
			classLogger.error("Interrupted while deleting node for file {}", fileName, e);
		} catch (Exception e) {
			classLogger.error("Failed to delete node for file {}", fileName, e);
		}

		// watchPath(fileName);
	}

	public void unregisterDB(String db) {
		// remove the db
		deleteNode(db, db);
	}

	public Object getIntraProcessLock(String path) {
		// can return null.. be wary
		return pathThreadLock.get(path);
	}

//	public static void main(String [] args)
//	{
//		DBSynchronizer dbw = DBSynchronizer.getInstance("c:/workspace/Semoss_Dev");
//		dbw.semossHome = "c:/workspace/Semoss_Dev";
//		dbw.registerDB("Diabetes__995cf169-6b44-4a42-b75c-af12f9f45c36");
//	}

}

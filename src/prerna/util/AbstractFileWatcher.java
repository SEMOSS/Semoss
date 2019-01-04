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
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;

/**
 * Interface that provides a common protocol for objects that wish to execute code while they are active. 
 * Used to filter filenames.
 * Opens up a thread and watches the file.
 */
public abstract class AbstractFileWatcher implements Runnable, FilenameFilter{
	
	// opens up a thread and watches the file
	// when available, it will upload it into the journal
	// may be this is a good time to put this on tomcat


	protected static final Logger logger = LogManager.getLogger(AbstractFileWatcher.class.getName());
	
	// processes the files with the given extension
	protected String folderToWatch = null;
	protected String extension = null;
	protected IEngine engine = null;
	Object monitor = null;
	
	// this is used for us to determine how to stop the thread
	private boolean stop = false;
	
	/**
	 * Sets folder to watch.
	 * @param folderToWatch String		Folder to watch.
	 */
	public void setFolderToWatch(String folderToWatch) {
		this.folderToWatch = folderToWatch;
	}

	/**
	 * Sets extension of files.
	 * @param extension String		Extension of files.
	 */
	public void setExtension(String extension) {
		this.extension = extension;
	}
	
	/**
	 * Sets engine.
	 * @param engine IEngine		Engine to be set.
	 */
	public void setEngine(IEngine engine) {
		this.engine = engine;
	}
	
	/**
	 * Sets monitor.
	 * @param monitor Object		Object to be monitored.
	 */
	public void setMonitor(Object monitor) {
		this.monitor = monitor;
	}
	
	/**
	 * Used in the starter class for loading files.
	 */
	public abstract void loadFirst();
	
	/**
	 * Processes the file.
	 * @param fileName String		Name of the file.
	 */
	public abstract void process(String fileName);
	
	/**
	 * Optional method to be overriden
	 * So that a watcher can perform any required operations
	 * Prior to starting up its own thread
	 */
	public void init() {
		
	}
	
	/**
	 * Starts the thread and processes new files from a given directory.
	 */
	@Override
	public void run() {
		WatchService watcher = null;
		try {
			watcher = FileSystems.getDefault().newWatchService();
		} catch(IOException e) {
			e.printStackTrace();
			return;
		}
		
		WatchKey key = null;
		Path dir = (new File(this.folderToWatch)).toPath();
		try {
		    key = dir.register(watcher, StandardWatchEventKinds.ENTRY_CREATE);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		while(!stop) {
			try {
				key = watcher.take();
			} catch (InterruptedException x) {
				if(watcher != null) {
					try {
						watcher.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				return;
			}

			for(WatchEvent<?> event: key.pollEvents()) {
				WatchEvent.Kind kind = event.kind();
				if(kind == StandardWatchEventKinds.ENTRY_CREATE) {
					String newFile = event.context() + "";
					if(newFile.endsWith(extension)) {
						// cause a delay
						// to ensure file is fully
						// written
						try {
							Thread.sleep(2000);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						
						try {
							process(newFile);
						} catch(RuntimeException ex) {
							ex.printStackTrace();
						}
					} else {
						logger.info("Ignoring File " + newFile);
					}
				}
			}

			// Reset the key 
			// required to receive further watch events
			key.reset();
		}

		// close streams
		if(key != null) {
			key.cancel();
		}
		if(watcher != null) {
			try {
				watcher.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}	

	/**
	 * Tests if a specified file should be included in a file list.
	 * @param arg0 File			Folder in which the file was found.
	 * @param arg1 String		Name of the file.
	
	 * @return 					True if the name should be included in the file list. */
	
	@Override
	public boolean accept(File arg0, String arg1) {
		return arg1.endsWith(extension);
	}
	
	/**
	 * Switch the thread to finish running
	 */
	public void shutdown() 	{
		this.stop  = true;
	}
}

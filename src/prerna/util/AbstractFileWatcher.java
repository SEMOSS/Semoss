/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
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
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;

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
	
	
	/**
	 * Sets folder to watch.
	 * @param folderToWatch String		Folder to watch.
	 */
	public void setFolderToWatch(String folderToWatch)
	{
		this.folderToWatch = folderToWatch;
	}

	/**
	 * Sets extension of files.
	 * @param extension String		Extension of files.
	 */
	public void setExtension(String extension)
	{
		this.extension = extension;
	}
	
	/**
	 * Sets engine.
	 * @param engine IEngine		Engine to be set.
	 */
	public void setEngine(IEngine engine)
	{
		this.engine = engine;
	}
	
	/**
	 * Sets monitor.
	 * @param monitor Object		Object to be monitored.
	 */
	public void setMonitor(Object monitor)
	{
		this.monitor = monitor;
	}
	
	/**
	 * Used in the starter class for loading files.
	 */
	public abstract void loadFirst();
	
	/**
	 * Starts the thread and processes new files from a given directory.
	 */
	@Override
	public void run() 
	{
		try
		{
			WatchService watcher = FileSystems.getDefault().newWatchService();
			String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");

			//Path dir2Watch = Paths.get(baseFolder + "/" + folderToWatch);

			Path dir2Watch = Paths.get(folderToWatch);

			WatchKey key = dir2Watch.register(watcher, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);
			while(true)
			{
				//WatchKey key2 = watcher.poll(1, TimeUnit.MINUTES);
				WatchKey key2 = watcher.take();
				
				for(WatchEvent<?> event: key2.pollEvents())
				{
					WatchEvent.Kind kind = event.kind();
					if(kind == StandardWatchEventKinds.ENTRY_CREATE)
					{
						String newFile = event.context() + "";
						if(newFile.endsWith(extension))
						{
							Thread.sleep(2000);	
							try
							{
								process(newFile);
								
							}catch(RuntimeException ex)
							{
								ex.printStackTrace();
							}
						}else
							logger.info("Ignoring File " + newFile);
					}
				}
				key2.reset();
			}
		}catch(RuntimeException ex)
		{
			logger.debug(ex);
			// do nothing - I will be working it in the process block
		} catch (InterruptedException ex) {
			logger.debug(ex);
			// do nothing - I will be working it in the process block
		} catch (IOException ex) {
			logger.debug(ex);
			// do nothing - I will be working it in the process block
		}
	}	

	/**
	 * Tests if a specified file should be included in a file list.
	 * @param arg0 File			Folder in which the file was found.
	 * @param arg1 String		Name of the file.
	
	 * @return 					True if the name should be included in the file list. */
	
	@Override
	public boolean accept(File arg0, String arg1) 
	{
		return arg1.endsWith(extension);
	}
	
	/**
	 * Processes the file.
	 * @param fileName String		Name of the file.
	 */
	public abstract void process(String fileName);	
	
}

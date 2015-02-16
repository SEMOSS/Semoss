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
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * This class opens a thread and watches a specific SMSS file.
 */
public class SMSSWebWatcher extends AbstractFileWatcher {

	/**
	 * Processes SMSS files.
	 * @param	Name of the file.
	 */
	@Override
	public void process(String fileName) {
		loadNewDB(fileName);
	}
	
	/**
	 * Returns an array of strings naming the files in the directory.
	 * Goes through list and loads an existing database.
	 */
	public void loadExistingDB()
	{
		File dir = new File(folderToWatch);
		String [] fileNames = dir.list(this);
		for(int fileIdx = 0;fileIdx < fileNames.length;fileIdx++)
		{
			try{
				String fileName = folderToWatch + "/" + fileNames[fileIdx];
				loadNewDB(fileNames[fileIdx]);
				//Utility.loadEngine(fileName, prop);				
			}catch(RuntimeException ex)
			{
				ex.printStackTrace();
				logger.fatal("Engine Failed " + "./db/" + fileNames[fileIdx]);
			}
		}	

	}
	
	/**
	 * Loads a new database by setting a specific engine with associated properties.
	 * @param 	Specifies properties to load 
	 */
	public void loadNewDB(String newFile)
	{
		FileInputStream fileIn = null;
		try{
			Properties prop = new Properties();
			String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
	
			fileIn = new FileInputStream(folderToWatch + "/"  +  newFile);
			prop.load(fileIn);
			
			String fileName = folderToWatch + "/" + newFile;
			System.err.println("Loading DB " + folderToWatch + "<>" + newFile);
			
			Utility.loadEngine(fileName, prop);
		}catch(IOException e){
			e.printStackTrace();
		}finally{
			try{
				if(fileIn!=null)
					fileIn.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	/**
	 * Used in the starter class for processing SMSS files.
	 */
	@Override
	public void loadFirst()
	{
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		File dir = new File(folderToWatch);
		System.err.println("Dir... " + dir);
		String [] fileNames = dir.list(this);
		for(int fileIdx = 0;fileIdx < fileNames.length;fileIdx++)
		{
			try{
				String fileName = folderToWatch + fileNames[fileIdx];
				Properties prop = new Properties();
				process(fileNames[fileIdx]);
			}catch(RuntimeException ex)
			{
				logger.fatal("Engine Failed " + folderToWatch + "/" + fileNames[fileIdx]);
			}
		}
	}

	
	/**
	 * Processes new SMSS files.
	 */
	@Override
	public void run()
	{
		logger.info("Starting thread");
		synchronized(monitor)
		{
			super.run();
		}
	}

}

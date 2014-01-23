/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.util;

import java.io.File;
import java.io.FileInputStream;
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
		try {
			//loadExistingDB();
			loadNewDB(fileName);							
		} catch(Exception ex) {
			// TODO: Specify exception
			ex.printStackTrace();
		}
	}
	
	/**
	 * Returns an array of strings naming the files in the directory.
	 * Goes through list and loads an existing database.
	 */
	public void loadExistingDB() throws Exception
	{
		File dir = new File(folderToWatch);
		String [] fileNames = dir.list(this);
		for(int fileIdx = 0;fileIdx < fileNames.length;fileIdx++)
		{
			try{
				String fileName = folderToWatch + "/" + fileNames[fileIdx];
				loadNewDB(fileNames[fileIdx]);
				//Utility.loadEngine(fileName, prop);				
			}catch(Exception ex)
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
	public void loadNewDB(String newFile) throws Exception
	{
		Properties prop = new Properties();
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");

		prop.load(new FileInputStream(baseFolder + "/" + folderToWatch + "/"  +  newFile));
		
		String fileName = folderToWatch + "/" + newFile;
		System.err.println("Loading DB " + newFile);
		
		Utility.loadEngine(folderToWatch +"/" + newFile, prop);
	}
	
	
	/**
	 * Used in the starter class for processing SMSS files.
	 */
	@Override
	public void loadFirst()
	{
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		File dir = new File(baseFolder + "/" + folderToWatch);
		String [] fileNames = dir.list(this);
		for(int fileIdx = 0;fileIdx < fileNames.length;fileIdx++)
		{
			try{
				String fileName = folderToWatch + fileNames[fileIdx];
				Properties prop = new Properties();
				process(fileNames[fileIdx]);
			}catch(Exception ex)
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

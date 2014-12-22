/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.util;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.openrdf.repository.RepositoryException;

import prerna.poi.specific.D3CSVLoader;


/**
 */
public class D3FileWatcher extends AbstractFileWatcher {

	/**
	 * Processes sample files.
	 * @param 	File name.
	 */
	@Override
	public void process(String fileName) {
		try {
			System.out.println("File Came through "  + folderToWatch + "/" + fileName);
			D3CSVLoader loader = new D3CSVLoader();
			loader.importFileWithConnection(engine.getEngineName(), folderToWatch + "/" + fileName, "http://semoss.org/ontologies", engine.getProperty(Constants.OWL));
			
			
		} catch(RuntimeException ex) {
			ex.printStackTrace();
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Runs the file watcher.
	 */
	@Override
	public void run()
	{
		logger.info("Engine Name is " + engine);
		System.out.println("Started up... ");
		super.run();
	}

	/**
	 * Used in the starter class for loading files.
	 */
	@Override
	public void loadFirst() {
		
	}
	
}

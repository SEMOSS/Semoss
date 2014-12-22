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


/**
 */
public class SampleFileWatcher extends AbstractFileWatcher {

	/**
	 * Processes sample files.
	 * @param 	File name.
	 */
	@Override
	public void process(String fileName) {
//		try {
//			//loadExistingDB();
//			// for the sample this will never get called
//		} catch(Exception ex) {
//			ex.printStackTrace();
//		}
	}
	
	/**
	 * Runs the file watcher.
	 */
	@Override
	public void run()
	{
		logger.info("Engine Name is " + engine);
	}

	/**
	 * Used in the starter class for loading files.
	 */
	@Override
	public void loadFirst() {
		
	}
	
}

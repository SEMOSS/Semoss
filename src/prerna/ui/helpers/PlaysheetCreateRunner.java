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
package prerna.ui.helpers;

import prerna.ui.components.api.IPlaySheet;

/**
 * This class helps with running the create view method for a playsheet.
 */
public class PlaysheetCreateRunner implements Runnable{

	IPlaySheet playSheet = null;
	
	/**
	 * Constructor for PlaysheetCreateRunner.
	 * @param playSheet IPlaySheet
	 */
	public PlaysheetCreateRunner(IPlaySheet playSheet)
	{
		this.playSheet = playSheet;
	}
	
	/**
	 * Method run.  Calls the create view method on the local play sheet.
	 */
	@Override
	public void run() {
		runWeb();
		playSheet.createView();
	}
	
	public void runWeb()
	{
		playSheet.createData();
		playSheet.runAnalytics();
	}
	
	/**
	 * Method setPlaySheet. Sets the playsheet to this playsheet.
	 * @param playSheet IPlaySheet
	 */
	public void setPlaySheet(IPlaySheet playSheet)
	{
		this.playSheet = playSheet;
	}
	
}

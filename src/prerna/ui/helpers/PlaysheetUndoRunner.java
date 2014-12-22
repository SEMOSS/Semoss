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

import prerna.ui.components.playsheets.GraphPlaySheet;

/**
 * This class helps with running the undo view method for a playsheet.
 */
public class PlaysheetUndoRunner implements Runnable{

	GraphPlaySheet playSheet = null;
	
	/**
	 * Constructor for PlaysheetUndoRunner.
	 * @param playSheet GraphPlaySheet
	 */
	public PlaysheetUndoRunner(GraphPlaySheet playSheet)
	{
		this.playSheet = playSheet;
	}
	
	/**
	 * Method run. Calls the undo view method on the local play sheet.
	 */
	@Override
	public void run() {
		playSheet.undoView();
	}
	
	/**
	 * Method setPlaySheet.
	 * @param playSheet GraphPlaySheet
	 */
	public void setPlaySheet(GraphPlaySheet playSheet)
	{
		this.playSheet = playSheet;
	}

}

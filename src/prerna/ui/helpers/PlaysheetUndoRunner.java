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

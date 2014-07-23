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

import prerna.ui.components.api.IPlaySheet;

/**
 * This class helps with running the create view method for a playsheet.
 */
public class PlaysheetCreateRunner implements Runnable{

	IPlaySheet playSheet = null;
	boolean createSwingView = true;
	
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
	
	/**
	 * Method setCreateSwingView. Sets the createSwingView boolean which determines whether the createView is called and Java Swing front end created
	 * @param createSwingView boolean
	 */
	public void setCreateSwingView(boolean createSwingView)
	{
		this.createSwingView = createSwingView;
	}
	
}

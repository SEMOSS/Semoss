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

import java.util.ArrayList;

import prerna.ui.components.playsheets.ClassifyClusterPlaySheet;

/**
 * This class helps with running the create view method for a playsheet.
 */
public class ClusteringModuleUpdateRunner implements Runnable{

	private ClassifyClusterPlaySheet playSheet = null;
	private String[] names;
	private ArrayList<Object[]> list;
	/**
	 * Constructor for PlaysheetCreateRunner.
	 * @param playSheet IPlaySheet
	 */
	public ClusteringModuleUpdateRunner(ClassifyClusterPlaySheet playSheet)
	{
		this.playSheet = playSheet;
	}
	
	/**
	 * Method run.  Calls the create view method on the local play sheet.
	 */
	@Override
	public void run() {
		playSheet.recreateSimBarChart(names, list);
	}
	
	public void setValues(String[] names, ArrayList<Object[]> list) {
		this.names = names;
		this.list = list;
	}
	
}

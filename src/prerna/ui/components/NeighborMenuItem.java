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
package prerna.ui.components;

import javax.swing.JList;
import javax.swing.JMenuItem;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.helpers.PlaysheetOverlayRunner;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;


/**
 * This class is used to create a menu item for the neighborhood.
 */
public class NeighborMenuItem extends JMenuItem{
	String query; 
	IEngine engine = null;
	String predicateURI = null;
	String name = null;
	
	Logger logger = Logger.getLogger(getClass());

	/**
	 * Constructor for NeighborMenuItem.
	 * @param name String
	 * @param query String
	 * @param engine IEngine
	 */
	public NeighborMenuItem(String name, String query, IEngine engine)
	{
		super(name);
		this.name = name;
		this.query = query;
		this.engine = engine;
	}
	
	/**
	 * Composes the query and paints the neighborhood.
	 */
	public void paintNeighborhood()
	{
		GraphPlaySheet playSheet = (GraphPlaySheet) QuestionPlaySheetStore.getInstance().getActiveSheet();
		logger.debug("Extending ");
		Runnable playRunner = null;
		// Here I need to get the active sheet
		// get everything with respect the selected node type
		// and then create the filter on top of the query
		// use the @filter@ to get this done / some of the 			

		// need to create playsheet extend runner
		playRunner = new PlaysheetOverlayRunner(playSheet);
		JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		// get the selected repository
		Object [] repos = (Object [])list.getSelectedValues();

		for(int repoIndex = 0;repoIndex < repos.length;repoIndex++)
		{
			IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(repos[repoIndex]+"");
			playSheet.setRDFEngine(engine);
			playSheet.setQuery(query);
		
		
			// thread
			Thread playThread = new Thread(playRunner);
			playThread.start();
		}
		
	}
	
	
}

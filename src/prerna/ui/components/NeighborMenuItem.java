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
package prerna.ui.components;

import javax.swing.JList;
import javax.swing.JMenuItem;

import org.apache.log4j.LogManager;
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
	
	static final Logger logger = LogManager.getLogger(NeighborMenuItem.class.getName());

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

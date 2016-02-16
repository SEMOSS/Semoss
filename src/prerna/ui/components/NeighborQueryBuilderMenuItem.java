/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.ui.components;

import javax.swing.JList;
import javax.swing.JMenuItem;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.om.InsightStore;
import prerna.rdf.query.builder.QueryBuilderData;
import prerna.ui.components.playsheets.AbstractGraphPlaySheet;
import prerna.ui.components.playsheets.SQLGraphPlaysheet;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.helpers.PlaysheetOverlayRunner;
import prerna.util.Constants;
import prerna.util.DIHelper;


/**
 * This class is used to create a menu item for the neighborhood.
 */
public class NeighborQueryBuilderMenuItem extends JMenuItem{
	QueryBuilderData data; 
	IEngine engine = null;
	String name = null;
	
	static final Logger logger = LogManager.getLogger(NeighborQueryBuilderMenuItem.class.getName());

	/**
	 * Constructor for NeighborMenuItem.
	 * @param name String
	 * @param query String
	 * @param engine IEngine
	 */
	public NeighborQueryBuilderMenuItem(String name, QueryBuilderData data, IEngine engine)
	{
		super(name);
		this.name = name;
		this.data = data;
		this.engine = engine;
	}
	
	/**
	 * Composes the query and paints the neighborhood.
	 */
	public void paintNeighborhood()
	{
//		if(QuestionPlaySheetStore.getInstance().getActiveSheet() instanceof GraphPlaySheet)
		if(InsightStore.getInstance().getActiveInsight().getPlaySheet() instanceof AbstractGraphPlaySheet)
		{
//			GraphPlaySheet playSheet = (GraphPlaySheet) QuestionPlaySheetStore.getInstance().getActiveSheet();
			AbstractGraphPlaySheet playSheet = (AbstractGraphPlaySheet) InsightStore.getInstance().getActiveInsight().getPlaySheet();
			logger.debug("Extending ");
			Runnable playRunner = null;
			// Here I need to get the active sheet
			// get everything with respect the selected node type
			// and then create the filter on top of the query
			// use the @filter@ to get this done / some of the 			
	
			// need to create playsheet extend runner
			JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
			// get the selected repository
			Object [] repos = (Object [])list.getSelectedValues();
	
			for(int repoIndex = 0;repoIndex < repos.length;repoIndex++)
			{
				DataMakerComponent dmc = new DataMakerComponent(repos[repoIndex]+"", data);
//				IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(repos[repoIndex]+"");
//				playSheet.setRDFEngine(engine);
//				playSheet.setQuery(query);
			

				playRunner = new PlaysheetOverlayRunner(playSheet, new DataMakerComponent[]{dmc});
				// thread
				Thread playThread = new Thread(playRunner);
				playThread.start();
			}
		}
	}
	
}

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
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.om.GraphDataModel;
import prerna.om.InsightStore;
import prerna.om.OldInsight;
import prerna.om.SEMOSSVertex;
import prerna.ui.components.BrowserTabSheetFullAddress;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Controls the running of node editor.
 */
public class NodeEditorListener implements ActionListener {
	
	static final Logger logger = LogManager.getLogger(NodeEditorListener.class.getName());
	SEMOSSVertex node;
	String htmlFileName= "/html/MHS-RDFNodeEditor/app/index.html#/rdfnode/";
	String dbType;
	IDatabaseEngine engine;
	GraphPlaySheet gps;

	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param arg0 ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		GraphPlaySheet playSheet = (GraphPlaySheet) ((OldInsight) InsightStore.getInstance().getActiveInsight()).getPlaySheet();
		String uri = node.getProperty(Constants.URI)+"";
		
		String replacedURI = "<"+uri.replaceAll("/", "^") +">";
		 
		String workingDir = Utility.getBaseFolder();
		
		String address = "file://" + workingDir + htmlFileName;
		String fullAddress = address+replacedURI;
		BrowserTabSheetFullAddress tabS = new BrowserTabSheetFullAddress();
		tabS.setFileName(fullAddress);
		NodeEditorNavigationListener navListener = new NodeEditorNavigationListener();
		navListener.setNode(node);
		navListener.setFilterHash(((GraphDataModel)playSheet.getDataMaker()).baseFilterHash);
//		navListener.setBrowser(tabS.browser);
		navListener.setEngine(engine);
		navListener.setGps(gps);
//		tabS.setNavListener(navListener);
		System.err.println(fullAddress);
		tabS.setPlaySheet(playSheet);
		playSheet.jTab.add("Node Editor", tabS);
		playSheet.jTab.setSelectedComponent(tabS);
		SPARQLExecuteFunction sparqlFunction = new SPARQLExecuteFunction();
	    sparqlFunction.setEngine(engine);
	    sparqlFunction.setGps(gps);
//	    tabS.browser.registerFunction("SPARQLExecute", sparqlFunction);
	    SPARQLExecuteFilterNoBaseFunction filterFunction = new SPARQLExecuteFilterNoBaseFunction();
	    filterFunction.setFilterHash(((GraphDataModel)playSheet.getDataMaker()).baseFilterHash);
	    filterFunction.setEngine(engine);
//	    tabS.browser.registerFunction("SPARQLExecuteFilterNoBase", filterFunction);
	    SPARQLExecuteFilterBaseFunction filterBaseFunction = new SPARQLExecuteFilterBaseFunction();
	    filterBaseFunction.setFilterHash(((GraphDataModel)playSheet.getDataMaker()).baseFilterHash);
	    filterBaseFunction.setEngine(engine);
//	    tabS.browser.registerFunction("SPARQLExecuteFilterBase", filterBaseFunction);
	    InferEngineFunction inferFunction = new InferEngineFunction();
	    inferFunction.setEngine(engine);
//	    tabS.browser.registerFunction("InferFunction", inferFunction);
	    RefreshPlaysheetFunction refreshFunction = new RefreshPlaysheetFunction();
	    refreshFunction.setGps(gps);
//	    tabS.browser.registerFunction("RefreshFunction", refreshFunction);
		tabS.navigate();
	}	
	
	/**
	 * Method setNode.  Sets the node that the listener will access.
	 * @param node DBCMVertex
	 */
	public void setNode (SEMOSSVertex node)
	{
		this.node = node;
	}
	/**
	 * Method setDBType.  Sets the db that the listener will access.
	 * @param dbType String
	 */
	public void setDBType (String	dbType)
	{
		this.dbType = dbType;
	}
	/**
	 * Method setEngine.  Sets the engine that the listener will access.
	 * @param engine IDatabase
	 */
	public void setEngine(IDatabaseEngine engine){
		this.engine = engine;
	}
	/**
	 * Method setGps.  Sets the graph play sheet that the listener will access.
	 * @param gps GraphPlaySheet
	 */
	public void setGps(GraphPlaySheet gps){
		this.gps = gps;
	}
}

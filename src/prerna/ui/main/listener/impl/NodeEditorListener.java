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
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import org.apache.log4j.Logger;

import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.BrowserTabSheetFullAddress;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;

/**
 * Controls the running of node editor.
 */
public class NodeEditorListener implements ActionListener {
	
	Logger logger = Logger.getLogger(getClass());
	SEMOSSVertex node;
	String htmlFileName= "/html/MHS-RDFNodeEditor/app/index.html#/rdfnode/";
	String dbType;
	IEngine engine;
	GraphPlaySheet gps;

	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param arg0 ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		GraphPlaySheet playSheet = (GraphPlaySheet) QuestionPlaySheetStore.getInstance().getActiveSheet();
		String uri = node.getProperty(Constants.URI)+"";
		
		String replacedURI = "<"+uri.replaceAll("/", "^") +">";
		 
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		
		String address = "file://" + workingDir + htmlFileName;
		String fullAddress = address+replacedURI;
		BrowserTabSheetFullAddress tabS = new BrowserTabSheetFullAddress();
		tabS.setFileName(fullAddress);
		NodeEditorNavigationListener navListener = new NodeEditorNavigationListener();
		navListener.setNode(node);
		navListener.setFilterHash(playSheet.getGraphData().baseFilterHash);
		navListener.setBrowser(tabS.browser);
		navListener.setEngine(engine);
		navListener.setGps(gps);
		tabS.setNavListener(navListener);
		System.err.println(fullAddress);
		tabS.setPlaySheet(playSheet);
		playSheet.jTab.add("Node Editor", tabS);
		playSheet.jTab.setSelectedComponent(tabS);

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
	 * @param engine IEngine
	 */
	public void setEngine(IEngine engine){
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

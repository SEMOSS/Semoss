/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
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

import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.Constants;

import com.google.gson.Gson;
import com.teamdev.jxbrowser.chromium.Browser;
import com.teamdev.jxbrowser.chromium.JSValue;
import com.teamdev.jxbrowser.chromium.events.FailLoadingEvent;
import com.teamdev.jxbrowser.chromium.events.FinishLoadingEvent;
import com.teamdev.jxbrowser.chromium.events.FrameLoadEvent;
import com.teamdev.jxbrowser.chromium.events.LoadEvent;
import com.teamdev.jxbrowser.chromium.events.LoadListener;
import com.teamdev.jxbrowser.chromium.events.ProvisionalLoadingEvent;
import com.teamdev.jxbrowser.chromium.events.StartLoadingEvent;

/**
 */
public class NodeEditorNavigationListener implements LoadListener{
	
	static final Logger logger = LogManager.getLogger(NodeEditorNavigationListener.class.getName());

	SEMOSSVertex node = null;
	Browser browser = null;
	Hashtable filterHash;
	GraphPlaySheet gps;
	IEngine engine;
	
	
	/**
	 * Method setGps.  Sets the graph play sheet that the listener will access.
	 * @param gps GraphPlaySheet
	 */
	public void setGps(GraphPlaySheet gps) {
		this.gps = gps;
	}

	/**
	 * Method setEngine.  Sets the engine that the listener will access.
	 * @param engine IEngine
	 */
	public void setEngine(IEngine engine) {
		this.engine = engine;
	}

	/**
	 * Method setFilterHash.  Sets the filter hash that the listener will access.
	 * @param filterHash Hashtable
	 */
	public void setFilterHash(Hashtable filterHash) {
		this.filterHash = filterHash;
	}

	/**
	 * Method getBrowser.  Gets the current browser.	
	 * @return Browser */
	public Browser getBrowser() {
		return browser;
	}

	/**
	 * Method setBrowser.  Sets the browser that the listener will access.
	 * @param browser Browser
	 */
	public void setBrowser(Browser browser) {
		this.browser = browser;
	}
	
	/**
	 * Method setNode.  Sets the node that the listener will access.
	 * @param node DBCMVertex
	 */
	public void setNode(SEMOSSVertex node) {
		this.node = node;
	}

	/**
	 * Method navigationStarted.  Occurs when the navigation starts.
	 * @param event NavigationEvent
	 */
    
    /**
     * Method getFullNodeType.  Gets the full node type.
     * @param uri String
     * @param filterFunction SPARQLExecuteFilterBaseFunction
    
     * @return String */
    public String getFullNodeType(String uri, SPARQLExecuteFilterBaseFunction filterFunction){
    	String nodeType = "";
    	
    	String query = "SELECT ?type WHERE {<"+uri+"> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type}";
    	String retHashJson = filterFunction.invoke(JSValue.create(query)).getString();
    	Gson gson = new Gson();
    	Hashtable retHash = gson.fromJson(retHashJson, Hashtable.class);
    	ArrayList<ArrayList> retArray = (ArrayList<ArrayList>) retHash.get("results");
    	ArrayList array = retArray.get(0);
    	nodeType = (String) array.get(0);
    	
    	return nodeType;
    }

	@Override
	public void onDocumentLoadedInFrame(FrameLoadEvent arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onDocumentLoadedInMainFrame(LoadEvent arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onFailLoadingFrame(FailLoadingEvent arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onFinishLoadingFrame(FinishLoadingEvent arg0) {
		
	    
	    //register the various functions for javascript to call
	    SPARQLExecuteFilterBaseFunction filterBaseFunction = new SPARQLExecuteFilterBaseFunction();
	    filterBaseFunction.setFilterHash(filterHash);
	    filterBaseFunction.setEngine(engine);

	    
	    //get the parameters to pass it
	    String uri = (String) node.getProperty(Constants.URI);
	    String nodeName = (String) node.getProperty(Constants.VERTEX_NAME);
	    String nodeType = getFullNodeType(uri, filterBaseFunction);
	    
	    browser.executeJavaScript("start('" + uri + "', '" + nodeName + "', '" + nodeType + "');");
	    //cp.callIt();
		
	}

	@Override
	public void onProvisionalLoadingFrame(ProvisionalLoadingEvent arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onStartLoadingFrame(StartLoadingEvent arg0) {
		// TODO Auto-generated method stub
		
	}
}

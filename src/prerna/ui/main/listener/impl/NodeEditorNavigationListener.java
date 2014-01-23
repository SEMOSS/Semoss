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

import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.log4j.Logger;

import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.Constants;

import com.google.gson.Gson;
import com.teamdev.jxbrowser.Browser;
import com.teamdev.jxbrowser.events.NavigationEvent;
import com.teamdev.jxbrowser.events.NavigationFinishedEvent;
import com.teamdev.jxbrowser.events.NavigationListener;

/**
 */
public class NodeEditorNavigationListener implements NavigationListener{
	
	Logger logger = Logger.getLogger(getClass());

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
	public void navigationStarted(NavigationEvent event) {
        logger.info("event.getUrl() = " + event.getUrl());
    }

    /**
     * Method navigationFinished.  Occurs when the navigation ends.
     * @param event NavigationFinishedEvent
     */
    public void navigationFinished(NavigationFinishedEvent event) {
        logger.info("event.getStatusCode() = " + event.getStatusCode());
			//browser.waitReady();
        
        //register the various functions for javascript to call
        SPARQLExecuteFunction sparqlFunction = new SPARQLExecuteFunction();
        sparqlFunction.setEngine(engine);
        sparqlFunction.setGps(gps);
        browser.registerFunction("SPARQLExecute", sparqlFunction);
        SPARQLExecuteFilterNoBaseFunction filterFunction = new SPARQLExecuteFilterNoBaseFunction();
        filterFunction.setFilterHash(filterHash);
        filterFunction.setEngine(engine);
        browser.registerFunction("SPARQLExecuteFilterNoBase", filterFunction);
        SPARQLExecuteFilterBaseFunction filterBaseFunction = new SPARQLExecuteFilterBaseFunction();
        filterBaseFunction.setFilterHash(filterHash);
        filterBaseFunction.setEngine(engine);
        browser.registerFunction("SPARQLExecuteFilterBase", filterBaseFunction);
        InferEngineFunction inferFunction = new InferEngineFunction();
        inferFunction.setEngine(engine);
        browser.registerFunction("InferFunction", inferFunction);
        RefreshPlaysheetFunction refreshFunction = new RefreshPlaysheetFunction();
        refreshFunction.setGps(gps);
        browser.registerFunction("RefreshFunction", refreshFunction);
        
        //get the parameters to pass it
        String uri = (String) node.getProperty(Constants.URI);
        String nodeName = (String) node.getProperty(Constants.VERTEX_NAME);
        String nodeType = getFullNodeType(uri, filterBaseFunction);
        
        browser.executeScript("start('" + uri + "', '" + nodeName + "', '" + nodeType + "');");
        //cp.callIt();
    }
    
    /**
     * Method getFullNodeType.  Gets the full node type.
     * @param uri String
     * @param filterFunction SPARQLExecuteFilterBaseFunction
    
     * @return String */
    public String getFullNodeType(String uri, SPARQLExecuteFilterBaseFunction filterFunction){
    	String nodeType = "";
    	
    	String query = "SELECT ?type WHERE {<"+uri+"> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type}";
    	String retHashJson = (String) filterFunction.invoke(query);
    	Gson gson = new Gson();
    	Hashtable retHash = gson.fromJson(retHashJson, Hashtable.class);
    	ArrayList<ArrayList> retArray = (ArrayList<ArrayList>) retHash.get("results");
    	ArrayList array = retArray.get(0);
    	nodeType = (String) array.get(0);
    	
    	return nodeType;
    }
}

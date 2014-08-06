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
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.playsheets.GraphPlaySheet;

import com.google.gson.Gson;
import com.teamdev.jxbrowser.chromium.Browser;


/**
 * Controls pulling data from the in-memory hashtable to use in the browser/chart functionality.
 */
public class ChartPullDataListener implements ActionListener {
	GraphPlaySheet ps = null;
	static final Logger logger = LogManager.getLogger(ChartPullDataListener.class.getName());
	Browser browser = null;
	
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param e ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent e)
	{
		callIt();
	}	
	
	/**
	 * Method setBrowser.  Sets the browser that the listener will access.
	 * @param browser Browser
	 */
	public void setBrowser (Browser browser)
	{
		this.browser = browser;
	}
	
    /**
     * Method setPlaySheet.  Sets the playsheet that the listener will access.
     * @param ps GraphPlaySheet
     */
    public void setPlaySheet(GraphPlaySheet ps)
    {
	    this.ps = ps;
    }

	/**
	 * Method callIt.
	 */
	public void callIt()
	{
		Hashtable nodeHash = ps.filterData.typeHash;
		Hashtable edgeHash = ps.filterData.edgeTypeHash;
		Hashtable newHash = new Hashtable();
		newHash.put("Nodes", nodeHash);
		//newHash.put("Edges", edgeHash);
		Gson gson = new Gson();
		logger.info("Converted " + gson.toJson(newHash));
	    browser.executeJavaScript("start('" + gson.toJson(newHash) + "');");
	}


}

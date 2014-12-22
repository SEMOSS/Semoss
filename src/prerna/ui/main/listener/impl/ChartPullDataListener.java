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

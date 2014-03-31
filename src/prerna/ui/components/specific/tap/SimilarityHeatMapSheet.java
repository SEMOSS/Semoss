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
package prerna.ui.components.specific.tap;

import java.awt.Dimension;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.ui.main.listener.specific.tap.SimilarityBarChartBrowserFunction;
import prerna.ui.main.listener.specific.tap.SimilarityRefreshBrowserFunction;
import prerna.util.Constants;
import prerna.util.DIHelper;

import com.google.gson.Gson;
import com.teamdev.jxbrowser.events.NavigationEvent;
import com.teamdev.jxbrowser.events.NavigationFinishedEvent;
import com.teamdev.jxbrowser.events.NavigationListener;

/**
 */
public class SimilarityHeatMapSheet extends BrowserPlaySheet{
	Logger logger = Logger.getLogger(getClass());
	public ArrayList<String> comparisonObjectList = new ArrayList<String>();
	final String crmKey = "!CRM!";
	String comparisonObjectType = "";
	public Hashtable allHash = new Hashtable();
	public Hashtable paramDataHash = new Hashtable();
	public Hashtable keyHash = new Hashtable();
	
	SimilarityRefreshBrowserFunction refreshFunction;
	
	/**
	 * Constructor for SimilarityHeatMapSheet.
	 */
	public SimilarityHeatMapSheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
	}

	/**
	 * Set-up the browser by adding listeners and navigating to the html
	 */
	@Override
	public void createView()
	{
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		browser.addNavigationListener(new NavigationListener() {
    	    public void navigationStarted(NavigationEvent event) {
    	    	logger.info("event.getUrl() = " + event.getUrl());
    	    }

    	    public void navigationFinished(NavigationFinishedEvent event) {
    	    	prepareNavigationFinished();
    			callIt();

    	    }
    	});
	       
		browser.navigate("file://" + workingDir + "/html/MHS-RDFSemossCharts/app/sysDup.html");
		
	}
	
	/**
	 * Sets type of the comparison object
	 */
	public void setComparisonObjectType(String comparisonObjectType)
	{
		this.comparisonObjectType = comparisonObjectType;
	}
	
	/**
	 * Adds the refresh and bar chart listeners when navigation has finished.
	 */
	public void prepareNavigationFinished()
	{
    	refreshFunction = new SimilarityRefreshBrowserFunction();
    	refreshFunction.setParamDataHash(paramDataHash);
    	refreshFunction.setKeyHash(keyHash);
    	refreshFunction.setBrowser(browser);
    	browser.registerFunction("refreshFunction",  refreshFunction);
    	SimilarityBarChartBrowserFunction barChartFunction = new SimilarityBarChartBrowserFunction();
    	barChartFunction.setParamDataHash(paramDataHash);
    	browser.registerFunction("barChartFunction",  barChartFunction);
	}
	/**
	 * Formats data hashtable into proper format needed for charting.
	 * 
	 * @param dataHash Hashtable<String,Hashtable<String,Double>>	Hashtable of data to be formatted
	 * 
	 * @return Hashtable	Formatted hashtable of data
	 */
	public Hashtable processHashForCharting(Hashtable<String, Hashtable<String,Double>>dataHash)
	{
		//first create hashtable of arraylist with comparisonObject as key and corresponding data + blu as the values
		Hashtable<String, Hashtable<String,String>> dataRetHash = new Hashtable<String, Hashtable<String,String>>();

		for(Entry<String, Hashtable<String, Double>> comparisonObjectEntry : dataHash.entrySet()) 
		{
			String comparisonObjectName = comparisonObjectEntry.getKey();
		    Hashtable<String,Double> comparisonObjectDataHash = comparisonObjectEntry.getValue();
		    for(Entry<String, Double> comparisonObjectCompEntry : comparisonObjectDataHash.entrySet()) 
			{
				String comparisonObjectName2 = comparisonObjectCompEntry.getKey();
			    double comparisonObjectCompValue = comparisonObjectCompEntry.getValue();
			    if (!comparisonObjectName.equals(comparisonObjectName2))
			    {
					Hashtable elementHash = new Hashtable();
					elementHash.put("Score", comparisonObjectCompValue*100);
					String key = comparisonObjectName +"-"+comparisonObjectName2;
					dataRetHash.put(key, elementHash);
					if(!keyHash.containsKey(key)){
						Hashtable keyElementHash = new Hashtable();
						keyElementHash.put(comparisonObjectType+"1", comparisonObjectName);
						keyElementHash.put(comparisonObjectType+"2", comparisonObjectName2);
						keyHash.put(key, keyElementHash);
					}
			    }

			}
		}
		return dataRetHash;
	}

	public void createData()
	{
	}
	
	public void callIt()
	{
		Gson gson = new Gson();
		ArrayList args = new ArrayList();
		Enumeration enumKey = paramDataHash.keys();
		int count = 0;
		while (enumKey.hasMoreElements())
		{
			args.add(enumKey.nextElement());
			count++;
		}
		Hashtable testHash = new Hashtable();
//		testHash.put("Deployment_(Theater/Garrison)", 0.90);
//		browser.executeScript("dataBuilder('" + gson.toJson(args) + "', '" + gson.toJson(testHash) + "');");
		ArrayList<Hashtable<String, Hashtable<String, Double>>> calculatedArray = refreshFunction.calculateHash(args, testHash);
		refreshFunction.sendData(calculatedArray);
		
		//send available dimensions:
		String availCatString = "dimensionData('" + gson.toJson(args) + "', 'categories');";
		System.out.println(availCatString);
		browser.executeScript(availCatString);
		
		enumKey = allHash.keys();
		while (enumKey.hasMoreElements())
		{
			String key = (String) enumKey.nextElement();
			Object value = (Object) allHash.get(key);
			
			browser.executeScript("dimensionData('" + gson.toJson(value) + "', '"+key+"');");
			//System.out.println("dimensionData('" + gson.toJson(value) + "', '"+key+"');");
		}
		browser.executeScript("start();");
		updateProgressBar("100%...Visualization Complete", 100);
		allHash.clear();
//		paramDataHash.clear();
	}
}

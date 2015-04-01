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
package prerna.ui.components.specific.tap;

import java.awt.Dimension;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.ui.main.listener.specific.tap.SimilarityBarChartBrowserFunction;
import prerna.ui.main.listener.specific.tap.SimilarityRefreshBrowserFunction;
import prerna.util.Constants;
import prerna.util.DIHelper;

import com.google.gson.Gson;
import com.teamdev.jxbrowser.chromium.LoggerProvider;


/**
 */
public class SimilarityHeatMapSheet extends BrowserPlaySheet{
	protected static final Logger logger = LogManager.getLogger(SimilarityHeatMapSheet.class.getName());
	public ArrayList<String> comparisonObjectList = new ArrayList<String>();
	final String crmKey = "!CRM!";
	String comparisonObjectTypeX = "";
	String comparisonObjectTypeY = "";
	public Hashtable allHash = new Hashtable();
	public Hashtable<String, Hashtable<String, Hashtable<String, Object>>> paramDataHash = new Hashtable<String, Hashtable<String, Hashtable<String, Object>>>();
	public Hashtable keyHash = new Hashtable();
	final String valueString = "Score";
	final String keyString = "key";
	int maxDataSize = 20000;
	ArrayList<String> orderedVars = new ArrayList<String>();
	
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

		registerFunctions();
		browser.loadURL("file://" + workingDir + "/html/MHS-RDFSemossCharts/app/sysDup.html");
		LoggerProvider.getBrowserLogger().setLevel(Level.OFF);
		LoggerProvider.getIPCLogger().setLevel(Level.OFF);
		LoggerProvider.getChromiumProcessLogger().setLevel(Level.OFF);
		while (browser.isLoading()) {
		    try {
				TimeUnit.MILLISECONDS.sleep(50);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		registerFunctions();

		callIt();

	}
	
	/**
	 * Sets type of the comparison objects
	 */
	public void setComparisonObjectTypes(String comparisonObjectTypeX, String comparisonObjectTypeY)
	{
		this.comparisonObjectTypeX = comparisonObjectTypeX;
		this.comparisonObjectTypeY = comparisonObjectTypeY;
	}
	
	
	/**
	 * Adds the refresh and bar chart listeners when navigation has finished.
	 */
	public void registerFunctions()
	{
    	refreshFunction = new SimilarityRefreshBrowserFunction();
    	refreshFunction.setSimHeatPlaySheet(this);
    	browser.registerFunction("refreshFunction",  refreshFunction);
    	SimilarityBarChartBrowserFunction barChartFunction = new SimilarityBarChartBrowserFunction();
    	barChartFunction.setSimHeatPlaySheet(this);
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
						keyElementHash.put(comparisonObjectTypeX, comparisonObjectName);
						keyElementHash.put(comparisonObjectTypeY, comparisonObjectName2);
						keyHash.put(key, keyElementHash);
					}
			    }

			}
		}
		return dataRetHash;
	}

	public void createData() {
		super.createData();
	}
	
	public ArrayList prepareOrderedVars(){
		ArrayList args = new ArrayList();
		Enumeration enumKey = paramDataHash.keys();
		
		ArrayList<Integer> sizes = new ArrayList<Integer>();
		for(String key : paramDataHash.keySet()){
			int size = paramDataHash.get(key).size();
			int index = 0;
			for(int storedSize : sizes){
				if(size > storedSize)
					index++;
			}
			sizes.add(index, size);
			orderedVars.add(index, key);
		}
		logger.info("Ordered var = " + orderedVars.toString());
		logger.info("Counts = " + sizes.toString());
		int count = 0;
		while (enumKey.hasMoreElements())
		{
			args.add(enumKey.nextElement());
			count++;
		}
		return args;
	}
	
	public void callIt()
	{
		Gson gson = new Gson();
		ArrayList args = prepareOrderedVars();
		Hashtable testHash = new Hashtable();
//		testHash.put("Deployment_(Theater/Garrison)", 0.90);
//		browser.executeScript("dataBuilder('" + gson.toJson(args) + "', '" + gson.toJson(testHash) + "');");
		ArrayList<Hashtable<String, Hashtable<String, Double>>> calculatedArray = calculateHash(args, testHash);
		sendData(calculatedArray);
		
		//send available dimensions:
		String availCatString = "dimensionData('" + gson.toJson(args) + "', 'categories');";
		System.out.println(availCatString);
		browser.executeJavaScript(availCatString);
		
		Enumeration enumKey = allHash.keys();
		while (enumKey.hasMoreElements())
		{
			String key = (String) enumKey.nextElement();
			Object value = (Object) allHash.get(key);
			
			browser.executeJavaScript("dimensionData('" + gson.toJson(value) + "', '"+key+"');");
			//System.out.println("dimensionData('" + gson.toJson(value) + "', '"+key+"');");
		}
		browser.executeJavaScript("start();");
		updateProgressBar("100%...Visualization Complete", 100);
		logger.info("Finished creating the visualization.");
		allHash.clear();
//		paramDataHash.clear();
	}
	public ArrayList getSimBarChartData(String cellKey, ArrayList<String> selectedVars, Hashtable<String, Double> specifiedWeights){
		logger.info("cellKey = " + cellKey);
		
		ArrayList<String> selectedVarsList = new ArrayList<String>();
		logger.info("Selected Vars are : ");
		for(String obj : selectedVars) {
			logger.info(obj);
			selectedVarsList.add(obj);
		}

		if(!specifiedWeights.isEmpty())
		{
			logger.info("Specified Weights are : ");
			for(String obj : specifiedWeights.keySet()) 
				logger.info(obj + " " + specifiedWeights.get(obj));
		}
		
		ArrayList calculatedArray = retrieveValues(selectedVarsList, specifiedWeights, cellKey);
		return calculatedArray;
	}
	
	public ArrayList retrieveValues(ArrayList<String> selectedVars, Hashtable<String, Double>minimumWeights, String key){
		ArrayList<Hashtable> retHash = new ArrayList<Hashtable>();

		//for each checked var, get scores for key above minVal for that var
		for(int varIdx = 0; varIdx < selectedVars.size(); varIdx++){
			String var = selectedVars.get(varIdx);
			Double minVal = minimumWeights.get(var);//need to see if minimum weight was specified for this param
			Hashtable<String, Hashtable<String, Object>> varHash = paramDataHash.get(var);
			// get the comparison object to object hashtable
			Hashtable elementHash = varHash.get(key);
			Double newVal = ((Double)elementHash.get(valueString));
			//make sure value is valid
			if(minVal == null || newVal>=minVal){
				Hashtable newHash = new Hashtable();
				newHash.put(keyString, var);
				newHash.put(valueString, newVal);
				retHash.add(newHash);
				
			}
			logger.info("Bar Chart Hash size = " + retHash.size());
		}
			
		return retHash;
	}
	
	
	public boolean refreshSimHeat(String[] selectedVars, Hashtable<String, Double> specifiedWeights){
//		logger.info("args: ");
//		for(Object arg : arg0)
//			System.out.println(arg);
		ArrayList<String> selectedVarsList = new ArrayList<String>();
		logger.info("Selected Vars are : ");
		for(String obj : selectedVars) {
			logger.info(obj);
			selectedVarsList.add(obj);
		}
		if(!specifiedWeights.isEmpty())
		{
			logger.info("Specified Weights are : ");
			for(String obj : specifiedWeights.keySet()) 
				logger.info(obj + " " + specifiedWeights.get(obj));
		}
		
		ArrayList<Hashtable<String, Hashtable<String, Double>>> calculatedHash = calculateHash(selectedVarsList, specifiedWeights);
		sendData(calculatedHash);
		
		System.out.println("Java is done -- calling function");
		browser.executeJavaScript("refreshDataFunction();");
		System.out.println("Java is REALLY done");
		
		return true;
	}
	
	public void sendData(ArrayList<Hashtable<String, Hashtable<String, Double>>> calculatedArray){
		Gson gson = new Gson();
		for(Hashtable hash : calculatedArray)
		{
			System.out.println("Sending hash with " + hash.size());
			browser.executeJavaScript("dataBuilder('" + gson.toJson(hash) + "');");
			System.out.println("Done sending");
		}
		
//		while(!calculatedHash.isEmpty()){
//			int count = 0;
//			Hashtable tempHash = new Hashtable();
//			while(count < maxDataSize && !calculatedHash.isEmpty()){
//				String key = calculatedHash.keys().nextElement() + "";
//				tempHash.put(key, calculatedHash.remove(key));
//				count ++;
//			}
//			System.out.println("Building data.........");
//			browser.executeScript("dataBuilder('" + gson.toJson(tempHash) + "');");
//			System.out.println("Done building");
//		}
	}
	
	public ArrayList<Hashtable<String, Hashtable<String, Double>>> calculateHash(ArrayList<String> selectedVars, Hashtable<String, Double >minimumWeights){
		ArrayList<Hashtable<String, Hashtable<String, Double>>> retArray = new ArrayList<Hashtable<String, Hashtable<String, Double>>> ();
		
		int totalVars = selectedVars.size();
		
		//get the smallest variable to start with
		String minVar = "";
		int minVarLoc = 0;
		while(minVar.isEmpty() && minVarLoc<orderedVars.size()){
			String var = orderedVars.get(minVarLoc);
			if(selectedVars.contains(var))//this means it is a valid var (aka checked)
			{
				minVar = var;
				break;
			}
			minVarLoc++;
		}
		if(minVar.isEmpty()){
			System.out.println("no variables selected");
			return new ArrayList();
		}
		// get the master keyset
		List<String> masterKeys = new ArrayList<String>(paramDataHash.get(minVar).keySet());
		
		//for each cell, for each selected variable, sum the scores
		//if cell ever doesn't exist for a variable, the cell does not get stored in retHash
		for(String cellKey : masterKeys){
			Hashtable finalCellHash = new Hashtable();
			Double score = 0.;
			Boolean storeCell = true;
			
			for(int orderedVarIdx = minVarLoc; orderedVarIdx < orderedVars.size(); orderedVarIdx++){
				String var = orderedVars.get(orderedVarIdx);
				if(selectedVars.contains(var)){//this means it is a valid var (aka checked)
					Double minVal = minimumWeights.get(var);//need to see if minimum weight was specified for this param
					Hashtable<String, Hashtable<String, Object>> varHash = paramDataHash.get(var);
					Hashtable elementHash = varHash.get(cellKey);
					if(elementHash != null){
						Double newVal = (Double) elementHash.get(valueString);
						if(minVal == null || newVal>=minVal){ // then it is valid
							score = score + (newVal / totalVars);
						}
						else{
							storeCell = false;
							break;
						}
					}
					else{
						storeCell = false;
						break;
					}
					
					//store all information if first time looking at the cell
					if(orderedVarIdx == minVarLoc)
						finalCellHash.putAll((Map) keyHash.get(cellKey));
				}
			}
			if(storeCell){
				finalCellHash.put(valueString, score);
				retArray = storeCellInArray(cellKey, finalCellHash, retArray);
			}
			
		}		
//		
//		for(int orderedVarIdx = 0; orderedVarIdx < orderedVars.size(); orderedVarIdx++){
//			//not guarenteed every system will have every parameter, but by starting with the smallest, we are most likely good to go
//			String var = orderedVars.get(orderedVarIdx);
//			if(selectedVars.contains(var)){//this means it is a valid var (aka checked)
//				Double minVal = minimumWeights.get(var);//need to see if minimum weight was specified for this param
//				Hashtable<String, Hashtable<String, Object>> varHash = paramDataHash.get(var);
//				Hashtable<String, Hashtable<String, Double>> currentlyValidHash = new Hashtable<String, Hashtable<String, Double>>(); // keep track of what is still valid after going through this param (not valid if key did not exist in new hash)
//				// for each system-system hashtable
//				for(String key : varHash.keySet())
//				{
//					Hashtable elementHash = varHash.get(key);
//					Double newVal = ((Double)elementHash.get(valueString));
//					if(minVal == null || newVal>=minVal){
//					
//						Hashtable newElementHash = retHash.get(key);
//						Double oldScore = 0.;
//						if(validVarCount == 0 && newElementHash == null){
//							newElementHash = new Hashtable();
//							newElementHash.putAll(elementHash);
//							oldScore = 0.;
//						}
//						else if (validVarCount !=0 && newElementHash == null)
//							continue;
//						else
//							oldScore = (Double) newElementHash.get(valueString);
//						
//						// update elementHash and store in currentlyValidHash
//						Double additionalScore = newVal / totalVars;
//						newElementHash.put(valueString, oldScore + additionalScore);
//						
//						currentlyValidHash.put(key, newElementHash);
//					}
//				}
//				//clear retHash and fill with currentlyValidHash--those that were not in the last processed param do not get to stay in retHash
//				retHash.clear();
//				retHash.putAll(currentlyValidHash);
//				currentlyValidHash.clear();
//				validVarCount++;
//				logger.info("Calculated Hash size = " + retHash.size());
//			}
//		}
		return retArray;
	}
	
	public ArrayList<Hashtable<String, Hashtable<String, Double>>> storeCellInArray(String key, Hashtable cellHash, ArrayList<Hashtable<String, Hashtable<String, Double>>> arrayStore){
		Hashtable hash = null;
		if(arrayStore.size()>0){
			hash = arrayStore.get(0);
			if(hash.size()>this.maxDataSize){
				hash = new Hashtable();
				arrayStore.add(0, hash);
			}
		}
		else{
			hash = new Hashtable();
			arrayStore.add(0, hash);
		}
		hash.put(key, cellHash);
		return arrayStore;
	}
	
	@Override
	public Hashtable<String, String> getDataTableAlign() {
		Hashtable<String, String> alignHash = new Hashtable<String, String>();
		alignHash.put("x", comparisonObjectTypeX);
		alignHash.put("y", comparisonObjectTypeY);
		alignHash.put("heat", valueString);
		return alignHash;
	}
}

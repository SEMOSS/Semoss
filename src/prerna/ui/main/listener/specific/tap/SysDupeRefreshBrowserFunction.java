package prerna.ui.main.listener.specific.tap;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;

import com.google.gson.Gson;
import com.teamdev.jxbrowser.Browser;
import com.teamdev.jxbrowser.BrowserFunction;

/**
 * An browser class for refreshing system duplication heat map based on selected parameters.
 */
public class SysDupeRefreshBrowserFunction implements BrowserFunction {

	Logger logger = Logger.getLogger(getClass());
	ArrayList<String> orderedVars = new ArrayList<String>();
	Hashtable<String, Hashtable<String, Hashtable<String, Object>>> paramDataHash;
	Browser browser;
	Hashtable<String, Hashtable<String, String>> keyHash = new Hashtable<String, Hashtable<String, String>>();
	final String valueString = "Score";
	Gson gson = new Gson();
	int maxDataSize = 20000;
	
	/**
	 * Method invoke.  Overrides the invoke method from BrowserFunction.
	 * @param arg0 Object[]
	
	 * @return Object */
	@Override
	public Object invoke(Object... arg0){
		logger.info("args: ");
		for(Object arg : arg0)
			System.out.println(arg);
		String[] selectedVars = gson.fromJson(arg0[0] + "", String[].class);
		ArrayList<String> selectedVarsList = new ArrayList<String>();
		logger.info("Selected Vars are : ");
		for(String obj : selectedVars) {
			logger.info(obj);
			selectedVarsList.add(obj);
		}
		Hashtable<String, Double> specifiedWeights = new Hashtable<String, Double> ();
		if(arg0.length>1)
		{
			specifiedWeights = gson.fromJson(arg0[1] + "", Hashtable.class);
			logger.info("Specified Weights are : ");
			for(String obj : specifiedWeights.keySet()) 
				logger.info(obj + " " + specifiedWeights.get(obj));
		}
		
		ArrayList<Hashtable<String, Hashtable<String, Double>>> calculatedHash = calculateHash(selectedVarsList, specifiedWeights);
		sendData(calculatedHash);
		
		System.out.println("Java is done -- calling function");
		browser.executeScript("refreshDataFunction();");
		System.out.println("Java is REALLY done");
		
		return true;
	}
	
	public void sendData(ArrayList<Hashtable<String, Hashtable<String, Double>>> calculatedArray){
		for(Hashtable hash : calculatedArray)
		{
			System.out.println("Sending hash with " + hash.size());
			browser.executeScript("dataBuilder('" + gson.toJson(hash) + "');");
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
					Hashtable<String, Hashtable<String, Object>> varHash = paramDataHash.get(var);
					Hashtable elementHash = varHash.get(cellKey);
					if(elementHash == null){
						storeCell = false;
						break;
					}
					else{
						score = score + ((Double) elementHash.get(valueString) / totalVars);
					}
					
					//store all information if first time looking at the cell
					if(orderedVarIdx == minVarLoc)
						finalCellHash.putAll(keyHash.get(cellKey));
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
//			
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
		hash.put(key,  cellHash);
		return arrayStore;
	}
	
	/**
	 * Method setParamDataHash.  Sets the hashtable and orders the keys for processing
	 * @param engine paramDataHash - The hashtable used for processing
	 */
	public void setParamDataHash(Hashtable<String, Hashtable<String, Hashtable<String, Object>>> paramDataHash){
		this.paramDataHash = paramDataHash;
		//set the orderedVars list--smallest to largest
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
	}
	
	public void setBrowser(Browser browser){
		this.browser = browser;
	}
	
	public void setKeyHash(Hashtable<String, Hashtable<String, String>> keyHash){
		this.keyHash = keyHash;
	}
}


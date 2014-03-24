package prerna.ui.main.listener.specific.tap;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;

import com.google.gson.Gson;
import com.teamdev.jxbrowser.BrowserFunction;

/**
 * An browser class for refreshing system duplication heat map based on selected parameters.
 */
public class SysDupeRefreshBrowserFunction implements BrowserFunction {

	Logger logger = Logger.getLogger(getClass());
	ArrayList<String> orderedVars = new ArrayList<String>();
	Hashtable<String, Hashtable<String, Hashtable<String, Object>>> paramDataHash;
	final String valueString = "Score";
	
	/**
	 * Method invoke.  Overrides the invoke method from BrowserFunction.
	 * @param arg0 Object[]
	
	 * @return Object */
	@Override
	public Object invoke(Object... arg0){
		logger.info("args: ");
		for(Object arg : arg0)
			System.out.println(arg);
		Gson gson = new Gson();
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
		
		Hashtable calculatedHash = calculateHash(selectedVarsList, specifiedWeights);
		String finalJson = gson.toJson(calculatedHash);
		System.out.println("Java is done");
		return finalJson;
	}
	
	private Hashtable<String, Hashtable<String, Double>> calculateHash(ArrayList<String> selectedVars, Hashtable<String, Double >minimumWeights){
		Hashtable<String, Hashtable<String, Double>> retHash = new Hashtable<String, Hashtable<String, Double>>();
		
		int totalVars = selectedVars.size();
		
		int validVarCount = 0;//if it is the first valid var, it is authorized to add to retHash.
		for(int orderedVarIdx = 0; orderedVarIdx < orderedVars.size(); orderedVarIdx++){
			//not guarenteed every system will have every parameter, but by starting with the smallest, we are most likely good to go
			String var = orderedVars.get(orderedVarIdx);
			if(selectedVars.contains(var)){//this means it is a valid var (aka checked)
				Double minVal = minimumWeights.get(var);//need to see if minimum weight was specified for this param
				Hashtable<String, Hashtable<String, Object>> varHash = paramDataHash.get(var);
				Hashtable<String, Hashtable<String, Double>> currentlyValidHash = new Hashtable<String, Hashtable<String, Double>>(); // keep track of what is still valid after going through this param (not valid if key did not exist in new hash)
				// for each system-system hashtable
				for(String key : varHash.keySet())
				{
					Hashtable elementHash = varHash.get(key);
					Double newVal = ((Double)elementHash.get(valueString));
					if(minVal == null || newVal>=minVal){
					
						Hashtable newElementHash = retHash.get(key);
						Double oldScore = 0.;
						if(validVarCount == 0 && newElementHash == null){
							newElementHash = new Hashtable();
							newElementHash.putAll(elementHash);
							oldScore = 0.;
						}
						else if (validVarCount !=0 && newElementHash == null)
							continue;
						else
							oldScore = (Double) newElementHash.get(valueString);
						
						// update elementHash and store in currentlyValidHash
						Double additionalScore = newVal / totalVars;
						newElementHash.put(valueString, oldScore + additionalScore);
						
						currentlyValidHash.put(key, newElementHash);
					}
				}
				//clear retHash and fill with currentlyValidHash--those that were not in the last processed param do not get to stay in retHash
				retHash.clear();
				retHash.putAll(currentlyValidHash);
				currentlyValidHash.clear();
				validVarCount++;
				logger.info("Calculated Hash size = " + retHash.size());
			}
			
		}
		return retHash;
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
}


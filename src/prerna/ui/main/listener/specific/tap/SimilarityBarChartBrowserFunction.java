package prerna.ui.main.listener.specific.tap;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;

import com.google.gson.Gson;
import com.teamdev.jxbrowser.BrowserFunction;

/**
 * An browser class for refreshing similarity comparison heat map based on selected parameters.
 */
public class SimilarityBarChartBrowserFunction implements BrowserFunction {

	Logger logger = Logger.getLogger(getClass());
	Hashtable<String, Hashtable<String, Hashtable<String, Object>>> paramDataHash;
	final String valueString = "Score";
	final String keyString = "key";
	
	/**
	 * Method invoke.  Overrides the invoke method from BrowserFunction.
	 * @param arg0 Object[]
	
	 * @return Object */
	@Override
	public Object invoke(Object... arg0){
		Gson gson = new Gson();
		String cellKey = arg0[0] + "";
		logger.info("cellKey = " + cellKey);
		
		String[] selectedVars = gson.fromJson(arg0[1] + "", String[].class);
		ArrayList<String> selectedVarsList = new ArrayList<String>();
		logger.info("Selected Vars are : ");
		for(String obj : selectedVars) {
			logger.info(obj);
			selectedVarsList.add(obj);
		}

		Hashtable<String, Double> specifiedWeights = new Hashtable<String, Double> ();
		if(arg0.length>2)
		{
			specifiedWeights = gson.fromJson(arg0[2] + "", Hashtable.class);
			logger.info("Specified Weights are : ");
			for(String obj : specifiedWeights.keySet()) 
				logger.info(obj + " " + specifiedWeights.get(obj));
		}
		
		ArrayList calculatedHash = retrieveValues(selectedVarsList, specifiedWeights, cellKey);
		String finalJson = gson.toJson(calculatedHash);
		System.out.println("Java is done");
		return finalJson;
	}
	
	private ArrayList retrieveValues(ArrayList<String> selectedVars, Hashtable<String, Double >minimumWeights, String key){
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
	
	/**
	 * Method setParamDataHash.  Sets the hashtable and orders the keys for processing
	 * @param engine paramDataHash - The hashtable used for processing
	 */
	public void setParamDataHash(Hashtable<String, Hashtable<String, Hashtable<String, Object>>> paramDataHash){
		this.paramDataHash = paramDataHash;
	}
}


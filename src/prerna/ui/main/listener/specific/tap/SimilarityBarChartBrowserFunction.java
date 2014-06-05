package prerna.ui.main.listener.specific.tap;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.specific.tap.SimilarityHeatMapSheet;

import com.google.gson.Gson;
import com.teamdev.jxbrowser.chromium.BrowserFunction;
import com.teamdev.jxbrowser.chromium.JSValue;

/**
 * An browser class for refreshing similarity comparison heat map based on selected parameters.
 */
public class SimilarityBarChartBrowserFunction implements BrowserFunction {

	Logger logger = Logger.getLogger(getClass());
	Hashtable<String, Hashtable<String, Hashtable<String, Object>>> paramDataHash;
	final String valueString = "Score";
	final String keyString = "key";
	SimilarityHeatMapSheet simHeat = null;
	
	/**
	 * Method invoke.  Overrides the invoke method from BrowserFunction.
	 * @param arg0 Object[]
	
	 * @return Object */
	@Override
	public JSValue invoke(JSValue... arg0){
		Gson gson = new Gson();

		String cellKey = arg0[0].getString();
		logger.info("cellKey = " + cellKey);
		String[] selectedVars = gson.fromJson(arg0[1].getString(), String[].class);
		Hashtable<String, Double> specifiedWeights = new Hashtable<String, Double> ();
		simHeat.getSimBarChartData(cellKey, selectedVars, specifiedWeights);
		JSValue finalJson = JSValue.create(simHeat.getSimBarChartData(cellKey, selectedVars, specifiedWeights));
		System.out.println("Java is done");
		return finalJson;
	}
	
	/**
	 * Method setParamDataHash.  Sets the hashtable and orders the keys for processing
	 * @param engine paramDataHash - The hashtable used for processing
	 */
	public void setSimHeatPlaySheet(SimilarityHeatMapSheet simHeat){
		this.simHeat = simHeat;
	}
}


package prerna.ui.main.listener.specific.tap;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.specific.tap.SimilarityHeatMapSheet;

import com.google.gson.Gson;
import com.teamdev.jxbrowser.chromium.Browser;
import com.teamdev.jxbrowser.chromium.BrowserFunction;
import com.teamdev.jxbrowser.chromium.JSValue;


/**
 * An browser class for refreshing similarity comparison heat map based on selected parameters.
 */
public class SimilarityRefreshBrowserFunction implements BrowserFunction {

	Logger logger = Logger.getLogger(getClass());
	ArrayList<String> orderedVars = new ArrayList<String>();
	Hashtable<String, Hashtable<String, Hashtable<String, Object>>> paramDataHash;
	Browser browser;
	Hashtable<String, Hashtable<String, String>> keyHash = new Hashtable<String, Hashtable<String, String>>();
	final String valueString = "Score";
	Gson gson = new Gson();
	int maxDataSize = 20000;
	SimilarityHeatMapSheet simHeat = null;
	
	/**
	 * Method invoke.  Overrides the invoke method from BrowserFunction.
	 * @param arg0 Object[]
	
	 * @return Object */
	
	@Override
	public JSValue invoke(JSValue... arg0){
//		logger.info("args: ");
//		for(Object arg : arg0)
//			System.out.println(arg);
		String[] selectedVars = gson.fromJson(arg0[0].getString(), String[].class);
		Hashtable<String, Double> specifiedWeights = gson.fromJson(arg0[1].getString(), Hashtable.class);
		
		boolean simBoo = simHeat.refreshSimHeat(selectedVars, specifiedWeights);
		
		return JSValue.create(simBoo);
	}
	
	
	
	public void setSimHeatPlaySheet(SimilarityHeatMapSheet simHeat){
		this.simHeat = simHeat;
	}

}


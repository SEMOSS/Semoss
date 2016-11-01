package prerna.sablecc;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.algorithm.api.ITableDataFrame;
import prerna.cache.CacheFactory;
import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.ui.helpers.InsightCreateRunner;
import prerna.util.Utility;

/**
 * 
 * This reactor opens a stored insight - code is sourced from createOutput in engine Resource
 * TODO: Currently does not support opening playsheets
 *
 */
public class OutputDataReactor extends AbstractReactor {

	// this stores the specific values that need to be aggregated from the child reactors
	// based on the child, different information is needed in order to properly add the 
	// data into the frame
	Hashtable <String, String[]> values2SyncHash = new Hashtable <String, String[]>();
	
	public OutputDataReactor() {
		String [] thisReacts = {PKQLEnum.WORD_OR_NUM};
		
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.OPEN_DATA;
	}
	@Override
	public Iterator process() {
		
		return null;
	}

	/**
	 * Gets the values to load into the reactor
	 * This is used to synchronize between the various reactors that can feed into this reactor
	 * @param input			The type of child reactor
	 */
	public String[] getValues2Sync(String input)
	{
		return values2SyncHash.get(input);
	}
	
	/**
	 * Create the return response based on the headers
	 * @param headers			The headers of the data used to insert data
	 * @return					String returning the response
	 */
	protected String createResponseString(String[] headers){
		return "Successfully Opened Insight";
	}
}

package prerna.sablecc;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import com.google.gson.Gson;

import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.util.Utility;

/**
 * 
 * This reactor opens a stored insight - code is sourced from createOutput in engine Resource
 * TODO: Currently does not support opening playsheets
 *
 */
public class OpenDataReactor extends AbstractReactor {

	// this stores the specific values that need to be aggregated from the child reactors
	// based on the child, different information is needed in order to properly add the 
	// data into the frame
	Hashtable <String, String[]> values2SyncHash = new Hashtable <String, String[]>();
	
	public OpenDataReactor() {
		String [] thisReacts = {PKQLEnum.WORD_OR_NUM};
		
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.OPEN_DATA;
	}
	@Override
	public Iterator process() {
		
		Gson gson = new Gson();
		
		//open a saved insight if we have the data
		String engine = (String)myStore.get("DATA_OPEN_ENGINE");
		engine = (String)myStore.get(engine);
		String engine_id = (String)myStore.get("DATA_OPEN_ID");
		engine_id = (String)myStore.get(engine_id);
		
		IEngine coreEngine = Utility.getEngine(engine);
		if(coreEngine == null) {
			//store error message
			myStore.put(PKQLEnum.OPEN_DATA, "Error Opening Insight");
			return null;
		}
		Insight insightObj = ((AbstractEngine)coreEngine).getInsight(engine_id).get(0);
		
//		IDataMaker insight = (IDataMaker)myStore.get("G");
//		if(insight != null) {
//			insightObj.setUserId(insight.getUserId());
//		}
		
//		String vizData = CacheFactory.getInsightCache(CacheFactory.CACHE_TYPE.DB_INSIGHT_CACHE).getVizData(insightObj);

		Object obj = null;
//		if(vizData != null) {
//			// insight has been cached, send it to the FE with a new insight id
//			String id = InsightStore.getInstance().put(insightObj);
//			
//			myStore.put(PKQLEnum.OPEN_DATA, id);
//			
//			Map<String, Object> uploaded = gson.fromJson(vizData, new TypeToken<Map<String, Object>>() {}.getType());
//			Map<String, Object> webData = getWebData(uploaded);
//			webData.put("recipe", insightObj.getPixelRecipe());
//			webData.put("insightID", id);
//			webData.put("core_engine", engine);
//			webData.put("core_engine_id", engine_id);
//			myStore.put("webData", webData);
//		} else {
			// insight visualization data has not been cached, run the insight
			try {
				String id = InsightStore.getInstance().put(insightObj);
				myStore.put(PKQLEnum.OPEN_DATA, id);
				
				Map<String, Object> insightOutput = (Map<String, Object>) insightObj.reRunInsight();
//				InsightCreateRunner run = new InsightCreateRunner(insightObj);
//				Map<String, Object> insightOutput = run.runWeb();//runSavedRecipe();
				
				Map<String, Object> webData = getWebData(insightOutput);
				webData.put("recipe", insightObj.getPixelRecipe());
				webData.put("core_engine", engine);
				webData.put("core_engine_id", engine_id);
				myStore.put("webData", webData);
			} catch (Exception ex) { //need to specify the different exceptions 
				ex.printStackTrace();
				Hashtable<String, String> errorHash = new Hashtable<String, String>();
				errorHash.put("Message", "Error occured processing question.");
				errorHash.put("Class", "");
			}
//		}
			
		return null;
	}
	
	private Map<String, Object> getWebData(Map<String, Object> output) {
//		Map<String, Object> webData = new HashMap<>();
//		webData.put("insightID", output.get("insightID"));
//		webData.put("recipe", output.get("recipe"));
//		webData.put("layout", output.get("layout"));
//		webData.put("title", output.get("title"));
//		webData.put("dataMakerName", output.get("dataMakerName"));
//		webData.put("uiOptions", output.get("uiOptions"));
		
		//data, headers, config, varMap
		
		output.remove("pkqlOutput");
		
		
		
		return output;
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
	@Override
	public IPkqlMetadata getPkqlMetadata() {
		// TODO Auto-generated method stub
		return null;
	}
}

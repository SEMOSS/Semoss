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
public class OutputDataReactor extends AbstractReactor {

	// this stores the specific values that need to be aggregated from the child reactors
	// based on the child, different information is needed in order to properly add the 
	// data into the frame
	Hashtable <String, String[]> values2SyncHash = new Hashtable <String, String[]>();
	
	public OutputDataReactor() {
		String [] thisReacts = {PKQLEnum.WORD_OR_NUM};
		
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.OUTPUT_DATA;
	}
	@Override
	public Iterator process() {
		
		Gson gson = new Gson();
		
		//open a saved insight if we have the data
		String engine = (String)myStore.get("DATA_OPEN_ENGINE");
		engine = (String)myStore.get(engine);
		String engine_id = (String)myStore.get("DATA_OPEN_ID");
		engine_id = (String)myStore.get(engine_id);
		String insightId = (String)myStore.get("INSIGHT_ID");
		
		IEngine coreEngine = Utility.getEngine(engine);
		if(coreEngine == null) {
			//store error message
			myStore.put(PKQLEnum.OUTPUT_DATA, "Error Opening Insight");
			return null;
		}
		Insight insightObj = ((AbstractEngine)coreEngine).getInsight(engine_id).get(0);
		
//		IDataMaker insight = (IDataMaker)myStore.get("G");
//		insightObj.setUserId(insight.getUserId());
		
//		String vizData = CacheFactory.getInsightCache(CacheFactory.CACHE_TYPE.DB_INSIGHT_CACHE).getVizData(insightObj);

//		Object obj = null;
		boolean runInsightRecipe = true;
//		if(vizData != null) {
//			// insight has been cached, send it to the FE with a new insight id
////			String id = InsightStore.getInstance().put(insightObj);
//			insightObj.setInsightId(insightId);
//			InsightStore.getInstance().put(insightId, insightObj);
//			myStore.put(PKQLEnum.OUTPUT_DATA, insightId);
//			
//			Map<String, Object> uploaded = gson.fromJson(vizData, new TypeToken<Map<String, Object>>() {}.getType());
//			uploaded.put("insightID", insightId);
//			
//			myStore.put("webData", uploaded);
//			
//			insightObj.loadInsightCache();
//			IDataMaker dm = insightObj.getDataMaker();
//			if(dm != null) {
//				myStore.put("G", dm);
//				runInsightRecipe = false;
//			}
//		}
		
		if(runInsightRecipe) {
			// insight visualization data has not been cached, run the insight
			try {
				insightObj.setInsightId(insightId);
				InsightStore.getInstance().put(insightId, insightObj);
				myStore.put(PKQLEnum.OUTPUT_DATA, insightId);
				
				Map<String, Object> insightOutput = (Map<String, Object>) insightObj.reRunInsight();
				
//				InsightCreateRunner run = new InsightCreateRunner(insightObj);
//				Map<String, Object> insightOutput = run.runSavedRecipe();
				
				myStore.put("webData", insightOutput);
				myStore.put("G", insightObj.getDataMaker());
				
				//Don't cache dashboards for now...too many issues with that
				//need to resolve updating insight ID for dashboards, as well as old insight IDs of insights stored in varMap
//				if(!(insightObj.getDataMaker() instanceof Dashboard)) {
//					String saveFileLocation = CacheFactory.getInsightCache(CacheFactory.CACHE_TYPE.DB_INSIGHT_CACHE).cacheInsight(insightObj, insightOutput);
//
//					if(saveFileLocation != null) {
//						saveFileLocation = saveFileLocation + "_Solr.txt";
//						File solrFile = new File(saveFileLocation);
//						String solrId = SolrIndexEngine.getSolrIdFromInsightEngineId(insightObj.getEngineName(), insightObj.getRdbmsId());
//						SolrDocumentExportWriter writer = new SolrDocumentExportWriter(solrFile);
//						writer.writeSolrDocument(SolrIndexEngine.getInstance().getInsight(solrId));
//						writer.closeExport();
//					}
//				}
				
			} catch (Exception ex) { //need to specify the different exceptions 
				ex.printStackTrace();
				Hashtable<String, String> errorHash = new Hashtable<String, String>();
				errorHash.put("Message", "Error occured processing question.");
				errorHash.put("Class", "");
			}
		}
			
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
	@Override
	public IPkqlMetadata getPkqlMetadata() {
		// TODO Auto-generated method stub
		return null;
	}
}

package prerna.sablecc2.reactor.insights;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IEngine;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.om.OldInsight;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.util.Utility;

public class OpenInsightReactor extends AbstractInsightReactor {

	@Override
	public NounMetadata execute() {
		// set the existing insight to the saved insight
		
		// get the recipe for the insight
		// need the engine name and id that has the recipe
		String engineName = getEngine();
		int rdbmsId = getRdbmsId();
		Object params = getParams();
		
		// get the engine so i can get the new insight
		IEngine engine = Utility.getEngine(engineName);
		List<Insight> in = engine.getInsight(rdbmsId + "");
		Insight newInsight = in.get(0);
				
		// OLD INSIGHT
		if(newInsight instanceof OldInsight) {
			Map<String, Object> insightMap = new HashMap<String, Object>();
			// return to the FE the recipe
			insightMap.put("name", newInsight.getInsightName());
			// keys below match those in solr
			insightMap.put("core_engine", newInsight.getEngineName());
			insightMap.put("core_engine_id", newInsight.getRdbmsId());
			return new NounMetadata(insightMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OLD_INSIGHT);
		}
		// yay... not legacy
		// add the insight to the insight store
		InsightStore.getInstance().put(newInsight);

		Map<String, Object> insightMap = new HashMap<String, Object>();
		// return to the FE the recipe
		insightMap.put("name", newInsight.getInsightName());
		// keys below match those in solr
		insightMap.put("core_engine", newInsight.getEngineName());
		insightMap.put("core_engine_id", newInsight.getRdbmsId());
		insightMap.put("insightData", newInsight.reRunPixelInsight());
		insightMap.put("params", params);

		// return the recipe steps
		return new NounMetadata(insightMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPEN_SAVED_INSIGHT);
	}
}
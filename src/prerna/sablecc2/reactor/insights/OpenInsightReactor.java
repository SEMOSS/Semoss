package prerna.sablecc2.reactor.insights;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.solr.client.solrj.SolrServerException;

import prerna.engine.api.IEngine;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.om.OldInsight;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.solr.SolrIndexEngine;
import prerna.util.Utility;
import prerna.util.ga.GATracker;

public class OpenInsightReactor extends AbstractInsightReactor {
	
	public OpenInsightReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.ID.getKey(), ReactorKeysEnum.PARAM_KEY.getKey(), ReactorKeysEnum.ADDITIONAL_PIXELS.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// set the existing insight to the saved insight

		// get the recipe for the insight
		// need the engine name and id that has the recipe
		String engineName = getEngine();
		String rdbmsId = getRdbmsId();
		Object params = getParams();
		List<String> additionalPixels = getAdditionalPixels();

		// get the engine so i can get the new insight
		IEngine engine = Utility.getEngine(engineName);
		if(engine == null) {
			throw new IllegalArgumentException("Cannot find engine = " + engineName);
		}
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
			insightMap.put("layout", ((OldInsight) newInsight).getOutput());
			return new NounMetadata(insightMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OLD_INSIGHT);
		}
		// yay... not legacy
		// add the insight to the insight store
		InsightStore.getInstance().put(newInsight);

		// add additional pixels if necessary
		if(additionalPixels != null && !additionalPixels.isEmpty()) {
			// just add it directly to the pixel list
			// and the reRunPiexelInsight will do its job
			newInsight.getPixelRecipe().addAll(additionalPixels);
		}
		
		Map<String, Object> insightMap = new HashMap<String, Object>();
		// return to the FE the recipe
		insightMap.put("name", newInsight.getInsightName());
		// keys below match those in solr
		insightMap.put("core_engine", newInsight.getEngineName());
		insightMap.put("core_engine_id", newInsight.getRdbmsId());
		
		// track GA data
		GATracker.getInstance().trackInsightExecution(this.insight, "openinsight", engineName, rdbmsId, newInsight.getInsightName());

		insightMap.put("insightData", newInsight.reRunPixelInsight());
		insightMap.put("params", params);

		// update the solr tracker
		try {
			SolrIndexEngine.getInstance().updateViewedInsight(engineName + "_" + rdbmsId);
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException
				| IOException e) {
			e.printStackTrace();
		}

		// return the recipe steps
		return new NounMetadata(insightMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPEN_SAVED_INSIGHT);
	}

	private List<String> getAdditionalPixels() {
		GenRowStruct additionalPixels = this.store.getNoun(keysToGet[3]);
		if(additionalPixels != null && !additionalPixels.isEmpty()) {
			List<String> pixels = new Vector<String>();
			int size = additionalPixels.size();
			for(int i = 0; i < size; i++) {
				pixels.add(additionalPixels.get(i).toString());
			}
			return pixels;
		}

		// no additional pixels to run
		return null;
	}
}
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
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.solr.SolrIndexEngine;
import prerna.util.Utility;
import prerna.util.ga.GATracker;

public class OpenInsightReactor extends AbstractInsightReactor {
	
	public OpenInsightReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.ID.getKey(), ReactorKeysEnum.PARAM_KEY.getKey(), ReactorKeysEnum.ADDITIONAL_PIXELS.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// set the existing insight to the saved insight

		// get the recipe for the insight
		// need the engine name and id that has the recipe
		String appName = getApp();
		if(appName == null) {
			throw new IllegalArgumentException("Need to input the app name");
		}
		String rdbmsId = getRdbmsId();
		if(rdbmsId == null) {
			throw new IllegalArgumentException("Need to input the id for the insight");
		}
		Object params = getExecutionParams();
		List<String> additionalPixels = getAdditionalPixels();

		// get the engine so i can get the new insight
		IEngine engine = Utility.getEngine(appName);
		if(engine == null) {
			throw new IllegalArgumentException("Cannot find app = " + appName);
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
		// set user 
		newInsight.setUser(this.insight.getUser());
		newInsight.setUser2(this.insight.getUser2());
		// get the insight output
		PixelRunner runner = null;
		// add additional pixels if necessary
		if(additionalPixels != null && !additionalPixels.isEmpty()) {
			// just add it directly to the pixel list
			// and the reRunPiexelInsight will do its job
			newInsight.getPixelRecipe().addAll(additionalPixels);
		}
		// rerun the insight
		runner = newInsight.reRunPixelInsight();
		
		// update the solr universal view count
		try {
			SolrIndexEngine.getInstance().updateViewedInsight(appName + "_" + rdbmsId);
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException
				| IOException e) {
			e.printStackTrace();
		}

		// track GA data
		GATracker.getInstance().trackInsightExecution(this.insight, "openinsight", appName, rdbmsId, newInsight.getInsightName());
		
		// return the recipe steps
		Map<String, Object> runnerWraper = new HashMap<String, Object>();
		runnerWraper.put("runner", runner);
		runnerWraper.put("params", params);
		runnerWraper.put("additionalPixels", additionalPixels);
		return new NounMetadata(runnerWraper, PixelDataType.PIXEL_RUNNER, PixelOperationType.OPEN_SAVED_INSIGHT);
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
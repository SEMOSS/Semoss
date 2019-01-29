package prerna.sablecc2.reactor.insights.dashboard;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IEngine;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;
import prerna.util.insight.InsightUtility;

public class DashboardInsightConfigReactor extends AbstractReactor {
	
	private static final String INSIGHT_KEY = "insights";
	private static final String OLD_ID_KEY = "oldIds";
	private static final String LAYOUT_KEY = "layout";
	
	public DashboardInsightConfigReactor() {
		this.keysToGet = new String[]{INSIGHT_KEY, OLD_ID_KEY, LAYOUT_KEY};
	}
	
	@Override
	public NounMetadata execute() {
		boolean dashboardCacheable = this.insight.isCacheable();
		
		List<String> insightStrings = getInsights();
		List<String> oldIds = getOldIds();
		int numInsights = insightStrings.size();
		if(numInsights != oldIds.size()) {
			throw new IllegalArgumentException("Saved dashboard does not contain equal number of insights and ids");
		}
		
		List<Map<String, String>> insightConfig = new ArrayList<Map<String, String>>();
		for(int i = 0; i < numInsights; i++) {
			Map<String, String> insightMap = new HashMap<String, String>();
			// return to the FE the recipe
			Insight insight = getInsight(insightStrings.get(i));
			insightMap.put("name", insight.getInsightName());
			// keys below match those in solr
			insightMap.put("app_id", insight.getEngineId());
			insightMap.put("app_insight_id", insight.getRdbmsId());
			insightMap.put("recipe", "META|ReloadInsight(cache=[" + dashboardCacheable + "])");
			// TODO: delete this once we update ids
			insightMap.put("core_engine", insight.getEngineId());
			insightMap.put("core_engine_id", insight.getRdbmsId());
			
			// the old id -> needed to properly update the dashboard config
			insightMap.put("oldId", oldIds.get(i));
			// and make a new insight for them to run this recipe on
			// this is used so they can automatically update the config 
			// without waiting on a new id to come back
			Insight newInsight = new Insight();
			newInsight.setEngineId(insight.getEngineId());
			newInsight.setEngineName(insight.getEngineName());
			newInsight.setRdbmsId(insight.getRdbmsId());
			newInsight.setInsightName(insight.getInsightName());
			newInsight.setPixelRecipe(insight.getPixelRecipe());
			
			InsightUtility.transferDefaultVars(this.insight, newInsight);
			InsightStore.getInstance().put(newInsight);
			InsightStore.getInstance().addToSessionHash(getSessionId(), newInsight.getInsightId());
			insightMap.put("newId", newInsight.getInsightId());
			
			// add to list
			insightConfig.add(insightMap);
		}
		
		Map<String, Object> dashboardInsightConfig = new HashMap<String, Object>();
		dashboardInsightConfig.put("insightConfig", insightConfig);
		dashboardInsightConfig.put("layoutConfig", deEncodeString(getLayout()));
		return new NounMetadata(dashboardInsightConfig, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DASHBOARD_INSIGHT_CONFIGURATION);
	}
	
	private Insight getInsight(String engineId_rdbmsId_concat) {
		String[] split = engineId_rdbmsId_concat.split("__");
		String engineId = split[0];
		String rdbmsId = split[1];
		// get the engine so i can get the new insight
		engineId = MasterDatabaseUtility.testEngineIdIfAlias(engineId);

		IEngine engine = Utility.getEngine(engineId);
		if(engine == null) {
			throw new IllegalArgumentException("Could not find engine " + engineId);
		}
		List<Insight> in = engine.getInsight(rdbmsId + "");
		if(in == null || in.size() == 0) {
			throw new IllegalArgumentException("Could not find insight with id " + rdbmsId + " within the engine " + engineId);
		}
		Insight insight = in.get(0);
		return insight;
	}
	
	private String getInsightRecipe(Insight insight) {
		List<String> recipeSteps = insight.getPixelRecipe();
		
		StringBuilder bigRecipe = new StringBuilder();
		int size = recipeSteps.size();
		int i = 0;
		for(; i < size; i++) {
			bigRecipe.append(recipeSteps.get(i));
		}
		
		return bigRecipe.toString();
	}
	
	private String deEncodeString(String s) {
		String decodedText = null;
		try {
			decodedText = URLDecoder.decode(s, "UTF-8").replaceAll("\\%20", "+");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return decodedText;
	}
	
	////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////

	/*
	 * Getters from the noun store
	 */
	
	private List<String> getInsights() {
		GenRowStruct insightGrs = this.store.getNoun(INSIGHT_KEY);
		if(insightGrs == null) {
			throw new IllegalArgumentException("Saved dashboard does not contain any insights");
		}
		int size = insightGrs.size();
		if(size == 0) {
			throw new IllegalArgumentException("Saved dashboard does not contain any insights");
		}
		List<String> insightsUsed = new ArrayList<String>();
		for(int index = 0; index < size; index++) {
			insightsUsed.add(insightGrs.get(index).toString());
		}
		return insightsUsed;
	}
	
	private List<String> getOldIds() {
		GenRowStruct oldIdGrs = this.store.getNoun(OLD_ID_KEY);
		if(oldIdGrs == null) {
			throw new IllegalArgumentException("Saved dashboard does not contain the old insight ids");
		}
		int size = oldIdGrs.size();
		if(size == 0) {
			throw new IllegalArgumentException("Saved dashboard does not contain the old insight ids");
		}
		List<String> oldIds = new ArrayList<String>();
		for(int index = 0; index < size; index++) {
			oldIds.add(oldIdGrs.get(index).toString());
		}
		return oldIds;
	}
	
	private String getLayout() {
		GenRowStruct layoutGrs = this.store.getNoun(LAYOUT_KEY);
		if(layoutGrs == null) {
			throw new IllegalArgumentException("Saved dashboard needs a layout config");
		}
		int size = layoutGrs.size();
		if(size == 0) {
			throw new IllegalArgumentException("Saved dashboard needs a layout config");
		}
		return layoutGrs.get(0).toString().trim();
	}
	
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(INSIGHT_KEY)) {
			return "The insights of the saved dashboard";
		} else if (key.equals(OLD_ID_KEY)) {
			return "The old insight ids of the saved dashboard";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}

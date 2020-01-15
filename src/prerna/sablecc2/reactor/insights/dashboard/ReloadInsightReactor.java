package prerna.sablecc2.reactor.insights.dashboard;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import prerna.cache.InsightCacheUtility;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.insights.OpenInsightReactor;

public class ReloadInsightReactor extends OpenInsightReactor {

	@Override
	public NounMetadata execute() {
		Boolean cacheable = getUserDefinedCacheable();
		if(cacheable == null) {
			cacheable = this.insight.isCacheable();
		}
		
		// TODO: i am cheating here
		// we do not cache dashboards or param insights currently
		// so adding the cacheable check before hand
		boolean isParam = cacheable && PixelUtility.hasParam(this.insight.getPixelRecipe());
		boolean isDashoard = cacheable && PixelUtility.isDashboard(this.insight.getPixelRecipe());
		
		// if not param or dashboard, we can try to load a cache
		// do we have a cached insight we can use
		boolean hasCache = false;
		Insight cachedInsight = null;
		if(cacheable && !isParam && !isDashoard) {
			try {
				cachedInsight = getCachedInsight(this.insight);
				if(cachedInsight != null) {
					hasCache = true;
					cachedInsight.setInsightId(this.insight.getInsightId());
					cachedInsight.setInsightName(this.insight.getInsightName());
				}
			} catch (IOException | RuntimeException e) {
				hasCache = true;
				e.printStackTrace();
			}
		}
		
		// get the insight output
		PixelRunner runner = null;
		NounMetadata additionalMeta = null;
		if(cacheable && hasCache && cachedInsight == null) {
			// this means we have a cache
			// but there was an error with it
			InsightCacheUtility.deleteCache(this.insight.getEngineId(), this.insight.getEngineName(), this.insight.getRdbmsId());
			additionalMeta = NounMetadata.getWarningNounMessage("An error occured with retrieving the cache for this insight. System has deleted the cache and recreated the insight.");
		} else if(cacheable && hasCache) {
			try {
				runner = getCachedInsightData(cachedInsight);
			} catch (IOException | RuntimeException e) {
				InsightCacheUtility.deleteCache(this.insight.getEngineId(), this.insight.getEngineName(), this.insight.getRdbmsId());
				additionalMeta = NounMetadata.getWarningNounMessage("An error occured with retrieving the cache for this insight. System has deleted the cache and recreated the insight.");
				e.printStackTrace();
			}
		}
		
		if(runner == null) {
			runner = runNewInsight(this.insight, getAdditionalPixels());
//			now I want to cache the insight
			if(cacheable && !isParam && !isDashoard) {
				try {
					InsightCacheUtility.cacheInsight(this.insight, getCachedRecipeVariableExclusion(runner));
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} else {
			// this means the cache worked!
			// lets swap the insight with the cached one
			
			// set user 
			cachedInsight.setUser(this.insight.getUser());
			// add the insight to the insight store
			this.insight = cachedInsight;
			InsightStore.getInstance().put(this.insight);
			InsightStore.getInstance().addToSessionHash(getSessionId(), this.insight.getInsightId());
			this.insight.setUser(this.insight.getUser());
		}
		
		// return the recipe steps
		Map<String, Object> runnerWraper = new HashMap<String, Object>();
		runnerWraper.put("runner", runner);
		NounMetadata noun = new NounMetadata(runnerWraper, PixelDataType.PIXEL_RUNNER, PixelOperationType.OPEN_SAVED_INSIGHT);
		if(additionalMeta != null) {
			noun.addAdditionalReturn(additionalMeta);
		}
		return noun;
	}
}

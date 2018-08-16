package prerna.sablecc2.reactor.insights.dashboard;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import prerna.om.Insight;
import prerna.om.InsightCacheUtility;
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
		boolean isParam = PixelUtility.hasParam(this.insight.getPixelRecipe());
		boolean isDashoard = PixelUtility.isDashboard(this.insight.getPixelRecipe());
		
		// if not param or dashboard, we can try to load a cache
		// do we have a cached insight we can use
		boolean hasCache = false;
		Insight cachedInsight = null;
		if(!isParam && !isDashoard) {
			cachedInsight = getCachedInsight(this.insight.getEngineId(), this.insight.getEngineName(), this.insight.getRdbmsId());
			if(cachedInsight != null) {
				hasCache = true;
				cachedInsight.setInsightId(this.insight.getInsightId());
				cachedInsight.setInsightName(this.insight.getInsightName());
				this.insight = cachedInsight;
			}
		}
		
		// add the insight to the insight store
		InsightStore.getInstance().putWithCurrentId(this.insight);
		InsightStore.getInstance().addToSessionHash(getSessionId(), this.insight.getInsightId());
		// set user 
		this.insight.setUser(this.insight.getUser());
		
		// get the insight output
		PixelRunner runner = null;
		if(hasCache) {
			runner = getCachedInsightData(cachedInsight);
		} else {
			runner = runNewInsight(this.insight, getAdditionalPixels());
//			now I want to cache the insight
			if(!isParam && !isDashoard) {
				try {
					InsightCacheUtility.cacheInsight(this.insight);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		// return the recipe steps
		Map<String, Object> runnerWraper = new HashMap<String, Object>();
		runnerWraper.put("runner", runner);
		return new NounMetadata(runnerWraper, PixelDataType.PIXEL_RUNNER, PixelOperationType.OPEN_SAVED_INSIGHT);
	}

}

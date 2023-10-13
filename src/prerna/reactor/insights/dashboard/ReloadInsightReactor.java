package prerna.reactor.insights.dashboard;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.cache.InsightCacheUtility;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.reactor.insights.OpenInsightReactor;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

@Deprecated
public class ReloadInsightReactor extends OpenInsightReactor {

	private static final Logger logger = LogManager.getLogger(ReloadInsightReactor.class);

	@Override
	public NounMetadata execute() {
		Boolean cacheable = getUserDefinedCacheable();
		if(cacheable == null) {
			cacheable = this.insight.isCacheable();
		}
		Map<String, Object> paramValues = getInsightParamValueMap();

		// TODO: i am cheating here
		// we do not cache dashboards or param insights currently
		// so adding the cacheable check before hand
		List<String> pixelRecipe = this.insight.getPixelList().getPixelRecipe();
		boolean isParam = cacheable && PixelUtility.isNotCacheable(pixelRecipe);
		boolean isDashoard = cacheable && PixelUtility.isDashboard(pixelRecipe);
		
		// if not param or dashboard, we can try to load a cache
		// do we have a cached insight we can use
		boolean hasCache = false;
		Insight cachedInsight = null;
		if(cacheable && !isParam && !isDashoard) {
			try {
				cachedInsight = getCachedInsight(this.insight, paramValues);
				if(cachedInsight != null) {
					hasCache = true;
					cachedInsight.setInsightId(this.insight.getInsightId());
					cachedInsight.setInsightName(this.insight.getInsightName());
				}
			} catch (IOException | RuntimeException e) {
				hasCache = true;
				logger.error(Constants.STACKTRACE, e);
			}
		}
		
		// get the insight output
		PixelRunner runner = null;
		NounMetadata additionalMeta = null;
		if(cacheable && hasCache && cachedInsight == null) {
			// this means we have a cache
			// but there was an error with it
			InsightCacheUtility.deleteCache(this.insight.getProjectId(), this.insight.getProjectName(), this.insight.getRdbmsId(), paramValues, true);
			additionalMeta = NounMetadata.getWarningNounMessage("An error occurred with retrieving the cache for this insight. System has deleted the cache and recreated the insight.");
		} else if(cacheable && hasCache) {
			try {
				runner = getCachedInsightData(cachedInsight, paramValues);
			} catch (IOException | RuntimeException e) {
				InsightCacheUtility.deleteCache(this.insight.getProjectId(), this.insight.getProjectName(), this.insight.getRdbmsId(), paramValues, true);
				additionalMeta = NounMetadata.getWarningNounMessage("An error occurred with retrieving the cache for this insight. System has deleted the cache and recreated the insight.");
				logger.error(Constants.STACKTRACE, e);
			}
		}
		
		if(runner == null) {
			runner = runNewInsight(this.insight, getAdditionalPixels());
//			now I want to cache the insight
			if(cacheable && !isParam && !isDashoard) {
				try {
					InsightCacheUtility.cacheInsight(this.insight, getCachedRecipeVariableExclusion(runner), paramValues);
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		} else {
			// this means the cache worked!
			// lets swap the insight with the cached one
			// set user
			if (cachedInsight != null && this.insight != null) {
				cachedInsight.setUser(this.insight.getUser());
				// add the insight to the insight store
				this.insight = cachedInsight;
				InsightStore.getInstance().put(this.insight);
				InsightStore.getInstance().addToSessionHash(getSessionId(), this.insight.getInsightId());
				this.insight.setUser(this.insight.getUser());
			}
		}
		
		// return the recipe steps
		Map<String, Object> runnerWraper = new HashMap<>();
		runnerWraper.put("runner", runner);
		NounMetadata noun = new NounMetadata(runnerWraper, PixelDataType.PIXEL_RUNNER, PixelOperationType.OPEN_SAVED_INSIGHT);
		if(additionalMeta != null) {
			noun.addAdditionalReturn(additionalMeta);
		}
		return noun;
	}
	
	/**
	 * Run an insight with the additional pixels
	 * @param insight
	 * @param additionalPixels
	 * @return
	 */
	protected PixelRunner runNewInsight(Insight insight, List<String> additionalPixels) {
		// add additional pixels if necessary
		if(additionalPixels != null && !additionalPixels.isEmpty()) {
			// just add it directly to the pixel list
			// and the reRunPiexelInsight will do its job
			insight.getPixelList().addPixel(additionalPixels);
		}
		
		// rerun the insight
		PixelRunner runner = insight.reRunPixelInsight(false);
		return runner;
	}
}

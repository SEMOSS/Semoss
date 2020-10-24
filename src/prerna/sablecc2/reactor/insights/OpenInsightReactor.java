package prerna.sablecc2.reactor.insights;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.logging.log4j.Logger;

import com.google.gson.JsonSyntaxException;

import prerna.algorithm.api.ITableDataFrame;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cache.InsightCacheUtility;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.om.OldInsight;
import prerna.om.PixelList;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.insight.InsightUtility;
import prerna.util.usertracking.UserTrackerFactory;

public class OpenInsightReactor extends AbstractInsightReactor {
	
	private static final String CLASS_NAME = OpenInsightReactor.class.getName();
	
	public OpenInsightReactor() {
		this.keysToGet = new String[]{
				ReactorKeysEnum.APP.getKey(), 
				ReactorKeysEnum.ID.getKey(), 
				ReactorKeysEnum.PARAM_KEY.getKey(), 
				ReactorKeysEnum.ADDITIONAL_PIXELS.getKey(),
				CACHEABLE};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		
		// get the recipe for the insight
		// need the engine name and id that has the recipe
		String appId = getApp();
		if(appId == null) {
			throw new IllegalArgumentException("Need to input the app name");
		}
		String rdbmsId = getRdbmsId();
		if(rdbmsId == null) {
			throw new IllegalArgumentException("Need to input the id for the insight");
		}
		
		if(AbstractSecurityUtils.securityEnabled()) {
			appId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), appId);
			if(!SecurityInsightUtils.userCanViewInsight(this.insight.getUser(), appId, rdbmsId)) {
				NounMetadata noun = new NounMetadata("User does not have access to this insight", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		} else {
			appId = MasterDatabaseUtility.testEngineIdIfAlias(appId);
		}
		
		Object params = getExecutionParams();
		List<String> additionalPixels = getAdditionalPixels();
		
		// get the engine so i can get the new insight
		IEngine engine = Utility.getEngine(appId);
		if(engine == null) {
			throw new IllegalArgumentException("Cannot find app = " + appId);
		}
		Insight newInsight = null;
		try {
			List<Insight> in = engine.getInsight(rdbmsId + "");
			newInsight = in.get(0);
		} catch (ArrayIndexOutOfBoundsException e) {
			logger.info("Pulling app from cloud storage, appid=" + appId);
			ClusterUtil.reactorPullInsightsDB(appId);
			// this is needed for the pipeline json
			ClusterUtil.reactorPullFolder(engine, AssetUtility.getAppAssetVersionFolder(engine.getEngineName(), appId));
			try {
				List<Insight> in = engine.getInsight(rdbmsId + "");
				newInsight = in.get(0);
			} catch(IllegalArgumentException e2) {
				NounMetadata noun = new NounMetadata(e2.getMessage(), PixelDataType.CONST_STRING, PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			} catch (ArrayIndexOutOfBoundsException e2) {
				NounMetadata noun = new NounMetadata("Insight does not exist", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
		
		InsightUtility.transferDefaultVars(this.insight, newInsight);
		
		// OLD INSIGHT
		if(newInsight instanceof OldInsight) {
			Map<String, Object> insightMap = new HashMap<String, Object>();
			// return to the FE the recipe
			insightMap.put("name", newInsight.getInsightName());
			// keys below match those in solr
			insightMap.put("app_id", newInsight.getEngineId());
			insightMap.put("app_name", newInsight.getEngineName());
			insightMap.put("app_insight_id", newInsight.getRdbmsId());
			
			// LEGACY PARAMS
			insightMap.put("core_engine", newInsight.getEngineName());
			insightMap.put("core_engine_id", newInsight.getRdbmsId());
			
			insightMap.put("layout", ((OldInsight) newInsight).getOutput());
			return new NounMetadata(insightMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OLD_INSIGHT);
		}
		
		// yay... not legacy
		
		Boolean cacheable = getUserDefinedCacheable();
		if(cacheable == null) {
			cacheable = newInsight.isCacheable();
		}
		// TODO: i am cheating here
		// we do not cache dashboards or param insights currently
		// so adding the cacheable check before hand
		boolean isParam = cacheable && (params != null || PixelUtility.hasParam(newInsight.getPixelList().getPixelRecipe()));
		boolean isDashoard = cacheable && PixelUtility.isDashboard(newInsight.getPixelList().getPixelRecipe());
		
		// if not param or dashboard, we can try to load a cache
		// do we have a cached insight we can use
		boolean hasCache = false;
		Insight cachedInsight = null;
		if(cacheable && !isParam && !isDashoard) {
			try {
				cachedInsight = getCachedInsight(newInsight);
				if(cachedInsight != null) {
					hasCache = true;
					cachedInsight.setInsightName(newInsight.getInsightName());
					newInsight = cachedInsight;
				}
			} catch (IOException | RuntimeException e) {
				hasCache = true;
				e.printStackTrace();
			}
		}
		
		// add the insight to the insight store
		InsightStore.getInstance().put(newInsight);
		InsightStore.getInstance().addToSessionHash(getSessionId(), newInsight.getInsightId());
		// set user 
		newInsight.setUser(this.insight.getUser());
		
		// get the insight output
		PixelRunner runner = null;
		NounMetadata additionalMeta = null;
		if(cacheable && hasCache && cachedInsight == null) {
			// this means we have a cache
			// but there was an error with it
			InsightCacheUtility.deleteCache(newInsight.getEngineId(), newInsight.getEngineName(), rdbmsId, true);
			additionalMeta = NounMetadata.getWarningNounMessage("An error occured with retrieving the cache for this insight. System has deleted the cache and recreated the insight.");
		} else if(cacheable && hasCache) {
			try {
				runner = getCachedInsightData(cachedInsight);
			} catch (IOException | RuntimeException e) {
				InsightCacheUtility.deleteCache(newInsight.getEngineId(), newInsight.getEngineName(), rdbmsId, true);
				additionalMeta = NounMetadata.getWarningNounMessage("An error occured with retrieving the cache for this insight. System has deleted the cache and recreated the insight.");
				e.printStackTrace();
			}
		}

		if(runner == null) {
			logger.info("Running insight");
			runner = runNewInsight(newInsight, additionalPixels);
			logger.info("Done running insight");
			logger.info("Painting results");
//			now I want to cache the insight
			if(cacheable && !isParam && !isDashoard) {
				try {
					InsightCacheUtility.cacheInsight(newInsight, getCachedRecipeVariableExclusion(runner));
					Path appFolder = Paths.get(DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "db"+ DIR_SEPARATOR + SmssUtilities.getUniqueName(engine.getEngineName(), appId));
					String cacheFolder = InsightCacheUtility.getInsightCacheFolderPath(newInsight);
					Path relative = appFolder.relativize( Paths.get(cacheFolder));
					ClusterUtil.reactorPushFolder(appId,cacheFolder, relative.toString());
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		// update the universal view count
		GlobalInsightCountUpdater.getInstance().addToQueue(appId, rdbmsId);

		// NEW TRACKING
		UserTrackerFactory.getInstance().trackInsightExecution(newInsight);
		
		// add to user workspace
		newInsight.setCacheInWorkspace(true);
		
		// return the recipe steps
		Map<String, Object> runnerWraper = new HashMap<String, Object>();
		runnerWraper.put("runner", runner);
		runnerWraper.put("params", params);
		runnerWraper.put("additionalPixels", additionalPixels);
		NounMetadata noun = new NounMetadata(runnerWraper, PixelDataType.PIXEL_RUNNER, PixelOperationType.OPEN_SAVED_INSIGHT);
		if(additionalMeta != null) {
			noun.addAdditionalReturn(additionalMeta);
		}
		return noun;
	}

	protected List<String> getAdditionalPixels() {
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
		PixelRunner runner = insight.reRunPixelInsight();
		return runner;
	}
	
	/**
	 * Get the cached insight
	 * @param engineId
	 * @param insightId
	 * @return
	 */
	protected Insight getCachedInsight(Insight existingInsight) throws IOException, JsonSyntaxException {
		Insight insight = null;
		
		String insightZipLoc = InsightCacheUtility.getInsightCacheFolderPath(existingInsight) + DIR_SEPARATOR + InsightCacheUtility.INSIGHT_ZIP;
		File insightZip = new File(insightZipLoc);
		if(!insightZip.exists()) {
			// just return null
			return insight;
		}
		
		insight = InsightCacheUtility.readInsightCache(insightZip, existingInsight);
		return insight;
	}
	
	/**
	 * Get cached insight view data
	 * @param cachedInsight
	 * @return
	 */
	protected PixelRunner getCachedInsightData(Insight insight) throws IOException, JsonSyntaxException {
		// so that I don't mess up the insight recipe
		// use the object as it contains a ton of metadata
		// around the pixel step
		PixelList orig = insight.getPixelList().copy();
		// clear the current insight recipe
		insight.setPixelRecipe(new Vector<String>());
		
		PixelRunner runner = new PixelRunner();
		runner.setInsight(insight);
		
		// send the view data
		try {
			Map<String, Object> viewData = InsightCacheUtility.getCachedInsightViewData(insight);
			List<Object> pixelReturn = (List<Object>) viewData.get("pixelReturn");
			if(!pixelReturn.isEmpty()) {
				runner.addResult("CACHED_DATA", new NounMetadata(pixelReturn, PixelDataType.CACHED_PIXEL_RUNNER), true);
			}
			// logic to get all the frame headers
			{
				// get all frame headers
				VarStore vStore = insight.getVarStore();
				Set<String> keys = vStore.getKeys();
				for(String k : keys) {
					NounMetadata noun = insight.getVarStore().get(k);
					PixelDataType type = noun.getNounType();
					if(type == PixelDataType.FRAME) {
						try {
							ITableDataFrame frame = (ITableDataFrame) noun.getValue();
							runner.addResult(0, "CACHED_FRAME_HEADERS", 
									new NounMetadata(frame.getFrameHeadersObject(), PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.FRAME_HEADERS));
						} catch(Exception e) {
							e.printStackTrace();
							// ignore
						}
					}
				}
			}
		} finally {
			// we need to reset the recipe
			insight.setPixelList(orig);
		}
		
		return runner;
	}
	
	/**
	 * Get the variables that the execution would eventually drop to exclude from 
	 * @return
	 */
	protected Set<String> getCachedRecipeVariableExclusion(PixelRunner runner) {
		Set<String> varsToExclude = new HashSet<String>();
		
		List<NounMetadata> results = runner.getResults();
		for(NounMetadata res : results) {
			if(res.getNounType() == PixelDataType.REMOVE_VARIABLE) {
				varsToExclude.add(res.getValue().toString());
			}
		}
		
		return varsToExclude;
	}
	
	
}
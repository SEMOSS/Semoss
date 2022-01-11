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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonSyntaxException;

import prerna.algorithm.api.ITableDataFrame;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityUserInsightUtils;
import prerna.auth.utils.SecurityUserProjectUtils;
import prerna.cache.InsightCacheUtility;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.SmssUtilities;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.om.Insight;
import prerna.om.InsightPanel;
import prerna.om.InsightStore;
import prerna.om.OldInsight;
import prerna.om.PixelList;
import prerna.project.api.IProject;
import prerna.query.parsers.ParamStruct;
import prerna.query.parsers.ParamStructDetails;
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
	
	private static final Logger classLogger = LogManager.getLogger(OpenInsightReactor.class);
	private static final String CLASS_NAME = OpenInsightReactor.class.getName();
	
	public OpenInsightReactor() {
		this.keysToGet = new String[]{
				ReactorKeysEnum.PROJECT.getKey(), 
				ReactorKeysEnum.ID.getKey(), 
				ReactorKeysEnum.PARAM_KEY.getKey(), 
				ReactorKeysEnum.ADDITIONAL_PIXELS.getKey(),
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey(),
				CACHEABLE,
				USE_EXISTING_OPEN};
	}

	@Override
	public NounMetadata execute() {
		/*
		 * Workflow for this insight
		 * 
		 * 1) Permission checks / pulling the recipe from the insights database
		 * 2) Legacy insight check - not really important for most developers
		 * 3) Do we want to use an existing insight that the user has opened? 
		 * ******* If yes and it is opened - redirect to the UI State
		 * 4) Running the base insight recipe or pulling the cached insight
		 * 5) Parameter insight
		 * ******* Params are set in THIS current insight (not the new insight being created from the reactor)
		 * ******* If they are set, we then look for Panel 0 and make sure it has a param view
		 * ******* If yes - then we do the replacement and run that pixel
		 * 6) Run any additional pixels that are also passed in 
		 * ******* This is things like scheduling an export post insight execution (but post the parameter filled recipe)
		 * 
		 */
		
		Logger logger = getLogger(CLASS_NAME);

		/*
		 * 1) Start Permission checks / pulling the recipe from the insights database
		 */
		
		// get the recipe for the insight
		// need the engine name and id that has the recipe
		String projectId = getProject();
		if(projectId == null) {
			throw new IllegalArgumentException("Need to input the project id");
		}
		String rdbmsId = getRdbmsId();
		if(rdbmsId == null) {
			throw new IllegalArgumentException("Need to input the id for the insight");
		}
		
		User user = this.insight.getUser();
		if(AbstractSecurityUtils.securityEnabled()) {
			projectId = SecurityUserProjectUtils.testUserProjectIdForAlias(user, projectId);
			if(!SecurityUserInsightUtils.userCanViewInsight(user, projectId, rdbmsId)) {
				NounMetadata noun = new NounMetadata("User does not have access to this insight", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		} else {
			projectId = MasterDatabaseUtility.testDatabaseIdIfAlias(projectId);
		}
		
		// get the engine so i can get the new insight
		IProject project = Utility.getProject(projectId);
		if(project == null) {
			throw new IllegalArgumentException("Cannot find project = " + projectId);
		}
		Insight newInsight = null;
		try {
			List<Insight> in = project.getInsight(rdbmsId + "");
			newInsight = in.get(0);
		} catch (ArrayIndexOutOfBoundsException e) {
			logger.info("Pulling project from cloud storage, projectId=" + projectId);
			ClusterUtil.reactorPullInsightsDB(projectId);
			// this is needed for the pipeline json
			ClusterUtil.reactorPullProjectFolder(project, AssetUtility.getProjectAssetVersionFolder(project.getProjectName(), projectId));
			try {
				List<Insight> in = project.getInsight(rdbmsId + "");
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
		
		/*
		 * 2) Legacy insight check - not really important for most developers
		 */
		
		// OLD INSIGHT
		if(newInsight instanceof OldInsight) {
			return getOldInsightReturn((OldInsight) newInsight);
		}
		
		// yay... not legacy
		
		/*
		 * 3) Do we want to use an existing insight that the user has opened? 
		 */
		if(user != null && useExistingInsightIfOpen()) {
			List<String> userOpen = user.getOpenInsightInstances(projectId, rdbmsId);
			if(userOpen != null && !userOpen.isEmpty()) {
				Insight alreadyOpenedInsight = InsightStore.getInstance().get(userOpen.get(0));
				if(alreadyOpenedInsight == null) {
					// this is weird -- just do a cleanup
					user.removeOpenInsight(projectId, rdbmsId, userOpen.get(0));
				} else {
					// return the recipe steps
					PixelRunner runner = InsightUtility.recreateInsightState(alreadyOpenedInsight);
					Map<String, Object> runnerWraper = new HashMap<String, Object>();
					runnerWraper.put("runner", runner);
					// this is old way of doing/passing params
					// where FE sends to the BE and then the BE echos it back to the FE
					NounMetadata noun = new NounMetadata(runnerWraper, PixelDataType.PIXEL_RUNNER, PixelOperationType.OPEN_SAVED_INSIGHT);
					return noun;
				}
			}
		}
		
		/*
		 * 4) Running the base insight recipe or pulling the cached insight
		 */
		
		Boolean cacheable = getUserDefinedCacheable();
		if(cacheable == null) {
			cacheable = newInsight.isCacheable();
		}
		// TODO: i am cheating here
		// we do not cache dashboards or param insights currently
		// so adding the cacheable check before hand
		Object params = getExecutionParams();
		boolean isParam = cacheable && (params != null || PixelUtility.isNotCacheable(newInsight.getPixelList().getPixelRecipe()));
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
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		// note! if we have a cached insight
		// the cached insight and new insight both point to the same object now
		
		// add the insight to the insight store
		InsightStore.getInstance().put(newInsight);
		InsightStore.getInstance().addToSessionHash(getSessionId(), newInsight.getInsightId());
		// set user 
		newInsight.setUser(user);
		
		// get the insight output
		PixelRunner runner = null;
		List<NounMetadata> additionalMetas = new Vector<>();
		// if we have additional pixels
		// do not use the cached insight
		if(cacheable && hasCache && cachedInsight == null) {
			// this means we have a cache
			// but there was an error with it
			InsightCacheUtility.deleteCache(newInsight.getProjectId(), newInsight.getProjectName(), rdbmsId, true);
			additionalMetas.add(getWarning("An error occured with retrieving the cache for this insight. System has deleted the cache and recreated the insight."));
		} else if(cacheable && hasCache) {
			try {
				logger.info("Pulling cached insight");
				runner = getCachedInsightData(cachedInsight);
			} catch (IOException | RuntimeException e) {
				logger.info("Error occured pulling cached insight. Deleting cache and executing original recipe.");
				InsightCacheUtility.deleteCache(newInsight.getProjectId(), newInsight.getProjectName(), rdbmsId, true);
				additionalMetas.add(getWarning("An error occured with retrieving the cache for this insight. System has deleted the cache and recreated the insight."));
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		
		if(runner == null) {
			logger.info("Running insight");
			runner = newInsight.reRunPixelInsight(false);
			logger.info("Done running insight");
			// now I want to cache the insight
			if(cacheable && !isParam && !isDashoard) {
				logger.info("Caching insight for future use");
				try {
					InsightCacheUtility.cacheInsight(newInsight, getCachedRecipeVariableExclusion(runner));
					Path appFolder = Paths.get(DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) 
							+ DIR_SEPARATOR + "project"+ DIR_SEPARATOR + SmssUtilities.getUniqueName(project.getProjectName(), projectId));
					String cacheFolder = InsightCacheUtility.getInsightCacheFolderPath(newInsight);
					Path relative = appFolder.relativize( Paths.get(cacheFolder));
					ClusterUtil.reactorPushProjectFolder(projectId, cacheFolder, relative.toString());
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		/*
		 * 5) Parameter insight
		 */
		
		// see if we have any insights
		Map<String, String> paramValues = getInsightParamValueMap();
		if(paramValues != null && !paramValues.isEmpty()) {
			logger.info("Executing parameters within insight recipe");
			String paramPixel = null;
			// we will assume we have panel 0
			// and that we are filling this in via the BE
			Insight paramInsight = runner.getInsight();
			Map<String, NounMetadata> insightParamInput = paramInsight.getVarStore().pullParameters();
			
			InsightPanel panel0 = paramInsight.getInsightPanel("0");
			// make sure we have a param
			if(panel0 != null) {
				String panel0View = panel0.getPanelView();
				if("param".equals(panel0View)) {
					Map<String, Map<String, Object>> panelViewMap = panel0.getPanelViewOptions();
					Map<String, Object> paramMap = panelViewMap.get("param");
					if(paramMap != null) {
						List<Map<String, Object>> jsonMapArray = (List<Map<String, Object>>) paramMap.get("json");
						if(jsonMapArray != null && !jsonMapArray.isEmpty()) {
							Map<String, Object> jsonMap = jsonMapArray.get(0);
							paramPixel = (String) jsonMap.get("query");
						}
					}
				}
			}
			
			if(paramPixel == null) {
				additionalMetas.add(getWarning("Could not properly parse the insight recipe to generate the parameterized pixel expression"));
			} else {
				// move the parameters into the new insight
				// and set the user defined default values
				VarStore newInsightVarStore = newInsight.getVarStore();
				for(String paramKey : insightParamInput.keySet()) {
					NounMetadata paramNoun = insightParamInput.get(paramKey);
					ParamStruct pStruct = (ParamStruct) paramNoun.getValue();
					
					Object setValue = paramValues.get(pStruct.getParamName());
					if(setValue != null) {
						List<ParamStructDetails> details = pStruct.getDetailsList();
						for(ParamStructDetails detail : details) {
							detail.setCurrentValue(setValue);
						}
					}
					
					// still set the value
					// since we will pull the default from the param struct otherwise
					newInsightVarStore.put(paramKey, paramNoun);
				}
				
				String executionPixel = "META | RunParameterRecipe(recipe=[\"<encode>" + paramPixel + "</encode>\"], fill=true);";
				// get a NEW runner object
				PixelRunner innerRunner = newInsight.runPixel(executionPixel);
				// pull the inner pixel runner out
				// since FE is not recursive in how it deals with the payload
				Map<String, Object> innerRunnerMap = (Map<String, Object>) innerRunner.getResults().get(0).getValue();
				runner = (PixelRunner) innerRunnerMap.get("runner");
			}
		}
		
		/*
		 * 6) Run any additional pixels that are also passed in 
		 */
		
		// after we have gotten back either the insight or cached insight
		// now we can add the additional pixels if any were past in / exist
		List<String> additionalPixels = getAdditionalPixels();
		if(additionalPixels != null && !additionalPixels.isEmpty()) {
			logger.info("Running additional pixels in addition to the insight recipe");
			// use the existing runner
			// and run the additional pixels
			newInsight.runPixel(runner, additionalPixels);
		}
		logger.info("Painting results");
		
		// add to the users opened insights
		if(user != null) {
			user.addOpenInsight(projectId, rdbmsId, newInsight.getInsightId());
		}
		
		// update the universal view count
		GlobalInsightCountUpdater.getInstance().addToQueue(projectId, rdbmsId);
		// tracking execution
		UserTrackerFactory.getInstance().trackInsightExecution(newInsight);
		
		// add to user workspace
		newInsight.setCacheInWorkspace(true);
		
		// return the recipe steps
		Map<String, Object> runnerWraper = new HashMap<String, Object>();
		runnerWraper.put("runner", runner);
		// this is old way of doing/passing params
		// where FE sends to the BE and then the BE echos it back to the FE
		runnerWraper.put("params", params);
		runnerWraper.put("additionalPixels", additionalPixels);
		NounMetadata noun = new NounMetadata(runnerWraper, PixelDataType.PIXEL_RUNNER, PixelOperationType.OPEN_SAVED_INSIGHT);
		if(additionalMetas != null && !additionalMetas.isEmpty()) {
			noun.addAllAdditionalReturn(additionalMetas);
		}
		return noun;
	}

	protected List<String> getAdditionalPixels() {
		GenRowStruct additionalPixels = this.store.getNoun(ReactorKeysEnum.ADDITIONAL_PIXELS.getKey());
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
	
	protected Map<String, String> getInsightParamValueMap() {
		GenRowStruct paramValues = this.store.getNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		if(paramValues != null && !paramValues.isEmpty()) {
			return (Map<String, String>) paramValues.get(0);
		}

		// no additional pixels to run
		return null;
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
	protected PixelRunner getCachedInsightData(Insight cachedInsight) throws IOException, JsonSyntaxException {
		// so that I don't mess up the insight recipe
		// use the object as it contains a ton of metadata
		// around the pixel step
		PixelList orig = cachedInsight.getPixelList().copy();
		// clear the current insight recipe
		cachedInsight.setPixelRecipe(new Vector<String>());
		
		PixelRunner runner = new PixelRunner();
		runner.setInsight(cachedInsight);
		
		// send the view data
		try {
			// logic to get all the frame headers
			// add this first to the return object
			{
				// get all frame headers
				VarStore vStore = cachedInsight.getVarStore();
				List<String> keys = vStore.getFrameKeysCopy();
				for(String k : keys) {
					NounMetadata noun = vStore.get(k);
					PixelDataType type = noun.getNounType();
					if(type == PixelDataType.FRAME) {
						try {
							ITableDataFrame frame = (ITableDataFrame) noun.getValue();
							runner.addResult("CACHED_FRAME_HEADERS", 
									new NounMetadata(frame.getFrameHeadersObject(), PixelDataType.CUSTOM_DATA_STRUCTURE, 
											PixelOperationType.FRAME_HEADERS), true);
						} catch(Exception e) {
							classLogger.error(Constants.STACKTRACE, e);
							// ignore
						}
					}
				}
			}
			// now add in the cached insight view dataw
			Map<String, Object> viewData = InsightCacheUtility.getCachedInsightViewData(cachedInsight);
			List<Object> pixelReturn = (List<Object>) viewData.get("pixelReturn");
			if(!pixelReturn.isEmpty()) {
				runner.addResult("CACHED_DATA", new NounMetadata(pixelReturn, PixelDataType.CACHED_PIXEL_RUNNER), true);
			}
			
			// get the insight config for layout
			NounMetadata insightConfig = cachedInsight.getVarStore().get(SetInsightConfigReactor.INSIGHT_CONFIG);
			if(insightConfig != null) {
				runner.addResult("META | GetInsightConfig()", insightConfig, true);
			}
		} finally {
			// we need to reset the recipe
			cachedInsight.setPixelList(orig);
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
	
	/**
	 * For legacy insights
	 * Do not use other than to handle that case
	 * @param oldInsight
	 * @return
	 */
	private NounMetadata getOldInsightReturn(OldInsight oldInsight) {
		Map<String, Object> insightMap = new HashMap<String, Object>();
		// return to the FE the recipe
		insightMap.put("name", oldInsight.getInsightName());
		// keys below match those in solr
		insightMap.put("app_id", oldInsight.getProjectId());
		insightMap.put("app_name", oldInsight.getProjectName());
		insightMap.put("app_insight_id", oldInsight.getRdbmsId());
		
		// LEGACY PARAMS
		insightMap.put("core_engine", oldInsight.getProjectName());
		insightMap.put("core_engine_id", oldInsight.getRdbmsId());
		
		insightMap.put("layout", oldInsight.getOutput());
		return new NounMetadata(insightMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OLD_INSIGHT);
	}
	
	
}
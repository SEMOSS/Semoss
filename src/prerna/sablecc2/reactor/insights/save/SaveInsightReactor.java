package prerna.sablecc2.reactor.insights.save;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.impl.InsightAdministrator;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.insights.AbstractInsightReactor;
import prerna.util.MosfetSyncHelper;
import prerna.util.Utility;

public class SaveInsightReactor extends AbstractInsightReactor {

	private static final String CLASS_NAME = SaveInsightReactor.class.getName();
	
	public SaveInsightReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.INSIGHT_NAME.getKey(), ReactorKeysEnum.LAYOUT_KEY.getKey(),
				HIDDEN_KEY, ReactorKeysEnum.RECIPE.getKey(), 
				ReactorKeysEnum.PARAM_KEY.getKey(), ReactorKeysEnum.PIPELINE.getKey(), ReactorKeysEnum.IMAGE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = this.getLogger(CLASS_NAME);
		// get the recipe for the insight
		// need the engine name and id that has the recipe
		String appId = getApp();
		
		// security
		if(AbstractSecurityUtils.securityEnabled()) {
			if(AbstractSecurityUtils.anonymousUsersEnabled() && this.insight.getUser().isAnonymous()) {
				throwAnonymousUserError();
			}
			
			if(!SecurityAppUtils.userCanEditEngine(this.insight.getUser(), appId)) {
				throw new IllegalArgumentException("User does not have permission to add insights in the app");
			}
		}
		
		String insightName = getInsightName();
		if(insightName == null || insightName.isEmpty()) {
			throw new IllegalArgumentException("Need to define the insight name");
		}
		String[] recipeToSave = getRecipe();
		String layout = getLayout();
		boolean hidden = getHidden();
		List<String> params = getParams();
		Map pipeline = getPipeline();

		// saving an empty recipe?
		if (recipeToSave == null || recipeToSave.length == 0) {
			recipeToSave = this.insight.getPixelRecipe().toArray(new String[] {});
		} else {
			// this is always encoded before it gets here
			recipeToSave = decodeRecipe(recipeToSave);
		}
		
		//Pull the insights db again incase someone just saved something 
		ClusterUtil.reactorSyncInsightsDB(appId);

		// get the new insight id
		String newInsightId = UUID.randomUUID().toString();
		// get an updated recipe if there are files used
		// and save the files in the correct location
		try {
			recipeToSave = saveFilesInInsight(recipeToSave, appId, newInsightId);
		} catch(Exception e) {
			throw new IllegalArgumentException("An error occured trying to identify file based sources to parameterize. The source error message is: " + e.getMessage());
		}
		if(params != null && !params.isEmpty()) {
			try {
				recipeToSave = getParamRecipe(recipeToSave, params, insightName);
			} catch(Exception e) {
				throw new IllegalArgumentException("An error occured trying to parameterize the insight recipe. The source error message is: " + e.getMessage());
			}
		}

		IEngine engine = Utility.getEngine(appId);
		if(engine == null) {
			// we may have the alias
			engine = Utility.getEngine(MasterDatabaseUtility.testEngineIdIfAlias(appId));
			if(engine == null) {
				throw new IllegalArgumentException("Cannot find app = " + appId);
			}
		}
		// add the recipe to the insights database
		InsightAdministrator admin = new InsightAdministrator(engine.getInsightDatabase());

		logger.info("1) Add insight " + insightName + " to rdbms store...");
		String newRdbmsId = admin.addInsight(newInsightId, insightName, layout, recipeToSave, hidden);
		logger.info("1) Done...");

		if(!hidden) {
			logger.info("2) Regsiter insight...");
			registerInsightAndMetadata(engine.getEngineId(), newRdbmsId, insightName, layout, "", new ArrayList<String>());
			logger.info("2) Done...");
		} else {
			logger.info("2) Insight is hidden ... do not add to solr");
		}
		
		//write recipe to file
		logger.info("3) Add recipe to file...");
		MosfetSyncHelper.makeMosfitFile(engine.getEngineId(), engine.getEngineName(), newRdbmsId, insightName, layout, recipeToSave, hidden);
		logger.info("3) Done...");
		
		// write pipeline
		if(pipeline != null && !pipeline.isEmpty()) {
			logger.info("4) Add pipeline to file...");
			writePipelineToFile(engine.getEngineId(), engine.getEngineName(), newRdbmsId, pipeline);
			logger.info("4) Done...");
		}
	
		// get base 64 image string and write to file
		String base64Image = getImage();
		if(base64Image != null && !base64Image.trim().isEmpty()) {
			storeImageFromPng(base64Image, newRdbmsId, engine.getEngineId(), engine.getEngineName());
		}
		
		// update the workspace cache for the saved insight
		this.insight.setEngineId(engine.getEngineId());
		this.insight.setEngineName(engine.getEngineName());
		this.insight.setRdbmsId(newRdbmsId);
		this.insight.setInsightName(insightName);
		// this is to reset it
		this.insight.setInsightFolder(null);
		
		ClusterUtil.reactorPushApp(appId);
		
		Map<String, Object> returnMap = new HashMap<String, Object>();
		returnMap.put("name", insightName);
		returnMap.put("app_insight_id", newRdbmsId);
		returnMap.put("app_name", engine.getEngineName());
		returnMap.put("app_id", engine.getEngineId());
		returnMap.put("recipe", recipeToSave);
		NounMetadata noun = new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.SAVE_INSIGHT);
		return noun;
	}
	
	/**
	 * Add an insight into solr
	 * @param appId
	 * @param appName
	 * @param insightIdToSave
	 * @param insightName
	 * @param layout
	 * @param description
	 * @param tags
	 * @param userId
	 */
	private void registerInsightAndMetadata(String appId, String insightIdToSave, String insightName, String layout, String description, List<String> tags) {
		// TODO: INSIGHTS ARE ALWAYS GLOBAL!!!
		SecurityInsightUtils.addInsight(appId, insightIdToSave, insightName, true, layout);
		if(this.insight.getUser() != null) {
			SecurityInsightUtils.addUserInsightCreator(this.insight.getUser(), appId, insightIdToSave);
		}
		// TODO: add the description + tags
	}
}

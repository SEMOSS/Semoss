package prerna.sablecc2.reactor.insights.save;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.auth.utils.SecurityUpdateUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.SmssUtilities;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.insights.AbstractInsightReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.MosfetSyncHelper;
import prerna.util.Utility;

public class SetInsightNameReactor extends AbstractInsightReactor {

	private static final String CLASS_NAME = SetInsightNameReactor.class.getName();

	public SetInsightNameReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.INSIGHT_NAME.getKey(), ReactorKeysEnum.ID.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = this.getLogger(CLASS_NAME);

		String appId = getApp();
		// need to know what we are updating
		String existingId = getRdbmsId();
		
		// we may have the alias
		if(AbstractSecurityUtils.securityEnabled()) {
			appId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), appId);
			if(!SecurityInsightUtils.userCanEditInsight(this.insight.getUser(), appId, existingId)) {
				throw new IllegalArgumentException("App does not exist or user does not have permission to edit this insight");
			}
		} else {
			appId = MasterDatabaseUtility.testEngineIdIfAlias(appId);
			if(!MasterDatabaseUtility.getAllEngineIds().contains(appId)) {
				throw new IllegalArgumentException("App " + appId + " does not exist");
			}
		}
		
		String insightName = getInsightName();
		if(insightName == null || insightName.isEmpty()) {
			throw new IllegalArgumentException("Need to define the insight name");
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

		// update insight db
		logger.info("1) Updating insight in rdbms");
		admin.updateInsightName(existingId, insightName);
		logger.info("1) Done");
		
		logger.info("2) Updating insight in index");
		SecurityUpdateUtils.updateInsightName(appId, existingId, insightName);
		logger.info("2) Done");
		
		logger.info("3) Update mosfet file for collaboration");
		updateRecipeFile(logger, engine.getEngineId(), engine.getEngineName(), existingId, insightName);
		logger.info("3) Done");

		ClusterUtil.reactorPushApp(appId);
		
		Map<String, Object> returnMap = new HashMap<String, Object>();
		returnMap.put("name", insightName);
		returnMap.put("app_insight_id", existingId);
		returnMap.put("app_name", engine.getEngineName());
		returnMap.put("app_id", engine.getEngineId());
		NounMetadata noun = new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.SAVE_INSIGHT);
		return noun;
	}

	/**
	 * Update recipe: delete the old file and save as new
	 * 
	 * @param engineName
	 * @param rdbmsID
	 * @param recipeToSave
	 */
	protected void updateRecipeFile(Logger logger, String appId, String appName, String rdbmsID, String insightName) {
		final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
		String recipeLocation = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) 
				+ DIR_SEPARATOR + Constants.DB + DIR_SEPARATOR + SmssUtilities.getUniqueName(appName, appId) + DIR_SEPARATOR + "version" 
				+ DIR_SEPARATOR + rdbmsID + DIR_SEPARATOR + MosfetSyncHelper.RECIPE_FILE;
		File mosfet = new File(recipeLocation);
		if(mosfet.exists()) {
			MosfetSyncHelper.updateMosfitFileInsightName(new File(recipeLocation), insightName);
		} else {
			logger.info("... Could not find existing mosfet file. Ignoring update.");
		}
	}
	
}

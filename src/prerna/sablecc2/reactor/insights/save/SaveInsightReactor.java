package prerna.sablecc2.reactor.insights.save;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;

import prerna.engine.api.IEngine;
import prerna.engine.impl.InsightAdministrator;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.insights.AbstractInsightReactor;
import prerna.solr.SolrIndexEngine;
import prerna.util.MosfetSyncHelper;
import prerna.util.Utility;
import prerna.util.ga.GATracker;

public class SaveInsightReactor extends AbstractInsightReactor {

	private static final String CLASS_NAME = SaveInsightReactor.class.getName();
	
	public SaveInsightReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.INSIGHT_NAME.getKey(), ReactorKeysEnum.LAYOUT_KEY.getKey(),
				HIDDEN_KEY, ReactorKeysEnum.RECIPE.getKey(), ReactorKeysEnum.PARAM_KEY.getKey(), ReactorKeysEnum.IMAGE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = this.getLogger(CLASS_NAME);
		// get the recipe for the insight
		// need the engine name and id that has the recipe
		String appId = getApp();
		if(appId == null || appId.isEmpty()) {
			throw new IllegalArgumentException("Need to define the app where the insight currently exists");
		}
		String insightName = getInsightName();
		if(insightName == null || insightName.isEmpty()) {
			throw new IllegalArgumentException("Need to define the insight name");
		}
		String[] recipeToSave = getRecipe();
		String layout = getLayout();
		boolean hidden = getHidden();
		List<String> params = getParams();
		
		// saving an empty recipe?
		if (recipeToSave == null || recipeToSave.length == 0) {
			recipeToSave = this.insight.getPixelRecipe().toArray(new String[] {});
		} else {
			// this is always encoded before it gets here
			recipeToSave = decodeRecipe(recipeToSave);
		}

		// get the new insight id
		String newInsightId = UUID.randomUUID().toString();
		// get an updated recipe if there are files used
		// and save the files in the correct location
		recipeToSave = saveFilesInInsight(recipeToSave, appId, newInsightId);
		
		if(params != null && !params.isEmpty()) {
			recipeToSave = getParamRecipe(recipeToSave, params, insightName);
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
			logger.info("2) Add insight to solr...");
			addNewInsightToSolr(engine.getEngineId(), engine.getEngineName(), newRdbmsId, insightName, layout, "", new ArrayList<String>(), "");
			logger.info("2) Done...");
		} else {
			logger.info("2) Insight is hidden ... do not add to solr");
		}
		
		//write recipe to file
		logger.info("3) Add recipe to file...");
		MosfetSyncHelper.makeMosfitFile(engine.getEngineId(), engine.getEngineName(), newRdbmsId, insightName, layout, recipeToSave, hidden);
		logger.info("3) Done...");
		
		// get base 64 image string and write to file
		String base64Image = getImage();
		if(base64Image != null && !base64Image.trim().isEmpty()) {
			storeImageFromPng(base64Image, newRdbmsId, engine.getEngineId(), engine.getEngineName());
		}
		
		Map<String, Object> returnMap = new HashMap<String, Object>();
		returnMap.put(SolrIndexEngine.STORAGE_NAME, insightName);
		returnMap.put(SolrIndexEngine.APP_INSIGHT_ID, newRdbmsId);
		returnMap.put(SolrIndexEngine.APP_NAME, engine.getEngineName());
		returnMap.put(SolrIndexEngine.APP_ID, engine.getEngineId());
		returnMap.put("recipe", recipeToSave);
		NounMetadata noun = new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.SAVE_INSIGHT);

		// track GA data
		GATracker.getInstance().trackInsightExecution(this.insight, "saveinsight", appId, newRdbmsId, insightName);

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
	private void addNewInsightToSolr(String appId, String appName, String insightIdToSave, String insightName, String layout, String description, List<String> tags, String userId) {
		Map<String, Object> solrInsights = new HashMap<>();
		DateFormat dateFormat = SolrIndexEngine.getDateFormat();
		Date date = new Date();
		String currDate = dateFormat.format(date);
		solrInsights.put(SolrIndexEngine.APP_ID, appId);
		solrInsights.put(SolrIndexEngine.APP_NAME, appName);
		solrInsights.put(SolrIndexEngine.STORAGE_NAME, insightName);
		solrInsights.put(SolrIndexEngine.TAGS, tags);
		solrInsights.put(SolrIndexEngine.LAYOUT, layout);
		solrInsights.put(SolrIndexEngine.CREATED_ON, currDate);
		solrInsights.put(SolrIndexEngine.MODIFIED_ON, currDate);
		solrInsights.put(SolrIndexEngine.LAST_VIEWED_ON, currDate);
		solrInsights.put(SolrIndexEngine.DESCRIPTION, description);
		solrInsights.put(SolrIndexEngine.APP_INSIGHT_ID, insightIdToSave);
		solrInsights.put(SolrIndexEngine.USER_ID, userId);
		solrInsights.put(SolrIndexEngine.VIEW_COUNT, 0);

		try {
			SolrIndexEngine.getInstance().addInsight(SolrIndexEngine.getSolrIdFromInsightEngineId(appId, insightIdToSave), solrInsights);
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException
				| IOException e1) {
			e1.printStackTrace();
		}
	}
}

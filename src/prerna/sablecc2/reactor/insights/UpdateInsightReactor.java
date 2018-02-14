package prerna.sablecc2.reactor.insights;

import java.io.File;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;

import prerna.engine.api.IEngine;
import prerna.engine.impl.InsightAdministrator;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.solr.SolrIndexEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.MosfetSyncHelper;
import prerna.util.Utility;

public class UpdateInsightReactor extends AbstractInsightReactor {

	private static final Logger LOGGER = Logger.getLogger(UpdateInsightReactor.class.getName());

	public UpdateInsightReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.INSIGHT_NAME.getKey(), ReactorKeysEnum.RECIPE.getKey(), ReactorKeysEnum.IMAGE_URL.getKey(), ReactorKeysEnum.ID.getKey(), ReactorKeysEnum.LAYOUT_KEY.getKey(), IMAGE};
	}

	@Override
	public NounMetadata execute() {
		String engineName = getApp();
		String insightName = getInsightName();
		String[] recipeToSave = getRecipe();
		// used for embed url
		String rdbmsId = getRdbmsId();
		if(rdbmsId == null) {
			throw new IllegalArgumentException("Need to define the rdbmsId for the insight we are updating");
		}

		// for testing... should always be passed in
		if (recipeToSave == null || recipeToSave.length == 0) {
			recipeToSave = this.insight.getPixelRecipe().toArray(new String[] {});
		} else {
			// this is always encoded before it gets here
			recipeToSave = decodeRecipe(recipeToSave);
		}

		// TODO: there can be more than 1 layout given clone...
		String layout = getLayout();
		if (layout == null) {
			layout = "grid";
		}

		IEngine engine = Utility.getEngine(engineName);
		if(engine == null) {
			throw new IllegalArgumentException("Cannot find engine = " + engineName);
		}
		// add the recipe to the insights database
		InsightAdministrator admin = new InsightAdministrator(engine.getInsightDatabase());

		// update insight db
		LOGGER.info("1) Updating insight in rdbms");
		admin.updateInsight(rdbmsId, insightName, layout, recipeToSave);
		LOGGER.info("1) Done");
		// update solr
		LOGGER.info("2) Update insight in solr");
		editExistingInsightInSolr(engineName, rdbmsId, insightName, layout, "", new ArrayList<String>(), "");
		LOGGER.info("2) Done");
		//update recipe text file
		LOGGER.info("3) Update "+ MosfetSyncHelper.RECIPE_FILE);
		updateRecipeFile(engineName, rdbmsId, insightName, layout, IMAGE_NAME, recipeToSave);
		LOGGER.info("3) Done");
		String base64Image = getImage();
		if(base64Image != null && !base64Image.trim().isEmpty()) {
			updateSolrImageFromPng(base64Image, rdbmsId, engineName);
		}

		Map<String, Object> returnMap = new HashMap<String, Object>();
		returnMap.put("name", insightName);
		returnMap.put("core_engine_id", rdbmsId);
		returnMap.put("core_engine", engineName);
		NounMetadata noun = new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.SAVE_INSIGHT);
		return noun;
	}

	/**
	 * Edit an existing insight saved within solr
	 * 
	 * @param engineName
	 * @param existingRdbmsId
	 * @param insightName
	 * @param layout
	 * @param description
	 * @param tags
	 * @param userId
	 * @param imageURL
	 */
	private void editExistingInsightInSolr(String engineName, String existingRdbmsId, String insightName, String layout,
			String description, List<String> tags, String userId) {
		Map<String, Object> solrModifyInsights = new HashMap<>();
		DateFormat dateFormat = SolrIndexEngine.getDateFormat();
		Date date = new Date();
		String currDate = dateFormat.format(date);
		solrModifyInsights.put(SolrIndexEngine.STORAGE_NAME, insightName);
		solrModifyInsights.put(SolrIndexEngine.TAGS, tags);
		solrModifyInsights.put(SolrIndexEngine.DESCRIPTION, description);
		solrModifyInsights.put(SolrIndexEngine.LAYOUT, layout);
		solrModifyInsights.put(SolrIndexEngine.MODIFIED_ON, currDate);
		solrModifyInsights.put(SolrIndexEngine.LAST_VIEWED_ON, currDate);
		solrModifyInsights.put(SolrIndexEngine.CORE_ENGINE, engineName);
		solrModifyInsights.put(SolrIndexEngine.ENGINES, new HashSet<String>().add(engineName));

		try {
			SolrIndexEngine.getInstance().modifyInsight(engineName + "_" + existingRdbmsId, solrModifyInsights);
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException
				| IOException e1) {
			e1.printStackTrace();
		}
	}

	/**
	 * Update recipe: delete the old file and save as new
	 * 
	 * @param engineName
	 * @param rdbmsID
	 * @param recipeToSave
	 */
	protected void updateRecipeFile(String engineName, String rdbmsID, String insightName, String layout, String imageName, String[] recipeToSave) {
		String recipeLocation = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) 
				+ "\\" + Constants.DB + "\\" + engineName + "\\version\\" + rdbmsID + "\\" + MosfetSyncHelper.RECIPE_FILE;
		MosfetSyncHelper.updateMosfitFile(new File(recipeLocation), engineName, rdbmsID, insightName, layout, imageName, recipeToSave);
	}

	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(IMAGE)) {
			return "The base64 image string - used if the the image URL is null";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}

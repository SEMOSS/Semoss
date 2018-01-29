package prerna.sablecc2.reactor.insights;

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
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.solr.SolrIndexEngine;
import prerna.util.MosfetSyncHelper;
import prerna.util.Utility;
import prerna.util.ga.GATracker;

public class SaveInsightReactor extends AbstractInsightReactor {

	private static final String CLASS_NAME = SaveInsightReactor.class.getName();
	
	public SaveInsightReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.INSIGHT_NAME.getKey(), ReactorKeysEnum.RECIPE.getKey(), ReactorKeysEnum.IMAGE_URL.getKey(), ReactorKeysEnum.LAYOUT_KEY.getKey(), IMAGE};
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = this.getLogger(CLASS_NAME);
		// get the recipe for the insight
		// need the engine name and id that has the recipe
		String engineName = getEngine();
		String insightName = getInsightName();
		String[] recipeToSave = getRecipe();
		// used for embed url
		String imageURL = getImageURL();
		
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

		logger.info("1) Add insight " + insightName + " to rdbms store...");
		String newRdbmsId = admin.addInsight(insightName, layout, recipeToSave);
		logger.info("1) Done...");

		logger.info("2) Add insight to solr...");
		addNewInsightToSolr(engineName, newRdbmsId, insightName, layout, "", new ArrayList<String>(), "");
		logger.info("2) Done...");
		
		//write recipe to file
		logger.info("3) Add recipe to file...");
		MosfetSyncHelper.makeMosfitFile(engineName, newRdbmsId, insightName, layout, recipeToSave);
		logger.info("3) Done...");
		
		// capture image from image url
		if (imageURL != null) {
			// fill in image url
			// http://SemossWebBaseURL/#!/insight?type=single&engine=<engine>&id=<id>&panel=0
			imageURL = imageURL.replace("<engine>", engineName);
			imageURL = imageURL.replace("<id>", newRdbmsId);
			logger.info("4) Generate insight image...");
			// we can't save these layouts so ignore image
			if (layout.toUpperCase().contains("GRID") || layout.toUpperCase().contains("VIVAGRAPH") || layout.toUpperCase().equals("MAP")) {
				logger.info("4) Invalid... insight contains a layout that we cannot save an image for!!!");
			} else {
				logger.info("4) Generate new thread to save image...");
				updateSolrImageByRecreatingInsight(newRdbmsId, newRdbmsId, imageURL, engineName);
			}
		} 
		// get base 64 image string and write to file
		else {
			String base64Image = getImage();
			if(base64Image != null && !base64Image.trim().isEmpty()) {
				updateSolrImageFromPng(base64Image, newRdbmsId, engineName);
			}
		}
		Map<String, Object> returnMap = new HashMap<String, Object>();
		returnMap.put("name", insightName);
		returnMap.put("core_engine_id", newRdbmsId);
		returnMap.put("core_engine", engineName);
		NounMetadata noun = new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.SAVE_INSIGHT);

		// track GA data
		GATracker.getInstance().trackInsightExecution(this.insight, "saveinsight", engineName, newRdbmsId, insightName);

		return noun;
	}

	/**
	 * Add an insight into solr
	 * @param engineName
	 * @param insightIdToSave
	 * @param insightName
	 * @param layout
	 * @param description
	 * @param tags
	 * @param userId
	 * @param imageURL
	 */
	private void addNewInsightToSolr(String engineName, String insightIdToSave, String insightName, String layout,
			String description, List<String> tags, String userId) {
		Map<String, Object> solrInsights = new HashMap<>();
		DateFormat dateFormat = SolrIndexEngine.getDateFormat();
		Date date = new Date();
		String currDate = dateFormat.format(date);
		solrInsights.put(SolrIndexEngine.STORAGE_NAME, insightName);
		solrInsights.put(SolrIndexEngine.TAGS, tags);
		solrInsights.put(SolrIndexEngine.LAYOUT, layout);
		solrInsights.put(SolrIndexEngine.CREATED_ON, currDate);
		solrInsights.put(SolrIndexEngine.MODIFIED_ON, currDate);
		solrInsights.put(SolrIndexEngine.LAST_VIEWED_ON, currDate);
		solrInsights.put(SolrIndexEngine.DESCRIPTION, description);
		solrInsights.put(SolrIndexEngine.CORE_ENGINE_ID, insightIdToSave);
		solrInsights.put(SolrIndexEngine.USER_ID, userId);
		solrInsights.put(SolrIndexEngine.VIEW_COUNT, 0);

		// TODO: figure out which engines are used within this insight
		solrInsights.put(SolrIndexEngine.CORE_ENGINE, engineName);
		solrInsights.put(SolrIndexEngine.ENGINES, new HashSet<String>().add(engineName));

		// the image will be updated in a later thread
		// for now, just send in an empty string
		// save image url to recreate image later
		solrInsights.put(SolrIndexEngine.IMAGE, "");

		try {
			SolrIndexEngine.getInstance().addInsight(engineName + "_" + insightIdToSave, solrInsights);
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException
				| IOException e1) {
			e1.printStackTrace();
		}
	}
	
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(IMAGE)) {
			return "The base64 image string - used if there is no image URL";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}

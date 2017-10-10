package prerna.sablecc2.reactor.insights;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
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

import cern.colt.Arrays;
import prerna.engine.api.IEngine;
import prerna.engine.impl.InsightAdministrator;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.solr.SolrIndexEngine;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.insight.InsightScreenshot;

public class SaveInsightReactor extends AbstractInsightReactor {

	private static final Logger LOGGER = Logger.getLogger(SaveInsightReactor.class.getName());

	@Override
	public NounMetadata execute() {
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

		IEngine coreEngine = Utility.getEngine(engineName);
		// add the recipe to the insights database
		InsightAdministrator admin = new InsightAdministrator(coreEngine.getInsightDatabase());

		LOGGER.info("1) Add insight to rdbms");
		String newRdbmsId = admin.addInsight(insightName, layout, recipeToSave);
		LOGGER.info("1) Done");

		// fill in image url
		// http://SemossWebBaseURL/#!/insight?type=single&engine=<engine>&id=<id>&panel=0
		imageURL = imageURL.replace("<engine>", engineName);
		imageURL = imageURL.replace("<id>", newRdbmsId);

		LOGGER.info("2) Add insight to solr");
		addNewInsightToSolr(engineName, newRdbmsId, insightName, layout, "", new ArrayList<String>(), "", imageURL);
		LOGGER.info("2) Done");

		// we can't save these layouts so ignore image
		if (layout.toUpperCase().contains("GRID") || layout.toUpperCase().contains("VIVAGRAPH")
				|| layout.toUpperCase().equals("MAP")) {
			LOGGER.error("Insight contains a layout that we cannot save an image for!!!");
		} else {
			updateSolrImage(newRdbmsId, newRdbmsId, imageURL, engineName);
		}

		Map<String, Object> returnMap = new HashMap<String, Object>();
		returnMap.put("name", insightName);
		returnMap.put("core_engine_id", newRdbmsId);
		returnMap.put("core_engine", engineName);
		NounMetadata noun = new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.SAVE_INSIGHT);
		return noun;
	}

	private String[] decodeRecipe(String[] recipe) {
		int size = recipe.length;
		String[] decodedRecipe = new String[size];
		for (int i = 0; i < size; i++) {
			try {
				decodedRecipe[i] = URLDecoder.decode(recipe[i], "UTF-8");
			} catch (UnsupportedEncodingException e) {
				// you are kinda screwed at this point...
				e.printStackTrace();
			}
		}
		return decodedRecipe;
	}

	/**
	 * Add an insight into solr
	 * 
	 * @param insightIdToSave
	 * @param insightName
	 * @param layout
	 * @param description
	 * @param tags
	 * @param userId
	 */
	private void addNewInsightToSolr(String engineName, String insightIdToSave, String insightName, String layout,
			String description, List<String> tags, String userId, String imageURL) {
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
		solrInsights.put(SolrIndexEngine.CORE_ENGINE_ID, Integer.parseInt(insightIdToSave));
		solrInsights.put(SolrIndexEngine.USER_ID, userId);

		// TODO: figure out which engines are used within this insight
		solrInsights.put(SolrIndexEngine.CORE_ENGINE, engineName);
		solrInsights.put(SolrIndexEngine.ENGINES, new HashSet<String>().add(engineName));

		// the image will be updated in a later thread
		// for now, just send in an empty string
		// save image url to recreate image later
		solrInsights.put(SolrIndexEngine.IMAGE, "");
		solrInsights.put(SolrIndexEngine.IMAGE_URL, imageURL);

		try {
			SolrIndexEngine.getInstance().addInsight(engineName + "_" + insightIdToSave, solrInsights);
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException
				| IOException e1) {
			e1.printStackTrace();
		}
	}

	/**
	 *
	 * @param solrInsightIDToUpdate
	 *            id used to update solr image string
	 * @param imageInsightIDToView
	 *            id used for embed url link to load in image capture browser
	 * @param baseURL
	 * @param engineName
	 */
	private void updateSolrImage(String solrInsightIDToUpdate, String imageInsightIDToView, String baseURL,
			String engineName) {
		String baseImagePath = DIHelper.getInstance().getProperty("BaseFolder");

		// solr id to update
		final String finalID = solrInsightIDToUpdate;
		// id used for embed link
		final String idForURL = imageInsightIDToView;
		final String imagePath = baseImagePath + "\\images\\" + engineName + "_" + finalID + ".png";

		// not supported by the embedded browser
		Runnable r = new Runnable() {
			public void run() {
				// generate image URL from saved insight
				String url = baseURL;

				// Set up the embedded browser
				InsightScreenshot screenshot = new InsightScreenshot();
				screenshot.showUrl(url, imagePath);

				// wait for screenshot
				long startTime = System.currentTimeMillis();
				try {
					screenshot.getComplete();
					long endTime = System.currentTimeMillis();
					LOGGER.info("IMAGE CAPTURE TIME " + (endTime - startTime) + " ms");

					// Update solr
					Map<String, Object> solrInsights = new HashMap<>();
					solrInsights.put(SolrIndexEngine.IMAGE, "\\images\\" + engineName + "_" + finalID + ".png");
					try {
						SolrIndexEngine.getInstance().modifyInsight(engineName + "_" + finalID, solrInsights);
						LOGGER.info("Updated solr id: " + finalID + " image");
						// clean up temp param insight data
						if (!finalID.equals(idForURL)) {
							// remove Solr data for temporary param
							List<String> insightsToRemove = new ArrayList<String>();
							insightsToRemove.add(engineName + "_" + idForURL);
							LOGGER.info("REMOVING TEMP SOLR INSTANCE FOR PARAM INSIGHT "
									+ Arrays.toString(insightsToRemove.toArray()));
							SolrIndexEngine.getInstance().removeInsight(insightsToRemove);
							IEngine engine = Utility.getEngine(engineName);
							InsightAdministrator admin = new InsightAdministrator(engine.getInsightDatabase());
							admin.dropInsight(idForURL);
						}
					} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException
							| IOException e) {
						e.printStackTrace();
					}
					// Catch browser image exception
				} catch (Exception e1) {
					LOGGER.error("Unable to capture image from " + url);
				}
			}
		};
		// use this for new image capture
		Thread t = new Thread(r);
		t.setPriority(Thread.MAX_PRIORITY);
		t.start();
	}

}

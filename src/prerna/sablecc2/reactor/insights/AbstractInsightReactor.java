package prerna.sablecc2.reactor.insights;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;

import cern.colt.Arrays;
import prerna.engine.api.IEngine;
import prerna.engine.impl.InsightAdministrator;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.solr.SolrIndexEngine;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.insight.InsightScreenshot;

public abstract class AbstractInsightReactor extends AbstractReactor {
	private static final Logger LOGGER = LogManager.getLogger(AbstractInsightReactor.class.getName());

	// used for running insights
	protected static final String ENGINE_KEY = "engine";
	protected static final String RDBMS_ID = "id";
	protected static final String PARAM_KEY = "params";

	// used for saving a base insight
	protected static final String INSIGHT_NAME = "insightName";
	protected static final String RECIPE = "recipe";
	protected static final String LAYOUT = "layout";
	protected static final String IMAGE_URL = "image";

	protected String getEngine() {
		// look at all the ways the insight panel could be passed
		// look at store if it was passed in
		GenRowStruct genericEngineGrs = this.store.getNoun(ENGINE_KEY);
		if(genericEngineGrs != null && !genericEngineGrs.isEmpty()) {
			return (String) genericEngineGrs.get(0);
		}
		
		// see if it is in the curRow
		// if it was passed directly in as a variable
		List<NounMetadata> stringNouns = this.curRow.getNounsOfType(PixelDataType.CONST_STRING);
		if(stringNouns != null && !stringNouns.isEmpty()) {
			return (String) stringNouns.get(0).getValue();
		}
		
		// well, you are out of luck
		return null;
	}
	
	/**
	 * This can either be passed specifically using the insightName key
	 * Or it is the second input in a list of values
	 * Save(engineName, insightName)
	 * @return
	 */
	protected String getInsightName() {
		// look at all the ways the insight panel could be passed
		// look at store if it was passed in
		GenRowStruct genericEngineGrs = this.store.getNoun(INSIGHT_NAME);
		if(genericEngineGrs != null && !genericEngineGrs.isEmpty()) {
			return (String) genericEngineGrs.get(0);
		}
		
		// see if it is in the curRow
		// if it was passed directly in as a variable
		// this will be the second input! (i.e. index 1)
		List<NounMetadata> stringNouns = this.curRow.getNounsOfType(PixelDataType.CONST_STRING);
		if(stringNouns != null && !stringNouns.isEmpty()) {
			return (String) stringNouns.get(1).getValue();
		}
		
		// well, you are out of luck
		return null;
	}
	
	protected int getRdbmsId() {
		// see if it was passed directly in with the lower case key ornaments
		GenRowStruct genericIdGrs = this.store.getNoun(RDBMS_ID);
		if(genericIdGrs != null && !genericIdGrs.isEmpty()) {
			return (int) genericIdGrs.get(0);
		}
		
		// see if it is in the curRow
		// if it was passed directly in as a variable
		List<NounMetadata> intNouns = this.curRow.getNounsOfType(PixelDataType.CONST_INT);
		if(intNouns != null && !intNouns.isEmpty()) {
			return (int) intNouns.get(0).getValue();
		}
		
		// well, you are out of luck
		return -1;
	}
	
	protected String[] getRecipe() {
		// it must be passed directly into its own grs
		GenRowStruct genericRecipeGrs = this.store.getNoun(RECIPE);
		if(genericRecipeGrs != null && !genericRecipeGrs.isEmpty()) {
			int size = genericRecipeGrs.size();
			String[] recipe = new String[size];
			for(int i = 0; i < size; i++) {
				recipe[i] = genericRecipeGrs.get(i).toString();
			}
			return recipe;
		}
		
		// well, you are out of luck
		return new String[0];
	}
	
	protected String getLayout() {
		// it must be passed directly into its own grs
		GenRowStruct genericLayoutGrs = this.store.getNoun(LAYOUT);
		if(genericLayoutGrs != null && !genericLayoutGrs.isEmpty()) {
			return genericLayoutGrs.get(0).toString();
		}
		
		// well, you are out of luck
		return null;
	}
	
	protected String getImageURL() {
		GenRowStruct genericBaseURLGrs = this.store.getNoun(IMAGE_URL);
		if(genericBaseURLGrs != null && !genericBaseURLGrs.isEmpty()) {
			try {
				String imageURL = URLDecoder.decode(genericBaseURLGrs.get(0).toString(), "UTF-8");
				return imageURL;
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		// well, you are out of luck
		return null;
	}
	
	protected Object getParams() {
		GenRowStruct paramGrs = this.store.getNoun(PARAM_KEY);
		if(paramGrs == null || paramGrs.isEmpty()) {
			return null;
		}
		
		if(paramGrs.size() == 1) {
			return paramGrs.get(0);
		} else {
			List<Object> params = new ArrayList<Object>();
			for(int i = 0; i < paramGrs.size(); i++) {
				params.add(paramGrs.get(i));
			}
			return params;
		}
	}
	
	/**
	 *	TODO remove param hack solr insightIDToView
	 *
	 * @param solrInsightIDToUpdate
	 *            id used to update solr image string
	 * @param imageInsightIDToView
	 *            id used for embed url link to load in image capture browser
	 * @param baseURL
	 * @param engineName
	 */
	protected void updateSolrImage(String solrInsightIDToUpdate, String imageInsightIDToView, String baseURL,
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

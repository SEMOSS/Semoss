package prerna.sablecc2.reactor.insights;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import cern.colt.Arrays;
import prerna.engine.api.IEngine;
import prerna.engine.impl.InsightAdministrator;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.solr.SolrIndexEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.insight.InsightScreenshot;

public abstract class AbstractInsightReactor extends AbstractReactor {
	private static final Logger LOGGER = LogManager.getLogger(AbstractInsightReactor.class.getName());

	

	// used for saving a base insight
	protected static final String IMAGE = "image";
	protected static final String RECIPE_FILE = ".mosfet";

	protected String getEngine() {
		// look at all the ways the insight panel could be passed
		// look at store if it was passed in
		GenRowStruct genericEngineGrs = this.store.getNoun(ReactorKeysEnum.ENGINE.getKey());
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
		GenRowStruct genericEngineGrs = this.store.getNoun(ReactorKeysEnum.INSIGHT_NAME.getKey());
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
	
	protected String getRdbmsId() {
		// see if it was passed directly in with the lower case key ornaments
		GenRowStruct genericIdGrs = this.store.getNoun(ReactorKeysEnum.INSIGHT_ID.getKey());
		if(genericIdGrs != null && !genericIdGrs.isEmpty()) {
			return genericIdGrs.get(0).toString();
		}
		
		// well, you are out of luck
		return null;
	}
	
	protected String[] getRecipe() {
		// it must be passed directly into its own grs
		GenRowStruct genericRecipeGrs = this.store.getNoun(ReactorKeysEnum.RECIPE.getKey());
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
		GenRowStruct genericLayoutGrs = this.store.getNoun(ReactorKeysEnum.LAYOUT_KEY.getKey());
		if(genericLayoutGrs != null && !genericLayoutGrs.isEmpty()) {
			return genericLayoutGrs.get(0).toString();
		}
		
		// well, you are out of luck
		return null;
	}
	
	protected String getImageURL() {
		GenRowStruct genericBaseURLGrs = this.store.getNoun(ReactorKeysEnum.IMAGE_URL.getKey());
		if(genericBaseURLGrs != null && !genericBaseURLGrs.isEmpty()) {
			try {
				String imageURL = URLDecoder.decode(genericBaseURLGrs.get(0).toString(), "UTF-8");
				return imageURL;
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
		
		// well, you are out of luck
		return null;
	}
	
	/**
	 * 
	 * @return base64 image string
	 */
	protected String getImage() {
		GenRowStruct genericBaseURLGrs = this.store.getNoun(IMAGE);
		if (genericBaseURLGrs != null && !genericBaseURLGrs.isEmpty()) {
			String image = genericBaseURLGrs.get(0).toString();
			return image;
		}

		// well, you are out of luck
		return null;
	}
	
	protected Object getParams() {
		GenRowStruct paramGrs = this.store.getNoun(ReactorKeysEnum.PARAM_KEY.getKey());
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
	
	protected String[] decodeRecipe(String[] recipe) {
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
	 * Save insight recipe to db/engineName/insightName/recipe.json
	 *  json includes 
	 *  	engine: engineName
	 *  	rdbmsID: rdbmsID
	 *  	recipe: pixel;pixel;...
	 * 
	 * @param engineName
	 * @param rdbmsID
	 * @param recipeToSave
	 */
	protected void saveRecipeToFile(String engineName, String rdbmsID, String insightName, String layout, String[] recipeToSave) {
		String recipePath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		recipePath += "\\" + Constants.DB + "\\" + engineName + "\\version\\" + rdbmsID;
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		// format recipe file
		HashMap<String, Object> output = new HashMap<String, Object>();
		output.put("engine", engineName);
		output.put("rdbmsId", rdbmsID);
		output.put("insightName", insightName);
		output.put("layout", layout);
		StringBuilder recipe = new StringBuilder();
		for (String pixel : recipeToSave) {
			recipe.append(pixel).append("\n");
		}
		output.put("recipe", recipe.toString());

		String json = gson.toJson(output);
		File path = new File(recipePath);
		// create insight directory
		if (path.mkdirs()) {
			recipePath += "\\" + RECIPE_FILE;
			// create file
			File f = new File(recipePath);
			StringBuilder sb = new StringBuilder();
			sb.append(json);
			try {
				// write json to file
				FileUtils.writeStringToFile(f, sb.toString());
			} catch (IOException e1) {
				LOGGER.error("Error in writing recipe file to path " + recipePath);
				e1.printStackTrace();
			}
		} else {
			LOGGER.error("Error in writing recipe file to path " + recipePath);
		}
	}
	
	/**
	 * Capture insight image from the embeded url link
	 * Save image to file Semoss/images/engineName_insightID.png
	 * 
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
					solrInsights.put(SolrIndexEngine.IMAGE_URL, url);
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
	
	/**
	 * Save base64 encoded image to file
	 * Semoss/images/engineName_insightID.png
	 * @param base64Image
	 * @param insightId
	 * @param engineName
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	protected void updateSolrImage(String base64Image, String insightId, String engineName) {
		// set up path to save image to file
		String baseImagePath = DIHelper.getInstance().getProperty("BaseFolder");
		String imagePath = baseImagePath + "\\images\\" + engineName + "_" + insightId + ".png";
		
		// decode image and write to file
		byte[] data = Base64.decodeBase64(base64Image);
		try (OutputStream stream = new FileOutputStream(imagePath)) {
			stream.write(data);
			// update solr with image path to file
			Map<String, Object> solrInsights = new HashMap<>();
			solrInsights.put(SolrIndexEngine.IMAGE, "\\images\\" + engineName + "_" + insightId + ".png");
			SolrIndexEngine.getInstance().modifyInsight(engineName + "_" + insightId, solrInsights);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (KeyManagementException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (KeyStoreException e) {
			e.printStackTrace();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}
	}

}

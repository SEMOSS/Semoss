package prerna.sablecc2.reactor.insights;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;

public abstract class AbstractInsightReactor extends AbstractReactor {

	private static final Logger LOGGER = LogManager.getLogger(AbstractInsightReactor.class.getName());

	// used for saving a base insight
	protected static final String IMAGE = "image";
	protected static final String IMAGE_NAME = "image.png";
	
	protected String getApp() {
		// look at all the ways the insight panel could be passed
		// look at store if it was passed in
		GenRowStruct genericEngineGrs = this.store.getNoun(ReactorKeysEnum.APP.getKey());
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
		GenRowStruct genericIdGrs = this.store.getNoun(ReactorKeysEnum.ID.getKey());
		if(genericIdGrs != null && !genericIdGrs.isEmpty()) {
			return genericIdGrs.get(0).toString();
		}
		
		// well, you are out of luck
		return null;
	}
	
	protected String getUrl() {
		// see if it was passed directly in with the lower case key ornaments
		GenRowStruct genericIdGrs = this.store.getNoun(ReactorKeysEnum.URL.getKey());
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
	
//	/**
//	 * Capture insight image from the embeded url link
//	 * Save image to file Semoss/images/engineName_insightID.png
//	 * 
//	 *	TODO remove param hack solr insightIDToView
//	 *
//	 * @param solrInsightIDToUpdate
//	 *            id used to update solr image string
//	 * @param imageInsightIDToView
//	 *            id used for embed url link to load in image capture browser
//	 * @param baseURL
//	 * @param engineName
//	 */
//	protected void updateSolrImageByRecreatingInsight(String solrInsightIDToUpdate, String imageInsightIDToView, String baseURL, String engineName) {
//
//		// solr id to update
//		final String finalID = solrInsightIDToUpdate;
//		// id used for embed link
//		final String idForURL = imageInsightIDToView;
//		final String imagePath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) 
//				+ "\\db\\" + engineName + "\\version\\" + finalID
//				+ "\\" + IMAGE_NAME;
//		
//		// not supported by the embedded browser
//		Runnable r = new Runnable() {
//			public void run() {
//				// Set up the embedded browser
//				InsightScreenshot screenshot = new InsightScreenshot();
//				screenshot.showUrl(baseURL, imagePath);
//
//				// wait for screenshot
//				long startTime = System.currentTimeMillis();
//				try {
//					screenshot.getComplete();
//					long endTime = System.currentTimeMillis();
//					LOGGER.info("IMAGE CAPTURE TIME " + (endTime - startTime) + " ms");
//
//					// Update solr
//					Map<String, Object> solrInsights = new HashMap<>();
//					solrInsights.put(SolrIndexEngine.IMAGE, "\\db\\" + engineName + "\\version\\" + finalID + "\\" + IMAGE_NAME);
//					solrInsights.put(SolrIndexEngine.IMAGE_URL, baseURL);
//					try {
//						SolrIndexEngine.getInstance().modifyInsight(engineName + "_" + finalID, solrInsights);
//						LOGGER.info("Updated solr id: " + finalID + " image");
//						// clean up temp param insight data
//						if (!finalID.equals(idForURL)) {
//							// remove Solr data for temporary param
//							List<String> insightsToRemove = new ArrayList<String>();
//							insightsToRemove.add(engineName + "_" + idForURL);
//							LOGGER.info("REMOVING TEMP SOLR INSTANCE FOR PARAM INSIGHT "
//									+ Arrays.toString(insightsToRemove.toArray()));
//							SolrIndexEngine.getInstance().removeInsight(insightsToRemove);
//							IEngine engine = Utility.getEngine(engineName);
//							InsightAdministrator admin = new InsightAdministrator(engine.getInsightDatabase());
//							admin.dropInsight(idForURL);
//						}
//					} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException
//							| IOException e) {
//						e.printStackTrace();
//					}
//					// Catch browser image exception
//				} catch (Exception e1) {
//					LOGGER.error("Unable to capture image from " + baseURL);
//				}
//			}
//		};
//		// use this for new image capture
//		Thread t = new Thread(r);
//		t.setPriority(Thread.MAX_PRIORITY);
//		t.start();
//	}
	
	/**
	 * Save base64 encoded image to file
	 * Semoss/images/engineName_insightID.png
	 * @param base64Image
	 * @param insightId
	 * @param engineName
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	protected void updateSolrImageFromPng(String base64Image, String insightId, String engineName) {
		// set up path to save image to file
		final String imagePath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) 
				+ "\\db\\" + engineName + "\\version\\" + insightId
				+ "\\image.png";
		
		// decode image and write to file
		byte[] data = Base64.decodeBase64(base64Image);
		try (OutputStream stream = new FileOutputStream(imagePath)) {
			stream.write(data);
//			// update solr with image path to file
//			Map<String, Object> solrInsights = new HashMap<>();
//			solrInsights.put(SolrIndexEngine.IMAGE, "\\db\\" + engineName + "\\version\\" + insightId + "\\" + IMAGE_NAME);
//			SolrIndexEngine.getInstance().modifyInsight(engineName + "_" + insightId, solrInsights);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
//		catch (KeyManagementException e) {
//			e.printStackTrace();
//		} catch (NoSuchAlgorithmException e) {
//			e.printStackTrace();
//		} catch (KeyStoreException e) {
//			e.printStackTrace();
//		} catch (SolrServerException e) {
//			e.printStackTrace();
//		}
	}

}

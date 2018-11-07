package prerna.sablecc2.reactor.insights;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.impl.SmssUtilities;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public abstract class AbstractInsightReactor extends AbstractReactor {

	// used for saving a base insight
	protected static final String IMAGE_NAME = "image.png";
	protected static final String HIDDEN_KEY = "hidden";
	protected static final String CACHEABLE = "cache";

	protected String getApp() {
		String appId = null;
		// look at all the ways the insight panel could be passed
		// look at store if it was passed in
		GenRowStruct genericEngineGrs = this.store.getNoun(ReactorKeysEnum.APP.getKey());
		if(genericEngineGrs != null && !genericEngineGrs.isEmpty()) {
			appId = (String) genericEngineGrs.get(0);
		}
		
		if(appId == null) {
			// see if it is in the curRow
			// if it was passed directly in as a variable
			List<NounMetadata> stringNouns = this.curRow.getNounsOfType(PixelDataType.CONST_STRING);
			if(stringNouns != null && !stringNouns.isEmpty()) {
				return (String) stringNouns.get(0).getValue();
			}
		}
		
		if(appId == null) {
			// well, you are out of luck
			throw new IllegalArgumentException("Need to define the app where the insight currently exists");
		}

		if(AbstractSecurityUtils.securityEnabled()) {
			appId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), appId);
		} else {
			appId = MasterDatabaseUtility.testEngineIdIfAlias(appId);
		}
		
		return appId;
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
		throw new IllegalArgumentException("Need to define the app where the insight currently exists");
	}
	
	protected boolean getHidden() {
		// see if it was passed directly in with the lower case key ornaments
		GenRowStruct genericIdGrs = this.store.getNoun(HIDDEN_KEY);
		if(genericIdGrs != null && !genericIdGrs.isEmpty()) {
			return (boolean) genericIdGrs.get(0);
		}
		
		// well, you are out of luck
		return false;
	}
	
	protected Boolean getUserDefinedCacheable() {
		// see if it was passed directly in with the lower case key ornaments
		GenRowStruct genericIdGrs = this.store.getNoun(CACHEABLE);
		if(genericIdGrs != null && !genericIdGrs.isEmpty()) {
			return (boolean) genericIdGrs.get(0);
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
		
		// TODO: there can be more than 1 layout given clone...
		return "grid";
	}
	
	/**
	 * 
	 * @return base64 image string
	 */
	protected String getImage() {
		GenRowStruct genericBaseURLGrs = this.store.getNoun(ReactorKeysEnum.IMAGE.getKey());
		if (genericBaseURLGrs != null && !genericBaseURLGrs.isEmpty()) {
			String image = genericBaseURLGrs.get(0).toString();
			return image;
		}

		// well, you are out of luck
		return null;
	}
	
	/**
	 * Get params needed for execution
	 * @return
	 */
	protected Object getExecutionParams() {
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
	
	protected List<String> getParams() {
		GenRowStruct paramGrs = this.store.getNoun(ReactorKeysEnum.PARAM_KEY.getKey());
		if(paramGrs == null || paramGrs.isEmpty()) {
			return null;
		}
		
		List<String> params = new ArrayList<String>();
		for(int i = 0; i < paramGrs.size(); i++) {
			params.add(paramGrs.get(i).toString());
		}
		return params;
	}
	
	protected String[] decodeRecipe(String[] recipe) {
		int size = recipe.length;
		String[] decodedRecipe = new String[size];
		for (int i = 0; i < size; i++) {
			decodedRecipe[i] = Utility.decodeURIComponent(recipe[i]);
		}
		return decodedRecipe;
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
	protected void storeImageFromPng(String base64Image, String insightId, String appId, String appName) {
		// set up path to save image to file
		final String DIR_SEP = java.nio.file.FileSystems.getDefault().getSeparator();
		final String imagePath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) 
				+ DIR_SEP + "db" + DIR_SEP + SmssUtilities.getUniqueName(appName, appId) + DIR_SEP + "version" + DIR_SEP + insightId + DIR_SEP + "image.png";
		
		// decode image and write to file
		byte[] data = Base64.decodeBase64(base64Image);
		try (OutputStream stream = new FileOutputStream(imagePath)) {
			stream.write(data);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Update the recipe to save the files in the insight location
	 * @param recipeToSave
	 * @param appId
	 * @param newInsightId
	 * @return
	 */
	protected String[] saveFilesInInsight(String[] recipeToSave, String appId, String newInsightId) {
		final String BASE = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
		
		String appName = SecurityQueryUtils.getEngineAliasForId(appId);
		
		// store modifications to be made
		List<Map<String, Object>> modificationList = new Vector<Map<String, Object>>();
		// need to go through and find the files so we can save them in the right location
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i< recipeToSave.length; i++) {
			sb.append(recipeToSave[i]);
		}
		String fullRecipe = sb.toString();
		// shift any csv files to be moved into the insight folder for the new insight
		List<Map<String, Object>> datasources = PixelUtility.getDatasourcesMetadata(this.insight.getUser(), fullRecipe);
		for(int i = 0; i < datasources.size(); i++) {
			Map<String, Object> datasourceMap = datasources.get(i);
			String datasourceType = datasourceMap.get("type").toString().toUpperCase();
			if(datasourceType.equals("FILEREAD")) {
				// we have a file we want to shift
				String fileLoc = ((Map<String, List<String>>) datasourceMap.get("params")).get("filePath").get(0);
				String filename = FilenameUtils.getName(fileLoc);
				File origF = new File(fileLoc);
				String newFileLoc = BASE + DIR_SEPARATOR + "db" + DIR_SEPARATOR + 
										SmssUtilities.getUniqueName(appName, appId) + DIR_SEPARATOR + 
										"version" + DIR_SEPARATOR +
										newInsightId + DIR_SEPARATOR + 
										"data";
				// create parent directory
				File newF = new File(newFileLoc);
				newF.mkdirs();
				newF = new File(newFileLoc + DIR_SEPARATOR + filename);		
				// copy file over
				if(!origF.getAbsolutePath().equals(newF.getAbsolutePath())) {
					try {
						FileUtils.copyFile(origF, newF);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				
				// need to make new pixel
				String newPixel = datasourceMap.get("expression").toString().replace(fileLoc, newF.getAbsolutePath());
				Map<String, Object> modificationMap = new HashMap<String, Object>();
				modificationMap.put("index", datasourceMap.get("pixelStepIndex"));
				modificationMap.put("pixel", newPixel);
				modificationList.add(modificationMap);
			}
		}
		
		// hey, we found something to change
		if(!modificationList.isEmpty()) {
			recipeToSave = PixelUtility.modifyInsightDatasource(this.insight.getUser(), fullRecipe, modificationList).toArray(new String[]{});
		}
		
		return recipeToSave;
	}
	
	protected String[] getParamRecipe(String[] recipe, List<String> params, String insightName) {
		String paramRecipe = PixelUtility.getParameterizedRecipe(this.insight.getUser(), recipe, params, insightName);
		return new String[]{paramRecipe};
	}
	
}

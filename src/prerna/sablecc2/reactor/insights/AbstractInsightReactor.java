package prerna.sablecc2.reactor.insights;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.impl.SmssUtilities;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.om.Insight;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.insight.InsightUtility;

public abstract class AbstractInsightReactor extends AbstractReactor {

	// used for saving a base insight
	protected static final String IMAGE_NAME = "image.png";
	protected static final String HIDDEN_KEY = "hidden";
	protected static final String CACHEABLE = "cache";
	protected static final String PIPELINE_FILE = "pipeline.json";
	
	public static String USER_SPACE_KEY = "USER";
	public static final String SPACE = ReactorKeysEnum.SPACE.getKey();
	
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
	
	protected List<String> getRecipe() {
		// it must be passed directly into its own grs
		GenRowStruct genericRecipeGrs = this.store.getNoun(ReactorKeysEnum.RECIPE.getKey());
		if(genericRecipeGrs != null && !genericRecipeGrs.isEmpty()) {
			int size = genericRecipeGrs.size();
			List<String> recipe = new Vector<>(size);
			for(int i = 0; i < size; i++) {
				recipe.add(genericRecipeGrs.get(i).toString());
			}
			return recipe;
		}
		
		// well, you are out of luck
		return null;
	}
	
	protected Map getPipeline() {
		GenRowStruct pipelineGrs = this.store.getNoun(ReactorKeysEnum.PIPELINE.getKey());
		if(pipelineGrs != null && !pipelineGrs.isEmpty()) {
			Map pipeline = (Map) pipelineGrs.get(0);
			if(pipeline.isEmpty()) {
				return null;
			}
			return pipeline;
		}
		
		return null;
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
	
	protected List<Map<String, Object>> getParams() {
		GenRowStruct paramGrs = this.store.getNoun(ReactorKeysEnum.PARAM_KEY.getKey());
		if(paramGrs == null || paramGrs.isEmpty()) {
			return null;
		}
		
		List<Map<String, Object>> params = new ArrayList<>();
		for(int i = 0; i < paramGrs.size(); i++) {
			params.add( (Map<String, Object>) paramGrs.get(i));
		}
		return params;
	}
	
	protected List<String> decodeRecipe(List<String> recipe) {
		int size = recipe.size();
		List<String> decodedRecipe = new Vector<>(size);
		for (int i = 0; i < size; i++) {
			decodedRecipe.add(Utility.decodeURIComponent(recipe.get(i)));
		}
		return decodedRecipe;
	}
	
	/**
	 * Get the tags to set for the insight
	 * @return
	 */
	protected List<String> getTags() {
		List<String> tags = new Vector<String>();
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.TAGS.getKey());
		if(grs != null && !grs.isEmpty()) {
			for(int i = 0; i < grs.size(); i++) {
				tags.add(grs.get(i).toString());
			}
		}
		
		return tags;
	}
	
	/**
	 * Get the description for the insight
	 * Assume it is passed by the key or it is the last string passed into the curRow
	 * @return
	 */
	protected String getDescription() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.DESCRIPTION.getKey());
		if(grs != null && !grs.isEmpty()) {
			return grs.get(0).toString();
		}
		
		return null;
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
	protected List<String> saveFilesInInsight(List<String> recipeToSave, String appId, String newInsightId) {
		final String BASE = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
		
		String appName = SecurityQueryUtils.getEngineAliasForId(appId);
		
		// store modifications to be made
		List<Map<String, Object>> modificationList = new Vector<Map<String, Object>>();
		// need to go through and find the files so we can save them in the right location
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i< recipeToSave.size(); i++) {
			sb.append(recipeToSave.get(i));
		}
		String fullRecipe = sb.toString();
		// shift any csv files to be moved into the insight folder for the new insight
		List<Map<String, Object>> datasources = PixelUtility.getDatasourcesMetadata(this.insight.getUser(), fullRecipe);
		for(int i = 0; i < datasources.size(); i++) {
			Map<String, Object> datasourceMap = datasources.get(i);
			String datasourceType = datasourceMap.get("type").toString().toUpperCase();
			if(datasourceType.equals("FILEREAD")) {
				// we have a file we want to shift
				String filePixelPortion = ((Map<String, List<String>>) datasourceMap.get("params")).get("filePath").get(0);
				String space = null;
				if (((Map<String, List<String>>) datasourceMap.get("params")).containsKey("space")) {
					space = ((Map<String, List<String>>) datasourceMap.get("params")).get("space").get(0);
				}
				
				String fileLoc = null;
				String filePrefix = null;
				if(space != null) {
					filePrefix = AssetUtility.getAssetBasePath(this.insight, space, false);
				}
				
				// this is for legacy recipes
				if (filePrefix != null) {
					fileLoc = filePixelPortion.replace("\\", "/").replace("INSIGHT_FOLDER", "");
					if(fileLoc.startsWith("\\") || fileLoc.startsWith("/")) {
						fileLoc = filePrefix + fileLoc;
					} else {
						fileLoc = filePrefix + "/" + fileLoc;
					}
				} else {
					fileLoc = this.insight.getAbsoluteInsightFolderPath(filePixelPortion);
				}
				
				//TODO: make sure file name is unique once we move it to the insight folder
				String filename = FilenameUtils.getName(fileLoc);
				
				// we only want to shift if it is an insight file
				// or if it is a user file
				// if app - keep in current app folder
				boolean isUserSpace = space != null && space.toUpperCase().equals(AssetUtility.USER_SPACE_KEY);
				if(space == null || space.isEmpty() || isUserSpace) {
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
							if(!isUserSpace) {
								origF.delete();
							}
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
					
					String newFilePixel = "data" + DIR_SEPARATOR + filename;
					// need to make new pixel
					String newPixel = datasourceMap.get("expression").toString().replace(filePixelPortion, newFilePixel);
					// if user, need to replace the space to be an empty string
					if(isUserSpace) {
						newPixel = newPixel.replace("space = [ \"" + space + "\" ]", "space = [\"\"]");
					}
					Map<String, Object> modificationMap = new HashMap<String, Object>();
					modificationMap.put("index", datasourceMap.get("pixelStepIndex"));
					modificationMap.put("pixel", newPixel);
					modificationList.add(modificationMap);
				}
			}
		}
		
		// hey, we found something to change
		if(!modificationList.isEmpty()) {
			// make a copy of the insight
			// so we do not mess up the state of execution
			Insight cInsight = new Insight();
			cInsight.setInsightId(this.insight.getInsightId());
			InsightUtility.transferDefaultVars(this.insight, cInsight);
			recipeToSave = PixelUtility.modifyInsightDatasource(cInsight, fullRecipe, modificationList);
		}
		
		return recipeToSave;
	}
	
	protected List<String> getParamRecipe(List<String> recipe, List<Map<String, Object>> params, String insightName) {
		return PixelUtility.getParameterizedRecipe(this.insight.getUser(), recipe, params, insightName);
	}
	
	/**
	 * Get the pipeline file location for an insight
	 * @param appId
	 * @param appName
	 * @param rdbmsID
	 * @return
	 */
	protected File getPipelineFileLocation(String appId, String appName, String rdbmsID) {
		String pipelinePath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER)
				+ DIR_SEPARATOR + "db"
				+ DIR_SEPARATOR + SmssUtilities.getUniqueName(appName, appId)
				+ DIR_SEPARATOR + "version" 
				+ DIR_SEPARATOR + rdbmsID;
		
		File f = new File(pipelinePath + DIR_SEPARATOR + PIPELINE_FILE);
		return f;
	}
	
	/**
	 * Save and persist the pipeline to a file
	 * @param appId
	 * @param appName
	 * @param rdbmsID
	 * @param pipeline
	 * @return
	 */
	protected File writePipelineToFile(String appId, String appName, String rdbmsID, Map pipeline) {
		File f = getPipelineFileLocation(appId, appName, rdbmsID);
		// delete file if it already exists
		if(f.exists()) {
			f.delete();
		}
		
		FileWriter writer = null;
		try {
			writer = new FileWriter(f);
			Gson gson = new GsonBuilder().setPrettyPrinting().create();
			gson.toJson(pipeline, writer);
		} catch(Exception e) {
			throw new IllegalArgumentException("An error occured with saving the pipeline", e);
		} finally {
			if(writer != null) {
				try {
					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		return f;
	}
	
}

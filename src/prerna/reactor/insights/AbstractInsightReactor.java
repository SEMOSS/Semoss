package prerna.reactor.insights;

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
import org.quartz.CronExpression;

import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.SmssUtilities;
import prerna.io.connector.couch.CouchException;
import prerna.io.connector.couch.CouchUtil;
import prerna.om.Pixel;
import prerna.om.PixelList;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.insight.InsightUtility;

public abstract class AbstractInsightReactor extends AbstractReactor {

	// used for saving a base insight
	protected static final String IMAGE_THEME_FILE = "insight_theme.json";
	protected static final String IMAGE_NAME = "image.png";
	protected static final String CACHEABLE = "cache";
	protected static final String CACHE_MINUTES = "cacheMinutes";
	protected static final String CACHE_CRON = "cacheCron";
	protected static final String CACHE_ENCRYPT = "cacheEncrypt";
	protected static final String ENCODED_KEY = "encoded";
	protected static final String PIPELINE_FILE = "pipeline.json";
	protected static final String USE_EXISTING_OPEN = "useExistingIfOpen";
	// used for jdbc
	protected static final String SCHEMA_NAME = "schemaName";
	
	public static String USER_SPACE_KEY = "USER";
	public static final String SPACE = ReactorKeysEnum.SPACE.getKey();
	
	protected String getProject() {
		String projectId = null;
		// look at all the ways the insight panel could be passed
		// look at store if it was passed in
		GenRowStruct genericEngineGrs = this.store.getNoun(ReactorKeysEnum.PROJECT.getKey());
		if(genericEngineGrs != null && !genericEngineGrs.isEmpty()) {
			projectId = (String) genericEngineGrs.get(0);
		}
		
		if(projectId == null) {
			// see if it is in the curRow
			// if it was passed directly in as a variable
			List<NounMetadata> stringNouns = this.curRow.getNounsOfType(PixelDataType.CONST_STRING);
			if(stringNouns != null && !stringNouns.isEmpty()) {
				return (String) stringNouns.get(0).getValue();
			}
		}
		
		// LEGACY
		// LEGACY
		// LEGACY
		// LEGACY
		if(projectId == null) {
			genericEngineGrs = this.store.getNoun("app");
			if(genericEngineGrs != null && !genericEngineGrs.isEmpty()) {
				projectId = (String) genericEngineGrs.get(0);
			}
		}
		
		if(projectId == null) {
			// well, you are out of luck
			throw new IllegalArgumentException("Need to define the project where the insight currently exists");
		}

		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		return projectId;
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
	
	protected boolean getGlobal() {
		// see if it was passed directly in with the lower case key ornaments
		GenRowStruct genericIdGrs = this.store.getNoun(ReactorKeysEnum.GLOBAL.getKey());
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
	
	protected Integer getUserDefinedCacheMinutes() {
		// see if it was passed directly in with the lower case key ornaments
		GenRowStruct genericIdGrs = this.store.getNoun(CACHE_MINUTES);
		if(genericIdGrs != null && !genericIdGrs.isEmpty()) {
			return (int) genericIdGrs.get(0);
		}
		
		// well, you are out of luck
		return null;
	}
	
	protected String getUserDefinedCacheCron() {
		// see if it was passed directly in with the lower case key ornaments
		GenRowStruct genericIdGrs = this.store.getNoun(CACHE_CRON);
		if(genericIdGrs != null && !genericIdGrs.isEmpty()) {
			String cronExpression = (String) genericIdGrs.get(0);
			if(cronExpression != null) {
				cronExpression = cronExpression.trim();
				if(cronExpression.isEmpty()) {
					return "";
				}
				if (!CronExpression.isValidExpression(cronExpression)) {
					throw new IllegalArgumentException("Cron expression '" + cronExpression + "' is not of a valid format");
				}
				return cronExpression;
			} 
		}
		
		// well, you are out of luck
		return null;
	}
	
	protected Boolean getUserDefinedCacheEncrypt() {
		// see if it was passed directly in with the lower case key ornaments
		GenRowStruct genericIdGrs = this.store.getNoun(CACHE_ENCRYPT);
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
	 * @return location of an image file
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
	@Deprecated
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
	
	/**
	 * 
	 * @return
	 */
	protected Map<String, Object> getInsightParamValueMap() {
		GenRowStruct paramValues = this.store.getNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		if(paramValues != null && !paramValues.isEmpty()) {
			return (Map<String, Object>) paramValues.get(0);
		}

		// no additional pixels to run
		return null;
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
	 * Is the recipe encoded
	 * @return 	default value of true
	 */
	protected boolean recipeEncoded() {
		GenRowStruct grs = this.store.getNoun(ENCODED_KEY);
		if(grs != null && !grs.isEmpty()) {
			return Boolean.parseBoolean(grs.get(0).toString());
		}
		
		// default to true
		return true;
	}
	
	/**
	 * Do we use the same insight if it is open
	 * @return 	default value of false
	 */
	protected boolean useExistingInsightIfOpen() {
		GenRowStruct grs = this.store.getNoun(USE_EXISTING_OPEN);
		if(grs != null && !grs.isEmpty()) {
			return Boolean.parseBoolean(grs.get(0).toString());
		}
		
		return false;
	}
	
	protected String getOrigin() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.ORIGIN.getKey());
		if(grs != null && !grs.isEmpty()) {
			return grs.get(0).toString();
		}
		
		return "NO ORIGIN";
	}
	
	/**
	 * Set the new file as the image for the insight
	 * Semoss/images/engineName_insightID.png
	 * @param base64Image
	 * @param insightId
	 * @param engineName
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	protected void storeImageFromFile(final String fileName, final String insightId, final String appId, final String appName) {
		// set up path to save image to file
		final String DIR_SEP = java.nio.file.FileSystems.getDefault().getSeparator();
		final String basePath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) 
				+ DIR_SEP + "project" 
				+ DIR_SEP + SmssUtilities.getUniqueName(appName, appId)
				+ DIR_SEP + "app_root" 
				+ DIR_SEP + "version" 
				+ DIR_SEP + insightId;
		final String newImageFile = basePath + DIR_SEP + fileName;
		final File newImage = new File(newImageFile);
		
		// TODO: potentially throw error
		if(!newImage.exists()) {
			return;
		}
		
		final String saveImageFileAs = basePath + DIR_SEP + "image." + FilenameUtils.getExtension(fileName);
		final File saveImageFile = new File(saveImageFileAs);
		
		if(CouchUtil.COUCH_ENABLED) {
			try {
				Map<String, String> selectors = new HashMap<>();
				selectors.put(CouchUtil.INSIGHT, insightId);
				selectors.put(CouchUtil.PROJECT, appId);
				CouchUtil.upload(CouchUtil.INSIGHT, selectors, saveImageFile);
			} catch (CouchException e) {
				e.printStackTrace();
			}
		}
		
		File[] oldImages = InsightUtility.findImageFile(basePath);
		for (File oldI : oldImages) {
			// don't delete the image we are about to save as the insight image
			if(!oldI.equals(newImage)) {
				oldI.delete();
			}
		}
		
		// now rename the file
		// if it isn't already the right format
		if(!saveImageFileAs.equals(newImageFile)) {
			try {
				Files.copy(newImage, saveImageFile);
				newImage.delete();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
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
				+ DIR_SEP + Constants.PROJECT_FOLDER 
				+ DIR_SEP + SmssUtilities.getUniqueName(appName, appId)
				+ DIR_SEP + "app_root" 
				+ DIR_SEP + "version" 
				+ DIR_SEP + insightId + DIR_SEP + "image.png";
		
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
	 * @param insightPixelList
	 * @param projectId
	 * @param projectName
	 * @param newInsightId
	 * @param deleteOrigFile
	 * @return
	 */
	protected boolean saveFilesInInsight(PixelList insightPixelList, String projectId, String projectName, String newInsightId, boolean deleteOrigFile, PixelList originalInsightPixelList) {
		boolean filesSaved = false;
		final String BASE = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
		
		for(Pixel p : insightPixelList) {
			// use the metadata captured to determine if this is a file read
			// that needs to be modified
			if(p.isFileRead()) {
				// shift any csv files to be moved into the insight folder for the new insight
				List<Map<String, Object>> datasources = PixelUtility.getDatasourcesMetadata(this.insight.getUser(), p.getPixelString());
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
						
						String baseFile = FilenameUtils.getBaseName(fileLoc);
						String extension = FilenameUtils.getExtension(fileLoc);
						String filename = baseFile + "." + extension;
						// we only want to shift if it is an insight file
						// or if it is a user file
						// if project - keep in current project folder
						boolean isUserSpace = space != null && space.toUpperCase().equals(AssetUtility.USER_SPACE_KEY);
						if(space == null || space.isEmpty() || isUserSpace) {
							File origF = new File(fileLoc);
							if(!origF.exists() || origF.isDirectory()) {
								// this might have already been moved 
								// weird situation but dont dont update anything
								continue;
							}
							String newFileLoc = AssetUtility.getProjectVersionFolder(projectName, projectId)
													+ DIR_SEPARATOR + newInsightId 
													+ DIR_SEPARATOR + "data";
							// create parent directory
							File newF = new File(newFileLoc);
							newF.mkdirs();
							newF = new File(newFileLoc + DIR_SEPARATOR + filename);
							// if the file is already in the correct folder
							// there is nothing to do
							if(origF.getParentFile().getAbsolutePath().equals(newF.getParentFile().getAbsolutePath())) {
								continue;
							}
							// make sure the file name is unique when we move it over
							if(newF.exists()) {
								int counter = 1;
								while(newF.exists()) {
									filename = baseFile + " (" + ++counter + ")." + extension;
									newF = new File(newFileLoc + DIR_SEPARATOR + filename);
								}
							}
							try {
								FileUtils.copyFile(origF, newF);
								String newFilePixel = "data" + DIR_SEPARATOR + filename;
								// need to make new pixel
								String newPixel = p.getPixelString().replace(filePixelPortion, newFilePixel);
								// if user, need to replace the space to be an empty string
								if(isUserSpace) {
									newPixel = newPixel.replace("space = [ \"" + space + "\" ]", "space = [\"\"]");
								}
								p.setPixelString(newPixel);
								filesSaved = true;
								
								Pixel originalP = originalInsightPixelList.getPixel(p.getId());
								originalP.setPixelString(newPixel);

								// only after successful setting of everything will we delete the original F
								if(!isUserSpace && deleteOrigFile) {
									origF.delete();
								}
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					}
				}
			}
		}
		
		return filesSaved;
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
				+ DIR_SEPARATOR + "project"
				+ DIR_SEPARATOR + SmssUtilities.getUniqueName(appName, appId)
				+ DIR_SEPARATOR + "app_root" 
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
			throw new IllegalArgumentException("An error occurred with saving the pipeline", e);
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
	
	/**
	 * Get user input schema name or use default name provided
	 * @param projectId
	 * @param defaultName
	 * @return
	 */
	protected String getUserSchemaNameOrDefaultAndValidate(String projectId, String defaultName) {
		if(projectId == null) {
			return null;
		}
		String potentialName = defaultName;
		GenRowStruct genericEngineGrs = this.store.getNoun(SCHEMA_NAME);
		if(genericEngineGrs != null && !genericEngineGrs.isEmpty()) {
			potentialName = (String) genericEngineGrs.get(0);
		}
		
		if(potentialName == null || (potentialName=potentialName.trim()).isEmpty() ) {
			return null;
		}
		
		return SecurityInsightUtils.makeInsightSchemaNameUnique(projectId, potentialName);
	}
	
	/**
	 * Get user input schema name or use existing insights schema name
	 * @param projectId
	 * @param existingInsightId
	 * @return
	 */
	protected String getUserSchemaNameOrExistingSchemaName(String projectId, String existingInsightId) {
		if(projectId == null) {
			return null;
		}
		GenRowStruct genericEngineGrs = this.store.getNoun(SCHEMA_NAME);
		if(genericEngineGrs != null && !genericEngineGrs.isEmpty()) {
			String potentialName = (String) genericEngineGrs.get(0);
			if(potentialName != null && !(potentialName=potentialName.trim()).isEmpty() ) {
				return SecurityInsightUtils.makeInsightSchemaNameUnique(projectId, potentialName);
			}
		}
		
		return SecurityInsightUtils.getInsightSchemaName(projectId, existingInsightId);
	}
	
}

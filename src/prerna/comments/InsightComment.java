package prerna.comments;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.util.AssetUtility;
import prerna.util.Utility;

public class InsightComment {
	
	private static Logger logger = LogManager.getLogger(InsightComment.class);
	private static Gson gson = new GsonBuilder().setPrettyPrinting().create();
	protected static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	// extension
	public static final String COMMENT_EXTENSION = ".c";

	// keys
	private static final String PROJECT_ID_KEY = "projectId";
	private static final String PROJECT_KEY = "project";
	private static final String INSGIHT_ID_KEY = "insightId";

	private static final String ID_KEY = "id";
	private static final String NEXT_ID_KEY = "nextId";
	private static final String PREVIOUS_ID_KEY = "prevId";
	private static final String COMMENT_KEY = "comment";
	private static final String ACTION_KEY = "action";

	private static final String RECIPE_KEY = "recipe";
	private static final String PICTURE_KEY = "picture";
	private static final String USER_KEY = "user";
	private static final String CREATED_TIME_STAMP_KEY = "createdTimeStamp";

	// comment types
	public static final String ADD_ACTION = "add";
	public static final String DELETE_ACTION = "edit";
	public static final String EDIT_ACTION = "delete";
	
	// class variables
	private String id;
	private String nextId;
	private String prevId;
	private String comment;
	private String action;
	// required to know where it belongs
	private String projectId;
	private String projectName;
	private String rdbmsId;
	
	private String recipe;
	private String picture;
	private String user;
	private String createdTimeStamp;
	
	private DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss");
	
	/**
	 * Constructor will generate the random comment id
	 */
	public InsightComment(String projectId, String projectName, String rdbmsId) {
		this.projectId = projectId;
		this.projectName = projectName;
		this.rdbmsId = rdbmsId;
		
		this.createdTimeStamp = getCurrentDate();
		this.id = UUID.randomUUID().toString() + "_" + this.createdTimeStamp;
	}
	
	/**
	 * Write the insight comment
	 * @param projectId
	 * @param rdbmsId
	 */
	public void writeToFile() {
		Map<String, String> map = moveDataToMap();

		String json = gson.toJson(map);
//		String baseDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) 
//				+ DIR_SEPARATOR + Constants.DB + DIR_SEPARATOR + SmssUtilities.getUniqueName(this.engineName, this.engineId) + DIR_SEPARATOR + "version" + DIR_SEPARATOR + this.rdbmsId;

		String baseDir = AssetUtility.getProjectVersionFolder(this.projectName, this.projectId) + DIR_SEPARATOR + this.rdbmsId;

		File path = new File(Utility.normalizePath(baseDir));
		// create insight directory if it doesn't exist
		path.mkdirs();
		String commentInfoPath = baseDir + DIR_SEPARATOR + this.id + "_" + this.createdTimeStamp +  COMMENT_EXTENSION;
		// create file
		File f = new File(Utility.normalizePath(commentInfoPath));
		try {
			// write json to file
			FileUtils.writeStringToFile(f, json);
		} catch (IOException e1) {
			logger.error(e1.getStackTrace());
		}
	}
	
	public String decrypt(String s) throws Exception {
		return s;
	}
	
	public String encrypt(String s) {
		return s;
	}
	
	/**
	 * Move all the instance variables into a map
	 * @return
	 */
	private Map<String, String> moveDataToMap() {
		Map<String, String> map = new TreeMap<>();
		map.put(PROJECT_ID_KEY, this.projectId);
		if(this.projectName != null) {
			map.put(PROJECT_KEY, this.projectName);
		}
		map.put(INSGIHT_ID_KEY, this.rdbmsId);

		map.put(ID_KEY, this.id);
		map.put(NEXT_ID_KEY, this.nextId);
		map.put(PREVIOUS_ID_KEY, this.prevId);
		map.put(COMMENT_KEY, this.comment);
		map.put(ACTION_KEY, this.action);
		
		map.put(USER_KEY, this.user);
		map.put(RECIPE_KEY, this.recipe);
		map.put(PICTURE_KEY, this.picture);
		map.put(CREATED_TIME_STAMP_KEY, this.createdTimeStamp);
		return map;
	}
	
	public void modifyExistingIdWithDate() {
		String existingId = getIdMinusTimestamp(this.id);
		this.id = existingId  + "_" + this.createdTimeStamp;
	}

	/**
	 * Get current date from given format
	 * @return
	 */
	public String getCurrentDate() {
		return formatter.format(new Date());
	}
	
	/**
	 * Load from comment info path
	 * @param commentInfoPath
	 */
	public static InsightComment loadFromFile(String commentInfoPath) {
		File fileInfo = new File(commentInfoPath);
		return loadFromFile(fileInfo);
	}
	
	public static InsightComment loadFromFile(File fileInfo) {
		Map<String, String> mapData = null;
		try {
			mapData = new ObjectMapper().readValue(fileInfo, Map.class);
		} catch(FileNotFoundException e) {
			throw new IllegalArgumentException("Comment info file could not be found at location: " + fileInfo.getPath());
		} catch (IOException e) {
			throw new IllegalArgumentException("Comment info file is not in valid JSON format");
		}

		String engineId = mapData.get(PROJECT_ID_KEY);
		String engineName = mapData.get(PROJECT_KEY);
		String insightId = mapData.get(INSGIHT_ID_KEY);

		InsightComment comment = new InsightComment(engineId, engineName, insightId);
		comment.id = mapData.get(ID_KEY);
		comment.nextId = mapData.get(NEXT_ID_KEY);
		comment.prevId = mapData.get(PREVIOUS_ID_KEY);
		comment.comment = mapData.get(COMMENT_KEY);
		comment.action = mapData.get(ACTION_KEY);
		comment.user = mapData.get(USER_KEY);
		comment.recipe = mapData.get(RECIPE_KEY);
		comment.picture = mapData.get(PICTURE_KEY);
		comment.createdTimeStamp = mapData.get(CREATED_TIME_STAMP_KEY);
		
		return comment;
	}
	
	/**
	 * 
	 * @param id
	 * @return
	 */
	public static String getIdMinusTimestamp(String id) {
		// parse out the time and set the index in case we go to edit or delete
		// because PK loves string parsing, here is magic number 19
		return id.substring(0, id.length()-20);
	}
	
	//////////////////////////////////////////////////
	//////////////////////////////////////////////////
	//////////////////////////////////////////////////

	/*
	 * Setters / Getters
	 */


	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getNextId() {
		return nextId;
	}

	public void setNextId(String nextId) {
		this.nextId = nextId;
	}

	public String getPrevId() {
		return prevId;
	}

	public void setPrevId(String prevId) {
		this.prevId = prevId;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public String getRecipe() {
		return recipe;
	}

	public void setRecipe(String recipe) {
		this.recipe = recipe;
	}

	public String getPicture() {
		return picture;
	}

	public void setPicture(String picture) {
		this.picture = picture;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getCreatedTimeStamp() {
		return createdTimeStamp;
	}

	public void setCreatedTimeStamp(String createdTimeStamp) {
		this.createdTimeStamp = createdTimeStamp;
	}
}

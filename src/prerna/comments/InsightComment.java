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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.util.Constants;
import prerna.util.DIHelper;

public class InsightComment {

	private static Gson GSON = new GsonBuilder().setPrettyPrinting().create();
	private static DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss");
	
	// extension
	public static final String COMMENT_EXTENSION = ".c";

	// keys
	private static final String ENGINE_KEY = "engine";
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

	// class variables
	private String id;
	private String nextId;
	private String prevId;
	private String comment;
	private String action;
	// required to know where it belongs
	private String engineName;
	private String rdbmsId;
	
	private String recipe;
	private String picture;
	private String user;
	private String createdTimeStamp;
	
	// comment types
	public static final String ADD_ACTION = "add";
	public static final String DELETE_ACTION = "edit";
	public static final String EDIT_ACTION = "delete";
	

	
	/**
	 * Constructor will generate the random comment id
	 */
	public InsightComment(String engineName, String rdbmsId) {
		this.engineName = engineName;
		this.rdbmsId = rdbmsId;
		
		this.createdTimeStamp = getCurrentDate();
		this.id = UUID.randomUUID().toString() + "_" + this.createdTimeStamp;
	}
	
	/**
	 * Write the insight comment
	 * @param engineName
	 * @param rdbmsId
	 */
	public void writeToFile() {
		Map<String, String> map = moveDataToMap();

		String json = GSON.toJson(map);
		String baseDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) 
				+ "\\" + Constants.DB + "\\" + this.engineName + "\\version\\" + this.rdbmsId;
		
		File path = new File(baseDir);
		// create insight directory if it doesn't exist
		path.mkdirs();
		String commentInfoPath = baseDir + "\\" + this.id + "_" + this.createdTimeStamp +  COMMENT_EXTENSION;
		// create file
		File f = new File(commentInfoPath);
		try {
			// write json to file
			FileUtils.writeStringToFile(f, json);
		} catch (IOException e1) {
			e1.printStackTrace();
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
		Map<String, String> map = new TreeMap<String, String>();
		map.put(ENGINE_KEY, this.engineName);
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
		String id = getIdMinusTimestamp(this.id);
		this.id = id  + "_" + this.createdTimeStamp;
	}

	/**
	 * Get current date from given format
	 * @return
	 */
	public static String getCurrentDate() {
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

		String engineName = mapData.get(ID_KEY);
		String insightId = mapData.get(ID_KEY);

		InsightComment comment = new InsightComment(engineName, insightId);
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

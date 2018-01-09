package prerna.comments;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.util.Constants;
import prerna.util.DIHelper;

public class InsightComment {

	private static Gson GSON = new GsonBuilder().setPrettyPrinting().create();
	private static DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	
	// extension
	private static final String COMMENT_MAP_EXTENSION = "_info";
	private static final String COMMENT_EXTENSION = "_message";

	// keys
	private static final String RECIPE_KEY = "recipe";
	private static final String COMMENT_ID_KEY = "commentId";
	private static final String PICTURE_KEY = "picture";
	private static final String USER_KEY = "user";
	private static final String CREATED_TIME_STAMP_KEY = "createdTimeStamp";
	private static final String LAST_MODIFIED_TIME_STAMP_KEY = "lastModifedTimeStamp";

	// this will be encrypted
	private String comment;
	
	private String recipe;
	private String commentId;
	private String picture;
	private String user;
	private String createdTimeStamp;
	private String lastModifedTimeStamp;
	
	/**
	 * Constructor will generate the random comment id
	 */
	public InsightComment() {
		this.commentId = UUID.randomUUID().toString();
	}
	
	/**
	 * Write the insight comment
	 * @param engineName
	 * @param rdbmsId
	 */
	public void writeToFile(String engineName, String rdbmsId, String comment) {
		Map<String, String> map = moveDataToMap();

		String json = GSON.toJson(map);
		String baseDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) 
				+ "\\" + Constants.DB + "\\" + engineName + "\\version\\" + rdbmsId;		
		
		File path = new File(baseDir);
		// create insight directory if it doesn't exist
		path.mkdirs();
		String commentInfoPath = baseDir + "\\" + this.commentId + COMMENT_MAP_EXTENSION;
		// create file
		File f = new File(commentInfoPath);
		try {
			// write json to file
			FileUtils.writeStringToFile(f, json);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		String encryptedComment = encrypt(comment);
		String commentPath = baseDir + "\\" + this.commentId + COMMENT_EXTENSION;
		// create file
		f = new File(commentPath);
		try {
			// write json to file
			FileUtils.writeStringToFile(f, encryptedComment);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
	
	/**
	 * Determine the path using the input and load the file
	 * @param engine
	 * @param rdbmsId
	 * @param commentId
	 */
	public void loadFromFile(String engine, String rdbmsId, String commentId) {
		// generate the file path
		String commentInfoPath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) 
				+ "\\" + Constants.DB + "\\" + engine + "\\version\\" + rdbmsId + "\\" + commentId + COMMENT_MAP_EXTENSION;
		// load it
		loadFromFile(commentInfoPath);
	}
	
	/**
	 * Load from comment info path
	 * @param commentInfoPath
	 */
	public void loadFromFile(String commentInfoPath) {
		File fileInfo = new File(commentInfoPath);
		Map<String, String> mapData = null;
		try {
			mapData = new ObjectMapper().readValue(fileInfo, Map.class);
		} catch(FileNotFoundException e) {
			throw new IllegalArgumentException("Comment info file could not be found at location: " + fileInfo.getPath());
		} catch (IOException e) {
			throw new IllegalArgumentException("Comment info file is not in valid JSON format");
		}
		
		this.commentId = mapData.get(COMMENT_ID_KEY);
		this.user = mapData.get(USER_KEY);
		this.recipe = mapData.get(RECIPE_KEY);
		this.picture = mapData.get(PICTURE_KEY);
		this.createdTimeStamp = mapData.get(CREATED_TIME_STAMP_KEY);
		this.lastModifedTimeStamp = mapData.get(LAST_MODIFIED_TIME_STAMP_KEY);
		
		String commentFileLocation = fileInfo.getParentFile().getAbsolutePath() + "\\" + this.commentId + COMMENT_EXTENSION;
		File commentFile = new File(commentFileLocation);
		String comment = null;
		try {
			comment = FileUtils.readFileToString(commentFile);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Could not read comment file");
		}
		
		try {
			this.comment = decrypt(comment);
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Cannot access comment");
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
		Map<String, String> map = new HashMap<String, String>();
		map.put(COMMENT_ID_KEY, this.commentId);
		map.put(USER_KEY, this.user);
		map.put(RECIPE_KEY, this.recipe);
		map.put(PICTURE_KEY, this.picture);
		map.put(CREATED_TIME_STAMP_KEY, this.createdTimeStamp);
		map.put(LAST_MODIFIED_TIME_STAMP_KEY, this.lastModifedTimeStamp);
		return map;
	}

	/**
	 * Get current date from given format
	 * @return
	 */
	public static String getCurrentDate() {
		return formatter.format(new Date());
	}
	
	//////////////////////////////////////////////////
	//////////////////////////////////////////////////
	//////////////////////////////////////////////////

	/*
	 * Setters / Getters
	 */

	public String getCommentId() {
		return commentId;
	}
	
	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
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

	public String getLastModifedTimeStamp() {
		return lastModifedTimeStamp;
	}

	public void setLastModifedTimeStamp(String lastModifedTimeStamp) {
		this.lastModifedTimeStamp = lastModifedTimeStamp;
	}
	
}

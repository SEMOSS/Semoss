package prerna.comments;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.util.Constants;
import prerna.util.DIHelper;

public class InsightComment {

	// comment
	private static Gson GSON = null;
	// extension
	private static final String COMMENT_MAP_EXTENSION = "info";
	
	private String recipe;
	private String commentId;
	private String picture;
	private String user;
	private String createdTimeStamp;
	private String lastModifedTimeStamp;

	public InsightComment() {
		if(InsightComment.GSON == null) {
			InsightComment.GSON = new GsonBuilder().setPrettyPrinting().create();
		}
		this.commentId = UUID.randomUUID().toString();
	}
	
	/**
	 * Write the insight comment
	 * @param engineName
	 * @param rdbmsId
	 */
	public void writeToFile(String engineName, String rdbmsId) {
		Map<String, String> map = moveDataToMap();

		String json = GSON.toJson(map);
		String recipePath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) 
				+ "\\" + Constants.DB + "\\" + engineName + "\\version\\" + rdbmsId;		
		
		File path = new File(recipePath);
		// create insight directory if it doesn't exist
		path.mkdirs();
		recipePath += "\\" + this.commentId + "_" + COMMENT_MAP_EXTENSION;
		// create file
		File f = new File(recipePath);
		try {
			// write json to file
			FileUtils.writeStringToFile(f, json);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
	
	public void decrypt() throws Exception {
		
	}
	
	public void encrypt() {
		
	}
	
	/**
	 * Move all the instance variables into a map
	 * @return
	 */
	private Map<String, String> moveDataToMap() {
		Map<String, String> map = new HashMap<String, String>();
		map.put("recipe", this.recipe);
		map.put("commentId", this.commentId);
		map.put("picture", this.picture);
		map.put("user", this.user);
		map.put("createdTimeStamp", this.createdTimeStamp);
		map.put("lastModifedTimeStamp", this.lastModifedTimeStamp);
		return map;
	}

	//////////////////////////////////////////////////
	//////////////////////////////////////////////////
	//////////////////////////////////////////////////

	/*
	 * Setters / Getters
	 */
	
	public void setRecipe(String recipe) {
		this.recipe = recipe;
	}
	
	public void setPicture(String picture) {
		this.picture = picture;
	}
	
	public void setUser(String user) {
		this.user = user;
	}
}

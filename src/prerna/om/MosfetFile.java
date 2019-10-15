package prerna.om;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;

public class MosfetFile {

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	public static final String RECIPE_FILE = ".mosfet";

	// insight identifiers
	private String engineId;
	private String rdbmsId;
	private String insightName;
	private String layout;
	private boolean hidden = false;
	
	// actual recipe
	private String[] recipe;
	
	// insight metadata
	private String[] tags;
	private String description;
	
	public MosfetFile() {
		
	}
	
	public static MosfetFile generateFromFile(File file) throws IOException {
		JsonReader jReader = null;
		FileReader fReader = null;
		try {
			Gson gson = new Gson();
			
			fReader = new FileReader(file);
			jReader = new JsonReader(fReader);
	        return gson.fromJson(jReader, MosfetFile.class);
	    } finally {
	    	if(fReader != null) {
	    		try {
					fReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
	    	}
	    	if(jReader != null) {
	    		try {
					jReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
	    	}
	    }
	}
	
	/**
	 * Write the mosfet file to disk
	 * @param folderLocation	The folder to write the file
	 * @param force				Delete the file if it already exists
	 * @throws IOException 
	 */
	public void write(String folderLocation, boolean force) throws IOException {
		File directory = new File(folderLocation);
		if(!directory.exists()) {
			directory.mkdirs();
		}
		
		File mosfet = new File(folderLocation + DIR_SEPARATOR + RECIPE_FILE);
		if(!force && mosfet.exists()) {
			throw new IOException("The mosfet file already exists");
		} else if(force && mosfet.exists()) {
			mosfet.delete();
		}
		
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		FileWriter writer = null;
		try {
			writer = new FileWriter(mosfet);
			gson.toJson(this, writer);
		} finally {
			if(writer != null) {
				try {
					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////

	/*
	 * Setters and getters
	 */
	
	public String getEngineId() {
		return engineId;
	}

	public void setEngineId(String engineId) {
		this.engineId = engineId;
	}

	public String getRdbmsId() {
		return rdbmsId;
	}

	public void setRdbmsId(String rdbmsId) {
		this.rdbmsId = rdbmsId;
	}

	public String getInsightName() {
		return insightName;
	}

	public void setInsightName(String insightName) {
		this.insightName = insightName;
	}

	public String getLayout() {
		return layout;
	}

	public void setLayout(String layout) {
		this.layout = layout;
	}

	public boolean isHidden() {
		return hidden;
	}

	public void setHidden(boolean hidden) {
		this.hidden = hidden;
	}

	public String[] getRecipe() {
		return recipe;
	}

	public void setRecipe(String[] recipe) {
		this.recipe = recipe;
	}

	public String[] getTags() {
		return tags;
	}

	public void setTags(String[] tags) {
		this.tags = tags;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

}

package prerna.om;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;

import prerna.util.Constants;
import prerna.util.Utility;

public class MosfetFile {

	private static final Logger classLogger = LogManager.getLogger(MosfetFile.class);

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	public static final String RECIPE_FILE = ".mosfet";

	// insight identifiers
	private String projectId;
	private String rdbmsId;
	private String insightName;
	private String layout;
	private boolean global = false;
	private boolean cacheable = true;
	private int cacheMinutes = -1;
	private String cacheCron;
	private LocalDateTime cachedOn;
	private boolean cacheEncrypt = false;
	
	// schema name
	private String schemaName;
	
	// actual recipe
	private List<String> recipe;
	
	// insight metadata
	private String[] tags;
	private String description;
	
	public MosfetFile() {
		
	}
	
	public static MosfetFile generateFromFile(File file) throws IOException {
		JsonReader jReader = null;
		BufferedReader fReader = null;
		try {
			Gson gson = new Gson();
			fReader = Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8);
			jReader = new JsonReader(fReader);
	        return gson.fromJson(jReader, MosfetFile.class);
	    } finally {
	    	if(fReader != null) {
	    		try {
					fReader.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
	    	}
	    	if(jReader != null) {
	    		try {
					jReader.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
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
		String normalizedFolderLocation = Utility.normalizePath(folderLocation);
		File directory = new File(normalizedFolderLocation);
		if(!directory.exists()) {
			directory.mkdirs();
		}
		
		File mosfet = new File(normalizedFolderLocation + DIR_SEPARATOR + RECIPE_FILE);
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
					classLogger.error(Constants.STACKTRACE, e);
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
	
	public String getProjectId() {
		return projectId;
	}

	public void setProjectId(String projectId) {
		this.projectId = projectId;
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

	public boolean isGlobal() {
		return global;
	}

	public void setGlobal(boolean global) {
		this.global = global;
	}
	
	public boolean isCacheable() {
		return cacheable;
	}

	public void setCacheable(boolean cacheable) {
		this.cacheable = cacheable;
	}

	public int getCacheMinutes() {
		return cacheMinutes;
	}

	public void setCacheMinutes(int cacheMinutes) {
		this.cacheMinutes = cacheMinutes;
	}
	
	public String getCacheCron() {
		return cacheCron;
	}

	public void setCacheCron(String cacheCron) {
		this.cacheCron = cacheCron;
	}

	public LocalDateTime getCachedOn() {
		return cachedOn;
	}

	public void setCachedOn(LocalDateTime cachedOn) {
		this.cachedOn = cachedOn;
	}

	public boolean isCacheEncrypt() {
		return cacheEncrypt;
	}

	public void setCacheEncrypt(boolean cacheEncrypt) {
		this.cacheEncrypt = cacheEncrypt;
	}

	public List<String> getRecipe() {
		return recipe;
	}

	public void setRecipe(List<String> recipe) {
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

	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}
	
}

package prerna.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityUpdateUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.SmssUtilities;
import prerna.om.Insight;

public class MosfetSyncHelper {

	// get the directory separator
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	public static final String RECIPE_FILE = ".mosfet";

	// ADDED
	public static final String ADD = "ADD";
	// MODIFIED
	public static final String MOD = "MOD";
	// DELETE
	public static final String DEL = "DEL";
	// RENAMED
	public static final String REN = "REN";

	public static final String ENGINE_ID_KEY = "engineId";
	public static final String RDBMS_ID_KEY = "rdbmsId";
	public static final String INSIGHT_NAME_KEY = "insightName";
	public static final String LAYOUT_KEY = "layout";
	public static final String RECIPE_KEY = "recipe";
	public static final String HIDDEN_KEY = "hidden";

	private MosfetSyncHelper() {

	}

	public static void synchronizeInsightChanges(Map<String, List<String>> filesChanged, Logger logger) {
		// process add
		if(filesChanged.containsKey(ADD)) {
			processAddedFiles(filesChanged.get(ADD), logger);
		}

		// process mod
		if(filesChanged.containsKey(MOD)) {
			processModifiedFiles(filesChanged.get(MOD), logger);
		}

		// TODO: how to handle rename
		//		// process ren
		//		if(filesChanged.containsKey(REN)) {
		//			processRenamed(filesChanged.get(REN));
		//		}

		// process delete
		if(filesChanged.containsKey(DEL)) {
			processDelete(filesChanged.get(DEL), logger);
		}
	}

	private static void processAddedFiles(List<String> list, Logger logger) {
		for(String fileLocation : list) {
			File mosfetFile = new File(fileLocation);
			Map<String, Object> mapData = null;
			try {
				mapData = getMosfitMap(mosfetFile);
			} catch(IllegalArgumentException e) {
				outputError(logger, e.getMessage());
			}
			if(mapData == null) {
				outputError(logger, "MOSFET file is not in valid JSON format");
				return;
			}
			processAddedFile(mapData, logger);
		}
	}

	private static void processAddedFile(Map<String, Object> mapData, Logger logger) {
		String appId = mapData.get(ENGINE_ID_KEY).toString();
		String id = mapData.get(RDBMS_ID_KEY).toString();
		String name = mapData.get(INSIGHT_NAME_KEY).toString();
		String layout = mapData.get(LAYOUT_KEY).toString();
		String recipe = mapData.get(RECIPE_KEY).toString();
		boolean hidden = false;
		if(mapData.containsKey(HIDDEN_KEY)) {
			hidden = (boolean) mapData.get(HIDDEN_KEY);
		}

		// need to add the insight in the rdbms engine
		IEngine engine = Utility.getEngine(appId);
		// we want to make sure the file isn't added because we made the insight
		// and is in fact a new one made by another collaborator
		Vector<Insight> ins = engine.getInsight(id);
		if(ins == null || ins.isEmpty() || (ins.size() == 1 && ins.get(0) == null) ) {
			addInsightToEngineRdbms(engine, id, name, layout, recipe, hidden);
		}
	}

	private static void processModifiedFiles(List<String> list, Logger logger) {
		for(String fileLocation : list) {
			File mosfetFile = new File(fileLocation);
			Map<String, Object> mapData = null;
			try {
				mapData = getMosfitMap(mosfetFile);
			} catch(IllegalArgumentException e) {
				outputError(logger, e.getMessage());
			}
			if(mapData == null) {
				outputError(logger, "MOSFET file is not in valid JSON format");
				continue;
			}
			processModifiedFiles(mapData, logger);
		}
	}

	private static void processModifiedFiles(Map<String, Object> mapData, Logger logger) {
		String appId = mapData.get(ENGINE_ID_KEY).toString();
		String id = mapData.get(RDBMS_ID_KEY).toString();
		String name = mapData.get(INSIGHT_NAME_KEY).toString();
		String layout = mapData.get(LAYOUT_KEY).toString();
		String recipe = mapData.get(RECIPE_KEY).toString();
		boolean hidden = false;
		if(mapData.containsKey(HIDDEN_KEY)) {
			hidden = (boolean) mapData.get(HIDDEN_KEY);
		}
		
		// need to update the insight in the rdbms engine
		modifyInsightInEngineRdbms(appId, id, name, layout, recipe, hidden);
	}

	private static void processDelete(List<String> list, Logger logger) {
		for(String fileLocation : list) {
			File mosfetFile = new File(fileLocation);
			Map<String, Object> mapData = null;
			try {
				mapData = getMosfitMap(mosfetFile);
			} catch(IllegalArgumentException e) {
				outputError(logger, e.getMessage());
			}
			if(mapData == null) {
				outputError(logger, "MOSFET file is not in valid JSON format");
				continue;
			}

			String engineId = mapData.get(ENGINE_ID_KEY).toString();
			String id = mapData.get(RDBMS_ID_KEY).toString();
			deleteInsightFromEngineRdbms(engineId, id);
		}
	}
	
	private static void addInsightToEngineRdbms(IEngine engine, String id, String insightName, String layout, String recipe, boolean hidden) {
		InsightAdministrator admin = new InsightAdministrator(engine.getInsightDatabase());
		// just put the recipe into an array
		String[] pixelRecipeToSave = new String[]{recipe};
		admin.addInsight(id, insightName, layout, pixelRecipeToSave, hidden);
		
		SecurityInsightUtils.addInsight(engine.getEngineId(), id, insightName, false, layout);
	}

	private static void modifyInsightInEngineRdbms(String appId, String id, String insightName, String layout, String recipe, boolean hidden) {
		IEngine engine = Utility.getEngine(appId);
		InsightAdministrator admin = new InsightAdministrator(engine.getInsightDatabase());
		// just put the recipe into an array
		String[] pixelRecipeToSave = new String[]{recipe};
		admin.updateInsight(id, insightName, layout, pixelRecipeToSave, hidden);
		
		SecurityInsightUtils.updateInsight(appId, id, insightName, false, layout);
	}

	private static void deleteInsightFromEngineRdbms(String engineId, String id) {
		IEngine engine = Utility.getEngine(engineId);
		InsightAdministrator admin = new InsightAdministrator(engine.getInsightDatabase());
		admin.dropInsight(id);
		
		SecurityInsightUtils.deleteInsight(engineId, id);
	}

	private static void outputError(Logger logger, String errorMessage) {
		if(logger != null) {
			logger.info("ERROR!!! " + errorMessage);
		}
	}

	public static Map<String, Object> getMosfitMap(File mosfetFile) {
		Map<String, Object> mapData = null;
		try {
			mapData = new ObjectMapper().readValue(mosfetFile, Map.class);
		} catch(FileNotFoundException e) {
			throw new IllegalArgumentException("MOSFET file could not be found at location: " + mosfetFile.getPath());
		} catch (IOException e) {
			throw new IllegalArgumentException("MOSFET file is not in valid JSON format");
		}
		return mapData;
	}
	
	/**
	 * Get the insight name for the input mosfet file
	 * @param mosfetFile
	 * @return
	 */
	public static String getInsightName(File mosfetFile) {
		Map<String, Object> mapData = getMosfitMap(mosfetFile);
		String name = mapData.get(INSIGHT_NAME_KEY).toString();
		return name;
	}

	public static File renameMosfit(File mosfetFile, String newInsightName, Logger logger) {
		Map<String, Object> mapData = getMosfitMap(mosfetFile);
		String origId = mapData.get(RDBMS_ID_KEY).toString();
		String engineId = mapData.get(ENGINE_ID_KEY).toString();
		
		String newRandomId = UUID.randomUUID().toString();

		// general structure is db / version / insight id / .mosfet
		// we have the .mosfet and want to go up to the version folder
		File versionDir = mosfetFile.getParentFile().getParentFile();
		// make a new directory for the insight
		String newInsightDirLoc = versionDir.getPath() + DIR_SEPARATOR + newRandomId;
		File newInsightDir = new File(newInsightDirLoc);
		newInsightDir.mkdirs();

		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		mapData.put(RDBMS_ID_KEY, newRandomId);
		mapData.put(INSIGHT_NAME_KEY, newInsightName);

		String json = gson.toJson(mapData);
		File newMosfetFile = new File(newInsightDirLoc + DIR_SEPARATOR + RECIPE_FILE);
		try {
			// write json to file
			FileUtils.writeStringToFile(newMosfetFile, json);
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		// now that we have the new one
		// let us add it
		processAddedFile(mapData, logger);
		// we want to delete the old
		deleteInsightFromEngineRdbms(engineId, origId);
		mosfetFile.delete();
		mosfetFile.getParentFile().delete();
		
		return newMosfetFile;
	}
	
	/**
	 * Save insight recipe to db/engineName/insightName/recipe.json
	 *  json includes 
	 *  	engine: engineName
	 *  	rdbmsID: rdbmsID
	 *  	recipe: pixel;pixel;...
	 * 
	 * @param engineName
	 * @param rdbmsID
	 * @param recipeToSave
	 */
	public static File makeMosfitFile(String appId, String appName, String rdbmsID, String insightName, String layout, String[] recipeToSave, boolean hidden) {
		String recipePath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER)
				+ DIR_SEPARATOR + "db"
				+ DIR_SEPARATOR + SmssUtilities.getUniqueName(appName, appId)
				+ DIR_SEPARATOR + "version" 
				+ DIR_SEPARATOR + rdbmsID;
		
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		// format recipe file
		HashMap<String, Object> output = new HashMap<String, Object>();
		output.put(ENGINE_ID_KEY, appId);
		output.put(RDBMS_ID_KEY, rdbmsID);
		output.put(INSIGHT_NAME_KEY, insightName);
		output.put(LAYOUT_KEY, layout);
		output.put(HIDDEN_KEY, hidden);
		StringBuilder recipe = new StringBuilder();
		for (String pixel : recipeToSave) {
			recipe.append(pixel);
		}
		output.put("recipe", recipe.toString());

		String json = gson.toJson(output);
		File path = new File(recipePath);
		// create insight directory
		path.mkdirs();
		recipePath += "\\" + RECIPE_FILE;
		// create file
		File f = new File(recipePath);
		try {
			// write json to file
			FileUtils.writeStringToFile(f, json);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		return f;
	}
	
	public static File updateMosfitFile(File mosfetFile, String appId, String appName, String rdbmsID, String insightName, String layout, String imageFileName, String[] recipeToSave, boolean hidden) {
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		// format recipe file
		HashMap<String, Object> output = new HashMap<String, Object>();
		output.put(ENGINE_ID_KEY, appId);
		output.put(RDBMS_ID_KEY, rdbmsID);
		output.put(INSIGHT_NAME_KEY, insightName);
		output.put(LAYOUT_KEY, layout);
		output.put(HIDDEN_KEY, hidden);
		
		StringBuilder recipe = new StringBuilder();
		for (String pixel : recipeToSave) {
			recipe.append(pixel);
		}
		output.put("recipe", recipe.toString());

		String json = gson.toJson(output);
		mosfetFile.delete();
		try {
			mosfetFile.createNewFile();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		try {
			// write json to file
			FileUtils.writeStringToFile(mosfetFile, json);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		return mosfetFile;
	}
	
	public static File updateMosfitFileInsightName(File mosfetFile, String insightName) {
		// read the existing file
		Map output = getMosfitMap(mosfetFile);
		// override the insight name
		output.put(INSIGHT_NAME_KEY, insightName);
		
		// write as normal
		Gson gson = new GsonBuilder().setPrettyPrinting().create();

		String json = gson.toJson(output);
		mosfetFile.delete();
		try {
			mosfetFile.createNewFile();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		try {
			// write json to file
			FileUtils.writeStringToFile(mosfetFile, json);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		return mosfetFile;
	}
	
}

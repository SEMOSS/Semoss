package prerna.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.engine.api.IEngine;
import prerna.engine.impl.InsightAdministrator;
import prerna.om.Insight;
import prerna.solr.SolrIndexEngine;
import prerna.solr.SolrUtility;

public class MosfetSyncHelper {

	public static final String RECIPE_FILE = ".mosfet";

	// ADDED
	private static final String ADD = "ADD";
	// MODIFIED
	private static final String MOD = "MOD";
	// DELETE
	private static final String DEL = "DEL";
	// RENAMED
	private static final String REN = "REN";

	private static final String ENGINE_KEY = "engine";
	private static final String RDBMS_ID_KEY = "rdbmsId";
	private static final String INSIGHT_NAME_KEY = "insightName";
	private static final String LAYOUT_KEY = "layout";
	private static final String RECIPE_KEY = "recipe";
	private static final String IMAGE_KEY = "image";

	private static SolrIndexEngine solrE;
	
	static {
		try {
			solrE = SolrIndexEngine.getInstance();
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e) {
			// well, this sucks
		}
	}

	private MosfetSyncHelper() {

	}

	public static void synchronizeInsightChanges(Map<String, List<String>> filesChanged, Logger logger) {
		// keeping track of the solr documents I want to add
		List<SolrInputDocument> solrDocsToAdd = new Vector<SolrInputDocument>();
		// keeping track of solr documents to remove
		List<String> solrDocsToRemove = new Vector<String>();

		// process add
		if(filesChanged.containsKey(ADD)) {
			processAddedFiles(filesChanged.get(ADD), solrDocsToAdd, logger);
		}

		// process mod
		if(filesChanged.containsKey(MOD)) {
			processModifiedFiles(filesChanged.get(MOD), solrDocsToAdd, logger);
		}

		// TODO: how to handle rename
		//		// process ren
		//		if(filesChanged.containsKey(REN)) {
		//			processRenamed(filesChanged.get(REN));
		//		}

		// process delete
		if(filesChanged.containsKey(DEL)) {
			processDelete(filesChanged.get(DEL), solrDocsToRemove, logger);
		}

		// we store the solr results because solr indexing is slow
		// so we do it all at once here
		addToSolr(solrDocsToAdd);
		// add all the solr documents we need to process
		removeFromSolr(solrDocsToRemove);
	}

	private static void processAddedFiles(List<String> list, List<SolrInputDocument> solrDocsToAdd, Logger logger) {
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
			processAddedFile(mapData, getCurDateForFile(mosfetFile), solrDocsToAdd, logger);
		}
	}

	private static void processAddedFile(Map<String, Object> mapData, String lastModDate, List<SolrInputDocument> solrDocsToAdd, Logger logger) {
		String engineName = mapData.get(ENGINE_KEY).toString();
		String id = mapData.get(RDBMS_ID_KEY).toString();
		String name = mapData.get(INSIGHT_NAME_KEY).toString();
		String layout = mapData.get(LAYOUT_KEY).toString();
		String recipe = mapData.get(RECIPE_KEY).toString();
		String image = mapData.get(IMAGE_KEY).toString();

		// solr is simple
		// we just go through and add it
		// if it is an add/modify, we have the same steps
		addSolrDocToProcess(solrDocsToAdd, lastModDate, engineName, id, name, layout, image);

		// need to add the insight in the rdbms engine
		IEngine engine = Utility.getEngine(engineName);
		// we want to make sure the file isn't added because we made the insight
		// and is in fact a new one made by another collaborator
		Vector<Insight> ins = engine.getInsight(id);
		if(ins == null || ins.isEmpty() || (ins.size() == 1 && ins.get(0) == null) ) {
			addInsightToEngineRdbms(engine, id, name, layout, recipe);
		}
	}

	private static void processModifiedFiles(List<String> list, List<SolrInputDocument> solrDocsToAdd, Logger logger) {
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
			processModifiedFiles(mapData, getCurDateForFile(mosfetFile), solrDocsToAdd, logger);
		}
	}

	private static void processModifiedFiles(Map<String, Object> mapData, String lastModDate, List<SolrInputDocument> solrDocsToAdd, Logger logger) {
		String engineName = mapData.get(ENGINE_KEY).toString();
		String id = mapData.get(RDBMS_ID_KEY).toString();
		String name = mapData.get(INSIGHT_NAME_KEY).toString();
		String layout = mapData.get(LAYOUT_KEY).toString();
		String recipe = mapData.get(RECIPE_KEY).toString();
		String image = mapData.get(IMAGE_KEY).toString();

		// solr is simple
		// we just go through and add it
		// if it is an add/modify, we have the same steps
		addSolrDocToProcess(solrDocsToAdd, lastModDate, engineName, id, name, layout, image);

		// need to update the insight in the rdbms engine
		modifyInsightInEngineRdbms(engineName, id, name, layout, recipe);
	}

	private static void processDelete(List<String> list, List<String> solrDocsToRemove, Logger logger) {
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

			String engineName = mapData.get(ENGINE_KEY).toString();
			String id = mapData.get(RDBMS_ID_KEY).toString();
			processDelete(engineName, id, solrDocsToRemove);
		}
	}
	
	private static void processDelete(String engineName, String id, List<String> solrDocsToRemove) {
		// solr is simple
		// we just add the id to remove
		solrDocsToRemove.add(engineName + "__" + id);

		// need to delete the insight in the rdbms engine
		deleteInsightFromEngineRdbms(engineName, id);
	}

	private static void addSolrDocToProcess(List<SolrInputDocument> solrDocsToAdd, String lastModDate, String engineName, String id, String name, String layout, String image) {
		// if the solr is active...
		if (solrE != null && solrE.serverActive()) {
			// set all the users to be default...
			String userID = "default";

			Set<String> engineSet = new HashSet<String>();
			engineSet.add(engineName);

			// have all the relevant fields now, so store with appropriate schema name
			// create solr document and add into docs list
			Map<String, Object>  queryResults = new  HashMap<> ();
			queryResults.put(SolrIndexEngine.STORAGE_NAME, name);
			queryResults.put(SolrIndexEngine.CREATED_ON, lastModDate);
			queryResults.put(SolrIndexEngine.MODIFIED_ON, lastModDate);
			queryResults.put(SolrIndexEngine.USER_ID, userID);
			queryResults.put(SolrIndexEngine.ENGINES, engineSet);
			queryResults.put(SolrIndexEngine.CORE_ENGINE, engineName);
			queryResults.put(SolrIndexEngine.CORE_ENGINE_ID, id);
			queryResults.put(SolrIndexEngine.LAYOUT, layout);
			queryResults.put(SolrIndexEngine.VIEW_COUNT, 0);
			queryResults.put(SolrIndexEngine.IMAGE, "\\db\\" + engineName + "\\version\\" + id + "\\" + image);

			try {
				solrDocsToAdd.add(SolrUtility.createDocument(SolrIndexEngine.ID, engineName + "_" + id, queryResults));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private static void addInsightToEngineRdbms(IEngine engine, String id, String insightName, String layout, String recipe) {
		InsightAdministrator admin = new InsightAdministrator(engine.getInsightDatabase());
		// just put the recipe into an array
		String[] pixelRecipeToSave = new String[]{recipe};
		admin.addInsight(id, insightName, layout, pixelRecipeToSave );
	}

	private static void modifyInsightInEngineRdbms(String engineName, String id, String insightName, String layout, String recipe) {
		IEngine engine = Utility.getEngine(engineName);
		InsightAdministrator admin = new InsightAdministrator(engine.getInsightDatabase());
		// just put the recipe into an array
		String[] pixelRecipeToSave = new String[]{recipe};
		admin.updateInsight(id, insightName, layout, pixelRecipeToSave);
	}

	private static void deleteInsightFromEngineRdbms(String engineName, String id) {
		IEngine engine = Utility.getEngine(engineName);
		InsightAdministrator admin = new InsightAdministrator(engine.getInsightDatabase());
		admin.dropInsight(id);
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
	
	private static void removeFromSolr(List<String> solrDocsToRemove) {
		// remove all the solr documents we need to remove
		if(solrE != null && !solrDocsToRemove.isEmpty()) {
			try {
				solrE.removeInsight(solrDocsToRemove);
			} catch (SolrServerException | IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private static void addToSolr(List<SolrInputDocument> solrDocsToAdd) {
		if(solrE != null && !solrDocsToAdd.isEmpty()) {
			try {
				solrE.addInsights(solrDocsToAdd);
			} catch (SolrServerException | IOException e) {
				e.printStackTrace();
			}
		}
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
		String engineName = mapData.get(ENGINE_KEY).toString();
		
		String newRandomId = UUID.randomUUID().toString();

		// general structure is db / version / insight id / .mosfet
		// we have the .mosfet and want to go up to the version folder
		File versionDir = mosfetFile.getParentFile().getParentFile();
		// make a new directory for the insight
		String newInsightDirLoc = versionDir.getPath() + "/" + newRandomId;
		File newInsightDir = new File(newInsightDirLoc);
		newInsightDir.mkdirs();

		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		mapData.put(RDBMS_ID_KEY, newRandomId);
		mapData.put(INSIGHT_NAME_KEY, newInsightName);

		String json = gson.toJson(mapData);
		File newMosfetFile = new File(newInsightDirLoc + "/" + RECIPE_FILE);
		try {
			// write json to file
			FileUtils.writeStringToFile(newMosfetFile, json);
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		// now that we have the new one
		// let us add it
		List<SolrInputDocument> solrDocsToAdd = new Vector<SolrInputDocument>();
		processAddedFile(mapData, getCurDate(), solrDocsToAdd, logger);
		addToSolr(solrDocsToAdd);
		// we want to delete the old
		List<String> solrDocsToRemove = new Vector<String>();
		processDelete(engineName, origId, solrDocsToRemove);
		removeFromSolr(solrDocsToRemove);
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
	public static File makeMosfitFile(String engineName, String rdbmsID, String insightName, String layout, String imageFileName, String[] recipeToSave) {
		String recipePath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		recipePath += "\\" + Constants.DB + "\\" + engineName + "\\version\\" + rdbmsID;
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		// format recipe file
		HashMap<String, Object> output = new HashMap<String, Object>();
		output.put("engine", engineName);
		output.put("rdbmsId", rdbmsID);
		output.put("insightName", insightName);
		output.put("layout", layout);
		output.put("image", imageFileName);
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
	
	public static File updateMosfitFile(File mosfetFile, String engineName, String rdbmsID, String insightName, String layout, String imageFileName, String[] recipeToSave) {
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		// format recipe file
		HashMap<String, Object> output = new HashMap<String, Object>();
		output.put("engine", engineName);
		output.put("rdbmsId", rdbmsID);
		output.put("insightName", insightName);
		output.put("layout", layout);
		output.put("image", imageFileName);
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
	/**
	 * Get the last modified date for a file
	 * @param mosfetFile
	 * @return
	 */
	private static String getCurDateForFile(File mosfetFile) {
		return SolrIndexEngine.getDateFormat().format(new Date(mosfetFile.lastModified()));
	}

	/**
	 * Get the current date
	 * @return
	 */
	private static String getCurDate() {
		return SolrIndexEngine.getDateFormat().format(new Date());
	}
}

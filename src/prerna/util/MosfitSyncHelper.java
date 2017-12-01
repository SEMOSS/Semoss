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
import java.util.Vector;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.engine.api.IEngine;
import prerna.engine.impl.InsightAdministrator;
import prerna.solr.SolrIndexEngine;

public class MosfitSyncHelper {

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

	private Logger logger;
	private SolrIndexEngine solrE;
	
	public MosfitSyncHelper() {
		
	}
	
	public void setLogger(Logger logger) {
		this.logger = logger;
	}
	
	// keeping track of the solr documents I want to add
	private List<SolrInputDocument> solrDocsToAdd = new Vector<SolrInputDocument>();
	// keeping track of solr documents to remove
	private List<String> solrDocsToRemove = new Vector<String>();

	public void synchronizeInsightChanges(Map<String, List<String>> filesChanged) {
		// get the solr index engine
		try {
			this.solrE = SolrIndexEngine.getInstance();
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e1) {
			outputError("Could not establish connnection with solr");
			return;
		}
		
		// process add
		if(filesChanged.containsKey(ADD)) {
			processAddedFiles(filesChanged.get(ADD));
		}
		
		// process mod
		if(filesChanged.containsKey(MOD)) {
			processModifiedFiles(filesChanged.get(MOD));
		}
		
		// TODO: how to handle rename
//		// process ren
//		if(filesChanged.containsKey(REN)) {
//			processRenamed(filesChanged.get(REN));
//		}
		
		// process delete
		if(filesChanged.containsKey(DEL)) {
			processDelete(filesChanged.get(DEL));
		}
		
		// we store the solr results because solr indexing is slow
		// so we do it all at once here
		
		// add all the solr documents we need to process
		if(!this.solrDocsToAdd.isEmpty()) {
			try {
				this.solrE.addInsights(this.solrDocsToAdd);
			} catch (SolrServerException | IOException e) {
				e.printStackTrace();
			}
		}
		// remove all the solr documents we need to remove
		if(!this.solrDocsToRemove.isEmpty()) {
			try {
				this.solrE.removeInsight(this.solrDocsToRemove);
			} catch (SolrServerException | IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void processAddedFiles(List<String> list) {
		for(String fileLocation : list) {
			File mosfetFile = new File(fileLocation);
			Map<String, Object> mapData = getMosfitMap(mosfetFile);
			if(mapData == null) {
				outputError("MOSFET file is not in valid JSON format");
				continue;
			}
			
			String engineName = mapData.get(ENGINE_KEY).toString();
			String id = mapData.get(RDBMS_ID_KEY).toString();
			String name = mapData.get(INSIGHT_NAME_KEY).toString();
			String layout = mapData.get(LAYOUT_KEY).toString();
			String recipe = mapData.get(RECIPE_KEY).toString();
			
			// solr is simple
			// we just go through and add it
			// if it is an add/modify, we have the same steps
			addSolrDocToProcess(mosfetFile, engineName, id, name, layout);
			
			// need to add the insight in the rdbms engine
			addInsightToEngineRdbms(engineName, id, name, layout, recipe);
		}
	}
	
	private void processModifiedFiles(List<String> list) {
		for(String fileLocation : list) {
			File mosfetFile = new File(fileLocation);
			Map<String, Object> mapData = getMosfitMap(mosfetFile);
			if(mapData == null) {
				outputError("MOSFET file is not in valid JSON format");
				continue;
			}
			
			String engineName = mapData.get(ENGINE_KEY).toString();
			String id = mapData.get(RDBMS_ID_KEY).toString();
			String name = mapData.get(INSIGHT_NAME_KEY).toString();
			String layout = mapData.get(LAYOUT_KEY).toString();
			String recipe = mapData.get(RECIPE_KEY).toString();

			// solr is simple
			// we just go through and add it
			// if it is an add/modify, we have the same steps
			addSolrDocToProcess(mosfetFile, engineName, id, name, layout);
			
			// need to update the insight in the rdbms engine
			modifyInsightInEngineRdbms(engineName, id, name, layout, recipe);
		}
	}
	
	private void processDelete(List<String> list) {
		for(String fileLocation : list) {
			File mosfetFile = new File(fileLocation);
			Map<String, Object> mapData = getMosfitMap(mosfetFile);
			if(mapData == null) {
				outputError("MOSFET file is not in valid JSON format");
				continue;
			}
			
			String engineName = mapData.get(ENGINE_KEY).toString();
			String id = mapData.get(RDBMS_ID_KEY).toString();

			// solr is simple
			// we just add the id to remove
			this.solrDocsToRemove.add(engineName + "__" + id);
			
			// need to delete the insight in the rdbms engine
			deleteInsightFromEngineRdbms(engineName, id);
		}
	}
	
	private void addSolrDocToProcess(File mosfetFile, String engineName, String id, String name, String layout) {
		// if the solr is active...
		if (this.solrE.serverActive()) {
			// get the current date which will be used to store in "created_on" and "modified_on" fields within schema
			String currDate = SolrIndexEngine.getDateFormat().format(new Date(mosfetFile.lastModified()));
			// set all the users to be default...
			String userID = "default";

			Set<String> engineSet = new HashSet<String>();
			engineSet.add(engineName);

			// have all the relevant fields now, so store with appropriate schema name
			// create solr document and add into docs list
			Map<String, Object>  queryResults = new  HashMap<> ();
			queryResults.put(SolrIndexEngine.STORAGE_NAME, name);
			queryResults.put(SolrIndexEngine.CREATED_ON, currDate);
			queryResults.put(SolrIndexEngine.MODIFIED_ON, currDate);
			queryResults.put(SolrIndexEngine.USER_ID, userID);
			queryResults.put(SolrIndexEngine.ENGINES, engineSet);
			queryResults.put(SolrIndexEngine.CORE_ENGINE, engineName);
			queryResults.put(SolrIndexEngine.CORE_ENGINE_ID, id);
			queryResults.put(SolrIndexEngine.LAYOUT, layout);

			try {
				this.solrDocsToAdd.add(this.solrE.createDocument(engineName + "_" + id, queryResults));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	private void addInsightToEngineRdbms(String engineName, String id, String insightName, String layout, String recipe) {
		IEngine engine = Utility.getEngine(engineName);
		InsightAdministrator admin = new InsightAdministrator(engine.getInsightDatabase());
		// just put the recipe into an array
		String[] pixelRecipeToSave = new String[]{recipe};
		admin.addInsight(id, insightName, layout, pixelRecipeToSave );
	}
	
	private void modifyInsightInEngineRdbms(String engineName, String id, String insightName, String layout, String recipe) {
		IEngine engine = Utility.getEngine(engineName);
		InsightAdministrator admin = new InsightAdministrator(engine.getInsightDatabase());
		// just put the recipe into an array
		String[] pixelRecipeToSave = new String[]{recipe};
		admin.updateInsight(id, insightName, layout, pixelRecipeToSave);
	}
	
	private void deleteInsightFromEngineRdbms(String engineName, String id) {
		IEngine engine = Utility.getEngine(engineName);
		InsightAdministrator admin = new InsightAdministrator(engine.getInsightDatabase());
		admin.dropInsight(id);
	}
	
	private void outputError(String errorMessage) {
		if(logger != null) {
			logger.info("ERROR!!! " + errorMessage);
		}
	}
	
	public Map<String, Object> getMosfitMap(File mosfetFile) {
		Map<String, Object> mapData = null;
		try {
			mapData = new ObjectMapper().readValue(mosfetFile, Map.class);
		} catch(FileNotFoundException e) {
			outputError("MOSFET file could not be found at location: " + mosfetFile.getPath());
		} catch (IOException e) {
			outputError("MOSFET file is not in valid JSON format");
		}
		return mapData;
	}
	
}

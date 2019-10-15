package prerna.poi.main;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import prerna.auth.utils.SecurityInsightUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.MosfetFile;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.AssetUtility;
import prerna.util.MosfetSyncHelper;
import prerna.util.git.GitRepoUtils;
import prerna.util.git.GitUtils;

public class RDFEngineCreationHelper {

	private RDFEngineCreationHelper() {
		
	}
	
	/**
	 * Insert new insights that return a distinct list of each concept
	 * @param rdfEngine
	 * @param pixelNames
	 */
	public static void insertSelectConceptsAsInsights(IEngine rdfEngine, Set<String> pixelNames) {
		String appId = rdfEngine.getEngineId();
		InsightAdministrator admin = new InsightAdministrator(rdfEngine.getInsightDatabase());
		
		//determine the # where the new questions should start
		String insightName = ""; 
		String[] recipeArray = null;
		String layout = ""; 

		try {
			for(String pixelName : pixelNames) {
				insightName = "Show first 500 records from " + pixelName;
				layout = "Grid";
				recipeArray = new String[5];
				recipeArray[0] = "AddPanel(0);";
				recipeArray[1] = "Panel(0)|SetPanelView(\"visualization\");";
				recipeArray[2] = "CreateFrame(grid).as([FRAME]);";
				recipeArray[3] = "Database(\"" + appId + "\") | SelectTable(" + pixelName + ") | Limit(500) | Import();"; 
				recipeArray[4] = "Frame() | QueryAll() | AutoTaskOptions(panel=[\"0\"], layout=[\"GRID\"]) | Collect(500);";
				
				List<String> tags = new Vector<String>();
				tags.add("default");
				tags.add("preview");
				String description = "Preview of the concept " + pixelName + " and all of its properties";

				String insightId = admin.addInsight(insightName, layout, recipeArray);
				admin.updateInsightTags(insightId, tags);
				admin.updateInsightDescription(insightId, description);

				// write recipe to file
				try {
					MosfetSyncHelper.makeMosfitFile(appId, rdfEngine.getEngineName(), 
							insightId, insightName, layout, recipeArray, false, description, tags);
					// add the insight to git
					String gitFolder = AssetUtility.getAppAssetVersionFolder(rdfEngine.getEngineName(), appId);
					List<String> files = new Vector<>();
					files.add(insightId + "/" + MosfetFile.RECIPE_FILE);
					GitRepoUtils.addSpecificFiles(gitFolder, files);				
					GitRepoUtils.commitAddedFiles(gitFolder, GitUtils.getDateMessage("Saved "+ insightName +" insight on"));
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				// insight security
				SecurityInsightUtils.addInsight(appId, insightId, insightName, false, layout);
				SecurityInsightUtils.updateInsightTags(appId, insightId, tags);
				SecurityInsightUtils.updateInsightDescription(appId, insightId, description);
			}
		} catch(RuntimeException e) {
			System.out.println("caught exception while adding question.................");
			e.printStackTrace();
		}
	}
	
	/**
	 * Insert new insights that return a distinct list of each concept
	 * Will run a query to make sure we are not adding an insight that was previously added via autogeneration
	 * @param rdfEngine
	 * @param pixelNames
	 */
	public static void insertNewSelectConceptsAsInsights(IEngine rdfEngine, Set<String> pixelNames) {
		String appId = rdfEngine.getEngineId();
		RDBMSNativeEngine insightsDatabase = rdfEngine.getInsightDatabase();
		InsightAdministrator admin = new InsightAdministrator(insightsDatabase);
		
		//determine the # where the new questions should start
		String insightName = ""; 
		String[] recipeArray = null;
		String layout = "";

		String query = null;
		try {
			NEXT_CONCEPT : for(String pixelName : pixelNames) {
				// make sure insight doesn't already exist
				insightName = "Show first 500 records from " + pixelName;

				query = "select id from question_id where question_name='"+insightName+"'";
				IRawSelectWrapper containsIt = WrapperManager.getInstance().getRawWrapper(insightsDatabase, query);
				while(containsIt.hasNext()) {
					// this question already exists
					// just continue though the loop
					containsIt.next();
					continue NEXT_CONCEPT;
				}
				
				layout = "Grid";
				recipeArray = new String[5];
				recipeArray[0] = "AddPanel(0);";
				recipeArray[1] = "Panel(0)|SetPanelView(\"visualization\");";
				recipeArray[2] = "CreateFrame(grid).as([FRAME]);";
				recipeArray[3] = "Database(\"" + appId + "\") | SelectTable(" + pixelName + ") | Limit(500) | Import();"; 
				recipeArray[4] = "Frame() | QueryAll() | AutoTaskOptions(panel=[\"0\"], layout=[\"GRID\"]) | Collect(500);";
				
				List<String> tags = new Vector<String>();
				tags.add("default");
				tags.add("preview");
				String description = "Preview of the concept " + pixelName + " and all of its properties";

				String insightId = admin.addInsight(insightName, layout, recipeArray);
				admin.updateInsightTags(insightId, tags);
				admin.updateInsightDescription(insightId, description);

				//write recipe to file
				try {
					MosfetSyncHelper.makeMosfitFile(appId, rdfEngine.getEngineName(), 
							insightId, insightName, layout, recipeArray, false, description, tags);
					// add the insight to git
					String gitFolder = AssetUtility.getAppAssetVersionFolder(rdfEngine.getEngineName(), appId);
					List<String> files = new Vector<>();
					files.add(insightId + "/" + MosfetFile.RECIPE_FILE);
					GitRepoUtils.addSpecificFiles(gitFolder, files);				
					GitRepoUtils.commitAddedFiles(gitFolder, GitUtils.getDateMessage("Saved "+ insightName +" insight on"));		
				} catch (IOException e) {
					e.printStackTrace();
				}
					
				// insight security
				SecurityInsightUtils.addInsight(appId, insightId, insightName, false, layout); 
				SecurityInsightUtils.updateInsightTags(appId, insightId, tags);
				SecurityInsightUtils.updateInsightDescription(appId, insightId, description);
			}
		} catch(RuntimeException e) {
			System.out.println("caught exception while adding question.................");
			e.printStackTrace();
		}
	}

	/**
	 * Insert new insights specific for NLP
	 * @param rdfEngine
	 */
	public static void insertNLPDefaultQuestions(IEngine rdfEngine) {
		String engineName = rdfEngine.getEngineId();
		InsightAdministrator admin = new InsightAdministrator(rdfEngine.getInsightDatabase());

		//determine the # where the new questions should start
		String insightName = ""; 
		String[] recipeArray = new String[3];
		String layout = ""; 

		// q1
		insightName = "Show all roles to object";
		layout = "Grid";
		recipeArray = new String[5];
		recipeArray[0] = "AddPanel(0);";
		recipeArray[1] = "Panel(0)|SetPanelView(\"visualization\");";
		recipeArray[2] = "CreateFrame(grid).as([FRAME]);";
		recipeArray[3] = "Database(" + engineName + ") | Select(Subject, Object) | Join((Subject, inner.join, Object)) | Limit(500) | Import();"; 
		recipeArray[4] = "Frame() | Select(f$Subject, f$Object) | Format ( type = [ 'table' ] ) | TaskOptions({\"0\":{\"layout\":\"Grid\",\"alignment\":{\"label\":[\"Subject\",\"Object\"]}}}) | Collect(500);"; 
		admin.addInsight(insightName, layout, recipeArray);
		
		// q2
		insightName = "Show all objects to actions";
		layout = "Grid";
		recipeArray = new String[5];
		recipeArray[0] = "AddPanel(0);";
		recipeArray[1] = "Panel(0)|SetPanelView(\"visualization\");";
		recipeArray[2] = "CreateFrame(grid).as([FRAME]);";
		recipeArray[3] = "Database(" + engineName + ") | Select(Object, Predicate) | Join((Predicate, inner.join, Object)) | Limit(500) | Import();"; 
		recipeArray[4] = "Frame() | Select(f$Subject, f$Object) | Format ( type = [ 'table' ] ) | TaskOptions({\"0\":{\"layout\":\"Grid\",\"alignment\":{\"label\":[\"Object\",\"Predicate\"]}}}) | Collect(500);"; 
		admin.addInsight(insightName, layout, recipeArray);

		// q3
		insightName = "Show all roles to actions";
		layout = "Grid";
		recipeArray = new String[5];
		recipeArray[0] = "AddPanel(0);";
		recipeArray[1] = "Panel(0)|SetPanelView(\"visualization\");";
		recipeArray[2] = "CreateFrame(grid).as([FRAME]);";
		recipeArray[3] = "Database(" + engineName + ") | Select(Subject, Predicate) | Join((Subject, inner.join, Predicate)) | Limit(500) | Import();"; 
		recipeArray[4] = "Frame() | Select(f$Subject, f$Object) | Format ( type = [ 'table' ] ) | TaskOptions({\"0\":{\"layout\":\"Grid\",\"alignment\":{\"label\":[\"Subject\",\"Predicate\"]}}}) | Collect(500);"; 
		admin.addInsight(insightName, layout, recipeArray);

		// q4
		insightName = "Show all roles to actions and what they are acting on";
		layout = "Grid";
		recipeArray = new String[5];
		recipeArray[0] = "AddPanel(0);";
		recipeArray[1] = "Panel(0)|SetPanelView(\"visualization\");";
		recipeArray[2] = "CreateFrame(grid).as([FRAME]);";
		recipeArray[3] = "Database(" + engineName + ") | Select(Subject, Object, Predicate) | Join((Subject, inner.join, Predicate), (Predicate, inner.join, Object)) | Limit(500) | Import();"; 
		recipeArray[4] = "Frame() | Select(f$Subject, f$Object) | Format ( type = [ 'table' ] ) | TaskOptions({\"0\":{\"layout\":\"Grid\",\"alignment\":{\"label\":[\"Subject\",\"Predicate\",\"Object\"]}}}) | Collect(500);"; 
		admin.addInsight(insightName, layout, recipeArray);

		//TODO: there are more insights that i need to add from the Default_NLP_Questions.properties in the Default folder in db directory
	}

}

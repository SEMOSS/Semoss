package prerna.poi.main;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import prerna.auth.utils.SecurityInsightUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.MosfetFile;
import prerna.project.api.IProject;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.AssetUtility;
import prerna.util.MosfetSyncHelper;
import prerna.util.Utility;
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
	public static void insertSelectConceptsAsInsights(IProject project, IDatabaseEngine rdfEngine, Set<String> pixelNames) {
		String appId = rdfEngine.getEngineId();
		InsightAdministrator admin = new InsightAdministrator(project.getInsightDatabase());
		
		//determine the # where the new questions should start
		String insightName = ""; 
		List<String> recipeArray = null;
		String layout = ""; 

		try {
			for(String pixelName : pixelNames) {
				insightName = "Show first 500 records from " + pixelName;
				layout = "Grid";
				recipeArray = new Vector<>(5);
				recipeArray.add("AddPanel(0);");
				recipeArray.add("Panel(0)|SetPanelView(\"visualization\");");
				recipeArray.add("CreateFrame(grid).as([FRAME]);");
				recipeArray.add("Database(\"" + appId + "\") | SelectTable(" + pixelName + ") | Limit(500) | Import();"); 
				recipeArray.add("Frame() | QueryAll() | AutoTaskOptions(panel=[\"0\"], layout=[\"GRID\"]) | Collect(500);");
				
				boolean global = true;
				boolean cacheable = Utility.getApplicationCacheInsight();
				int cacheMinutes = Utility.getApplicationCacheInsightMinutes();
				boolean cacheEncrypt = Utility.getApplicationCacheEncrypt();
				String cacheCron = Utility.getApplicationCacheCron();
				ZonedDateTime cachedOn = null;
				
				List<String> tags = new Vector<String>();
				tags.add("default");
				tags.add("preview");
				String description = "Preview of the concept " + pixelName + " and all of its properties";
				String schemaName = SecurityInsightUtils.makeInsightSchemaNameUnique(project.getProjectId(), insightName);

				String insightId = admin.addInsight(insightName, layout, recipeArray, global, 
						cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, schemaName);
				admin.updateInsightTags(insightId, tags);
				admin.updateInsightDescription(insightId, description);

				// write recipe to file
				try {
					MosfetSyncHelper.makeMosfitFile(project.getProjectId(), project.getProjectName(), 
							insightId, insightName, layout, recipeArray, global, 
							cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, description, tags, schemaName);
					// add the insight to git
					String gitFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(), project.getProjectId());
					List<String> files = new Vector<>();
					files.add(insightId + "/" + MosfetFile.RECIPE_FILE);
					GitRepoUtils.addSpecificFiles(gitFolder, files);				
					GitRepoUtils.commitAddedFiles(gitFolder, GitUtils.getDateMessage("Saved "+ insightName +" insight on"));
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				// insight security
				SecurityInsightUtils.addInsight(project.getProjectId(), insightId, insightName, global, layout, 
						cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt,
						recipeArray, schemaName);
				SecurityInsightUtils.updateInsightTags(project.getProjectId(), insightId, tags);
				SecurityInsightUtils.updateInsightDescription(project.getProjectId(), insightId, description);
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
	public static void insertNewSelectConceptsAsInsights(IProject project, IDatabaseEngine rdfEngine, Set<String> pixelNames) {
		String appId = rdfEngine.getEngineId();
		RDBMSNativeEngine insightsDatabase = project.getInsightDatabase();
		InsightAdministrator admin = new InsightAdministrator(insightsDatabase);
		
		//determine the # where the new questions should start
		String insightName = ""; 
		List<String> recipeArray = null;
		String layout = "";

		String query = null;
		try {
			NEXT_CONCEPT : for(String pixelName : pixelNames) {
				// make sure insight doesn't already exist
				insightName = "Show first 500 records from " + pixelName;

				query = "select id from question_id where question_name='"+insightName+"'";
				IRawSelectWrapper containsIt = null;
				try {
					containsIt = WrapperManager.getInstance().getRawWrapper(insightsDatabase, query);
					while(containsIt.hasNext()) {
						// this question already exists
						// just continue though the loop
						containsIt.next();
						continue NEXT_CONCEPT;
					}
				} catch (Exception e1) {
					e1.printStackTrace();
				} finally {
					if(containsIt != null) {
						try {
							containsIt.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
				
				layout = "Grid";
				recipeArray = new Vector<>(5);
				recipeArray.add("AddPanel(0);");
				recipeArray.add("Panel(0)|SetPanelView(\"visualization\");");
				recipeArray.add("CreateFrame(grid).as([FRAME]);");
				recipeArray.add("Database(\"" + appId + "\") | SelectTable(" + pixelName + ") | Limit(500) | Import();");
				recipeArray.add("Frame() | QueryAll() | AutoTaskOptions(panel=[\"0\"], layout=[\"GRID\"]) | Collect(500);");
				
				boolean global = true;
				boolean cacheable = Utility.getApplicationCacheInsight();
				int cacheMinutes = Utility.getApplicationCacheInsightMinutes();
				boolean cacheEncrypt = Utility.getApplicationCacheEncrypt();
				String cacheCron = Utility.getApplicationCacheCron();
				ZonedDateTime cachedOn = null;
				
				List<String> tags = new Vector<String>();
				tags.add("default");
				tags.add("preview");
				String description = "Preview of the concept " + pixelName + " and all of its properties";
				String schemaName = SecurityInsightUtils.makeInsightSchemaNameUnique(project.getProjectId(), insightName);

				String insightId = admin.addInsight(insightName, layout, recipeArray, global, 
						cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, schemaName);
				admin.updateInsightTags(insightId, tags);
				admin.updateInsightDescription(insightId, description);

				//write recipe to file
				try {
					MosfetSyncHelper.makeMosfitFile(project.getProjectId(), project.getProjectName(), 
							insightId, insightName, layout, recipeArray, 
							global, cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt,
							description, tags, schemaName);
					// add the insight to git
					String gitFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(), project.getProjectId());
					List<String> files = new Vector<>();
					files.add(insightId + "/" + MosfetFile.RECIPE_FILE);
					GitRepoUtils.addSpecificFiles(gitFolder, files);				
					GitRepoUtils.commitAddedFiles(gitFolder, GitUtils.getDateMessage("Saved "+ insightName +" insight on"));		
				} catch (Exception e) {
					e.printStackTrace();
				}
					
				// insight security
				SecurityInsightUtils.addInsight(project.getProjectId(), insightId, insightName, 
						global, layout, cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, recipeArray, schemaName); 
				SecurityInsightUtils.updateInsightTags(project.getProjectId(), insightId, tags);
				SecurityInsightUtils.updateInsightDescription(project.getProjectId(), insightId, description);
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
	public static void insertNLPDefaultQuestions(IProject project, IDatabaseEngine rdfEngine) {
		String engineName = rdfEngine.getEngineId();
		InsightAdministrator admin = new InsightAdministrator(project.getInsightDatabase());

		//determine the # where the new questions should start
		String insightName = ""; 
		String[] recipeArray = new String[3];
		String layout = ""; 
		boolean hidden = false;
		boolean cacheable = Utility.getApplicationCacheInsight();
		int cacheMinutes = Utility.getApplicationCacheInsightMinutes();
		boolean cacheEncrypt = Utility.getApplicationCacheEncrypt();
		String cacheCron = Utility.getApplicationCacheCron();
		ZonedDateTime cachedOn = null;
		String schemaName = null;
		
		// q1
		insightName = "Show all roles to object";
		layout = "Grid";
		recipeArray = new String[5];
		recipeArray[0] = "AddPanel(0);";
		recipeArray[1] = "Panel(0)|SetPanelView(\"visualization\");";
		recipeArray[2] = "CreateFrame(grid).as([FRAME]);";
		recipeArray[3] = "Database(" + engineName + ") | Select(Subject, Object) | Join((Subject, inner.join, Object)) | Limit(500) | Import();"; 
		recipeArray[4] = "Frame() | Select(f$Subject, f$Object) | Format ( type = [ 'table' ] ) | TaskOptions({\"0\":{\"layout\":\"Grid\",\"alignment\":{\"label\":[\"Subject\",\"Object\"]}}}) | Collect(500);"; 
		schemaName = SecurityInsightUtils.makeInsightSchemaNameUnique(project.getProjectId(), insightName);
		admin.addInsight(insightName, layout, recipeArray, hidden, cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, schemaName);
		
		// q2
		insightName = "Show all objects to actions";
		layout = "Grid";
		recipeArray = new String[5];
		recipeArray[0] = "AddPanel(0);";
		recipeArray[1] = "Panel(0)|SetPanelView(\"visualization\");";
		recipeArray[2] = "CreateFrame(grid).as([FRAME]);";
		recipeArray[3] = "Database(" + engineName + ") | Select(Object, Predicate) | Join((Predicate, inner.join, Object)) | Limit(500) | Import();"; 
		recipeArray[4] = "Frame() | Select(f$Subject, f$Object) | Format ( type = [ 'table' ] ) | TaskOptions({\"0\":{\"layout\":\"Grid\",\"alignment\":{\"label\":[\"Object\",\"Predicate\"]}}}) | Collect(500);"; 
		schemaName = SecurityInsightUtils.makeInsightSchemaNameUnique(project.getProjectId(), insightName);
		admin.addInsight(insightName, layout, recipeArray, hidden, cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, schemaName);

		// q3
		insightName = "Show all roles to actions";
		layout = "Grid";
		recipeArray = new String[5];
		recipeArray[0] = "AddPanel(0);";
		recipeArray[1] = "Panel(0)|SetPanelView(\"visualization\");";
		recipeArray[2] = "CreateFrame(grid).as([FRAME]);";
		recipeArray[3] = "Database(" + engineName + ") | Select(Subject, Predicate) | Join((Subject, inner.join, Predicate)) | Limit(500) | Import();"; 
		recipeArray[4] = "Frame() | Select(f$Subject, f$Object) | Format ( type = [ 'table' ] ) | TaskOptions({\"0\":{\"layout\":\"Grid\",\"alignment\":{\"label\":[\"Subject\",\"Predicate\"]}}}) | Collect(500);"; 
		schemaName = SecurityInsightUtils.makeInsightSchemaNameUnique(project.getProjectId(), insightName);
		admin.addInsight(insightName, layout, recipeArray, hidden, cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, schemaName);

		// q4
		insightName = "Show all roles to actions and what they are acting on";
		layout = "Grid";
		recipeArray = new String[5];
		recipeArray[0] = "AddPanel(0);";
		recipeArray[1] = "Panel(0)|SetPanelView(\"visualization\");";
		recipeArray[2] = "CreateFrame(grid).as([FRAME]);";
		recipeArray[3] = "Database(" + engineName + ") | Select(Subject, Object, Predicate) | Join((Subject, inner.join, Predicate), (Predicate, inner.join, Object)) | Limit(500) | Import();"; 
		recipeArray[4] = "Frame() | Select(f$Subject, f$Object) | Format ( type = [ 'table' ] ) | TaskOptions({\"0\":{\"layout\":\"Grid\",\"alignment\":{\"label\":[\"Subject\",\"Predicate\",\"Object\"]}}}) | Collect(500);"; 
		schemaName = SecurityInsightUtils.makeInsightSchemaNameUnique(project.getProjectId(), insightName);
		admin.addInsight(insightName, layout, recipeArray, hidden, cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, schemaName);

		//TODO: there are more insights that i need to add from the Default_NLP_Questions.properties in the Default folder in db directory
	}

}

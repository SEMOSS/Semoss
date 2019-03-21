package prerna.poi.main;

import java.util.Set;

import prerna.auth.utils.SecurityInsightUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.InsightAdministrator;
import prerna.rdf.engine.wrappers.WrapperManager;

public class RDFEngineCreationHelper {

	private RDFEngineCreationHelper() {
		
	}
	
	/**
	 * Insert new insights that return a distinct list of each concept
	 * @param rdfEngine
	 * @param conceptualNames
	 */
	public static void insertSelectConceptsAsInsights(IEngine rdfEngine, Set<String> conceptualNames) {
		String engineId = rdfEngine.getEngineId();
		InsightAdministrator admin = new InsightAdministrator(rdfEngine.getInsightDatabase());
		
		//determine the # where the new questions should start
		String insightName = ""; 
		String[] recipeArray = null;
		String layout = ""; 

		try {
			for(String conceptualName : conceptualNames) {
				insightName = "Show first 500 records from " + conceptualName;
				layout = "Grid";
				recipeArray = new String[5];
				recipeArray[0] = "AddPanel(0);";
				recipeArray[1] = "Panel(0)|SetPanelView(\"visualization\");";
				recipeArray[2] = "CreateFrame(grid).as([FRAME]);";
				recipeArray[3] = "Database(\"" + engineId + "\") | Select(" + conceptualName + ") | Limit(500) | Import();"; 
				
				StringBuilder viewPixel = new StringBuilder("Frame() | Select(").append(conceptualName)
						.append(") | Format ( type = [ 'table' ] ) | TaskOptions({\"0\":{\"layout\":\"Grid\",\"alignment\":{\"label\":[\"")
						.append(conceptualName).append("\"").append("]}}}) | Collect(500);"); 
				recipeArray[4] = viewPixel.toString();
				String id = admin.addInsight(insightName, layout, recipeArray);
				SecurityInsightUtils.addInsight(engineId, id, insightName, false, layout); 
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
	 * @param conceptualNames
	 */
	public static void insertNewSelectConceptsAsInsights(IEngine rdfEngine, Set<String> conceptualNames) {
		String engineId = rdfEngine.getEngineId();
		IEngine insightsDatabase = rdfEngine.getInsightDatabase();
		InsightAdministrator admin = new InsightAdministrator(insightsDatabase);
		
		//determine the # where the new questions should start
		String insightName = ""; 
		String[] recipeArray = null;
		String layout = "";

		String query = null;
		try {
			NEXT_CONCEPT : for(String conceptualName : conceptualNames) {
				// make sure insight doesn't already exist
				insightName = "Show first 500 records from " + conceptualName;

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
				recipeArray[3] = "Database(\"" + engineId + "\") | Select(" + conceptualName + ") | Limit(500) | Import();"; 
				
				StringBuilder viewPixel = new StringBuilder("Frame() | Select(").append(conceptualName)
						.append(") | Format ( type = [ 'table' ] ) | TaskOptions({\"0\":{\"layout\":\"Grid\",\"alignment\":{\"label\":[\"")
						.append(conceptualName).append("\"").append("]}}}) | Collect(500);"); 
				recipeArray[4] = viewPixel.toString();
				String id = admin.addInsight(insightName, layout, recipeArray);
				SecurityInsightUtils.addInsight(engineId, id, insightName, false, layout); 
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

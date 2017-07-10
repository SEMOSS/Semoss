package prerna.poi.main;

import java.util.Set;

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
		String engineName = rdfEngine.getEngineName();
		InsightAdministrator admin = new InsightAdministrator(rdfEngine.getInsightDatabase());
		
		//determine the # where the new questions should start
		String insightName = ""; 
		String[] pkqlRecipeToSave = null;
		String layout = ""; 

		try {
			for(String conceptualName : conceptualNames) {
				insightName = "Select Distinct " + conceptualName;
				layout = "Grid";
				pkqlRecipeToSave = new String[3];
				pkqlRecipeToSave[0] = "data.frame('grid');";
				pkqlRecipeToSave[1] = "data.import(api:" + engineName + ".query([c:" + conceptualName + "]));";
				pkqlRecipeToSave[2] = "panel[0].viz ( Grid , [ ] , { 'offset' : 0 , 'limit' : 1000 } );";

				admin.addInsight(insightName, layout, pkqlRecipeToSave);
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
		String engineName = rdfEngine.getEngineName();
		IEngine insightsDatabase = rdfEngine.getInsightDatabase();
		InsightAdministrator admin = new InsightAdministrator(insightsDatabase);
		
		//determine the # where the new questions should start
		String insightName = ""; 
		String[] pkqlRecipeToSave = null;
		String layout = "";

		String query = null;
		try {
			for(String conceptualName : conceptualNames) {
				// make sure insight doesn't already exist
				insightName = "Select Distinct " + conceptualName;

				query = "select id where question_name='"+insightName+"'";
				IRawSelectWrapper containsIt = WrapperManager.getInstance().getRawWrapper(insightsDatabase, query);
				while(containsIt.hasNext()) {
					// this question already exists
					// just continue though the loop
					containsIt.next();
					continue;
				}
				
				layout = "Grid";
				pkqlRecipeToSave = new String[3];
				pkqlRecipeToSave[0] = "data.frame('grid');";
				pkqlRecipeToSave[1] = "data.import(api:" + engineName + ".query([c:" + conceptualName + "]));";
				pkqlRecipeToSave[2] = "panel[0].viz ( Grid , [ ] , { 'offset' : 0 , 'limit' : 1000 } );";

				admin.addInsight(insightName, layout, pkqlRecipeToSave);
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
		String engineName = rdfEngine.getEngineName();
		InsightAdministrator admin = new InsightAdministrator(rdfEngine.getInsightDatabase());

		//determine the # where the new questions should start
		String insightName = ""; 
		String[] pkqlRecipeToSave = new String[3];
		String layout = ""; 

		// q1
		insightName = "Show all roles to object";
		layout = "Grid";
		pkqlRecipeToSave = new String[3];
		pkqlRecipeToSave[0] = "data.frame('grid');";
		pkqlRecipeToSave[1] = "data.import(api:" + engineName + ".query([c:Subject, c:Object],([c:Subject, inner.join, c:Object])));";
		pkqlRecipeToSave[2] = "panel[0].viz ( Grid , [ ] , { 'offset' : 0 , 'limit' : 1000 } );";
		admin.addInsight(insightName, layout, pkqlRecipeToSave);

		
		// q2
		insightName = "Show all objects to actions";
		layout = "Grid";
		pkqlRecipeToSave = new String[3];
		pkqlRecipeToSave[0] = "data.frame('grid');";
		pkqlRecipeToSave[1] = "data.import(api:" + engineName + ".query([c:Object, c:Predicate],([c:Predicate, inner.join, c:Object])));";
		pkqlRecipeToSave[2] = "panel[0].viz ( Grid , [ ] , { 'offset' : 0 , 'limit' : 1000 } );";
		admin.addInsight(insightName, layout, pkqlRecipeToSave);

				
		// q3
		insightName = "Show all roles to actions";
		layout = "Grid";
		pkqlRecipeToSave = new String[3];
		pkqlRecipeToSave[0] = "data.frame('grid');";
		pkqlRecipeToSave[1] = "data.import(api:" + engineName + ".query([c:Subject, c:Predicate],([c:Subject, inner.join, c:Predicate])));";
		pkqlRecipeToSave[2] = "panel[0].viz ( Grid , [ ] , { 'offset' : 0 , 'limit' : 1000 } );";
		admin.addInsight(insightName, layout, pkqlRecipeToSave);

		// q4
		insightName = "Show all roles to actions and what they are acting on";
		layout = "Grid";
		pkqlRecipeToSave = new String[3];
		pkqlRecipeToSave[0] = "data.frame('grid');";
		pkqlRecipeToSave[1] = "data.import(api:" + engineName + ".query([c:Subject, c:Predicate, c:Object],([c:Subject, inner.join, c:Predicate], [c:Predicate, inner.join, c:Object], [c:Object, inner.join, c:Subject])));";
		pkqlRecipeToSave[2] = "panel[0].viz ( Grid , [ ] , { 'offset' : 0 , 'limit' : 1000 } );";
		admin.addInsight(insightName, layout, pkqlRecipeToSave);
		
		//TODO: there are more insights that i need to add from the Default_NLP_Questions.properties in the Default folder in db directory
	}

}

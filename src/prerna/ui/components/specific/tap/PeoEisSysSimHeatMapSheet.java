package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.Hashtable;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.DIHelper;

public class PeoEisSysSimHeatMapSheet extends SysSimHeatMapSheet {
	ArrayList<ArrayList<Object>> sourceFileSelection = null;
	
	@Override
	public void createData()
	{
		if (!(this.query).equals("NULL") || this.query.isEmpty()) {
			/*super.createData();
			list = this.dataFrame.getRawData();
			names = this.dataFrame.getColumnHeaders();
			
			if (list!=null && list.isEmpty()) {
				Utility.showError("Query returned no results.");
				return;
			}*/
			sourceFileSelection = runQuery(this.engine.getEngineId(), this.query, true);
		}
		SimilarityFunctions sdf = new SimilarityFunctions();
		if(this.pane!=null)
			addPanel();
		Hashtable dataBLUCompleteHash = new Hashtable();
		//Gets the overall list of systems
		updateProgressBar("10%...Getting all systems for evaluation", 10);
		String defaultSystemsQuery = "SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?System <http://semoss.org/ontologies/Relation/ReportedIn> ?Source}{?System <http://semoss.org/ontologies/Relation/Contains/NeedsAssessment> 'Yes'}}";
		defaultSystemsQuery = addBindings(defaultSystemsQuery);
		comparisonObjectList = sdf.createComparisonObjectList(this.engine.getEngineId(), defaultSystemsQuery);
		sdf.setComparisonObjectList(comparisonObjectList);
		
		//Query to return a list of categories
		String allCategoriesQuery = "SELECT DISTINCT ?Category WHERE { {?Category <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CRA_Category>}}";
		ArrayList<ArrayList<Object>> categoryList = runQuery(this.engine.getEngineId(), allCategoriesQuery, false);

		//For each category, query the data (System and associated category value) and send to the param data hash for processing
		for (int i = 0; i < categoryList.size(); i++) {
			ArrayList<Object> row = categoryList.get(i);
			String category = (String) row.get(0);
			String query = "SELECT DISTINCT ?System ?Value WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}{?System <http://semoss.org/ontologies/Relation/ReportedIn> ?Source}{?System <http://semoss.org/ontologies/Relation/Contains/NeedsAssessment> 'Yes'}{?CRA_Value <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CRA_Value>} {?System <http://semoss.org/ontologies/Relation/HasCRA> ?CRA_Value} {?CRA_Value <http://semoss.org/ontologies/Relation/Contains/Value> ?Value} {?CRA_Category <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CRA_Category>}{?CRA_Category <http://semoss.org/ontologies/Relation/Contain> ?CRA_Value} BIND (<http://semoss.org/ontologies/Concept/CRA_Category/@CRA_Category@> AS ?CRA_Category) }";
			query = query.replace("@CRA_Category@", category);
			query = addBindings(query);
			Hashtable catHash = sdf.compareObjectParameterScore(this.engine.getEngineId(), query, SimilarityFunctions.VALUE);
			catHash = processHashForCharting(catHash);
			updateProgressBar("...Evaluating Data", 20 + i*2);
			if (catHash != null && !catHash.isEmpty()) {
				paramDataHash.put(category, catHash);
			}
		}
		
		boolean allQueriesAreEmpty = true;
		updateProgressBar("80%...Creating Heat Map Visualization", 80);
		
		allHash.put("title",  "System Similarity");
		allHash.put("xAxisTitle", comparisonObjectTypeX);
		allHash.put("yAxisTitle", comparisonObjectTypeY);
		allHash.put("value", "Score");
		allHash.put("sysDup", true);

	}
	
	public String addBindings(String sysSimQuery) {
		//BK: Set this up to pass in selection of Source File via the param drop down
		String defaultBindings = "BINDINGS ?Source {(<http://semoss.org/ontologies/Concept/SourceFile/PEO_EIS>)}";		
			
		//If a query is not specifed, append the default SystemUser bindings
		if ((this.query).equals("NULL") || (this.query).equals("null") || (this.query).equals("Null") || this.query == null) {
			sysSimQuery = sysSimQuery + defaultBindings;
		}
		//if a query is specified, bind the system list to the system similarity query.
		else {
			//only create the bindings string once
			if (createSystemBindings == true) {
				String systemURIs = "{";
				for( int i = 0; i < sourceFileSelection.size(); i++) {
					ArrayList<Object> values = sourceFileSelection.get(i);
					String system = "";
					for (Object systemResult : values) {
						system = "(<" + systemResult.toString() + ">)";
					}
					systemURIs = systemURIs + system;
				}
				systemURIs = systemURIs + "}";
				//systemListBindings = systemListBindings + systemURIs;
				systemListBindings = "BINDINGS ?Source " + systemURIs;
				createSystemBindings = false;
			}
			sysSimQuery = sysSimQuery + systemListBindings;
		}
		
		return sysSimQuery;		
	}
	
	/**
	 * Runs a query on a specific database and returns the result as an ArrayList
	 * @param engineName 	String containing the name of the database engine to be queried
	 * @param query 		String containing the SPARQL query to run
	 * @return list 		ArrayList<ArrayList<Object>> containing the results of the query 
	 */
	public ArrayList runQuery(String engineName, String query, boolean raw) {
		IDatabaseEngine engine = (IDatabaseEngine) DIHelper.getInstance().getLocalProp(engineName);

		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
		
		String[] names = wrapper.getVariables();
		ArrayList<ArrayList<Object>> list = new ArrayList<ArrayList<Object>>();
		while (wrapper.hasNext()) {
			ISelectStatement sjss = wrapper.next();
			ArrayList<Object> values = new ArrayList<Object>();
			for (int colIndex = 0; colIndex < names.length; colIndex++) {
				if (sjss.getVar(names[colIndex]) != null) {
					if (sjss.getVar(names[colIndex]) instanceof Double) {
						if (raw) {
							values.add(colIndex, sjss.getRawVar(names[colIndex]));
						} else values.add(colIndex, (Double) sjss.getVar(names[colIndex]));
					}
					else {
						if (raw) {
							values.add(colIndex, sjss.getRawVar(names[colIndex]));					
						} else values.add(colIndex, (String) sjss.getVar(names[colIndex]));
					}
				}
			}
			list.add(values);
		}

		return list;
	}
}

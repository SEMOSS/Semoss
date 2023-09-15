package prerna.ui.components.specific.ousd;

import java.util.Map;

import prerna.annotations.BREAKOUT;
import prerna.ui.components.playsheets.GridPlaySheet;

@BREAKOUT
public class BudgetInflationPlaySheet extends GridPlaySheet{

	private static Double inflationRate = 0.0;
	
	@Override
	public void createData(){
		//execute queries and pass the results into conversion methods: 
		//may want one method for each of the following:
		//put SourceTarget-FiscalYear into a map
		//put System-Budget in a map<string, map<string, string>> that maps a system to multiple years.
		
		//query for the System-Budget(s): SELECT DISTINCT ?System ?SystemBudgetGLItem ?Budget WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?SystemBudgetGLItem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemBudgetGLItem>}{?Cost <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FiscalYear>}{?System <http://semoss.org/ontologies/Relation/Has> ?SystemBudgetGLItem}{?SystemBudgetGLItem <http://semoss.org/ontologies/Relation/Contains/Cost> ?Budget}}
		//query for SourceTarget-FiscalYear: SELECT DISTINCT ?System ?SourceTarget ?FiscalYear WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?SourceTarget <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SourceTarget>}{?FiscalYear <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FiscalYear>}{?System <http://semoss.org/ontologies/Relation/TransitionsFrom> ?SourceTarget}{?SourceTarget <http://semoss.org/ontologies/Relation/TransitionsIn> ?FiscalYear}}

		//pass maps into the costCalculator method for processing
	}
	
	@Override
	public void setQuery(String query){
		//break up the query string into the query for system-budget(s), and SourceTarget-FiscalYear
		//set to class variables
	}
	
	private void createTable(){
		//takes results and turns them into output
	}
	
//	@Override
//	public Hashtable getData(){
//		//sets up the results for the web
//		return null;
//	}

	//main logic. creates a map that maps a system to a mapping of years-budget
	private Map<String, Map<String, String>> costCalculator(){
		//getting System-Budget, SourceTarget-FiscalYear maps
		//run SourceTarget list into systemYearFinder
		
		//for each system in System-Budget, if SourceTarget-FiscalYear map contains the system, we have the fiscalYear and can mark all previous years as -
		//then for each subsequent year we take the budget from the previous year and add in the inflationRate. then set the value in the map.
		return null;
	}
	
	private Map<String, String> systemYearFinder(){
		//takes the SourceTarget-FiscalYear and removes the Target from SourceTarget leaving just the source systems. update map and return map
		return null;
	}
	
}

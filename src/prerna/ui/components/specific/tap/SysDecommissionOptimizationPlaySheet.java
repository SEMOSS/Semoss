package prerna.ui.components.specific.tap;

import java.util.ArrayList;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SysDecommissionOptimizationPlaySheet extends GridPlaySheet{

	public int resource;
	public double time;
	
	@Override
	public void createData() {
		
		String db = "TAP_Core_Data";
		String sysQuery = "SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> }}";
		String dataQuery = "SELECT DISTINCT ?Data WHERE { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}}";

		ArrayList<String> sysList = runListQuery(db, sysQuery);
		ArrayList<String> dataList = runListQuery(db, dataQuery);

		
		
		SysDecommissionOptimizationFunctions optFunctions = new SysDecommissionOptimizationFunctions();
		
		names = new String[6];
		names[0] = "System";
		names[1] ="Time to Transform";
		names[2] = "Number of Sites Deployed At";
		names[3] = "Resource Allocation";
		names[4] = "Number of Systems Transformed Simultaneously";
		names[5] = "Total Cost for System";
//		names[5] = "Min time for system";
		
		optFunctions.setSysList(sysList);
		optFunctions.setDataList(dataList);;
		if(query.equals("Constrain Resource"))
		{
			optFunctions.resourcesConstraint = resource;
			optFunctions.optimizeTime();

		}
		else
		{
			optFunctions.timeConstraint = time;
			optFunctions.optimizeResource();
		}

		list = optFunctions.outputList;
	}

	/**
	 * Runs a query on a specific engine to make a list of systems to report on
	 * @param engineName 	String containing the name of the database engine to be queried
	 * @param query 		String containing the SPARQL query to run
	 */
	public ArrayList<String> runListQuery(String engineName, String query) {
		ArrayList<String> list = new ArrayList<String>();
		try {
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
	
			SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
			wrapper.setQuery(query);
			wrapper.setEngine(engine);
			wrapper.executeQuery();
	
			String[] names = wrapper.getVariables();
			while (wrapper.hasNext()) {
				SesameJenaSelectStatement sjss = wrapper.next();
				list.add((String) sjss.getVar(names[0]));
				}
		} catch (Exception e) {
			Utility.showError("Cannot find engine: "+engineName);
		}
		return list;
	}
	
	
	public void runPlaySheet(String typeConstraint,int resource, double time) {
		query = typeConstraint;
		this.resource = resource;
		this.time = time;
		createData();
		runAnalytics();
		createView();
	}

}

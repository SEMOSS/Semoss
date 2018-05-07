package prerna.ui.components.specific.iatdd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import prerna.ui.components.playsheets.BrowserPlaySheet;

public class AOAQueuingDashboard extends BrowserPlaySheet {

	private static final String COST_QUERY = " SELECT DISTINCT ?Vendor ?Package ?License ?Hardware ?Deployment ?Maintenance ?Modification "
			+ "WHERE {{?Vendor <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Vendor>;}"
			+ "{?Package <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Package>;}"
			+ "{?Vendor <http://semoss.org/ontologies/Relation/Supports> ?Package;}"
			+ "{?Package <http://semoss.org/ontologies/Relation/Contains/License> ?License;}"
			+ "{?Package <http://semoss.org/ontologies/Relation/Contains/Hardware> ?Hardware;}"
			+ "{?Package <http://semoss.org/ontologies/Relation/Contains/Deployment> ?Deployment;}"
			+ "{?Package <http://semoss.org/ontologies/Relation/Contains/Modification> ?Modification;}"
			+ "{?Package <http://semoss.org/ontologies/Relation/Contains/Maintenance> ?Maintenance;} }";

	private static final String OVERALL_SCORE_QUERY = " SELECT DISTINCT ?Vendor ?Package ?Requirement ?Package_Requirement ?Fulfillment_Def ?Fulfillment_Score ?And_Row ?AND_Package ?OR_Package "
			+"WHERE{"
			+"{?Vendor <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Vendor>;}" 
			+"{?Package <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Package>;}" 
			+"{?supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports> ;}" 
			+"{?Vendor ?supports ?Package;}" 
			+"{?Product <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Product>;}" 
			+"{?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has> ;}"
			+"{?Package ?has ?Product}"
			+"{?Package_Requirement <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Package_Requirement>;}" 
			+"{?fulfills <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Fulfills> ;}" 
			+"{?Package ?fulfills ?Package_Requirement}"
			+"{?Package_Requirement <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Package_Requirement>;}" 
			+"{?Requirement <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Requirement>;}"
			+"{?fulfills2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Fulfills> ;}" 
			+"{?Package_Requirement ?fulfills2 ?Requirement}"
			+"OPTIONAL{{?DependsOn <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DependsOn>;}"
			+"{?AND_Package <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/AND_Package>;}" 
			+"{?Package_Requirement ?DependsOn ?AND_Package}}"
			+"{?has2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has> ;}"
			+"{?Fulfillment_Def <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Fulfillment_Def>;}"
			+"{?Fulfillment_Def <http://semoss.org/ontologies/Relation/Contains/Fulfillment_Score> ?Fulfillment_Score;}" 
			+"{?Package_Requirement ?has2 ?Fulfillment_Def}"
			+"{?Package_Requirement<http://semoss.org/ontologies/Relation/Contains/And_Row> ?And_Row;}"
//			+"#{?DependsOn2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DependsOn>;}"
//			+"#{?OR_Package <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/OR_Package>;}" 
//			+"#{?Package_Requirement ?DependsOn2 ?OR_Package}"
			+ "}";
	
	private static final String HEAT_MAP_QUERY = "SELECT DISTINCT ?Vendor ?Package ?Product ?Mission_Task (AVG(?Air_Force_Service_Score) AS ?Air_Force_Service_Score)"
			 +"(AVG(?Army_Service_Score) AS ?Army_Service_Score) (AVG(?Navy_Service_Score) AS ?Navy_Service_Score) (AVG(?Fulfillment_Score) AS ?Fulfillment_Score) WHERE{"
			 +"{?Vendor <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Vendor>;}"
			 +"{?Package <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Package>;}" 
			 +"{?Vendor <http://semoss.org/ontologies/Relation/Supports> ?Package;}" 
			 +"{?Product <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Product>;}" 
			 +"{?Package <http://semoss.org/ontologies/Relation/Has> ?Product;}" 
			 +"{?Package_Requirement <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Package_Requirement>;}" 
			 +"{?Package <http://semoss.org/ontologies/Relation/Fulfills> ?Package_Requirement;}" 
			 +"{?Sub_Level_Requirement <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Requirement>;}"
			 +"{?fufills2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Fulfills> ;}" 
			 +"{?Package_Requirement ?fufills2 ?Sub_Level_Requirement;}" 
			 +"{?has2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has> ;}"
			 +"{?Fulfillment_Def <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Fulfillment_Def>;}"
			 +"{?Package_Requirement ?has2 ?Fulfillment_Def}"
			 +"{?Fulfillment_Def <http://semoss.org/ontologies/Relation/Contains/Fulfillment_Score> ?Fulfillment_Score;}" 
			 +"{?Sub_Level_Requirement <http://semoss.org/ontologies/Relation/Contains/Air_Force_Service_Score> ?Air_Force_Service_Score;}" 
			 +"{?Sub_Level_Requirement <http://semoss.org/ontologies/Relation/Contains/Navy_Service_Score> ?Navy_Service_Score;}" 
			 +"{?Sub_Level_Requirement <http://semoss.org/ontologies/Relation/Contains/Army_Service_Score> ?Army_Service_Score;}" 
			 +"{?Functional_Requirement <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Functional_Requirement>;}" 
			 +"{?Sub_Level_Requirement <http://semoss.org/ontologies/Relation/Supports> ?Functional_Requirement;}" 
			 +"{?Mission_Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Mission_Task>;}" 
			 +"{?Functional_Requirement <http://semoss.org/ontologies/Relation/Supports> ?Mission_Task;}" 
			 +"{?Mission_Task <http://semoss.org/ontologies/Relation/Contains/Ranking_Score> ?Ranking_Score;}"
			 +"} GROUP BY ?Vendor ?Package ?Product ?Mission_Task" ;

	private AOACostGridPlaySheet costData;
	private AOAHeatMapPlaySheet heatmap;
	private AOAOverallScoreGridPlaySheet overallsheet;
	private AOACostSavingsPlaySheet costSavingsData;

	private ArrayList<Object> costHash;
	private ArrayList<Object> costHeaders;
	private ArrayList<Object> heatHash;
	private ArrayList<Object> heatHeaders;
	private ArrayList<Object> overallHash;
	private ArrayList<Object> overallHeaders;	
	private ArrayList<Object> costSavingsHash;
	private ArrayList<Object> costSavingsHeaders;
	private Hashtable heatMapTableAlign;
	private Hashtable costTableAlign;
	private Map productScoreHeatMapHash;
	private Map overallScoreHash;
	private Map packages;
	private Map requirements;
	private Map stakeholders;
	
	@Override
	public void createData() { //createData...
		costData = new AOACostGridPlaySheet();
		costData.setRDFEngine(engine);
		costData.setQuery(COST_QUERY);
		costData.createData();
		costData.runAnalytics();
		costHash = (ArrayList<Object>) costData.getData(); //Store Data
		costHeaders = new ArrayList<Object>();
		costHeaders.add("Vendor");
		costHeaders.add("License");
		costHeaders.add("Hardware");
		costHeaders.add("Deployment");
		costHeaders.add("Maintenance");
		costHeaders.add("Modification");

		heatmap = new AOAHeatMapPlaySheet();
		heatmap.setRDFEngine(engine);
		heatmap.setQuery(HEAT_MAP_QUERY);
		heatmap.createData();
		heatmap.runDefaultRankingScore();
		heatmap.runAnalytics();
		heatmap.processQueryData();
		heatHash =(ArrayList<Object>) heatmap.getData();
		heatHeaders = new ArrayList<Object>();
		heatHeaders.add("Package");
		heatHeaders.add("Mission Task");
		heatHeaders.add("Product Score");
		heatMapTableAlign = new Hashtable();
		heatMapTableAlign.put("x", "Package");
		heatMapTableAlign.put("y", "Mission_Task");
		heatMapTableAlign.put("heat", "Package Score");
		//productScoreHeatMapHash = (Map) heatmap.getData();

		overallsheet = new AOAOverallScoreGridPlaySheet();
		overallsheet.setRDFEngine(engine);
		overallsheet.setQuery(OVERALL_SCORE_QUERY);
		overallsheet.createData();
		//overallsheet.setCheckedPackages(packages);
		overallsheet.runDefaultRankingScore();
		overallsheet.runReqtoMissionTask();
		overallsheet.runReqtoStakeHolderScore();
		overallsheet.runAnalytics();
		overallsheet.processQueryData();
		overallHash =(ArrayList<Object>) overallsheet.getData();
		overallHeaders = new ArrayList<Object> ();
		overallHeaders.add("Vendor");
		overallHeaders.add("Score");
		
/*		costSavingsData = new AOACostSavingsPlaySheet();
		costSavingsData.setRDFEngine(engine);
		costSavingsData.createData();
		costSavingsData.runAnalytics();
		costSavingsData.processQueryData();
		costSavingsHash = (ArrayList<Object>) costSavingsData.getData(); //Store Data
		costSavingsHeaders = new ArrayList<Object>();
		costSavingsHeaders.add("Vendor");
		costSavingsHeaders.add("Year");
		costSavingsHeaders.add("Hardware");
		costSavingsHeaders.add("Software");
		costSavingsHeaders.add("Deployment");
		costSavingsHeaders.add("Maintenance");
		costSavingsHeaders.add("Modification");
		costSavingsHeaders.add("Total");
		costTableAlign = new Hashtable();
		costTableAlign.put("label", "Year");
		costTableAlign.put("value 1", "Hardware");
		costTableAlign.put("value 2", "Software");
		costTableAlign.put("value 3", "Deployment");
		costTableAlign.put("value 4", "Maintenance");
		costTableAlign.put("value 5", "Modification");
		costTableAlign.put("value 7", "Total");
*/		
	}
	
	@Override
	public Map<String, Object> getDataMakerOutput(String... selectors){
		Hashtable returnData = new Hashtable();
		Hashtable[] charts = new Hashtable[3];
		Hashtable<String, ArrayList<Object>> cost = new Hashtable<String, ArrayList<Object>>();
		cost.put("data", costHash);
		cost.put("headers", costHeaders);
		charts[0] = cost;
		Hashtable<String, Object> heat = new Hashtable<String, Object>();
		heat.put("data", heatHash);
		heat.put("headers", heatHeaders);
		heat.put("dataTableAlign", heatMapTableAlign);
		heat.put("title", "Product Score Heatmap");
		charts [1] = heat;
		Hashtable<String, ArrayList<Object>> overall = new Hashtable<String, ArrayList<Object>>();
		overall.put("data", overallHash);
		overall.put("headers", overallHeaders);
		charts [2] = overall;
/*		Hashtable<String, Object> costSavings = new Hashtable<String, Object>();
		costSavings.put("data", costSavingsHash);
		costSavings.put("headers", costSavingsHeaders);
		costSavings.put("dataTableAlign", costTableAlign);
		costSavings.put("title", "Cost Savings");
		charts[3] = costSavings;
*/		Hashtable inner = new Hashtable();
		inner.put("charts", charts);
		//TODO: delete this, use layout keyword which is appended to object sent to front end
		returnData.put("layout", "prerna.ui.components.specific.iatdd.AOAQueuingDashboard");
		returnData.put("playsheet", "prerna.ui.components.specific.iatdd.AOAQueuingDashboard");
		returnData.put("data", inner);
		returnData.put("title", "Queuing Dashboard");
		returnData.put("engineName", engine.getEngineName());
		return returnData;
		
	}
	
	public Hashtable updateInfo(Map<String, Object> userInputValues) { //get user input values 
		Map<String, String> packagesSelected = (Map<String, String>) userInputValues.get("packages");
		Map<String, Double> requirementsSelected = (Map<String, Double>) userInputValues.get("requirements");
		Map<String, Boolean> stakeholdersSelected = (Map<String, Boolean>) userInputValues.get("stakeholders");
		String savingsToggle = (String) userInputValues.get("savingsToggle");

		Map<String, String> requirementsSelectedString = (Map<String, String>)  new HashMap<String, String>();
		for (String keys: requirementsSelected.keySet()){
			requirementsSelectedString.put(keys, requirementsSelected.get(keys).toString());
		}
		
		costData.setCheckedPackages(packagesSelected);
		costData.runAnalytics();
		costHash = (ArrayList<Object>) costData.getData();

		heatmap.setRankings(requirementsSelectedString);
		heatmap.setStakeHolders(stakeholdersSelected);
		heatmap.runAnalytics();
		heatmap.processQueryData();
		heatHash = (ArrayList<Object>) heatmap.getData();

		overallsheet.setCheckedPackages(packagesSelected);
		overallsheet.setRankings(requirementsSelectedString);
		overallsheet.setStakeHolders(stakeholdersSelected);
		overallsheet.runAnalytics();
		overallsheet.processQueryData();
		overallHash = (ArrayList<Object>) overallsheet.getData();
		
/*		costSavingsData.setCheckedPackages(packagesSelected);
		costSavingsData.runAnalytics();
		costSavingsHash = (ArrayList<Object>) costSavingsData.getData();
*/
		return (Hashtable) getDataMakerOutput(); //returnData to Front-End again 
	}

}


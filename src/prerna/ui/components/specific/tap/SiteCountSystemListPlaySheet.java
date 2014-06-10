package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.Hashtable;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SiteCountSystemListPlaySheet extends GridPlaySheet {

	private String GET_SYSTEM_SITE_COUNT = "SELECT DISTINCT ?System (COUNT(?SystemDCSite) AS ?SiteCount) (SAMPLE(?DCSite) AS ?ExampleSite) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>;} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?System  <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite} {?SystemDCSite  <http://semoss.org/ontologies/Relation/DeployedAt> ?DCSite;} } GROUP BY ?System";
	private String siteEngineName = "TAP_Site_Data";
	
	@Override
	public void createData() {
		list = new ArrayList<Object[]>();
		Hashtable<String, Hashtable<String, Object>> siteData = new Hashtable<String, Hashtable<String, Object>>();
		siteData = runSiteQuery(); 
		if(siteData.keySet().size() != 0) {
			list = processQuery(siteData);
		}
	}
	
	private ArrayList<Object[]> processQuery(Hashtable<String, Hashtable<String, Object>> siteData) {
		ArrayList<Object[]> newList = new ArrayList<Object[]>();

		logger.info("PROCESSING QUERY: " + query);
		SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		//run the query against the engine provided
		sjsw.setEngine(engine);
		sjsw.setQuery(query);
		sjsw.executeQuery();
		
		names = sjsw.getVariables();
		
		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			String sys = sjss.getVar(names[0]).toString();
			String des = sjss.getVar(names[1]).toString();
			String prob = sjss.getVar(names[2]).toString();
			prob = prob.replace("\"", "");
			
			if(prob.equals("High") || prob.equals("Question")) {
				prob = "High";
			} else {
				prob = "Low";
			}
			
			if(siteData.containsKey(sys))
			{
				newList.add(new Object[]{prob, sys, des, siteData.get(sys).get("Count"), siteData.get(sys).get("Site")});
			}
		}
		
		String[] newNames = new String[]{"Probability", "System", "Descriptioin", "Count", "ExampleSite"};
		names = newNames;
		
		return newList;
	}

	private Hashtable<String, Hashtable<String, Object>> runSiteQuery() {
		Hashtable<String, Hashtable<String, Object>> siteData = new Hashtable<String, Hashtable<String, Object>>();
		
		logger.info("PROCESSING QUERY: " + GET_SYSTEM_SITE_COUNT);
		SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		//run the query against the site engine provided
		IEngine siteEngine = (IEngine) DIHelper.getInstance().getLocalProp(siteEngineName);
		
		if(siteEngine != null)
		{
			sjsw.setEngine(siteEngine);
			sjsw.setQuery(GET_SYSTEM_SITE_COUNT);
			sjsw.executeQuery();
			
			names = sjsw.getVariables();
			
			while(sjsw.hasNext())
			{
				SesameJenaSelectStatement sjss = sjsw.next();
				String sys = sjss.getVar(names[0]).toString();
				Object count = sjss.getVar(names[1]);
				String site = sjss.getVar(names[2]).toString();
				
				Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
				innerHash.put("Count", count);
				innerHash.put("Site", site);
				siteData.put(sys, innerHash);
			}
			
			return siteData;
		} else {
			Utility.showError("Cannot find TAP_Site_Data database");
		}
		
		return siteData;
	}


}

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

	private String GET_SYSTEM_SITE_COUNT = "SELECT DISTINCT ?System (COUNT(?SystemDCSite) AS ?SiteCount) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?DeployedAt <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?System ?DeployedAt ?SystemDCSite} } GROUP BY ?System";
	private String siteEngineName = "TAP_Site_Data";
	
	@Override
	public void createData() {
		list = new ArrayList<Object[]>();
		Hashtable<String, Object> siteData = new Hashtable<String, Object>();
		siteData = runSiteQuery(); 
		if(siteData.keySet().size() != 0) {
			list = processQuery(siteData);
		}
	}
	
	private ArrayList<Object[]> processQuery(Hashtable<String, Object> siteData) {
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
			
			if(siteData.containsKey(sys))
			{
				newList.add(new Object[]{sys, siteData.get(sys)});
			}
		}
		
		// add the new column in output to the names array
		String[] newNames = new String[]{"System", "Count"};
		names = newNames;
		
		return newList;
	}

	private Hashtable<String, Object> runSiteQuery() {
		Hashtable<String, Object> siteData = new Hashtable<String, Object>();
		
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
				Object cost = sjss.getVar(names[1]);
				
				siteData.put(sys, cost);
			}
			
			return siteData;
		} else {
			Utility.showError("Cannot find TAP_Site_Data database");
		}
		
		return siteData;
	}


}

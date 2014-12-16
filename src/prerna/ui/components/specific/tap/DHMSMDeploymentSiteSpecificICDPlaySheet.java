package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.playsheets.DualEngineGridPlaySheet;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.ArrayListUtilityMethods;
import prerna.util.ArrayUtilityMethods;
import prerna.util.DIHelper;

public class DHMSMDeploymentSiteSpecificICDPlaySheet extends GridPlaySheet {
	
	HashMap<String, ArrayList<String>> siteData;
	Set<String> centrallyDeployedSystems;
	
	
	@Override
	public void createData() {
		ArrayList<Object[]> retList = new ArrayList<Object[]>();
		
		DualEngineGridPlaySheet degp = new DualEngineGridPlaySheet();
		degp.setQuery(query);
		degp.createData();
		ArrayList<Object[]> combinedResults = degp.getList();
		names = degp.getNames();
		
		getSysData();
		Set<String> icdList = new HashSet<String>();
		Iterator<Object[]> resultIterator = combinedResults.iterator();
		while(resultIterator.hasNext()) {
			Object[] values = resultIterator.next();
			String site = values[0].toString();
			ArrayList<String> sysList = siteData.get(site);
			sysList.add("DHMSM");
			String sys1 = values[1].toString();
			String sys2 = values[2].toString();
			String icd = values[3].toString();
			if( (sysList.contains(sys1) && sysList.contains(sys2)) || 
					(centrallyDeployedSystems.contains(sys1) || centrallyDeployedSystems.contains(sys2)) ) {
				if(!icdList.contains(icd)) {
					// in order to not repeat ICDs
					icdList.add(icd);
					retList.add(values);
				}
			}
		}
		
		list = ArrayListUtilityMethods.removeColumnFromList(retList, 1);
		list = ArrayListUtilityMethods.removeColumnFromList(list, 1);
		names = ArrayUtilityMethods.removeNameFromList(names, 1);
		names = ArrayUtilityMethods.removeNameFromList(names, 1);
		
		if(list == null) {
			list = new ArrayList<Object[]>();
		}
		if(names == null) {
			names = new String[]{};
		}
	}
	
	public void getSysData() {
		if(siteData == null) {
			siteData = DHMSMDeploymentHelper.getSysAtSitesInDeploymentPlan(engine);
		}
		if(centrallyDeployedSystems == null) {
			IEngine hrCore = (IEngine) DIHelper.getInstance().getLocalProp("HR_Core");
			centrallyDeployedSystems = DHMSMDeploymentHelper.getCentrallyDeployedSystems(hrCore);
		}
	}
}

/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.ui.components.specific.tap;

import java.util.List;

import prerna.ds.rdbms.h2.H2Frame;
import prerna.ui.components.playsheets.DualEngineGridPlaySheet;
import prerna.ui.components.playsheets.GridPlaySheet;

public class DHMSMDeploymentGapAnalysis extends GridPlaySheet{

	private final String sitesInPlanWithHPSystemQuery = "SELECT DISTINCT ?HostSite ?System WHERE { {?Region <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Region>} {?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?Region <http://semoss.org/ontologies/Relation/Deploys> ?Wave} {?HostSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?Wave <http://semoss.org/ontologies/Relation/Contains> ?HostSite} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?HostSite} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite} } ORDER BY ?Region ?Wave ?System &SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> }{?System <http://semoss.org/ontologies/Relation/Contains/Disposition> 'High'}{?System <http://semoss.org/ontologies/Relation/Contains/Device> 'N'} {?System <http://semoss.org/ontologies/Relation/Contains/Review_Status> ?Review_Status}FILTER (?Review_Status in('FAC_Approved','FCLG_Approved'))} ORDER BY ?System&false&false";
	private final String sitesWithHPSystemQuery = "SELECT DISTINCT ?HostSite ?System WHERE { {?HostSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?HostSite} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite} } ORDER BY ?System &SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> }{?System <http://semoss.org/ontologies/Relation/Contains/Disposition> 'High'}{?System <http://semoss.org/ontologies/Relation/Contains/Device> 'N'} {?System <http://semoss.org/ontologies/Relation/Contains/Review_Status> ?Review_Status}FILTER (?Review_Status in('FAC_Approved','FCLG_Approved'))} ORDER BY ?System&false&false";
	
	private String mainEngineName = "TAP_Core_Data";
	private String siteEngineName = "TAP_Site_Data";
	
	public void setEngineNames(String mainEngineName, String siteEngineName) {
		this.mainEngineName = mainEngineName;
		this.siteEngineName = siteEngineName;
	}
	
	@Override
	public void createData() {
		DualEngineGridPlaySheet dugp1 = new DualEngineGridPlaySheet();
		dugp1.setQuery(siteEngineName + "&" + mainEngineName + "&" + sitesInPlanWithHPSystemQuery);
		dugp1.createData();
		List<Object[]> sitesInPlanWithHPSystems = dugp1.getList();
		
		DualEngineGridPlaySheet dugp2 = new DualEngineGridPlaySheet();
		dugp2.setQuery(siteEngineName + "&" + mainEngineName + "&" + sitesWithHPSystemQuery);
		dugp2.createData();
		List<Object[]> sitesWithHPSystems = dugp2.getList();
		
		String[] names = new String[]{"HostSite","System"};
		this.dataFrame = new H2Frame(names);
		
		int i;
		int j;
		int allSiteWithHPListSize = sitesWithHPSystems.size();
		int allSiteInPlanWithHPListSize = sitesInPlanWithHPSystems.size();
		for(i = 0; i < allSiteWithHPListSize; i++) {
			Object[] values1 = sitesWithHPSystems.get(i);
			String dcSite1 = values1[0].toString();
			String sys1 = values1[1].toString();
			
			boolean match = false;
			for(j = 0; j < allSiteInPlanWithHPListSize; j++) {
				Object[] values2 = sitesInPlanWithHPSystems.get(j);
				String dcSite2 = values2[0].toString();
				String sys2 = values2[1].toString();
				
				if(dcSite1.equals(dcSite2) && sys1.equals(sys2)) {
					match = true;
					break;
				}
			}
			
			if(!match) {
				dataFrame.addRow(new Object[]{values1[0], values1[1]}, names);
			}
		}
	}
	
	public List<Object[]> getList() {
		return dataFrame.getData();
	}
}
/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.HashMap;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.playsheets.DualEngineGridPlaySheet;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SavingsPerFiscalYearBySystemPlaySheet extends GridPlaySheet {


	private final String query1 = "SELECT DISTINCT ?System (SUM(?Cost) AS ?Sustainment) ?FY WHERE { { BIND(<http://health.mil/ontologies/Concept/GLTag/O&M_Total> AS ?OMTag) {?OMTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?SystemBudget <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemBudgetGLItem>} {?System <http://semoss.org/ontologies/Relation/Has> ?SystemBudget} {?SystemBudget <http://semoss.org/ontologies/Relation/TaggedBy> ?OMTag} {?SystemBudget <http://semoss.org/ontologies/Relation/Contains/Cost> ?Cost} {?FY <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag>} {?SystemBudget <http://semoss.org/ontologies/Relation/OccursIn> ?FY} } UNION { BIND(<http://health.mil/ontologies/Concept/GLTag/O&M_Total> AS ?OMTag) {?OMTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?GLTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?SystemBudget <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemBudgetGLItem>} {?System <http://semoss.org/ontologies/Relation/Has> ?SystemBudget} {?SystemBudget <http://semoss.org/ontologies/Relation/TaggedBy> ?GLTag} {?OMTag <http://semoss.org/ontologies/Relation/Includes> ?GLTag} {?SystemBudget <http://semoss.org/ontologies/Relation/Contains/Cost> ?Cost} {?FY <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag>} {?SystemBudget <http://semoss.org/ontologies/Relation/OccursIn> ?FY} } UNION { BIND(<http://health.mil/ontologies/Concept/GLTag/O&M_Total> AS ?OMTag) {?OMTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?SystemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>} {?SystemServiceBudget <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemServiceBudgetGLItem>} {?System <http://semoss.org/ontologies/Relation/ConsistsOf> ?SystemService} {?SystemService <http://semoss.org/ontologies/Relation/Has> ?SystemServiceBudget} {?SystemServiceBudget <http://semoss.org/ontologies/Relation/TaggedBy> ?OMTag} {?SystemServiceBudget <http://semoss.org/ontologies/Relation/Contains/Cost> ?Cost} {?FY <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag>} {?SystemServiceBudget <http://semoss.org/ontologies/Relation/OccursIn> ?FY} } UNION { BIND(<http://health.mil/ontologies/Concept/GLTag/O&M_Total> AS ?OMTag) {?OMTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?GLTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?SystemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>} {?SystemServiceBudget <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemServiceBudgetGLItem>} {?System <http://semoss.org/ontologies/Relation/ConsistsOf> ?SystemService} {?SystemService <http://semoss.org/ontologies/Relation/Has> ?SystemServiceBudget} {?SystemServiceBudget <http://semoss.org/ontologies/Relation/TaggedBy> ?GLTag} {?OMTag <http://semoss.org/ontologies/Relation/Includes> ?GLTag} {?SystemServiceBudget <http://semoss.org/ontologies/Relation/Contains/Cost> ?Cost} {?FY <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag>} {?SystemServiceBudget <http://semoss.org/ontologies/Relation/OccursIn> ?FY} } } GROUP BY ?System ?FY ORDER BY ?System";
	private final String query2 = "SELECT ?System (COUNT(?DCSite) AS ?Sites) WHERE { {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite} {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?DCSite} } GROUP BY ?System";
	private final String query3 = "SELECT DISTINCT ?Region ?Date WHERE {{?Region <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Region>}{?Date <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year-Quarter>} {?Region <http://semoss.org/ontologies/Relation/BeginsOn> ?Date} }";
	private final String query4 = "SELECT DISTINCT ?Wave1 ?Wave2 WHERE {{?Wave1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?Wave2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?Wave1 <http://semoss.org/ontologies/Relation/Preceeds> ?Wave2}} ";
	private final String query5 = "TAP_Site_Data&HR_Core&SELECT DISTINCT ?Region ?Wave ?DCSite ?System WHERE {{?Region <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Region>} {?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?Region <http://semoss.org/ontologies/Relation/Deploys> ?Wave} {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?Wave <http://semoss.org/ontologies/Relation/Contains> ?DCSite} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?DCSite} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite} }&SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> }{?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability}}BINDINGS ?Probability {('High') ('Question')}&false&false";
	private final String engineName1 = "TAP_Portfolio";
	private final String engineName2 = "TAP_Site_Data";
	private IEngine engine1;
	private IEngine engine2;
	private DualEngineGridPlaySheet dualQueries = new DualEngineGridPlaySheet();	
	private HashMap<String, Double[]> query1Data = new HashMap<String, Double[]>();
	private HashMap<String, Double> query2Data = new HashMap<String, Double>();
	private HashMap<String, String> query3Data = new HashMap<String, String>();
	private HashMap<String, String> query4Data = new HashMap<String, String>();
	private ArrayList<Object[]> list;//holds the results of the dual query
	
	@Override
	public void createData(){
		runAllQueries();
		processData();
	}
	
	private void processData() {
		
		
	}

	public void runAllQueries() {
		this.engine1 = (IEngine) DIHelper.getInstance().getLocalProp(engineName1);
		this.engine2 = (IEngine) DIHelper.getInstance().getLocalProp(engineName2);
		
		Double sustCost;
		
		if(query1Data.isEmpty()){
			SesameJenaSelectWrapper sjsw = Utility.processQuery(engine1, query1);
			String[] names1 = sjsw.getVariables();
			while(sjsw.hasNext()) {
				SesameJenaSelectStatement sjss = sjsw.next();
				String sysName = sjss.getVar(names1[0]).toString();
				if(sjss.getVar(names1[1]).toString().equals("")){
					sustCost = 0.0;
				} else {
					sustCost = Double.parseDouble(sjss.getVar(names1[1]).toString());
				}
				String fiscalYear = sjss.getVar(names1[2]).toString();;
				// order fiscal year information
				int position = 0;
				switch(fiscalYear) {
					case "FY15" : position = 0; break;
					case "FY16" : position = 1; break;
					case "FY17" : position = 2; break;
					case "FY18" : position = 3; break;
					case "FY19" : position = 4; break;
				}
				Double [] sysSustCost;
				if(query1Data.containsKey(sysName)) {
					sysSustCost = query1Data.get(sysName);
					sysSustCost[position] = sustCost;
				} else {
					sysSustCost = new Double[5];
					sysSustCost[position] = sustCost;
					query1Data.put(sysName, sysSustCost);
				}
			}
		}
		
		if(query2Data.isEmpty()) {
			SesameJenaSelectWrapper sjsw = Utility.processQuery(engine2, query2);
			String[] names = sjsw.getVariables();
			while(sjsw.hasNext()) {
				SesameJenaSelectStatement sjss = sjsw.next();
				String sysName = sjss.getVar(names[0]).toString();
				Double siteCount = (Double) sjss.getVar(names[1]);
				query2Data.put(sysName, siteCount);
			}
		}
		
		if(query3Data.isEmpty()) {
			SesameJenaSelectWrapper sjsw = Utility.processQuery(engine2, query3);
			String[] names = sjsw.getVariables();
			while(sjsw.hasNext()) {
				SesameJenaSelectStatement sjss = sjsw.next();
				String regionName = sjss.getVar(names[0]).toString();
				String regionStartDate = sjss.getVar(names[1]).toString();
				query3Data.put(regionName, regionStartDate);
			}
		}
		
		if(query4Data.isEmpty()) {
			SesameJenaSelectWrapper sjsw = Utility.processQuery(engine2, query4);
			String[] names = sjsw.getVariables();
			while(sjsw.hasNext()) {
				SesameJenaSelectStatement sjss = sjsw.next();
				String wave1 = sjss.getVar(names[0]).toString();
				String wave2 = sjss.getVar(names[1]).toString();
				query4Data.put(wave1, wave2);
			}
		}
		
		//Use Dual Engine Grid to process the dual query that gets cost info
		dualQueries.setQuery(query5);
		dualQueries.createData();
		list = dualQueries.getList();
	}
	
	
	@Override
	public void createView() {
		Utility.showMessage("Success!");
	}
}


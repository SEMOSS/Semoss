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

import prerna.ui.components.playsheets.DualEngineGridPlaySheet;
import prerna.ui.components.playsheets.GridPlaySheet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.StringTokenizer;

import javax.swing.JDesktopPane;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SavingsPerFiscalYearBySystemPlaySheet extends GridPlaySheet {


	private final String query1 = "SELECT DISTINCT ?System (SUM(?Cost) AS ?Sustainment) ?FY WHERE { { BIND(<http://health.mil/ontologies/Concept/GLTag/O&M_Total> AS ?OMTag) {?OMTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?SystemBudget <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemBudgetGLItem>} {?System <http://semoss.org/ontologies/Relation/Has> ?SystemBudget} {?SystemBudget <http://semoss.org/ontologies/Relation/TaggedBy> ?OMTag} {?SystemBudget <http://semoss.org/ontologies/Relation/Contains/Cost> ?Cost} {?FY <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag>} {?SystemBudget <http://semoss.org/ontologies/Relation/OccursIn> ?FY} } UNION { BIND(<http://health.mil/ontologies/Concept/GLTag/O&M_Total> AS ?OMTag) {?OMTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?GLTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?SystemBudget <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemBudgetGLItem>} {?System <http://semoss.org/ontologies/Relation/Has> ?SystemBudget} {?SystemBudget <http://semoss.org/ontologies/Relation/TaggedBy> ?GLTag} {?OMTag <http://semoss.org/ontologies/Relation/Includes> ?GLTag} {?SystemBudget <http://semoss.org/ontologies/Relation/Contains/Cost> ?Cost} {?FY <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag>} {?SystemBudget <http://semoss.org/ontologies/Relation/OccursIn> ?FY} } UNION { BIND(<http://health.mil/ontologies/Concept/GLTag/O&M_Total> AS ?OMTag) {?OMTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?SystemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>} {?SystemServiceBudget <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemServiceBudgetGLItem>} {?System <http://semoss.org/ontologies/Relation/ConsistsOf> ?SystemService} {?SystemService <http://semoss.org/ontologies/Relation/Has> ?SystemServiceBudget} {?SystemServiceBudget <http://semoss.org/ontologies/Relation/TaggedBy> ?OMTag} {?SystemServiceBudget <http://semoss.org/ontologies/Relation/Contains/Cost> ?Cost} {?FY <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag>} {?SystemServiceBudget <http://semoss.org/ontologies/Relation/OccursIn> ?FY} } UNION { BIND(<http://health.mil/ontologies/Concept/GLTag/O&M_Total> AS ?OMTag) {?OMTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?GLTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?SystemService <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemService>} {?SystemServiceBudget <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemServiceBudgetGLItem>} {?System <http://semoss.org/ontologies/Relation/ConsistsOf> ?SystemService} {?SystemService <http://semoss.org/ontologies/Relation/Has> ?SystemServiceBudget} {?SystemServiceBudget <http://semoss.org/ontologies/Relation/TaggedBy> ?GLTag} {?OMTag <http://semoss.org/ontologies/Relation/Includes> ?GLTag} {?SystemServiceBudget <http://semoss.org/ontologies/Relation/Contains/Cost> ?Cost} {?FY <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag>} {?SystemServiceBudget <http://semoss.org/ontologies/Relation/OccursIn> ?FY} } } GROUP BY ?System ?FY ORDER BY ?System";
	private final String query2 = "SELECT ?System (COUNT(?DCSite) AS ?Sites) WHERE { {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite} {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?DCSite} } GROUP BY ?System";
	private final String query3 = "TAP_Site_Data&HR_Core&SELECT DISTINCT ?Wave ?DCSite ?System WHERE {{?Region <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Region>} {?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?Region <http://semoss.org/ontologies/Relation/Deploys> ?Wave} {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} {?Wave <http://semoss.org/ontologies/Relation/Contains> ?DCSite} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>} {?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?DCSite} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite} }&SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> }{?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability}}BINDINGS ?Probability {('High') ('Question')}&false&false";
	private final String engineName1 = "TAP_Portfolio";
	private final String engineName2 = "TAP_Site_Data";
	private IEngine engine1;
	private IEngine engine2;
	private DualEngineGridPlaySheet dualQueries = new DualEngineGridPlaySheet();	
	private HashMap<Object, ArrayList<Object>> dataHash1 = new HashMap<Object, ArrayList<Object>>();
	private HashMap<Object, ArrayList<String>> dataHash2 = new HashMap<Object, ArrayList<String>>();
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
		
		//Use Dual Engine Grid to process the dual query that gets cost info
		dualQueries.setQuery(query3);
		dualQueries.createData();
		list = dualQueries.getList();
		
		if(dataHash1.isEmpty()){
			SesameJenaSelectWrapper sjsw = Utility.processQuery(engine1, query1);
			String[] names1 = sjsw.getVariables();
			while(sjsw.hasNext()) {
				SesameJenaSelectStatement sjss = sjsw.next();
				String sysName = sjss.getVar(names1[0]).toString();
				Double sustCost = (double) sjss.getVar(names1[1]);
				String fiscalYear = sjss.getVar(names1[2]).toString();;
				ArrayList<Object> sysSustCost;
				if(dataHash1.containsKey(sysName)) {
					sysSustCost = dataHash1.get(sysName);
					sysSustCost.add(sustCost);
					sysSustCost.add(fiscalYear);
				} else {
					sysSustCost = new ArrayList<Object>();
					sysSustCost.add(sustCost);
					sysSustCost.add(fiscalYear);
					dataHash1.put(sysName, sysSustCost);
				}
			}
		}
		
		if(dataHash2.isEmpty()) {
			SesameJenaSelectWrapper sjsw = Utility.processQuery(engine2, query2);
			String[] names = sjsw.getVariables();
			while(sjsw.hasNext()) {
				SesameJenaSelectStatement sjss = sjsw.next();
				String sysName = sjss.getVar(names[0]).toString();
				String siteCount = sjss.getVar(names[1]).toString();
				ArrayList<String> waveList;
				if(dataHash2.containsKey(sysName)) {
					waveList = dataHash2.get(sysName);
					waveList.add(siteCount);
				} else {
					waveList = new ArrayList<String>();
					waveList.add(siteCount);
					dataHash2.put(sysName, waveList);
				}
			}
		}
	}
	@Override
	public void createView() {
		Utility.showMessage("Success!");
	}
}


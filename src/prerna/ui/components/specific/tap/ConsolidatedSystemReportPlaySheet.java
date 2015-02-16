/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
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

import java.util.ArrayList;
import java.util.Hashtable;

import javax.swing.JFrame;
import javax.swing.JOptionPane;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.error.EngineException;
import prerna.poi.specific.ConsolidatedSystemReportWriter;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.BooleanProcessor;
import prerna.ui.components.UpdateProcessor;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;


@SuppressWarnings("serial")
public class ConsolidatedSystemReportPlaySheet extends GridPlaySheet {

	static final Logger logger = LogManager.getLogger(ConsolidatedSystemReportPlaySheet.class.getName());
	
	private static String modernizationProp = "InterfaceModernizationCost";
	
	private String checkModPropQuery = "ASK WHERE { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} BIND(<http://semoss.org/ontologies/Relation/Contains/" + modernizationProp + "> AS ?contains) {?p ?contains ?prop ;} }";
	private String modPropDeleteQuery = "DELETE { ?system ?contains ?prop } WHERE { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} BIND(<http://semoss.org/ontologies/Relation/Contains/" + modernizationProp + "> AS ?contains) {?p ?contains ?prop ;} }";
	
	private String lpiSystemListQuery = "SELECT DISTINCT ?entity WHERE { {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?entity <http://semoss.org/ontologies/Relation/Contains/Received_Information> 'Y'} {?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability} {?entity <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'Y' }} ORDER BY ?entity BINDINGS ?Probability {('Low')('Medium')('Medium-High')}";
	private String lpniSystemListQuery = "SELECT DISTINCT ?entity WHERE { {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?entity <http://semoss.org/ontologies/Relation/Contains/Received_Information> 'Y'} {?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability} {?entity <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'N' }} ORDER BY ?entity BINDINGS ?Probability {('Low')('Medium')('Medium-High')}";
	private String systemOwnerQuery = "SELECT DISTINCT ?system (GROUP_CONCAT(DISTINCT ?Owner; SEPARATOR = ', ') AS ?sys_owner) WHERE { SELECT DISTINCT ?system (SUBSTR(STR(?owner),50) AS ?Owner) WHERE { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?owner <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemOwner>} {?system <http://semoss.org/ontologies/Relation/OwnedBy> ?owner} } BINDINGS ?system {@BINDINGS_STRING@} } GROUP BY ?system";
	private String systemBudgetQuery = "SELECT DISTINCT ?system ?year ?cost WHERE { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?systembudget <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemBudgetGLItem>} {?system <http://semoss.org/ontologies/Relation/Has> ?systembudget} {?year <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag> } {?systembudget <http://semoss.org/ontologies/Relation/OccursIn> ?year} {?systembudget <http://semoss.org/ontologies/Relation/Contains/Cost> ?cost} } BINDINGS ?system {@BINDINGS_STRING@}";
	private String systemInterfaceModCostQuery = "SELECT DISTINCT ?system ?cost WHERE { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?system <http://semoss.org/ontologies/Relation/Contains/InterfaceModernizationCost> ?cost} } BINDINGS ?system {@BINDINGS_STRING@}";
	private String systemHWSWcostQuery = "SELECT ?System (SUM(COALESCE(?swTotalCost, 0) + COALESCE( ?hwTotalCost, 0)) AS ?total) WHERE { { SELECT DISTINCT ?System (COALESCE(?unitcost*?Quantity,0) AS ?swTotalCost) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?SoftwareModule <http://semoss.org/ontologies/Relation/Contains/Quantity> ?Quantity} {?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>} {?SoftwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareModule>} {?TypeOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TypeOf>} {?SoftwareVersion <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareVersion>} {?System ?Has ?SoftwareModule} {?SoftwareModule ?TypeOf ?SoftwareVersion} {?SoftwareVersion <http://semoss.org/ontologies/Relation/Contains/Price> ?unitcost} } } UNION  {SELECT DISTINCT ?System (COALESCE(?unitcost*?Quantity,0) AS ?hwTotalCost) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?HardwareModule <http://semoss.org/ontologies/Relation/Contains/Quantity> ?Quantity} {?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>} {?HardwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HardwareModule>} {?TypeOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TypeOf>} {?HardwareVersion <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HardwareVersion>} {?System ?Has ?HardwareModule} {?HardwareModule ?TypeOf ?HardwareVersion } {?HardwareVersion <http://semoss.org/ontologies/Relation/Contains/Price> ?unitcost} } } } GROUP BY ?System BINDINGS ?System {@BINDINGS_STRING@}";	
	private String systemDIACAPQuery = "SELECT DISTINCT ?system ?ato WHERE { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?system <http://semoss.org/ontologies/Relation/Contains/ATO_Date> ?ato} } BINDINGS ?system {@BINDINGS_STRING@}";
	
	ArrayList<String> lpiSystemList = new ArrayList<String>();
	ArrayList<String> lpniSystemList = new ArrayList<String>();
	Hashtable<String, Object> ownerHashtable = new Hashtable<String, Object>(); //systemName -> owner
	Hashtable<String, Hashtable<String, Double>> budgetHashtable = new Hashtable<String, Hashtable<String, Double>>(); //systemName -> year -> budget
	Hashtable<String, Object> hwswHashtable = new Hashtable<String, Object>(); //systemName -> cost
	Hashtable<String, Object> interfaceModHashtable = new Hashtable<String, Object>(); //systemName -> cost
	Hashtable<String, Object> diacapHashtable = new Hashtable<String, Object>(); //systemName -> diacapDate

	private IEngine TAP_Portfolio;
	
	@Override
	public void createView(){
		Utility.showMessage("Success");
	}
	
	@Override
	public void createData() {
		
		//check for portfolio data
		try {
			TAP_Portfolio = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Portfolio");
		} catch (RuntimeException ex) {
			ex.printStackTrace();
		}
		//check if interface modernization cost property exists on system
		//if it does not, run insert
		//if it does, ask if override  -> delete and insert
		
		boolean modernizationPropExists = checkModernizationProp();
		
		if(!modernizationPropExists) 
		{
			// show continue popup
			JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
			Object[] buttons = {"Cancel", "Continue With Calculation"};
			int response = JOptionPane.showOptionDialog(playPane, "The selected RDF store does not " +
					"contain necessary calculated values.  Would you like to calculate now?", 
					"Warning", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE, null, buttons, buttons[1]);
			
			// if user chooses to run insert, run the thing
			if(response == 1)
			{
				try {
					runModernizationPropInsert();
				} catch (EngineException e) {
					Utility.showError(e.getMessage());
					e.printStackTrace();
				}
			}
			else{
				return;
			}
		}
		else
		{
			// show override popup
			JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
			Object[] buttons = {"Continue with stored values", "Recalculate"};
			int response = JOptionPane.showOptionDialog(playPane, "The selected RDF store already " +
					"contains calculated values.  Would you like to recalculate?", 
					"Warning", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE, null, buttons, buttons[1]);
			
			// if user chooses to overwrite, delete then insert
			if(response == 1)
			{
				deleteModernizationProp();
				try {
					runModernizationPropInsert();
				} catch (EngineException e) {
					Utility.showError(e.getMessage());
					e.printStackTrace();
				}
			}
		}
		
		// modernization prop is all set to go.....
		// time to start generation of the table.....

		collectTableData();
		createTable();
		
	}
	
	private void collectTableData(){
		logger.info("Gathering table data");
		// fill lists
		this.lpiSystemList = getList(this.lpiSystemListQuery);
		this.lpniSystemList = getList(this.lpniSystemListQuery);
		
		//fill hashtables using only the systems we care about
		String bindingsString = "";
		for(String system: lpiSystemList){
			bindingsString += "(<" + system + ">)";
		}
		for(String system: lpniSystemList){
			bindingsString += "(<" + system + ">)";
		}

		this.budgetHashtable = getBudgetHashtable(this.systemBudgetQuery, bindingsString, TAP_Portfolio);
		this.hwswHashtable = getHashtable(this.systemHWSWcostQuery, bindingsString, this.engine);
		this.ownerHashtable = getHashtable(this.systemOwnerQuery, bindingsString, this.engine);
		this.interfaceModHashtable = getHashtable(this.systemInterfaceModCostQuery, bindingsString, this.engine);
		this.diacapHashtable = getHashtable(this.systemDIACAPQuery, bindingsString, this.engine);
		
	}
	
	private void createTable(){
		logger.info("Creating table");
		//use hashtables and arrayLists for generating the report
		ConsolidatedSystemReportWriter writer = new ConsolidatedSystemReportWriter(lpiSystemList, lpniSystemList, ownerHashtable, budgetHashtable, this.hwswHashtable, this.interfaceModHashtable, diacapHashtable);
		writer.runWriter();
		
	}
	
	private void runModernizationPropInsert() throws EngineException{
		//mahers code
		InsertInterfaceModernizationProperty inserter = new InsertInterfaceModernizationProperty();
		inserter.insert();
	}
	
	// query must return two variables... will be put in key->value
	private Hashtable<String, Object> getHashtable(String passedQuery, String bindingsNames, IEngine passedEngine){
		Hashtable<String, Object> retHash = new Hashtable<String, Object>();

		passedQuery = passedQuery.replace("@BINDINGS_STRING@", bindingsNames);
		ISelectWrapper sjsw = processSelectQuery(passedEngine, passedQuery);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			String key = sjss.getRawVar(names[0]).toString();
			Object value = sjss.getVar(names[1]);
			retHash.put(key, value);
		}
		return retHash;
	}
	
	private Hashtable<String, Hashtable<String, Double>> getBudgetHashtable(String passedQuery, String bindingsNames, IEngine passedEngine){
		Hashtable<String, Hashtable<String, Double>> retHash = new Hashtable<String, Hashtable<String, Double>>();

		passedQuery = passedQuery.replace("@BINDINGS_STRING@", bindingsNames);
		ISelectWrapper sjsw = processSelectQuery(passedEngine, passedQuery);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			String key = sjss.getRawVar(names[0]).toString();
			Hashtable<String, Double> innerHash = retHash.get(key);
			if(innerHash == null){
				innerHash = new Hashtable<String, Double>();
			}
			
			String innerKey = sjss.getVar(names[1]).toString();
			Object value = sjss.getVar(names[2]);
			if(value instanceof Double){
				Double innerValue = (Double) value;
				innerHash.put(innerKey, innerValue);
			}
			
			retHash.put(key, innerHash);
		}
		return retHash;
	}
	
	private ArrayList<String> getList(String passedQuery){
		ArrayList<String> retArray = new ArrayList<String>();

		ISelectWrapper sjsw = processSelectQuery(this.engine, passedQuery);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			String sysURI = sjss.getRawVar(names[0]) + "";
			retArray.add(sysURI);
		}
		return retArray;
	}

	private ISelectWrapper processSelectQuery(IEngine engine, String query){
		logger.info("PROCESSING SELECT QUERY: " + query);
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);

		/*SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		//run the query against the engine provided
		sjsw.setEngine(engine);
		sjsw.setQuery(query);
		sjsw.executeQuery();	
		return sjsw;*/
		return wrapper;
	}
	
	private void deleteModernizationProp(){
		logger.info("Deleting modernization prop");
		UpdateProcessor upProc = new UpdateProcessor();
		upProc.setEngine(this.engine);
		upProc.setQuery(modPropDeleteQuery);
		upProc.processQuery();
	}

	private boolean checkModernizationProp(){
		logger.info("Checking modernization prop");
		boolean exists = false;

		BooleanProcessor proc = new BooleanProcessor();
		proc.setEngine(this.engine);
		proc.setQuery(checkModPropQuery);
		exists = proc.processQuery();
		logger.info("Modernization prop exists: " + exists);
		return exists;
	}
}

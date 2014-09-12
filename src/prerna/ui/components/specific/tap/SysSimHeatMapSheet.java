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

import java.awt.Dimension;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.ui.main.listener.specific.tap.SimilarityBarChartBrowserFunction;
import prerna.ui.main.listener.specific.tap.SysSimHealthGridListener;
import prerna.ui.main.listener.specific.tap.SimilarityRefreshBrowserFunction;
import prerna.util.Constants;
import prerna.util.DIHelper;

import com.google.gson.Gson;


/**
 */
public class SysSimHeatMapSheet extends SimilarityHeatMapSheet{
	String tapCoreDB = "TAP_Core_Data";
	boolean createSystemBindings = true;
	String systemListBindings = "BINDINGS ?System ";
	
	/**
	 * Constructor for SysSimeHeatMapSheet.
	 */
	public SysSimHeatMapSheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		setComparisonObjectTypes("System1", "System2");
	}

	/**
	 * Adds the health grid, refresh, and bar chart listeners when the navigation has finished.
	 */
	@Override
	public void registerFunctions()
	{
		super.registerFunctions();
    	SysSimHealthGridListener healthGridCall = new SysSimHealthGridListener();
    	browser.registerFunction("healthGrid",  healthGridCall);
	}
	
	@Override
	public void createData()
	{
		super.createData();
		SimilarityFunctions sdf = new SimilarityFunctions();
		addPanel();
		// this would be create the data
		Hashtable dataHash = new Hashtable();
//		Hashtable overallHash;
		//get list of systems first
		updateProgressBar("10%...Getting all systems for evaluation", 10);
		String defaultSystemsQuery = "SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?System ?UsedBy ?SystemUser}}";
		defaultSystemsQuery = addBindings(defaultSystemsQuery);
		comparisonObjectList = sdf.createComparisonObjectList(tapCoreDB, defaultSystemsQuery);
		sdf.setComparisonObjectList(comparisonObjectList);
		
		//first get databack from the 
		updateProgressBar("20%...Evaluating Data/BLU Score", 20);
		String dataQuery = "SELECT DISTINCT ?System ?Data ?CRM WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?System ?UsedBy ?SystemUser}{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?provide <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;}{?System ?provide ?Data .}}";
		dataQuery = addBindings(dataQuery);
		String bluQuery = "SELECT DISTINCT ?System ?BLU WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}{?System ?UsedBy ?SystemUser}{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?System ?provide ?BLU }}";
		bluQuery = addBindings(bluQuery);
		Hashtable<String, Hashtable<String,Double>> dataBLUHash = sdf.getDataBLUDataSet(tapCoreDB, dataQuery, bluQuery, SimilarityFunctions.VALUE);
		dataHash = processHashForCharting(dataBLUHash);
		
		String theaterQuery = "SELECT DISTINCT ?System ?Theater WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?System ?UsedBy ?SystemUser}{?System <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> ?Theater}}";
		theaterQuery = addBindings(theaterQuery);
		updateProgressBar("30%...Evaluating Deployment Score", 30);
		Hashtable theaterHash = sdf.stringCompareBinaryResultGetter(tapCoreDB, theaterQuery, "Theater", "Garrison", "Both");
		theaterHash = processHashForCharting(theaterHash);
		//dataHash = processOverallScore(dataHash, theaterHash);
		
		String dwQuery = "SELECT DISTINCT ?System ?Trans WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?System ?UsedBy ?SystemUser}{?System <http://semoss.org/ontologies/Relation/Contains/Transactional> ?Trans}}";
		dwQuery = addBindings(dwQuery);
		updateProgressBar("40%...Evaluating System Transactional Score", 40);
		Hashtable dwHash = sdf.stringCompareBinaryResultGetter(tapCoreDB, dwQuery, "'Yes'", "'No'", "Both");
		dwHash = processHashForCharting(dwHash);
		//dataHash = processOverallScore(dataHash, dwHash);
		
		//BP
		String bpQuery ="SELECT DISTINCT ?System ?BusinessProcess WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}{?System ?UsedBy ?SystemUser}{?Supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>;} {?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess> ;} {?System ?Supports ?BusinessProcess}}";
		bpQuery = addBindings(bpQuery);
		updateProgressBar("50%...Evaluating System Supporting Business Processes", 50);
		Hashtable bpHash = sdf.compareObjectParameterScore(tapCoreDB, bpQuery, SimilarityFunctions.VALUE);
		bpHash = processHashForCharting(bpHash);
		
		String actQuery ="SELECT DISTINCT ?System ?Activity WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}{?System ?UsedBy ?SystemUser}{?Supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>;} {?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;} {?System ?Supports ?Activity}}";
		actQuery = addBindings(actQuery);
		updateProgressBar("55%...Evaluating System Supporting Activity", 55);
		Hashtable actHash = sdf.compareObjectParameterScore(tapCoreDB, actQuery, SimilarityFunctions.VALUE);
		actHash = processHashForCharting(actHash);
		
		String userQuery ="SELECT DISTINCT ?System ?Personnel WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?System ?UsedBy ?SystemUser}{?UsedBy2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/UsedBy>;} {?Personnel <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Personnel> ;} {?System ?UsedBy2 ?Personnel}}";
		userQuery = addBindings(userQuery);
		updateProgressBar("60%...Evaluating System Users", 60);
		Hashtable userHash = sdf.compareObjectParameterScore(tapCoreDB, userQuery, SimilarityFunctions.VALUE);
		userHash = processHashForCharting(userHash);
		
		String uiQuery ="SELECT DISTINCT ?System ?UserInterface WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?System ?UsedBy ?SystemUser}{?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;} {?UserInterface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/UserInterface> ;} {?System ?Utilizes ?UserInterface}}";
		uiQuery = addBindings(uiQuery);
		updateProgressBar("70%...Evaluating User Interface", 70);
		Hashtable uiHash = sdf.compareObjectParameterScore(tapCoreDB, uiQuery, SimilarityFunctions.VALUE);
		uiHash = processHashForCharting(uiHash);
		
		ArrayList<Hashtable> hashArray = new ArrayList<Hashtable>();
		//hashArray.add(bpHash);
		//hashArray.add(actHash);
		//hashArray.add(userHash);
		//dataHash = processOverallScoreByAverage(dataHash,hashArray);
		
		updateProgressBar("80%...Creating Heat Map Visualization", 80);
		paramDataHash.put("Process_Supported", bpHash);
		paramDataHash.put("Activities_Supported", actHash);
		paramDataHash.put("Data_and_Business_Logic_Supported", dataHash);
		paramDataHash.put("Deployment_(Theater/Garrison)",  theaterHash);
		paramDataHash.put("Transactional_(Yes/No)", dwHash);
		paramDataHash.put("User_Types", userHash);
		paramDataHash.put("User_Interface_Types_(PC/Mobile/etc.)", uiHash);
		
		//allHash.put("dataSeries", testDataHash);
		allHash.put("title",  "System Similarity");
		allHash.put("xAxisTitle", "System1");
		allHash.put("yAxisTitle", "System2");
		allHash.put("value", "Score");
		allHash.put("sysDup", true);

	}
	
	public String addBindings(String sysSimQuery) {
		
		String defaultBindings = "BINDINGS ?SystemUser {(<http://health.mil/ontologies/Concept/SystemOwner/Central>)(<http://health.mil/ontologies/Concept/SystemUser/Army>)(<http://health.mil/ontologies/Concept/SystemUser/Navy>)(<http://health.mil/ontologies/Concept/SystemUser/Air_Force>)}";
		
		
		//If a query is not specifed, append the default SystemUser bindings
		if ((this.query).equals("NULL") || (this.query).equals("null") || (this.query).equals("Null") || this.query == null) {
			sysSimQuery = sysSimQuery + defaultBindings;
		}
		//if a query is specified, bind the system list to the system similarity query.
		else {
			//only create the bindings string once
			if (createSystemBindings == true) {
				String systemURIs = "{";
				for( int i = 0; i < list.size(); i++) {
					Object[] values = list.get(i);
					String system = "";
					for (Object systemResult : values) {
						system = "(<http://health.mil/ontologies/Concept/System/" + systemResult.toString() + ">)";
					}
					systemURIs = systemURIs + system;
				}
				systemURIs = systemURIs + "}";
				systemListBindings = systemListBindings + systemURIs;
				createSystemBindings = false;
			}
			sysSimQuery = sysSimQuery + systemListBindings;
		}
		
		return sysSimQuery;		
	}

}

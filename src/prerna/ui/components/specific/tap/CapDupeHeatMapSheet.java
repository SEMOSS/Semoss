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
import prerna.ui.main.listener.specific.tap.SysDupeHealthGridListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

import com.google.gson.Gson;
import com.teamdev.jxbrowser.events.NavigationEvent;
import com.teamdev.jxbrowser.events.NavigationFinishedEvent;
import com.teamdev.jxbrowser.events.NavigationListener;

/**
 */
public class CapDupeHeatMapSheet extends DuplicationHeatMapSheet{

	String hrCoreDB = "HR_Core";
	
	/**
	 * Constructor for CapDupeHeatMapSheet.
	 */
	public CapDupeHeatMapSheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		setComparisonObjectType("Capability");
	}
	
	public void createData()
	{
		DuplicationFunctions sdf = new DuplicationFunctions();
		addPanel();
		// this would be create the data
		Hashtable dataHash = new Hashtable();
		Hashtable overallHash;
		//get list of capabilities first
		updateProgressBar("10%...Getting all capabilities for evaluation", 10);
		query = "SELECT DISTINCT ?Capability WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}}";
		comparisonObjectList = sdf.createComparisonObjectList(hrCoreDB, query);
		sdf.setComparisonObjectList(comparisonObjectList);
		
		//first get databack from the 
		updateProgressBar("20%...Evaluating Data/BLU Score", 20);
		String dataQuery = "SELECT DISTINCT ?Capability ?Data ?CRM WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Capability ?Consists ?Task.}{?Task ?Needs ?Data.} }";
		String bluQuery = "SELECT DISTINCT ?Capability ?BusinessLogicUnit WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?BusinessLogicUnit <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>} {?Task_Needs_BusinessLogicUnit <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?Capability ?Consists ?Task.}{?Task ?Task_Needs_BusinessLogicUnit ?BusinessLogicUnit}}";
		Hashtable<String, Hashtable<String,Double>> dataBLUHash = sdf.getDataBLUDataSet(hrCoreDB, dataQuery, bluQuery, DuplicationFunctions.VALUE);
		dataHash = processHashForCharting(dataBLUHash);
	
		//Participants
		String participantQuery ="SELECT DISTINCT ?Capability ?Participant WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability> ;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;} {?Requires <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Requires>;}{?Participant <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Participant>;}{?Capability ?Consists ?Task.}{?Task ?Requires ?Participant.}}";
		updateProgressBar("50%...Evaluating Capability Supporting Participants", 50);
		Hashtable participantHash = sdf.compareObjectParameterScore(hrCoreDB, participantQuery, DuplicationFunctions.VALUE);
		participantHash = processHashForCharting(participantHash);

		//BP
		String bpQuery ="SELECT DISTINCT ?Capability ?BusinessProcess WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;} {?Supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>;} {?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;} {?Capability ?Supports ?BusinessProcess.} }";
		updateProgressBar("50%...Evaluating Capability Supporting Business Processes", 50);
		Hashtable bpHash = sdf.compareObjectParameterScore(hrCoreDB, bpQuery, DuplicationFunctions.VALUE);
		bpHash = processHashForCharting(bpHash);
		
		String attributeQuery ="SELECT DISTINCT ?Capability ?Attribute WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?Attribute <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Attribute>;}{?Capability ?Consists ?Task.}{?Task ?Has ?Attribute.}}";
		updateProgressBar("55%...Evaluating Capability Supporting Attribute", 55);
		Hashtable attributeHash = sdf.compareObjectParameterScore(hrCoreDB, attributeQuery, DuplicationFunctions.VALUE);
		attributeHash = processHashForCharting(attributeHash);	

		ArrayList<Hashtable> hashArray = new ArrayList<Hashtable>();
		
		updateProgressBar("80%...Creating Heat Map Visualization", 80);
		paramDataHash.put("Process_Supported", bpHash);
		paramDataHash.put("Attributes_Supported", attributeHash);
		paramDataHash.put("Participants_Supported", participantHash);
		paramDataHash.put("Data_and_Business_Logic_Supported", dataHash);
		
		allHash.put("title",  "Capability Duplication");
		allHash.put("xAxisTitle", "Capability1");
		allHash.put("yAxisTitle", "Capability2");
		allHash.put("value", "Score");
	}
	
}

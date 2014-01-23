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

import com.google.gson.Gson;
import com.teamdev.jxbrowser.events.NavigationEvent;
import com.teamdev.jxbrowser.events.NavigationFinishedEvent;
import com.teamdev.jxbrowser.events.NavigationListener;

import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.ui.main.listener.specific.tap.SysDupeHealthGridListener;

/**
 */
public class SysDupeHeatMapSheet extends BrowserPlaySheet{
	Logger logger = Logger.getLogger(getClass());
	ArrayList<String> sysList = new ArrayList<String>();
	final String crmKey = "!CRM!";
	Hashtable allHash = new Hashtable();
	Hashtable paramDataHash = new Hashtable();
	String tapCoreDB = "TAP_Core_Data";
	
	/**
	 * Constructor for SysDupeHeatMapSheet.
	 */
	public SysDupeHeatMapSheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
	}

	/**
	 * Processes all Sys Dupe queries and shows results in sysdupe.html format.
	 */
	@Override
	public void createView()
	{
		SysDupeFunctions sdf = new SysDupeFunctions();
		
		
		/*Hashtable dataHash = new Hashtable();
		Hashtable overallHash;
		//get list of systems first
		updateProgressBar("10%...Getting all systems for evaluation", 10);
		query = "SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?System ?Has ?SystemCategory}}BINDINGS ?SystemCategory {(<http://health.mil/ontologies/Concept/SystemCategory/Central>)(<http://health.mil/ontologies/Concept/SystemCategory/Army>)(<http://health.mil/ontologies/Concept/SystemCategory/Navy>)(<http://health.mil/ontologies/Concept/SystemCategory/Air_Force>)}";
		sysList = sdf.createSystemList(tapCoreDB, query);
		sdf.setSysList(sysList);
		
		//first get databack from the 
		updateProgressBar("20%...Evaluating Data/BLU Score", 20);
		String dataQuery = "SELECT DISTINCT ?System ?Data ?CRM WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?System ?Has ?SystemCategory}{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?provide <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;}{?System ?provide ?Data .}}BINDINGS ?SystemCategory {(<http://health.mil/ontologies/Concept/SystemCategory/Central>)(<http://health.mil/ontologies/Concept/SystemCategory/Army>)(<http://health.mil/ontologies/Concept/SystemCategory/Navy>)(<http://health.mil/ontologies/Concept/SystemCategory/Air_Force>)}";
		String bluQuery = "SELECT DISTINCT ?System ?BLU WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}{?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?System ?Has ?SystemCategory}{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?System ?provide ?BLU }}BINDINGS ?SystemCategory {(<http://health.mil/ontologies/Concept/SystemCategory/Central>)(<http://health.mil/ontologies/Concept/SystemCategory/Army>)(<http://health.mil/ontologies/Concept/SystemCategory/Navy>)(<http://health.mil/ontologies/Concept/SystemCategory/Air_Force>)}";
		Hashtable<String, Hashtable<String,Double>> dataBLUHash = sdf.getDataBLUDataSet(tapCoreDB, dataQuery, bluQuery, SysDupeFunctions.VALUE);
		dataHash = processHashForCharting(dataBLUHash);
		
		String theaterQuery = "SELECT DISTINCT ?System ?Theater WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?System ?Has ?SystemCategory}{?System <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> ?Theater}}BINDINGS ?SystemCategory {(<http://health.mil/ontologies/Concept/SystemCategory/Central>)(<http://health.mil/ontologies/Concept/SystemCategory/Army>)(<http://health.mil/ontologies/Concept/SystemCategory/Navy>)(<http://health.mil/ontologies/Concept/SystemCategory/Air_Force>)}";
		updateProgressBar("30%...Evaluating Deployment Score", 30);
		Hashtable theaterHash = sdf.stringCompareBinaryResultGetter(tapCoreDB, theaterQuery, "Theater", "Garrison", "Both");
		theaterHash = processHashForCharting(theaterHash);
		//dataHash = processOverallScore(dataHash, theaterHash);
		
		String dwQuery = "SELECT DISTINCT ?System ?Trans WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?System ?Has ?SystemCategory}{?System <http://semoss.org/ontologies/Relation/Contains/Transactional> ?Trans}}BINDINGS ?SystemCategory {(<http://health.mil/ontologies/Concept/SystemCategory/Central>)(<http://health.mil/ontologies/Concept/SystemCategory/Army>)(<http://health.mil/ontologies/Concept/SystemCategory/Navy>)(<http://health.mil/ontologies/Concept/SystemCategory/Air_Force>)}";
		updateProgressBar("40%...Evaluating System Transactional Score", 40);
		Hashtable dwHash = sdf.stringCompareBinaryResultGetter(tapCoreDB, dwQuery, "Yes", "No", "Both");
		dwHash = processHashForCharting(dwHash);
		//dataHash = processOverallScore(dataHash, dwHash);
		
		//BP
		String bpQuery ="SELECT DISTINCT ?System ?BusinessProcess WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?System ?Has ?SystemCategory}{?Supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>;} {?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess> ;} {?System ?Supports ?BusinessProcess}}BINDINGS ?SystemCategory {(<http://health.mil/ontologies/Concept/SystemCategory/Central>)(<http://health.mil/ontologies/Concept/SystemCategory/Army>)(<http://health.mil/ontologies/Concept/SystemCategory/Navy>)(<http://health.mil/ontologies/Concept/SystemCategory/Air_Force>)}";
		updateProgressBar("50%...Evaluating System Supporting Business Processes", 50);
		Hashtable bpHash = sdf.compareSystemParameterScore(tapCoreDB, bpQuery, SysDupeFunctions.VALUE);
		bpHash = processHashForCharting(bpHash);
		
		String actQuery ="SELECT DISTINCT ?System ?Activity WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?System ?Has ?SystemCategory}{?Supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>;} {?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;} {?System ?Supports ?Activity}}BINDINGS ?SystemCategory {(<http://health.mil/ontologies/Concept/SystemCategory/Central>)(<http://health.mil/ontologies/Concept/SystemCategory/Army>)(<http://health.mil/ontologies/Concept/SystemCategory/Navy>)(<http://health.mil/ontologies/Concept/SystemCategory/Air_Force>)}";
		updateProgressBar("55%...Evaluating System Supporting Activity", 55);
		Hashtable actHash = sdf.compareSystemParameterScore(tapCoreDB, actQuery, SysDupeFunctions.VALUE);
		actHash = processHashForCharting(actHash);
		
		String userQuery ="SELECT DISTINCT ?System ?User WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?System ?Has ?SystemCategory}{?UsedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/UsedBy>;} {?User <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/User> ;} {?System ?UsedBy ?User}}BINDINGS ?SystemCategory {(<http://health.mil/ontologies/Concept/SystemCategory/Central>)(<http://health.mil/ontologies/Concept/SystemCategory/Army>)(<http://health.mil/ontologies/Concept/SystemCategory/Navy>)(<http://health.mil/ontologies/Concept/SystemCategory/Air_Force>)}";
		updateProgressBar("60%...Evaluating System Users", 60);
		Hashtable userHash = sdf.compareSystemParameterScore(tapCoreDB, userQuery, SysDupeFunctions.VALUE);
		userHash = processHashForCharting(userHash);
		
		String uiQuery ="SELECT DISTINCT ?System ?UserInterface WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?System ?Has ?SystemCategory}{?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;} {?UserInterface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/UserInterface> ;} {?System ?Utilizes ?UserInterface}}BINDINGS ?SystemCategory {(<http://health.mil/ontologies/Concept/SystemCategory/Central>)(<http://health.mil/ontologies/Concept/SystemCategory/Army>)(<http://health.mil/ontologies/Concept/SystemCategory/Navy>)(<http://health.mil/ontologies/Concept/SystemCategory/Air_Force>)}";
		updateProgressBar("70%...Evaluating User Interface", 70);
		Hashtable uiHash = sdf.compareSystemParameterScore(tapCoreDB, uiQuery, SysDupeFunctions.VALUE);
		uiHash = processHashForCharting(uiHash);
		
		ArrayList<Hashtable> hashArray = new ArrayList<Hashtable>();
		//hashArray.add(bpHash);
		//hashArray.add(actHash);
		//hashArray.add(userHash);
		//dataHash = processOverallScoreByAverage(dataHash,hashArray);
		
		updateProgressBar("80%...Creating Heat Map Visualization", 80);
		Hashtable testDataHash = new Hashtable();
		testDataHash.put("Process_Supported", bpHash);
		testDataHash.put("Activities_Supported", actHash);
		testDataHash.put("Data_and_Business_Logic_Supported", dataHash);
		testDataHash.put("Deployment_(Theater/Garrison)",  theaterHash);
		testDataHash.put("Transactional_(Yes/No)", dwHash);
		testDataHash.put("User_Types", userHash);
		testDataHash.put("User_Interface_Types_(PC/Mobile/etc.)", uiHash);
		
		final Hashtable allHash = new Hashtable();
		allHash.put("dataSeries", testDataHash);
		allHash.put("title",  "System Duplication");
		allHash.put("xAxisTitle", "System1");
		allHash.put("yAxisTitle", "System2");
		allHash.put("value", "Score");
		*/
//		Hashtable dataHash = dataHash = calculateDataAndBLUScore();
//		Hashtable allDataHash = new Hashtable();
//		Hashtable allHash = new Hashtable();
//		allHash.put("dataSeries", allDataHash);
//		allDataHash.put("DataObject", dataHash);
//		allDataHash.put("BLU", dataHash);
//		String[] var = wrapper.getVariables();
//		allHash.put("title",  "System Duplication");
//		allHash.put("xAxisTitle", "System1");
//		allHash.put("yAxisTitle", "System2");
//		allHash.put("value", "Score");
//		
		//FactSheetSysDupeCalculator fs = new FactSheetSysDupeCalculator();
		
		String workingDir = System.getProperty("user.dir");
		browser.addNavigationListener(new NavigationListener() {
    	    public void navigationStarted(NavigationEvent event) {
    	    	logger.info("event.getUrl() = " + event.getUrl());
    	    }

    	    public void navigationFinished(NavigationFinishedEvent event) {
    	    	SysDupeHealthGridListener healthGridCall = new SysDupeHealthGridListener();
    	    	browser.registerFunction("healthGrid",  healthGridCall);
    			callIt();
    	    }
    	});
	       
		browser.navigate("file://" + workingDir + "/html/MHS-RDFSemossCharts/app/sysDup.html");
		//browser.waitReady();
		//browser.registerFunction("healthGrid",  healthGridCall);
		//callIt(allHash);
		
	}
	
	/**
	 * Formats data hashtable into proper format needed for charting.
	 * 
	 * @param dataHash Hashtable<String,Hashtable<String,Double>>	Hashtable of data to be formatted
	 * 
	 * @return Hashtable	Formatted hashtable of data
	 */
	public Hashtable processHashForCharting(Hashtable<String, Hashtable<String,Double>>dataHash)
	{
		//first create hashtable of arraylist with system as key and corresponding data + blu as the values
		Hashtable<String, Hashtable<String,String>> dataRetHash = new Hashtable<String, Hashtable<String,String>>();

		for(Entry<String, Hashtable<String, Double>> sysEntry : dataHash.entrySet()) 
		{
			String sysName = sysEntry.getKey();
		    Hashtable<String,Double> sysDataHash = sysEntry.getValue();
		    for(Entry<String, Double> sysCompEntry : sysDataHash.entrySet()) 
			{
				String sysName2 = sysCompEntry.getKey();
			    double sysCompValue = sysCompEntry.getValue();
				Hashtable elementHash = new Hashtable();
				elementHash.put("System1", sysName);
				elementHash.put("System2", sysName2);
				elementHash.put("Score", sysCompValue*100);
				dataRetHash.put(sysName +"-"+sysName2, elementHash);
			}
		}
		return dataRetHash;
	}

	/**
	 * Generic function that can compare a given property of a system given three choices where doubleOverlap fulfills the first two.
	 * 
	 * @param scoreTable Hashtable<String,Hashtable>	Table of scores assigned
	 * @param updateTable Hashtable<String,Hashtable>	Table of update values corresponding to data
	
	 * @return Hashtable	All overall scores, system -> new score values
	 */
	private Hashtable processOverallScore(Hashtable<String,Hashtable> scoreTable, Hashtable<String,Hashtable> updateTable)
	{
		//TODO: Comments, var name refactoring
		
		//iterate through original table, if update doesn't exist, then we dont have data, and if we don't have data we cannot show any duplication
		Hashtable newTable = new Hashtable();
		for(Entry<String,Hashtable> e : scoreTable.entrySet()) 
		{
			String heatKey = e.getKey();
			Hashtable valueHash = e.getValue();
			double score = (Double) valueHash.get("Score");
			
			if(updateTable.get(heatKey)!=null)
			{
				Hashtable valueHash2 = updateTable.get(heatKey);
				double score2 = (Double) valueHash2.get("Score");
				double newScore = score*score2/100;
				String sysName1 = (String) valueHash2.get("System1");
				String sysName2 = (String) valueHash2.get("System2");
				valueHash.put("Score",  score*score2/100);
				if(newScore>=0)
				{
					newTable.put(heatKey,  valueHash);
				}
				if(sysName1.equals(sysName2))
				{
					newTable.remove(sysName1 +"-"+sysName2);
				}
			}
		}
		return newTable;
	}
	
	/**
	 * Function to compute overall scores by computing averages.
	 * 
	 * @param startHash Hashtable<String,Hashtable>	Table of base data
	 * @param hashArray ArrayList<Hashtable>		Table of update values corresponding to data
	
	 * @return Hashtable	All overall scores, system -> new score values
	 */
	private Hashtable processOverallScoreByAverage(Hashtable<String,Hashtable> startHash, ArrayList<Hashtable> hashArray)
	{
		//TODO: Comments, var names refactoring
		
		//iterate through original table, if update doesn't exist, then we dont have data, and if we don't have data we cannot show any duplication
		Hashtable newTable = new Hashtable();
		
		for(Entry<String,Hashtable> e : startHash.entrySet()) 
		{
			String heatKey = e.getKey();
			Hashtable valueHash = e.getValue();
			double score = (Double) valueHash.get("Score")/100;
			String sysName1 = (String) valueHash.get("System1");
			String sysName2 = (String) valueHash.get("System2");
			boolean includeValue = true;
			
			for (int hashIdx = 0; hashIdx <hashArray.size();hashIdx++)
			{
				Hashtable<String, Hashtable> multHash = hashArray.get(hashIdx);
				if(multHash.containsKey(heatKey))
				{
					Hashtable valueHash2 = multHash.get(heatKey);
					double newScore = (Double) valueHash2.get("Score");
					score = score+newScore/100;
				}
				else
				{
					includeValue = false;
					break;
				}
			}
			if(includeValue)
			{
				valueHash.put("Score",  score/(hashArray.size()+1)*100);
				newTable.put(heatKey,  valueHash);
			}
			if(sysName1.equals(sysName2))
			{
				newTable.remove(heatKey);
			}
		}
		
		return newTable;
	}
	
	public void createData()
	{
		SysDupeFunctions sdf = new SysDupeFunctions();
		addPanel();
		// this would be create the data
		Hashtable dataHash = new Hashtable();
		Hashtable overallHash;
		//get list of systems first
		updateProgressBar("10%...Getting all systems for evaluation", 10);
		query = "SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?System ?Has ?SystemCategory}}BINDINGS ?SystemCategory {(<http://health.mil/ontologies/Concept/SystemCategory/Central>)(<http://health.mil/ontologies/Concept/SystemCategory/Army>)(<http://health.mil/ontologies/Concept/SystemCategory/Navy>)(<http://health.mil/ontologies/Concept/SystemCategory/Air_Force>)}";
		sysList = sdf.createSystemList(tapCoreDB, query);
		sdf.setSysList(sysList);
		
		//first get databack from the 
		updateProgressBar("20%...Evaluating Data/BLU Score", 20);
		String dataQuery = "SELECT DISTINCT ?System ?Data ?CRM WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?System ?Has ?SystemCategory}{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?provide <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;}{?System ?provide ?Data .}}BINDINGS ?SystemCategory {(<http://health.mil/ontologies/Concept/SystemCategory/Central>)(<http://health.mil/ontologies/Concept/SystemCategory/Army>)(<http://health.mil/ontologies/Concept/SystemCategory/Navy>)(<http://health.mil/ontologies/Concept/SystemCategory/Air_Force>)}";
		String bluQuery = "SELECT DISTINCT ?System ?BLU WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}{?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?System ?Has ?SystemCategory}{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?System ?provide ?BLU }}BINDINGS ?SystemCategory {(<http://health.mil/ontologies/Concept/SystemCategory/Central>)(<http://health.mil/ontologies/Concept/SystemCategory/Army>)(<http://health.mil/ontologies/Concept/SystemCategory/Navy>)(<http://health.mil/ontologies/Concept/SystemCategory/Air_Force>)}";
		Hashtable<String, Hashtable<String,Double>> dataBLUHash = sdf.getDataBLUDataSet(tapCoreDB, dataQuery, bluQuery, SysDupeFunctions.VALUE);
		dataHash = processHashForCharting(dataBLUHash);
		
		String theaterQuery = "SELECT DISTINCT ?System ?Theater WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?System ?Has ?SystemCategory}{?System <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> ?Theater}}BINDINGS ?SystemCategory {(<http://health.mil/ontologies/Concept/SystemCategory/Central>)(<http://health.mil/ontologies/Concept/SystemCategory/Army>)(<http://health.mil/ontologies/Concept/SystemCategory/Navy>)(<http://health.mil/ontologies/Concept/SystemCategory/Air_Force>)}";
		updateProgressBar("30%...Evaluating Deployment Score", 30);
		Hashtable theaterHash = sdf.stringCompareBinaryResultGetter(tapCoreDB, theaterQuery, "Theater", "Garrison", "Both");
		theaterHash = processHashForCharting(theaterHash);
		//dataHash = processOverallScore(dataHash, theaterHash);
		
		String dwQuery = "SELECT DISTINCT ?System ?Trans WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?System ?Has ?SystemCategory}{?System <http://semoss.org/ontologies/Relation/Contains/Transactional> ?Trans}}BINDINGS ?SystemCategory {(<http://health.mil/ontologies/Concept/SystemCategory/Central>)(<http://health.mil/ontologies/Concept/SystemCategory/Army>)(<http://health.mil/ontologies/Concept/SystemCategory/Navy>)(<http://health.mil/ontologies/Concept/SystemCategory/Air_Force>)}";
		updateProgressBar("40%...Evaluating System Transactional Score", 40);
		Hashtable dwHash = sdf.stringCompareBinaryResultGetter(tapCoreDB, dwQuery, "Yes", "No", "Both");
		dwHash = processHashForCharting(dwHash);
		//dataHash = processOverallScore(dataHash, dwHash);
		
		//BP
		String bpQuery ="SELECT DISTINCT ?System ?BusinessProcess WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?System ?Has ?SystemCategory}{?Supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>;} {?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess> ;} {?System ?Supports ?BusinessProcess}}BINDINGS ?SystemCategory {(<http://health.mil/ontologies/Concept/SystemCategory/Central>)(<http://health.mil/ontologies/Concept/SystemCategory/Army>)(<http://health.mil/ontologies/Concept/SystemCategory/Navy>)(<http://health.mil/ontologies/Concept/SystemCategory/Air_Force>)}";
		updateProgressBar("50%...Evaluating System Supporting Business Processes", 50);
		Hashtable bpHash = sdf.compareSystemParameterScore(tapCoreDB, bpQuery, SysDupeFunctions.VALUE);
		bpHash = processHashForCharting(bpHash);
		
		String actQuery ="SELECT DISTINCT ?System ?Activity WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?System ?Has ?SystemCategory}{?Supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>;} {?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;} {?System ?Supports ?Activity}}BINDINGS ?SystemCategory {(<http://health.mil/ontologies/Concept/SystemCategory/Central>)(<http://health.mil/ontologies/Concept/SystemCategory/Army>)(<http://health.mil/ontologies/Concept/SystemCategory/Navy>)(<http://health.mil/ontologies/Concept/SystemCategory/Air_Force>)}";
		updateProgressBar("55%...Evaluating System Supporting Activity", 55);
		Hashtable actHash = sdf.compareSystemParameterScore(tapCoreDB, actQuery, SysDupeFunctions.VALUE);
		actHash = processHashForCharting(actHash);
		
		String userQuery ="SELECT DISTINCT ?System ?User WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?System ?Has ?SystemCategory}{?UsedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/UsedBy>;} {?User <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/User> ;} {?System ?UsedBy ?User}}BINDINGS ?SystemCategory {(<http://health.mil/ontologies/Concept/SystemCategory/Central>)(<http://health.mil/ontologies/Concept/SystemCategory/Army>)(<http://health.mil/ontologies/Concept/SystemCategory/Navy>)(<http://health.mil/ontologies/Concept/SystemCategory/Air_Force>)}";
		updateProgressBar("60%...Evaluating System Users", 60);
		Hashtable userHash = sdf.compareSystemParameterScore(tapCoreDB, userQuery, SysDupeFunctions.VALUE);
		userHash = processHashForCharting(userHash);
		
		String uiQuery ="SELECT DISTINCT ?System ?UserInterface WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?System ?Has ?SystemCategory}{?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;} {?UserInterface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/UserInterface> ;} {?System ?Utilizes ?UserInterface}}BINDINGS ?SystemCategory {(<http://health.mil/ontologies/Concept/SystemCategory/Central>)(<http://health.mil/ontologies/Concept/SystemCategory/Army>)(<http://health.mil/ontologies/Concept/SystemCategory/Navy>)(<http://health.mil/ontologies/Concept/SystemCategory/Air_Force>)}";
		updateProgressBar("70%...Evaluating User Interface", 70);
		Hashtable uiHash = sdf.compareSystemParameterScore(tapCoreDB, uiQuery, SysDupeFunctions.VALUE);
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
		allHash.put("title",  "System Duplication");
		allHash.put("xAxisTitle", "System1");
		allHash.put("yAxisTitle", "System2");
		allHash.put("value", "Score");
		
//		Hashtable dataHash = dataHash = calculateDataAndBLUScore();
//		Hashtable allDataHash = new Hashtable();
//		Hashtable allHash = new Hashtable();
//		allHash.put("dataSeries", allDataHash);
//		allDataHash.put("DataObject", dataHash);
//		allDataHash.put("BLU", dataHash);
//		String[] var = wrapper.getVariables();
//		allHash.put("title",  "System Duplication");
//		allHash.put("xAxisTitle", "System1");
//		allHash.put("yAxisTitle", "System2");
//		allHash.put("value", "Score");
//		
		//FactSheetSysDupeCalculator fs = new FactSheetSysDupeCalculator();

	}
	
	public void callIt()
	{
		Enumeration enumKey = paramDataHash.keys();
		while (enumKey.hasMoreElements())
		{
			String key = (String) enumKey.nextElement();
			Object value = (Object) paramDataHash.get(key);
			//if value equal to 0, dont need to calculate
			Gson gson = new Gson();
			//.info("Converted " + gson.toJson(table));
			browser.executeScript("dataBuilder('" + gson.toJson(value) + "', '"+key+"');");
		}
		enumKey = allHash.keys();
		while (enumKey.hasMoreElements())
		{
			String key = (String) enumKey.nextElement();
			Object value = (Object) allHash.get(key);
			//if value equal to 0, dont need to calculate
			Gson gson = new Gson();
			//.info("Converted " + gson.toJson(table));
			browser.executeScript("dimensionData('" + gson.toJson(value) + "', '"+key+"');");
		}
		browser.executeScript("start();");
		updateProgressBar("100%...Visualization Complete", 100);
	}
}

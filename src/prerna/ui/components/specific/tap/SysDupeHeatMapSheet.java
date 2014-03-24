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
import prerna.ui.main.listener.specific.tap.SysDupeBarChartBrowserFunction;
import prerna.ui.main.listener.specific.tap.SysDupeHealthGridListener;
import prerna.ui.main.listener.specific.tap.SysDupeRefreshBrowserFunction;
import prerna.util.Constants;
import prerna.util.DIHelper;

import com.google.gson.Gson;
import com.teamdev.jxbrowser.events.NavigationEvent;
import com.teamdev.jxbrowser.events.NavigationFinishedEvent;
import com.teamdev.jxbrowser.events.NavigationListener;

/**
 */
public class SysDupeHeatMapSheet extends BrowserPlaySheet{
	Logger logger = Logger.getLogger(getClass());
	ArrayList<String> sysList = new ArrayList<String>();
	final String crmKey = "!CRM!";
	Hashtable allHash = new Hashtable();
	Hashtable paramDataHash = new Hashtable();
	Hashtable keyHash = new Hashtable();
	String tapCoreDB = "TAP_Core_Data";
	SysDupeRefreshBrowserFunction refreshFunction;
	
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
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		browser.addNavigationListener(new NavigationListener() {
    	    public void navigationStarted(NavigationEvent event) {
    	    	logger.info("event.getUrl() = " + event.getUrl());
    	    }

    	    public void navigationFinished(NavigationFinishedEvent event) {
    	    	SysDupeHealthGridListener healthGridCall = new SysDupeHealthGridListener();
    	    	browser.registerFunction("healthGrid",  healthGridCall);
    	    	refreshFunction = new SysDupeRefreshBrowserFunction();
    	    	refreshFunction.setParamDataHash(paramDataHash);
    	    	refreshFunction.setKeyHash(keyHash);
    	    	refreshFunction.setBrowser(browser);
    	    	browser.registerFunction("refreshFunction",  refreshFunction);
    	    	SysDupeBarChartBrowserFunction barChartFunction = new SysDupeBarChartBrowserFunction();
    	    	barChartFunction.setParamDataHash(paramDataHash);
    	    	browser.registerFunction("barChartFunction",  barChartFunction);
    			callIt();
    	    }
    	});
	       
		browser.navigate("file://" + workingDir + "/html/MHS-RDFSemossCharts/app/sysDup.html");
		
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
			    if (!sysName.equals(sysName2))
			    {
					Hashtable elementHash = new Hashtable();
//					elementHash.put("System1", sysName);
//					elementHash.put("System2", sysName2);
					elementHash.put("Score", sysCompValue*100);
					String key = sysName +"-"+sysName2;
					dataRetHash.put(key, elementHash);
					if(!keyHash.containsKey(key)){
						Hashtable keyElementHash = new Hashtable();
						keyElementHash.put("System1", sysName);
						keyElementHash.put("System2", sysName2);
						keyHash.put(key, keyElementHash);
					}
			    }

			}
		}
		return dataRetHash;
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
		query = "SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?System ?UsedBy ?SystemUser}}BINDINGS ?SystemUser {(<http://health.mil/ontologies/Concept/SystemOwner/Central>)(<http://health.mil/ontologies/Concept/SystemUser/Army>)(<http://health.mil/ontologies/Concept/SystemUser/Navy>)(<http://health.mil/ontologies/Concept/SystemUser/Air_Force>)}";
		sysList = sdf.createSystemList(tapCoreDB, query);
		sdf.setSysList(sysList);
		
		//first get databack from the 
		updateProgressBar("20%...Evaluating Data/BLU Score", 20);
		String dataQuery = "SELECT DISTINCT ?System ?Data ?CRM WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?System ?UsedBy ?SystemUser}{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?provide <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM;}{?System ?provide ?Data .}}BINDINGS ?SystemUser {(<http://health.mil/ontologies/Concept/SystemOwner/Central>)(<http://health.mil/ontologies/Concept/SystemUser/Army>)(<http://health.mil/ontologies/Concept/SystemUser/Navy>)(<http://health.mil/ontologies/Concept/SystemUser/Air_Force>)}";
		String bluQuery = "SELECT DISTINCT ?System ?BLU WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}{?System ?UsedBy ?SystemUser}{?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?System ?provide ?BLU }}BINDINGS ?SystemUser {(<http://health.mil/ontologies/Concept/SystemOwner/Central>)(<http://health.mil/ontologies/Concept/SystemUser/Army>)(<http://health.mil/ontologies/Concept/SystemUser/Navy>)(<http://health.mil/ontologies/Concept/SystemUser/Air_Force>)}";
		Hashtable<String, Hashtable<String,Double>> dataBLUHash = sdf.getDataBLUDataSet(tapCoreDB, dataQuery, bluQuery, SysDupeFunctions.VALUE);
		dataHash = processHashForCharting(dataBLUHash);
		
		String theaterQuery = "SELECT DISTINCT ?System ?Theater WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?System ?UsedBy ?SystemUser}{?System <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> ?Theater}}BINDINGS ?SystemUser {(<http://health.mil/ontologies/Concept/SystemOwner/Central>)(<http://health.mil/ontologies/Concept/SystemUser/Army>)(<http://health.mil/ontologies/Concept/SystemUser/Navy>)(<http://health.mil/ontologies/Concept/SystemUser/Air_Force>)}";
		updateProgressBar("30%...Evaluating Deployment Score", 30);
		Hashtable theaterHash = sdf.stringCompareBinaryResultGetter(tapCoreDB, theaterQuery, "Theater", "Garrison", "Both");
		theaterHash = processHashForCharting(theaterHash);
		//dataHash = processOverallScore(dataHash, theaterHash);
		
		String dwQuery = "SELECT DISTINCT ?System ?Trans WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?System ?UsedBy ?SystemUser}{?System <http://semoss.org/ontologies/Relation/Contains/Transactional> ?Trans}}BINDINGS ?SystemUser {(<http://health.mil/ontologies/Concept/SystemOwner/Central>)(<http://health.mil/ontologies/Concept/SystemUser/Army>)(<http://health.mil/ontologies/Concept/SystemUser/Navy>)(<http://health.mil/ontologies/Concept/SystemUser/Air_Force>)}";
		updateProgressBar("40%...Evaluating System Transactional Score", 40);
		Hashtable dwHash = sdf.stringCompareBinaryResultGetter(tapCoreDB, dwQuery, "Yes", "No", "Both");
		dwHash = processHashForCharting(dwHash);
		//dataHash = processOverallScore(dataHash, dwHash);
		
		//BP
		String bpQuery ="SELECT DISTINCT ?System ?BusinessProcess WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}{?System ?UsedBy ?SystemUser}{?Supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>;} {?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess> ;} {?System ?Supports ?BusinessProcess}}BINDINGS ?SystemUser {(<http://health.mil/ontologies/Concept/SystemOwner/Central>)(<http://health.mil/ontologies/Concept/SystemUser/Army>)(<http://health.mil/ontologies/Concept/SystemUser/Navy>)(<http://health.mil/ontologies/Concept/SystemUser/Air_Force>)}";
		updateProgressBar("50%...Evaluating System Supporting Business Processes", 50);
		Hashtable bpHash = sdf.compareSystemParameterScore(tapCoreDB, bpQuery, SysDupeFunctions.VALUE);
		bpHash = processHashForCharting(bpHash);
		
		String actQuery ="SELECT DISTINCT ?System ?Activity WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}{?System ?UsedBy ?SystemUser}{?Supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>;} {?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;} {?System ?Supports ?Activity}}BINDINGS ?SystemUser {(<http://health.mil/ontologies/Concept/SystemOwner/Central>)(<http://health.mil/ontologies/Concept/SystemUser/Army>)(<http://health.mil/ontologies/Concept/SystemUser/Navy>)(<http://health.mil/ontologies/Concept/SystemUser/Air_Force>)}";
		updateProgressBar("55%...Evaluating System Supporting Activity", 55);
		Hashtable actHash = sdf.compareSystemParameterScore(tapCoreDB, actQuery, SysDupeFunctions.VALUE);
		actHash = processHashForCharting(actHash);
		
		String userQuery ="SELECT DISTINCT ?System ?Personnel WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?System ?UsedBy ?SystemUser}{?UsedBy2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/UsedBy>;} {?Personnel <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Personnel> ;} {?System ?UsedBy2 ?Personnel}}BINDINGS ?SystemUser {(<http://health.mil/ontologies/Concept/SystemOwner/Central>)(<http://health.mil/ontologies/Concept/SystemUser/Army>)(<http://health.mil/ontologies/Concept/SystemUser/Navy>)(<http://health.mil/ontologies/Concept/SystemUser/Air_Force>)}";
		updateProgressBar("60%...Evaluating System Users", 60);
		Hashtable userHash = sdf.compareSystemParameterScore(tapCoreDB, userQuery, SysDupeFunctions.VALUE);
		userHash = processHashForCharting(userHash);
		
		String uiQuery ="SELECT DISTINCT ?System ?UserInterface WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?System ?UsedBy ?SystemUser}{?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;} {?UserInterface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/UserInterface> ;} {?System ?Utilizes ?UserInterface}}BINDINGS ?SystemUser {(<http://health.mil/ontologies/Concept/SystemOwner/Central>)(<http://health.mil/ontologies/Concept/SystemUser/Army>)(<http://health.mil/ontologies/Concept/SystemUser/Navy>)(<http://health.mil/ontologies/Concept/SystemUser/Air_Force>)}";
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

	}
	
	public void callIt()
	{
		Gson gson = new Gson();
		ArrayList args = new ArrayList();
		Enumeration enumKey = paramDataHash.keys();
		int count = 0;
		while (enumKey.hasMoreElements())
		{
			args.add(enumKey.nextElement());
			count++;
		}
		Hashtable testHash = new Hashtable();
//		testHash.put("Deployment_(Theater/Garrison)", 0.90);
//		browser.executeScript("dataBuilder('" + gson.toJson(args) + "', '" + gson.toJson(testHash) + "');");
		ArrayList<Hashtable<String, Hashtable<String, Double>>> calculatedArray = refreshFunction.calculateHash(args, testHash);
		refreshFunction.sendData(calculatedArray);
		
		//send available dimensions:
		String availCatString = "dimensionData('" + gson.toJson(args) + "', 'categories');";
		System.out.println(availCatString);
		browser.executeScript(availCatString);
		
		enumKey = allHash.keys();
		while (enumKey.hasMoreElements())
		{
			String key = (String) enumKey.nextElement();
			Object value = (Object) allHash.get(key);
			
			browser.executeScript("dimensionData('" + gson.toJson(value) + "', '"+key+"');");
			//System.out.println("dimensionData('" + gson.toJson(value) + "', '"+key+"');");
		}
		browser.executeScript("start();");
		updateProgressBar("100%...Visualization Complete", 100);
		allHash.clear();
//		paramDataHash.clear();
	}
}

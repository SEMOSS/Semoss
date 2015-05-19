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
package prerna.algorithm.impl.specific.tap;

import java.awt.GridLayout;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Hashtable;
import java.util.Random;

import javax.swing.JDesktopPane;

import prerna.engine.api.IEngine;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.components.specific.tap.OptChartUtilityMethods;
import prerna.ui.components.specific.tap.OptimizationOrganizer;
import prerna.ui.components.specific.tap.SerOptGraphFunctions;
import prerna.ui.components.specific.tap.SerOptPlaySheet;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;
import prerna.util.Utility;

/**
 * This class is used to optimize the calculations for univariate services.
 */
public class UnivariateSvcOptimizer extends UnivariateOpt{
	
//	static final Logger logger = LogManager.getLogger(UnivariateSvcOptimizer.class.getName());
	
	String bindStr = "";
	OptimizationOrganizer optOrg;
	public String[] optSys;
	
	/**
	 * Sets the list of systems for optimizations.
	 * @param system 	List of systems.
	 */
	public void setSystems(String[] system)
	{
		this.optSys=system;
	}
	/**
	 * Runs the optimization for services.
	 */
	public void optimize()
	{
		playSheet.consoleArea.setText("Retrieving cost data from TAP_Cost_db...");
        progressBar = playSheet.progressBar;
        f.setProgressBar(progressBar);
        progressBar.setString("Collecting Data");
        ((UnivariateSvcOptFunction)f).setVariables(maxYears, hourlyCost, interfaceCost/hourlyCost,serMainPerc,attRate, hireRate,infRate, disRate, scdLT, iniLC, scdLC);

        optOrg = new OptimizationOrganizer();
        optOrg.runOrganizer(optSys);
        ((UnivariateSvcOptFunction)f).setData(optOrg);
        
        playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nData Collection Complete!");

        f.setConsoleArea(playSheet.consoleArea);
        f.setWriteBoolean (true);
	}
	
	/**
	 * Runs a specific iteration of the optimization.
	 */
	public void runOptIteration()
	{
        f.setWriteBoolean (false);
        f.value(optBudget);
        displayResults(((UnivariateSvcOptFunction)f).lin);
        displaySpecifics(((UnivariateSvcOptFunction)f).lin);
        displaySystemSpecifics(((UnivariateSvcOptFunction)f).lin);
        createTimelineData(((UnivariateSvcOptFunction)f).lin);
        if(!bindStr.equals("{}")) createGraphSheet();
        playSheet.setGraphsVisible(true);
	}
	
	/**
	 * Creates the graph sheet for optimization analysis.
	 */
	public void createGraphSheet()
	{
		GraphPlaySheet gps = null;
		try {
			gps = (GraphPlaySheet)Class.forName("prerna.ui.components.playsheets.GraphPlaySheet").getConstructor(null).newInstance(null);
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		((SerOptPlaySheet)playSheet).playSheetPanel.removeAll();
		((SerOptPlaySheet)playSheet).playSheetPanel.setLayout(new GridLayout(1, 0, 0, 0));
		JDesktopPane jdp = new JDesktopPane();
		((SerOptPlaySheet)playSheet).playSheetPanel.add(jdp);
		//playSheet.playSheetPanel.add(gps);
		String query="CONSTRUCT {?year ?transitioned ?ser. ?transitioned ?subprop ?relation. ?year ?subclass ?concept. ?trans ?transRel ?year. ?transRel ?subprop ?relation. ?trans ?subclass ?concept} WHERE { BIND(SUBSTR(STR(?fullName), 5) AS ?serName) BIND(SUBSTR(STR(?fullName), 0, 4) AS ?yearName) BIND(URI(CONCAT(\"http://semoss.org/ontologies/Concept/Year/\", ?yearName)) AS ?year)  BIND(URI(CONCAT(\"http://semoss.org/ontologies/Concept/Service/\", ?serName)) AS ?ser) BIND(URI(CONCAT(\"http://semoss.org/ontologies/Relation\",STR(?yearName), \":\", SUBSTR(STR(?ser), 45))) AS ?transitioned) BIND(<http://www.w3.org/2000/01/rdf-schema#subPropertyOf> AS ?subprop) BIND(<http://semoss.org/ontologies/Relation> AS ?relation) BIND(<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> AS ?subclass) BIND(<http://semoss.org/ontologies/Concept> AS ?concept) BIND(URI(\"http://semoss.org/ontologies/Concept/Transition/SOA_Transition\") AS ?trans) BIND(URI(CONCAT(\"http://semoss.org/ontologies/Relation\",SUBSTR(STR(?trans), 47), \":\", STR(?yearName))) AS ?transRel) } BINDINGS ?fullName" +bindStr;
		//String query="CONSTRUCT {?Capability ?support ?BusinessProcess.} WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;} {?support <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>;} {?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;}{?Capability ?support ?BusinessProcess;} }";
		gps.setRDFEngine((IEngine)DIHelper.getInstance().getLocalProp("TAP_Core_Data"));
		gps.setJDesktopPane(jdp);
		gps.setQuestionID("Graph Analysis PlaySheet");
		
		gps.setTitle("Graph Analysis");
		gps.setQuery(query);
		//playSheet.tabbedPane.setSelectedComponent(playSheet.playSheetPanel);
		QuestionPlaySheetStore.getInstance().put("Graph Analysis PlaySheet", gps);
		gps.createData();
		gps.runAnalytics();
		gps.createView();
		//jdp.add((Component) gps);
	}
	
	/**
	 * Displays specific information about a system in the playsheet.
	 * This includes year, system, service, cost, GLItem, phase, tag, and input.
	 * @param lin 	Optimizer used for TAP-specific calculations.
	 */
	public void displaySystemSpecifics(ServiceOptimizer lin)
	{
		Hashtable<String, ArrayList<Object []>> masterHash = optOrg.masterHash;
		ArrayList<Object []> list = new ArrayList<Object []>();
		String[] colNames = new String[8];
		colNames[0]="Year";
		colNames[1]="System";
		colNames[2]="Service";
		colNames[3]="Cost";
		colNames[4]="GLItem";
		colNames[5]="Phase";
		colNames[6]="Tag";
		colNames[7]="Input";
		for (int i = 0;i<lin.yearlyServicesList.size();i++)
		{
			int buildYear = 2014+i;
			ArrayList<String> serList = lin.yearlyServicesList.get(i);
			for (int j=0;j<serList.size();j++)
			{
				ArrayList<Object[]> returnedTable = (ArrayList<Object[]>) masterHash.get(serList.get(j));
				ArrayList<Object[]> newTable = new ArrayList<Object[]>();
				//need to add the year to the table
				for(Object[] array: returnedTable){
					Object[] newRow = new Object[8];
					newRow[0]=buildYear;
					for(int oldRowIdx = 0; oldRowIdx<array.length; oldRowIdx++){
						if(oldRowIdx == 2)//need to adjust the cost
							newRow[oldRowIdx+1] = Math.round(((Double) array[oldRowIdx]/f.learningConstants[i])*hourlyCost);
						else 
							newRow[oldRowIdx+1] = array[oldRowIdx];
					}
					newTable.add(newRow);
				}
				list.addAll(newTable);
			}
		}
		
		displayListOnTab(colNames,list,((SerOptPlaySheet)playSheet).specificSysAlysPanel);
	}
	

	/**
	 * Displays information about costs of a service in the playsheet.
	 * This includes year, provider cost, consumer cost, generic cost, and total implementation cost.
	 * @param lin 	Optimizer used for TAP-specific calculations.
	 */
	public void displaySpecifics(ServiceOptimizer lin)
	{
		Hashtable<String,Hashtable<String,Double>> serviceDetailsHash = optOrg.detailedServiceCostHash;
		Hashtable<String,Double> providerHash = serviceDetailsHash.get("provider");
		Hashtable<String,Double> consumerHash = serviceDetailsHash.get("consumer");
		Hashtable<String,Double> genericHash = serviceDetailsHash.get("generic");
		ArrayList <Object []> list = new ArrayList<Object []>();
		String[] colNames = new String[6];
		colNames[0]="Year";
		colNames[1]="Service";
		colNames[2]="Provider Cost";
		colNames[3]="Consumer Cost";
		colNames[4]="Generic Cost";
		colNames[5]="Total Implementation Cost";
		bindStr = "{";
		for (int i = 0;i<lin.yearlyServicesList.size();i++)
		{
			int buildYear = 2014+i;
			ArrayList<String> serList = lin.yearlyServicesList.get(i);
			for (int j=0;j<serList.size();j++)
			{
				Object[] listElement = new Object[6];
				listElement[0]=buildYear;
				String service = (String) serList.get(j);
				listElement[1]=service;
				bindStr = bindStr+"(\"" +buildYear+serList.get(j)+"\")";
				listElement[5]=Math.round(((Double)lin.ORIGserCostHash.get(serList.get(j))/f.learningConstants[i])*hourlyCost);
				
				if(providerHash.containsKey(service))
					listElement[2] = Math.round(((Double) providerHash.get(service)/f.learningConstants[i])*hourlyCost);
				if(consumerHash.containsKey(service))
					listElement[3] = Math.round(((Double) consumerHash.get(service)/f.learningConstants[i])*hourlyCost);
				if(genericHash.containsKey(service))
					listElement[4] = Math.round(((Double) genericHash.get(service)/f.learningConstants[i])*hourlyCost);
				
				list.add(listElement);
			}
		}
		bindStr=bindStr+"}";
		
		displayListOnTab(colNames,list,((SerOptPlaySheet)playSheet).specificAlysPanel);
	}
	
	/**
	 * Gets the end date based on number of work days and total days.
	 * @param startDate 	Start date.
	 * @param loe 			Level of effort expressed as a double.
	
	 * @return Date 		End date. */
	public Date getEndDate (Date startDate, double loe)
	{
		Integer workDays = (int) Math.ceil(loe/8);
		Integer totalDays = (int) (workDays + Math.floor(workDays/5)*2);
		Calendar cal = Calendar.getInstance();
		cal.setTime(startDate);
		cal.add(Calendar.DATE, totalDays);
		Date endDate = cal.getTime();
		return endDate;
	}
	
	/**
	 * Creates timeline data for all of the yearly services.
	 * Puts event in the overall hash  as long as the cost exists.
	 * Stores information about start date, end date, and required LOE for an event.
	 * @param lin 	Optimizer used for TAP-specific calculations.
	 */
	public void createTimelineData (ServiceOptimizer lin)
	{
		Hashtable<String,Hashtable<String,String>> allHash = new Hashtable<String,Hashtable<String,String>>();
		Hashtable<String,ArrayList<Object []>> masterHash = optOrg.masterHash;
//		ArrayList <Object []> list = new ArrayList();
		String[] colNames = new String[8];
		colNames[0]="Year";
		colNames[1]="System";
		colNames[2]="Service";
		colNames[3]="Cost";
		colNames[4]="GLItem";
		colNames[5]="Phase";
		colNames[6]="Tag";
		colNames[7]="Input";
		String[] sdlcPhase = new String[5];
		sdlcPhase[0]="Requirements";
		sdlcPhase[1]="Design";
		sdlcPhase[2]="Develop";
		sdlcPhase[3]="Test";
		sdlcPhase[4]="Deploy";
		for (int i = 0;i<lin.yearlyServicesList.size();i++)
		{
			int buildYear = 2014+i;
			SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
			Date startYearlyDate = null;
			try {
				startYearlyDate = sdf.parse("01/10/"+buildYear);
			} catch (ParseException e) {
				e.printStackTrace();
			}

			//startDate = Calendar.set(Calendar.YEAR, buildYear).
			//startDate.setYear(arg0)
			ArrayList<String> serList = lin.yearlyServicesList.get(i);
			int trackCounter = 0;
			for (int j=0;j<serList.size();j++)
			{
				
				Random randomGenerator = new Random();
				int red = randomGenerator.nextInt(255);
				int green = randomGenerator.nextInt(255);
				int blue = randomGenerator.nextInt(255);

	//			Color randomColour = new Color(red,green,blue);
				Date startDate = startYearlyDate;
				ArrayList<Object[]> returnedTable = (ArrayList<Object[]>) masterHash.get(serList.get(j));
				//need to add the year to the table
				for (String sdlc:sdlcPhase)
				{
					Hashtable<String,String> eventHash = new Hashtable<String, String>();
					String eventName = serList.get(j)+": "+sdlc;
					double totalCost = 0.0;
					double highestCost = 0.0;
					for (Object[]array: returnedTable)
					{
						String phase = (String) array[4];
						if (phase.equals(sdlc))
						{
							totalCost = totalCost+(Double)array[2];
							if((Double)array[2]>highestCost)
							{
								highestCost = (Double)array[2];
							}
						}
					}
					//only put event in overall Hash if a cost exists
					if(highestCost != 0.0)
					{
						double workers = Math.round(totalCost/highestCost);
						double loePerWorker = totalCost/workers;
						int numOfDays = (int) Math.ceil(loePerWorker/8);
						Date endDate = getEndDate(startDate, loePerWorker);
						workers = Utility.round(workers,1);
						String startDateString = prepareDateForJSON (startDate);
						eventHash.put("start",  startDateString);
						startDate = endDate;
						String endDateString = prepareDateForJSON (endDate);
						eventHash.put("end",  endDateString);
						eventHash.put("color", "rgb("+red+","+green+","+blue+")");
						eventHash.put("textColor",  "rgb(0,0,0)");
						String desString = "Total LOE: " + totalCost + " hours~Resources Needed:" +workers + " people~Total Time Elasped: " +numOfDays +" business days";
						eventHash.put("description", desString);
						eventHash.put("title",  eventName);
						eventHash.put("trackNum", trackCounter+"");
						trackCounter++;
						logger.info(eventName);
						logger.info(startDateString);
						logger.info(endDateString);
						logger.info(desString);
						allHash.put(eventName, eventHash);
					}
				
					
				}

			}
		}
		
		((SerOptPlaySheet)playSheet).timeline.callIt(allHash);
	}
	
	/**
	 * Prepares the date for JSON data-interchange.
	 * @param date 			Reflects coordinated universal time.
	
	 * @return retString	Date returned in string format. */
	public String prepareDateForJSON(Date date)
	{
		String retString = "";
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		//Date endDate = cal.getTime();
		int year = cal.get(Calendar.YEAR);
		int month = cal.get(Calendar.MONTH)+1;
		int day = cal.get(Calendar.DAY_OF_MONTH);
		
		retString = month+ " " + day + " " + year;
		return retString;
	}
	
	/**
	 * Displays the results from various optimization calculations. 
	 * These include profit, ROI, Recoup, and breakeven functions.
	 * @param lin 	Optimizer used for TAP-specific calculations. 
	 */
	public void displayResults(ServiceOptimizer lin)
	{
		double totalBuildCost=0.0;
		double profit = 0;
		double mu = (1+infRate)/(1+disRate);
		if(infRate!=disRate)
		{
			for (int i=0 ;i< lin.actualBudgetList.size();i++)
			{
				totalBuildCost = totalBuildCost+lin.actualBudgetList.get(i)*Math.pow(mu, i+1);
			}
		}
		else
		{
			for (int i=0 ;i< lin.actualBudgetList.size();i++)
			{
				totalBuildCost = totalBuildCost+lin.actualBudgetList.get(i);
			}
		}
		
		ProfitFunction pf = new ProfitFunction();
		pf.infRate = infRate;
		pf.disRate = disRate;
		pf.totalYrs = maxYears;
		profit = pf.getProfit(lin.objectiveValueList, lin.actualBudgetList);
		String profitString = Utility.sciToDollar(profit);
		
		ROIFunction rf = new ROIFunction();
		rf.infRate = infRate;
		rf.disRate = disRate;
		rf.totalYrs = maxYears;
		double ROI = rf.getROI(lin.objectiveValueList, lin.actualBudgetList);
		ROI = Utility.round(ROI*100,2);
		
		RecoupFunction bf = new RecoupFunction();
		bf.infRate = infRate;
		bf.disRate = disRate;
		bf.totalYrs = maxYears;
		double breakeven = bf.getBK(lin.objectiveValueList, lin.actualBudgetList);
		breakeven = Utility.round(breakeven,2);
		
		BreakevenFunction bkf = new BreakevenFunction();
		bkf.setSvcOpt(lin);
		double zero = bkf.getZero(maxYears);
		zero = Utility.round(zero,2);
		
		String costString = Utility.sciToDollar(totalBuildCost);
		((SerOptPlaySheet)playSheet).recoupLbl.setText(Double.toString(breakeven)+" Years");
		((SerOptPlaySheet)playSheet).savingLbl.setText(profitString);
		((SerOptPlaySheet)playSheet).roiLbl.setText(Double.toString(ROI)+"%");
		((SerOptPlaySheet)playSheet).costLbl.setText(costString);
		((SerOptPlaySheet)playSheet).bkevenLbl.setText(Double.toString(zero)+" Years");
		
		SerOptGraphFunctions graphF= new SerOptGraphFunctions();
		graphF.setOptimzer(this);
		graphF.setSvcOpt(lin);
		Hashtable chartHash1 = graphF.createBuildCostChart();
		Hashtable chartHash2 = graphF.createServiceSavings();
		Hashtable chartHash3 = graphF.createCostChart();
		Hashtable chartHash4 = graphF.createCumulativeSavings();
		Hashtable chartHash5 = graphF.createBreakevenGraph();
		Hashtable chartHash6 = OptChartUtilityMethods.createLearningCurve(2014, iniLC, scdLT, scdLC, f.learningConstants);
		((SerOptPlaySheet)playSheet).tab1.callIt(chartHash1);
		((SerOptPlaySheet)playSheet).tab2.callIt(chartHash2);
		((SerOptPlaySheet)playSheet).tab3.callIt(chartHash3);
		((SerOptPlaySheet)playSheet).tab4.callIt(chartHash4);
		((SerOptPlaySheet)playSheet).tab5.callIt(chartHash5);
		((SerOptPlaySheet)playSheet).tab6.callIt(chartHash6);
	}
}

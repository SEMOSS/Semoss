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

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Hashtable;

import javax.swing.JPanel;
import javax.swing.JProgressBar;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.IAlgorithm;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.GridScrollPane;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.specific.tap.DHMSMSysDecommissionSchedulingPlaySheet;
import prerna.ui.components.specific.tap.SysDecommissionOptimizationFunctions;
import prerna.ui.components.specific.tap.SysDecommissionScheduleGraphFunctions;
import prerna.ui.components.specific.tap.SystemPropertyGridPlaySheet;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * This class is used to optimize the calculations for univariate services.
 */
public class SysDecommissionScheduleOptimizer implements IAlgorithm{
	
	static final Logger logger = LogManager.getLogger(SysDecommissionScheduleOptimizer.class.getName());
	
	DHMSMSysDecommissionSchedulingPlaySheet playSheet;
	public int maxYears;
	double serMainPerc;
	double minBudget;
	double maxBudget;
	double hourlyCost;
	double percentOfPilot = 0.20;
	double budget = 100000000.0;
	double totalTransformCost=0.0;

	public double totalSavings=0.0;
	public double investment=0.0;

	String bindStr = "";
	JProgressBar progressBar;
	SysDecommissionOptimizationFunctions optFunctions;
    SysDecommissionSchedulingSavingsOptimizer opt;
    SystemPropertyGridPlaySheet sysBudgetSheet;

	private static String hrCoreDB = "HR_Core";
	private static String systemListQuery = "SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?HighProb} {?System <http://semoss.org/ontologies/Relation/Contains/MHS_Specific> 'N'} FILTER(?HighProb in('High','Question'))}";
	
	private static String systemBudgetQuery = "High;All;All; SELECT DISTINCT ?System (GROUP_CONCAT(?OwnerName ; SEPARATOR = ', ') AS ?Owner) ?GarrisonTheater ?MHS_Specific ?Transaction_Count ?ATO_Date ?End_Of_Support ?Num_Users WHERE { SELECT DISTINCT ?System (COALESCE(SUBSTR(STR(?Own),50),'') AS ?OwnerName) (COALESCE(?GT, 'Garrison') AS ?GarrisonTheater) ?MHS_Specific (COALESCE(?TC,'') AS ?Transaction_Count) (COALESCE(SUBSTR(STR(?ATO),0,10),'') AS ?ATO_Date) (COALESCE(SUBSTR(STR(?ES),0,10),'') AS ?End_Of_Support) (COALESCE(?NU,'') AS ?Num_Users) ?Probability WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?System <http://semoss.org/ontologies/Relation/Contains/Received_Information> 'Y'} {?System <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'} {?System <http://semoss.org/ontologies/Relation/Contains/MHS_Specific> 'N'} BIND('N' AS ?MHS_Specific)OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> ?GT}} OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/Transaction_Count> ?TC}} OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/ATO_Date> ?ATO}} OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/End_of_Support_Date> ?ES}} OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/Number_of_Users> ?NU}} OPTIONAL{{?System <http://semoss.org/ontologies/Relation/OwnedBy> ?Own}} } }  GROUP BY ?System ?GarrisonTheater ?MHS_Specific ?Transaction_Count ?ATO_Date ?End_Of_Support ?Num_Users ORDER BY ?System";

	private ArrayList<String> systemList = new ArrayList<String>();
	private Hashtable<String, Double[]> sysToBudgetHash;
	
	
	/**
	 * Method setVariables.
	 * @param maxYears int
	 * @param interfaceCost double
	 * @param minBudget double
	 * @param maxBudget double
	 */
	public void setVariables(int maxYears,double serMainPerc,double minBudget, double maxBudget)
	{
		this.maxYears = maxYears;
		this.serMainPerc = serMainPerc;
		this.minBudget = minBudget*1000000;
		this.maxBudget = maxBudget*1000000;
	}
	
	public void setProgressBar (JProgressBar progressBar) {
		progressBar.setVisible(true);
		progressBar.setIndeterminate(true);
		progressBar.setStringPainted(true);
		this.progressBar = progressBar;
	}
	
	public SysDecommissionSchedulingSavingsOptimizer getSavingsOpt() {
		return opt;
	}
	
	public void addTextToConsole(String text) {
		playSheet.consoleArea.setText(playSheet.consoleArea.getText()+text);
	}
	
	/**
	 * Runs the optimization for services.
	 */
	public void optimize()
	{
        collectData();
        
       	boolean success=true;
        opt = new SysDecommissionSchedulingSavingsOptimizer();
        //to do put in sustainment cost
        opt.setDataSet(playSheet.consoleArea,systemList, optFunctions.sysToSiteCountHash,optFunctions.getSysToWorkVolHashPerSite(),deepCopy(sysToBudgetHash),budget, maxYears, percentOfPilot,serMainPerc);
        int count = 1;
        progressBar.setString("Iteration: "+count);
       	addTextToConsole("\n\nStarting Iteration "+count+". Attempting budget of "+budget+".");
        int timeToComplete = opt.runScheduling();
        if(timeToComplete>maxYears)
        	addTextToConsole("\nFor budget "+budget+", time to complete decommissioning is greater than "+maxYears+" years.");
        else
        	addTextToConsole("\nFor budget "+budget+", time to complete decommissioning is "+timeToComplete+" years.");
        while(timeToComplete!=maxYears&&count<20)
        {
        	if(timeToComplete<maxYears)
        		budget = budget*timeToComplete/maxYears;
        	else
        		budget = budget*totalTransformCost/opt.getTotalInvestment();
        	count++;
            progressBar.setString("Iteration: "+count);
           	addTextToConsole("\n\nStarting Iteration "+count+". Attempting budget of "+budget+".");
        	opt.setBudget(budget);
        	timeToComplete = opt.runScheduling();
           	addTextToConsole("\nFor budget "+budget+", time to complete decommissioning is "+timeToComplete+" years.");
        }
        if(count==20)
        	success = false;
        if(success) {
        	addTextToConsole("\n\nSolution found. Decommissioning completed in "+maxYears+" years with budget of "+budget+".");
        	displayResults();
        }
        else
        {
            addTextToConsole("\n\nSolution not found. Please increase the maximum amount of years for decommissioning.");
            clearPlaysheet();
        }
        progressBar.setIndeterminate(false);
        progressBar.setVisible(false);
	}
	
	public void printDataToConsole()
	{
       	addTextToConsole("\nSystems to be decommissioned:");
		for(String sys : systemList)
			addTextToConsole(" "+sys+",");
		printHashTable("Number of sites for each system:",optFunctions.sysToSiteCountHash);
		printHashTable("Cost to transition first site for each system:",optFunctions.getSysToWorkVolHashPerSite());
		//to do: print out right sustaincosts
		printBudgetHashTable("Current sustainment cost for each system:",sysToBudgetHash);
	}
	
	public void printHashTable(String nameOfHash,Hashtable hashToPrint)
	{
       	addTextToConsole("\n"+nameOfHash);
		for(Object key : hashToPrint.keySet())
			addTextToConsole(" "+key+": "+hashToPrint.get(key)+",");
	}
	
	public void printBudgetHashTable(String nameOfHash,Hashtable<String,Double[]> sysBudgetHash)
	{
       	addTextToConsole("\n"+nameOfHash);
		for(Object key : sysBudgetHash.keySet())
		{
			addTextToConsole(" "+key+": ");
			Double[] row = sysBudgetHash.get(key);
			for(int i=0;i<5;i++)
			{
				addTextToConsole(row[i]+", ");
			}
			addTextToConsole(";");
		}
	}
	public Hashtable<String,Double[]> deepCopy(Hashtable<String,Double[]> hash)
	{
		Hashtable<String,Double[]> hashCopy = new Hashtable<String,Double[]>();
		for(String key : hash.keySet())
		{
			Double[] row = hash.get(key);
			Double[] rowCopy = new Double[row.length];
			for(int i=0;i<row.length;i++)
				rowCopy[i] = row[i];
			hashCopy.put(key, rowCopy);
		}
		return hashCopy;
	}
	public void collectData()
	{
		playSheet.consoleArea.setText("Retrieving cost data from TAP_Cost_db...");
        setProgressBar(playSheet.progressBar);
        progressBar.setString("Collecting Data");

		createSystemList();
		optFunctions = new SysDecommissionOptimizationFunctions();
		
		optFunctions.setSysList(systemList);
		optFunctions.setPilotBoolean(true);
		optFunctions.setFirstSiteBoolean(false);
		optFunctions.setstoreWorkVolInDays(false);
		optFunctions.setIncludeArchiveBoolean(true);
		optFunctions.instantiate();
		
		calculateTotalTransformCost();
		
	    sysBudgetSheet = new SystemPropertyGridPlaySheet();
	    sysBudgetSheet.setQuery(systemBudgetQuery);
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(hrCoreDB);
		sysBudgetSheet.setRDFEngine(engine);
		sysBudgetSheet.setAccountingFormat(false);
	    sysBudgetSheet.createData();
	    processSysBudgetHash(sysBudgetSheet.getList());
	    
        printDataToConsole();
       	addTextToConsole("\nData Collection Complete!");
	}
	
	public void processSysBudgetHash(ArrayList<Object[]> sysBudgetList)
	{
		sysToBudgetHash = new Hashtable<String,Double[]>();
		for(int sysInd=0;sysInd<sysBudgetList.size();sysInd++)
		{
			Object[] fullRow = sysBudgetList.get(sysInd);
			String sys = (String)fullRow[0];
			Double[] fyRow = new Double[5];
			for(int yearInd=0;yearInd<fyRow.length;yearInd++)
			{
				Double sysBudget=null;
				if(fullRow[fullRow.length-5+yearInd] instanceof Long)
					sysBudget = 1.0*((Long)fullRow[fullRow.length-5+yearInd]);
				else{
					System.out.println("Budget is not a numerical value for "+sys);
				}
//					sysBudget = ((DecimalFormat) fullRow[fullRow.length-5+yearInd]).get
				fyRow[yearInd] = sysBudget;
			}
			sysToBudgetHash.put(sys,fyRow);
		}
	}
	public void calculateTotalTransformCost()
	{
		Hashtable<String,Double> workVolHash = optFunctions.getSysToWorkVolHashPerSite();
		Hashtable<String,Integer> sysToSiteCountHash = optFunctions.sysToSiteCountHash;
		for(String sys : workVolHash.keySet())
		{
			double workVol = workVolHash.get(sys);
			int sites = 1;
			if(sysToSiteCountHash.containsKey(sys))
				sites = sysToSiteCountHash.get(sys);
			totalTransformCost +=workVol*sites;
		}
	}
	
	public ISelectWrapper executeQuery(String engineName,String query) {

		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);

		/*SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.setEngineType(IEngine.ENGINE_TYPE.SESAME);
		*/
		
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);

		/*
		try{
			wrapper.executeQuery();	
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}*/		
		return wrapper;
	}
	
	public void createSystemList() {
		
		ISelectWrapper wrapper = executeQuery(hrCoreDB,systemListQuery);

		// get the bindings from it
		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				ISelectStatement sjss = wrapper.next();
				systemList.add((String)getVariable(names[0], sjss));
			}
		} catch (Exception e) {
			logger.fatal(e);
		}
	}
	
	
	public Object getVariable(String varName, ISelectStatement sjss){
		return sjss.getVar(varName);
	}

	
	/**
	 * Creates and displays the results from various optimization calculations. 
	 * These include savings, net savings, invest, budget, roi labels and charts on overall Analysis Tab.
	 * Also includes system analysis tab
	 */
	public void displayResults()
	{
        progressBar.setString("Creating Visualizations");
        SysDecommissionScheduleGraphFunctions graphF= new SysDecommissionScheduleGraphFunctions();
		graphF.setOptimzer(this);
		Hashtable chartHash1 = graphF.createBuildCostChart();
		Hashtable chartHash2 = graphF.createSavingsByYear();
		Hashtable chartHash3 = graphF.createCostChart();
		Hashtable chartHash4 = graphF.createYearlySavings();
		Hashtable chartHash5 = graphF.createBreakevenGraph();
		playSheet.tab1.callIt(chartHash1);
		playSheet.tab2.callIt(chartHash2);
		playSheet.tab3.callIt(chartHash3);
		playSheet.tab4.callIt(chartHash4);
		playSheet.tab5.callIt(chartHash5);
		playSheet.tab1.setVisible(true);
		playSheet.tab2.setVisible(true);
		playSheet.tab3.setVisible(true);
		playSheet.tab4.setVisible(true);
		playSheet.tab5.setVisible(true);
        displayLabels();
        displaySystemSiteSpecifics();
        displaySystemCostSavingsSpecifics();
	}
	
	public void displayLabels()
	{
		String savingString = Utility.sciToDollar(totalSavings);
		
		String netSavingString = Utility.sciToDollar(totalSavings - investment);
		
		double ROI = (totalSavings-investment)/investment;
		ROI = Utility.round(ROI*100,2);

		String investmentString = Utility.sciToDollar(investment);
		
		String budgetString = Utility.sciToDollar(budget);

		playSheet.savingLbl.setText(savingString);
		playSheet.netSavingLbl.setText(netSavingString);
		playSheet.roiLbl.setText(Double.toString(ROI)+"%");
		playSheet.investLbl.setText(investmentString);
		playSheet.budgetLbl.setText(budgetString);

	}
	
	/**
	 * Clears the playsheet by removing information from all panels.
	 */
	public void clearPlaysheet(){
		clearLabels();
		clearGraphs();
		playSheet.sysNumSitesAnalPanel.removeAll();
		playSheet.sysCostSavingsAnalPanel.removeAll();
	}
	
	/**
	 * Clears graphs within the playsheets.
	 */
	public void clearGraphs()
	{
		playSheet.tab1.setVisible(false);
		playSheet.tab2.setVisible(false);
		playSheet.tab3.setVisible(false);
		playSheet.tab4.setVisible(false);
		playSheet.tab5.setVisible(false);
	}
	
	/**
	 * Sets N/A or $0 for values in optimizations. Allows for different TAP algorithms to be run as empty functions.
	 */
	public void clearLabels()
	{
        playSheet.savingLbl.setText("$0");
        playSheet.netSavingLbl.setText("$0");
		playSheet.roiLbl.setText("N/A");
        playSheet.investLbl.setText("$0");
        playSheet.budgetLbl.setText("$0");
	}
	
	/**
	 * Displays information about the number of sites transformed for each system for each year.
	 */
	public void displaySystemSiteSpecifics()
	{
		ArrayList <Object []> list = new ArrayList<Object []>();
		String[] colNames = new String[maxYears+1];
		colNames[0]="System";
		for(int i=1;i<=maxYears;i++)
			colNames[i] = "Sites Transitioned in Year T+"+i;
		ArrayList<Double[]> systemSiteMatrix = opt.getSysNumSitesMatrix();
		for(int sysInd = 0;sysInd<systemSiteMatrix.get(0).length;sysInd++)
		{
			Object[] rowForSys = new Object[maxYears+1];
			rowForSys[0] = systemList.get(sysInd);
			for(int i=1;i<=maxYears;i++)
				rowForSys[i] = (systemSiteMatrix.get(i-1)[sysInd]).intValue();
			list.add(rowForSys);
		}
		displayListOnTab(colNames,list,playSheet.sysNumSitesAnalPanel);
	}
	
	/**
	 * Displays information about the savings and cost for transitioning each system for each year.
	 */
	public void displaySystemCostSavingsSpecifics()
	{
		Hashtable<String,String> sysToOwner = optFunctions.getSysToOwnerHash();
		ArrayList <Object []> list = new ArrayList<Object []>();
		String[] colNames = new String[maxYears+3];
		colNames[0] = "System";
		colNames[1] = "System Owner";
		colNames[2] = "Item";
		for(int i=1;i<maxYears+1;i++)
			colNames[i+2] = "Year T+"+i;
		ArrayList<Double[]> systemInvestCostMatrix = opt.getSysInvestCostMatrix();
		ArrayList<Double[]> systemSavingsMatrix = opt.getSysSavingsMatrix();
		
		for(int sysInd = 0;sysInd<systemInvestCostMatrix.get(0).length;sysInd++)
		{
			String sys = systemList.get(sysInd);
			String owner = "";
			if(sysToOwner.containsKey(sys))
				owner = sysToOwner.get(sys);
			Object[] budgetRow = new Object[maxYears+3];
			budgetRow[0] = sys;
			budgetRow[1] = owner;
			budgetRow[2] = "System Budget";
			Double[] fyRow = sysToBudgetHash.get(sys);
			if(fyRow==null)
				for(int i=0;i<maxYears;i++)
					budgetRow[i+3] = "";
			else {
				for(int i=0;i<maxYears;i++) {
					if(i<5) {
						if(fyRow[i]==null)
							budgetRow[i+3]=null;
						else {
							DecimalFormat nf = new DecimalFormat("\u00A4 #,##0.00");
							budgetRow[i+3] = nf.format(Math.round(fyRow[i]));
						}
					}
					else {
						if(fyRow[4]==null)
							budgetRow[i+3]=null;
						else {
							DecimalFormat nf = new DecimalFormat("\u00A4 #,##0.00");
							budgetRow[i+3] = nf.format(Math.round(fyRow[4]));
						}
					}
				}
			}
			list.add(budgetRow);
			Object[] systemCostRow = createSystemCostSavingsRow(sys,owner,"Decommissioning Costs",systemInvestCostMatrix,sysInd);
			list.add(systemCostRow);
			Object[] systemSavingsRow = createSystemCostSavingsRow(sys,owner,"Estimated Savings",systemSavingsMatrix,sysInd);
			list.add(systemSavingsRow);
		}
		displayListOnTab(colNames,list,playSheet.sysCostSavingsAnalPanel);
	}

	public Object[] createSystemCostSavingsRow(String sys, String owner, String item, ArrayList<Double[]> systemCostSavingsMatrix,int sysInd)
	{
		Object[] systemCostSavingsRow = new Object[maxYears+3];
		systemCostSavingsRow[0] = sys;
		systemCostSavingsRow[1] = owner;//to put system owner here
		systemCostSavingsRow[2] = item;
		for(int i=1;i<maxYears+1;i++) {
			DecimalFormat nf = new DecimalFormat("\u00A4 #,##0.00");
			systemCostSavingsRow[i+2] = nf.format(Math.round(systemCostSavingsMatrix.get(i-1)[sysInd]));
		}
		return systemCostSavingsRow;
	}
	
	public void displayListOnTab(String[] colNames,ArrayList <Object []> list,JPanel panel)
	{
		GridScrollPane pane = new GridScrollPane(colNames, list);
		for(int i=colNames.length-1;i>=(colNames.length-maxYears);i--)
			pane.rightAlignColumn(i);
		
		panel.removeAll();
		GridBagLayout gridBagLayout = new GridBagLayout();
		gridBagLayout.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gridBagLayout.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		panel.setLayout(gridBagLayout);
		GridBagConstraints gbc_panel_1_1 = new GridBagConstraints();
		gbc_panel_1_1.insets = new Insets(0, 0, 5, 5);
		gbc_panel_1_1.fill = GridBagConstraints.BOTH;
		gbc_panel_1_1.gridx = 0;
		gbc_panel_1_1.gridy = 0;
		panel.add(pane, gbc_panel_1_1);
		panel.repaint();
	}
		
	/**
	 * Sets the passed playsheet as a service optimization playsheet.
	 * @param 	playSheet	Playsheet to be cast.
	 */
	@Override
	public void setPlaySheet(IPlaySheet playSheet) {
		this.playSheet = (DHMSMSysDecommissionSchedulingPlaySheet) playSheet;
		
	}

	/**
	 * Gets variable names.
	
	 * //TODO: Return empty object instead of null
	 * @return String[] */
	@Override
	public String[] getVariables() {
		return null;
	}

	/**
	 * Executes the optimization.
	 */
	@Override
	public void execute(){
		optimize();
		
	}

	/**
	 * Gets the name of the algorithm.
	
	 * //TODO: Return empty object instead of null
	 * @return 	Algorithm name. */
	@Override
	public String getAlgoName() {
		return null;
	}
	
}

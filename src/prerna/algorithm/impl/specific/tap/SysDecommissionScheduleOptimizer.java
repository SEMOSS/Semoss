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
package prerna.algorithm.impl.specific.tap;

import java.util.ArrayList;
import java.util.Hashtable;

import javax.swing.JProgressBar;

import org.apache.log4j.Logger;

import prerna.algorithm.api.IAlgorithm;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.specific.tap.DHMSMSysDecommissionSchedulingPlaySheet;
import prerna.ui.components.specific.tap.SysDecommissionOptimizationFunctions;
import prerna.util.DIHelper;

/**
 * This class is used to optimize the calculations for univariate services.
 */
public class SysDecommissionScheduleOptimizer implements IAlgorithm{
	
	Logger logger = Logger.getLogger(getClass());
	
	DHMSMSysDecommissionSchedulingPlaySheet playSheet;
	public int maxYears;
	double serMainPerc;
//	int noOfPts;
	double minBudget;
	double maxBudget;
	double hourlyCost;
	double percentOfPilot = 0.20;
	double budget = 100000000.0;
	double totalTransformCost=0.0;

	//change to SysDecommissionScheduleOptFuncgtion
	double optBudget =0.0;
	
	String bindStr = "";
	JProgressBar progressBar;
	SysDecommissionOptimizationFunctions optFunctions;
//	OptimizationOrganizer optOrg;
//	public String[] optSys;

	private static String hrCoreDB = "HR_Core";
	private static String systemListQuery = "SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?System <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?HighProb} FILTER(?HighProb in('High','Question'))}";

	private ArrayList<String> systemList = new ArrayList<String>();
 
	
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
        SysDecommissionSchedulingSavingsOptimizer opt = new SysDecommissionSchedulingSavingsOptimizer();
        opt.setDataSet(playSheet.consoleArea,systemList, optFunctions.sysToSiteCountHash,optFunctions.getSysToWorkVolHashPerSite(),optFunctions.sysToSustainmentCost, budget, maxYears, percentOfPilot,serMainPerc);
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
		//should fill with the data collected
       	addTextToConsole("\nSystems to be decommissioned:");
		for(String sys : systemList)
			addTextToConsole(" "+sys+",");
		printHashTable("Number of sites for each system:",optFunctions.sysToSiteCountHash);
		printHashTable("Cost to transition first site for each system:",optFunctions.getSysToWorkVolHashPerSite());
		printHashTable("Current systainment cost for each system:",optFunctions.sysToSustainmentCost);
	}
	
	public void printHashTable(String nameOfHash,Hashtable hashToPrint)
	{
       	addTextToConsole("\n"+nameOfHash);
		for(Object key : hashToPrint.keySet())
			addTextToConsole(" "+key+": "+hashToPrint.get(key)+",");
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
		optFunctions.instantiate();
		
		calculateTotalTransformCost();
		
        printDataToConsole();
       	addTextToConsole("\nData Collection Complete!");
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
	
	public SesameJenaSelectWrapper executeQuery(String engineName,String query) {
		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.setEngineType(IEngine.ENGINE_TYPE.SESAME);
		try{
			wrapper.executeQuery();	
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}		
		return wrapper;
	}
	
	public void createSystemList() {
		
		SesameJenaSelectWrapper wrapper = executeQuery(hrCoreDB,systemListQuery);

		// get the bindings from it
		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.next();
				systemList.add((String)getVariable(names[0], sjss));
			}
		} catch (Exception e) {
			logger.fatal(e);
		}
	}
	
	
	public Object getVariable(String varName, SesameJenaSelectStatement sjss){
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
        displayLabels();
        //displayCharts();
	}
	
	public void displayLabels()
	{
		double totalBuildCost=0.0;
		double profit = 0;
	
//		ProfitFunction pf = new ProfitFunction();
//		pf.infRate = infRate;
//		pf.disRate = disRate;
//		pf.totalYrs = maxYears;
//		profit = pf.getProfit(lin.objectiveValueList, lin.actualBudgetList);
//		String profitString = Utility.sciToDollar(profit);
//		
//		ROIFunction rf = new ROIFunction();
//		rf.infRate = infRate;
//		rf.disRate = disRate;
//		rf.totalYrs = maxYears;
//		double ROI = rf.getROI(lin.objectiveValueList, lin.actualBudgetList);
//		ROI = Utility.round(ROI*100,2);
//		
//		RecoupFunction bf = new RecoupFunction();
//		bf.infRate = infRate;
//		bf.disRate = disRate;
//		bf.totalYrs = maxYears;
//		double breakeven = bf.getBK(lin.objectiveValueList, lin.actualBudgetList);
//		breakeven = Utility.round(breakeven,2);
//		
//		BreakevenFunction bkf = new BreakevenFunction();
//		bkf.setSvcOpt(lin);
//		double zero = bkf.getZero(maxYears);
//		zero = Utility.round(zero,2);
//		
//		String costString = Utility.sciToDollar(totalBuildCost);
//      playSheet.savingLbl.setText(profitString);
//		playSheet.roiLbl.setText(Double.toString(ROI)+"%");
//		playSheet.costLbl.setText(costString);
//		playSheet.bkevenLbl.setText(Double.toString(zero)+" Years");
//		ArrayList<double[]> savingsPerYearList = new ArrayList<double[]>();
//		

	}
	
	public void displayCharts(){
//		SerOptGraphFunctions graphF= new SerOptGraphFunctions();
//		graphF.setOptimzer(this);
//		graphF.setSvcOpt(lin);
//		Hashtable chartHash1 = graphF.createBuildCostChart();
//		Hashtable chartHash2 = graphF.createServiceSavings();
//		Hashtable chartHash3 = graphF.createCostChart();
//		Hashtable chartHash4 = graphF.createCumulativeSavings();
//		Hashtable chartHash5 = graphF.createBreakevenGraph();
//		playSheet.tab1.callIt(chartHash1);
//		playSheet.tab2.callIt(chartHash2);
//		playSheet.tab3.callIt(chartHash3);
//		playSheet.tab4.callIt(chartHash4);
//		playSheet.tab5.callIt(chartHash5);
	}
	
	
	/**
	 * Clears the playsheet by removing information from all panels.
	 */
	public void clearPlaysheet(){
		clearLabels();
		clearGraphs();
		playSheet.specificSysAlysPanel.removeAll();
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
//	
//	/**
//	 * Displays specific information about a system in the playsheet.
//	 * This includes year, system, service, cost, GLItem, phase, tag, and input.
//	 * @param lin 	Optimizer used for TAP-specific calculations.
//	 */
//	public void displaySystemSpecifics(ServiceOptimizer lin)
//	{
//		Hashtable masterHash = optOrg.masterHash;
//		ArrayList <Object []> list = new ArrayList();
//		String[] colNames = new String[8];
//		colNames[0]="Year";
//		colNames[1]="System";
//		colNames[2]="Service";
//		colNames[3]="Cost";
//		colNames[4]="GLItem";
//		colNames[5]="Phase";
//		colNames[6]="Tag";
//		colNames[7]="Input";
//		for (int i = 0;i<lin.yearlyServicesList.size();i++)
//		{
//			int buildYear = 2014+i;
//			ArrayList serList = lin.yearlyServicesList.get(i);
//			for (int j=0;j<serList.size();j++)
//			{
//				ArrayList<Object[]> returnedTable = (ArrayList<Object[]>) masterHash.get(serList.get(j));
//				ArrayList<Object[]> newTable = new ArrayList();
//				//need to add the year to the table
//				for(Object[] array: returnedTable){
//					Object[] newRow = new Object[8];
//					newRow[0]=buildYear;
//					for(int oldRowIdx = 0; oldRowIdx<array.length; oldRowIdx++){
//						if(oldRowIdx == 2)//need to adjust the cost
//							newRow[oldRowIdx+1] = Math.round(((Double) array[oldRowIdx]/f.learningConstants[i])*hourlyCost);
//						else 
//							newRow[oldRowIdx+1] = array[oldRowIdx];
//					}
//					newTable.add(newRow);
//				}
//				list.addAll(newTable);
//			}
//		}
//		GridScrollPane pane = new GridScrollPane(colNames, list);
//		
//		playSheet.specificSysAlysPanel.removeAll();
//		JPanel panel = new JPanel();
//		GridBagLayout gridBagLayout = new GridBagLayout();
//		gridBagLayout.columnWeights = new double[]{1.0, Double.MIN_VALUE};
//		gridBagLayout.rowWeights = new double[]{1.0, Double.MIN_VALUE};
//		playSheet.specificSysAlysPanel.setLayout(gridBagLayout);
//		GridBagConstraints gbc_panel_1_1 = new GridBagConstraints();
//		gbc_panel_1_1.insets = new Insets(0, 0, 5, 5);
//		gbc_panel_1_1.fill = GridBagConstraints.BOTH;
//		gbc_panel_1_1.gridx = 0;
//		gbc_panel_1_1.gridy = 0;
//		playSheet.specificSysAlysPanel.add(pane, gbc_panel_1_1);
//		playSheet.specificSysAlysPanel.repaint();
//	}
//	
//
//	/**
//	 * Gets the end date based on number of work days and total days.
//	 * @param startDate 	Start date.
//	 * @param loe 			Level of effort expressed as a double.
//	
//	 * @return Date 		End date. */
//	public Date getEndDate (Date startDate, double loe)
//	{
//		Integer workDays = (int) Math.ceil(loe/8);
//		Integer totalDays = (int) (workDays + Math.floor(workDays/5)*2);
//		Calendar cal = Calendar.getInstance();
//		cal.setTime(startDate);
//		cal.add(Calendar.DATE, totalDays);
//		Date endDate = cal.getTime();
//		return endDate;
//	}
//	
//	/**
//	 * Creates timeline data for all of the yearly services.
//	 * Puts event in the overall hash  as long as the cost exists.
//	 * Stores information about start date, end date, and required LOE for an event.
//	 * @param lin 	Optimizer used for TAP-specific calculations.
//	 */
//	public void createTimelineData (ServiceOptimizer lin)
//	{
//		Hashtable allHash = new Hashtable();
//		Hashtable masterHash = optOrg.masterHash;
//		ArrayList <Object []> list = new ArrayList();
//		String[] colNames = new String[8];
//		colNames[0]="Year";
//		colNames[1]="System";
//		colNames[2]="Service";
//		colNames[3]="Cost";
//		colNames[4]="GLItem";
//		colNames[5]="Phase";
//		colNames[6]="Tag";
//		colNames[7]="Input";
//		String[] sdlcPhase = new String[5];
//		sdlcPhase[0]="Requirements";
//		sdlcPhase[1]="Design";
//		sdlcPhase[2]="Develop";
//		sdlcPhase[3]="Test";
//		sdlcPhase[4]="Deploy";
//		for (int i = 0;i<lin.yearlyServicesList.size();i++)
//		{
//			int buildYear = 2014+i;
//			SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
//			Date startYearlyDate = null;
//			try {
//				startYearlyDate = sdf.parse("01/10/"+buildYear);
//			} catch (ParseException e) {
//				e.printStackTrace();
//			}
//
//			//startDate = Calendar.set(Calendar.YEAR, buildYear).
//			//startDate.setYear(arg0)
//			ArrayList serList = lin.yearlyServicesList.get(i);
//			int trackCounter = 0;
//			for (int j=0;j<serList.size();j++)
//			{
//				
//				Random randomGenerator = new Random();
//				int red = randomGenerator.nextInt(255);
//				int green = randomGenerator.nextInt(255);
//				int blue = randomGenerator.nextInt(255);
//
//				Color randomColour = new Color(red,green,blue);
//				Date startDate = startYearlyDate;
//				ArrayList<Object[]> returnedTable = (ArrayList<Object[]>) masterHash.get(serList.get(j));
//				//need to add the year to the table
//				for (String sdlc:sdlcPhase)
//				{
//					Hashtable eventHash = new Hashtable();
//					String eventName = serList.get(j)+": "+sdlc;
//					double totalCost = 0.0;
//					double highestCost = 0.0;
//					for (Object[]array: returnedTable)
//					{
//						String phase = (String) array[4];
//						if (phase.equals(sdlc))
//						{
//							totalCost = totalCost+(Double)array[2];
//							if((Double)array[2]>highestCost)
//							{
//								highestCost = (Double)array[2];
//							}
//						}
//					}
//					//only put event in overall Hash if a cost exists
//					if(highestCost != 0.0)
//					{
//						double workers = Math.round(totalCost/highestCost);
//						double loePerWorker = totalCost/workers;
//						int numOfDays = (int) Math.ceil(loePerWorker/8);
//						Date endDate = getEndDate(startDate, loePerWorker);
//						workers = Utility.round(workers,1);
//						String startDateString = prepareDateForJSON (startDate);
//						eventHash.put("start",  startDateString);
//						startDate = endDate;
//						String endDateString = prepareDateForJSON (endDate);
//						eventHash.put("end",  endDateString);
//						eventHash.put("color", "rgb("+red+","+green+","+blue+")");
//						eventHash.put("textColor",  "rgb(0,0,0)");
//						String desString = "Total LOE: " + totalCost + " hours~Resources Needed:" +workers + " people~Total Time Elasped: " +numOfDays +" business days";
//						eventHash.put("description", desString);
//						eventHash.put("title",  eventName);
//						eventHash.put("trackNum", trackCounter+"");
//						trackCounter++;
//						logger.info(eventName);
//						logger.info(startDateString);
//						logger.info(endDateString);
//						logger.info(desString);
//						allHash.put(eventName, eventHash);
//					}
//				
//					
//				}
//
//			}
//		}
//		
//		playSheet.timeline.callIt(allHash);
//	}
//	
//	/**
//	 * Prepares the date for JSON data-interchange.
//	 * @param date 			Reflects coordinated universal time.
//	
//	 * @return retString	Date returned in string format. */
//	public String prepareDateForJSON(Date date)
//	{
//		String retString = "";
//		Calendar cal = Calendar.getInstance();
//		cal.setTime(date);
//		//Date endDate = cal.getTime();
//		int year = cal.get(Calendar.YEAR);
//		int month = cal.get(Calendar.MONTH)+1;
//		int day = cal.get(Calendar.DAY_OF_MONTH);
//		
//		retString = month+ " " + day + " " + year;
//		return retString;
//	}

	
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

package prerna.ui.components.specific.ousd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import lpsolve.LpSolve;
import lpsolve.LpSolveException;
import prerna.algorithm.impl.LPOptimizer;
import prerna.annotations.BREAKOUT;

@BREAKOUT
public class SystemRoadmapOptimizer extends LPOptimizer{

	//TODO: loop for multiple runs of optimization
	protected static final Logger LOGGER = LogManager.getLogger(BLUSystemOptimizer.class.getName());

	String[] sysList;
	boolean limit;
	int year;
	int totalYears;
	int totalSystemCount;
	Map<String, Double> sysBudgets;
	Map<String, Double> systemCostPenalty = new HashMap<String, Double>();
	Map<String, List<String>> retirementMap; //retirement type -> systems that have that retirementType
	Map<String, List<String>> granularBluMap; //granular blu -> list of systems that support this granular blu
	Map<String, List<String>> bluMap; //blu -> list of systems that support this blu.
	Map<String, List<String>> dataMap; //data object -> list of systems that support this data object
	int sysListSize;
	double[] results;
	double interfaceCost;
	double replacementPercentage;
	List<String> keptSystems;
	List<String> enduringSystems = new ArrayList<String>();


	/**
	 * 
	 */
	public SystemRoadmapOptimizer(){
		super();
	}

	/**
	 * Makes the new LpSolver and sets variables in model.
	 * Variables are: one for each system at every site,
	 * one for each system to say deployed at any site (kept or not),
	 * one for each centrally deployed system to say deployed at all sites (kept or not)
	 * Also declares all variables to be binary and gives a starting point for optimization.
	 */
	@Override
	public void setVariables() throws LpSolveException {

		//make the lp solver with enough variables
		solver = LpSolve.makeLp(0, sysListSize);

		System.out.println("TANGO::setting "+sysListSize+" variables");
		//one variable for each system. simply is the system needed or decommissioned
		for(int i=0; i<sysListSize; i++) {
			solver.setBinary(i + 1, true);
			solver.setColName(i+1, sysList[i]);
		}
	}

	/**
	 * Sets the objection function to minimize sustainment cost.
	 * Sums the sustainment cost for the local systems kept at sites + their central maintenance costs
	 * + the sustainment cost for the centrally hosted systems.
	 */
	@Override
	public void setObjFunction() throws LpSolveException{
		int[] colno = new int[sysListSize];
		double[] row = new double[sysListSize];
		int i =0;
		int interfaceCount = 0;

		/**
		 * each row calculated below is one entry in the sum of the objective function
		 */
		for(String system: sysList){
			colno[i] = i + 1;
			row[i] = (double) sysBudgets.get(system) + (double)systemCostPenalty.get(system);
			i++;
		}

		solver.setObjFnex(sysListSize, row, colno);
		solver.setMinim();
	}

	@Override
	public void setConstraints() throws LpSolveException{

		long startTime;
		long endTime;

		//adding constraints for data and blu at each site
		startTime = System.currentTimeMillis();
		addFunctionalityConstraints();
		endTime = System.currentTimeMillis();
		System.out.println("Time to run add functionality constraint " + (endTime - startTime) / 1000 );

	}

	private void addFunctionalityConstraints() {

		for(String granularBlu: granularBluMap.keySet()){
			List<String> supportingSysList = granularBluMap.get(granularBlu);

			int[] colno = new int[sysListSize];
			double[] row = new double[sysListSize];
			for(int j=0; j<sysListSize; j++) {

				colno[j] = j + 1;
				if(supportingSysList.contains(sysList[j])){
					row[j] = 1;
				}
				else {
					row[j] = 0;
				}
			}

			try {
				if(constraintCalculator(granularBlu, granularBluMap, false) >0){
					solver.addConstraintex(sysListSize, row, colno, LpSolve.GE, constraintCalculator(granularBlu, granularBluMap, true));
				}
			} catch (LpSolveException e) {
				e.printStackTrace();
			}
		}

		for(String dataObj: dataMap.keySet()){
			List<String> supportingSysList = dataMap.get(dataObj);

			int[] colno = new int[sysListSize];
			double[] row = new double[sysListSize];
			for(int j=0; j<sysListSize; j++) {

				colno[j] = j + 1;
				if(supportingSysList.contains(sysList[j])){
					row[j] = 1;
				}
				else {
					row[j] = 0;
				}
			}

			try {
				solver.addConstraintex(sysListSize, row, colno, LpSolve.GE, constraintCalculator(dataObj, dataMap, true));
			} catch (LpSolveException e) {
				e.printStackTrace();
			}
		}

		System.out.println("ENDURING TOTAL:::"+enduringSystems.size());
		for(int j=0; j<sysListSize; j++){
			int[] colno = new int[sysListSize];
			double[] row = new double[sysListSize];

			if(enduringSystems.contains(sysList[j])){
				System.out.println("ENDURING::::::"+sysList[j]);
				colno[j] = j+1;
				row[j] = 1;
				try {
					solver.addConstraintex(sysListSize, row, colno, LpSolve.EQ, 1);
				} catch (LpSolveException e) {
					e.printStackTrace();
				}
			}
		}

		if(limit){
			int[] colno = new int[sysListSize];
			double[] row = new double[sysListSize];

			for(int j=0; j<sysListSize; j++){
				colno[j] = j+1;
				if(enduringSystems.contains(sysList[j])){
					row[j] = 0;
				}else{				
					row[j] = 1;
				}
			}

			try {
				solver.addConstraintex(sysListSize, row, colno, LpSolve.GE, yearLimitCalculator());
			} catch (LpSolveException e) {
				e.printStackTrace();
			}
		}
	}

	private int yearLimitCalculator(){
		double sysPerYear = (((double)totalSystemCount-(double)enduringSystems.size())/(double) totalYears);
		int step = (int) Math.ceil(sysPerYear) * year;
		int totalSys = totalSystemCount - enduringSystems.size();
		int limit = totalSys - step; 
		return limit;
	}

	/**
	 * @param constraintVariable
	 * @param constraintVariables
	 * @param shouldLog
	 * @return
	 */
	private int constraintCalculator(String constraintVariable, Map<String, List<String>> constraintVariables, boolean shouldLog){

		List<String> systems = constraintVariables.get(constraintVariable);
		int variableMax = systems.size();

		int variableMin = variableMax;

		for(String system: systems){
			if(!enduringSystems.contains(system)){
				variableMin--;
			}
		}

		if(shouldLog){
			//			System.out.println("Constraint for "+constraintVariable+" has min value of "+variableMin);
			//			System.out.println("Constraint for "+constraintVariable+" has max value of "+variableMax);
		}

		Double step = Math.ceil(((double)variableMax-(double)variableMin)/(double)totalYears);
		int retirementTotalPerYear = year*step.intValue();

		if(shouldLog){
			//			System.out.println("Constraint for "+constraintVariable+" has step value of "+retirementTotalPerYear);
		}		

		int constraintValue = Math.max((variableMax -retirementTotalPerYear), variableMin); 

		if(shouldLog){
			//			System.out.println("Determined value is of "+constraintVariable+" is "+constraintValue);
			//			System.out.println();
		}

		return constraintValue;
	}

	/**
	 * Executes the optimization.
	 */
	@Override
	public void execute(){

		super.execute();

		int index = 0;
		int nConstraints = solver.getNorigRows();

		this.results = new double[sysListSize];
		this.keptSystems = new ArrayList<String>();

		if(solved == LpSolve.OPTIMAL) {
			try {
				double objectiveVal = solver.getObjective();
				System.out.println("objective val::::::::::::::::::: " + objectiveVal);
				for(int i = 0; i < sysListSize; i++ ) {

					this.results[i] = solver.getVarPrimalresult(nConstraints + index + 1);
					System.out.println("System "+sysList[i]+" has result "+this.results[i]);
					if(this.results[i] == 1.0){
						this.keptSystems.add(this.sysList[i]);
					}
					index++;
				}
			} catch(LpSolveException e) {
				LOGGER.error("Unable to get solution. Take no action.");
			}

		} else {
			LOGGER.error("Solution is not optimal. Take no action.");
		}

	}

	/**
	 * @param sysNames
	 * @param budgets
	 * @param bluMap
	 * @param interfaceMap
	 * @param dataMap
	 * @param granularBLU
	 * @param retirementTypes
	 * @param year
	 * @param totalYears
	 * @param replacementPercent
	 */
	public void setSystemData(String[] sysNames, Map<String, Double> budgets, Map<String, List<String>> bluMap, Map<String, List<String>> dataMap, Map<String, List<String>> granularBLU, Map<String, List<String>> retirementMap){

		for(String system: sysNames){
			boolean contained =  false;
			for(String key: retirementMap.keySet()){
				if(retirementMap.get(key).contains(system)){
					contained = true;
				}
			}
			if(!contained){
				enduringSystems.add(system);
			}
		}

		this.granularBluMap = granularBLU;
		this.bluMap = bluMap;
		this.dataMap = dataMap;
		this.sysListSize = sysNames.length;
		this.sysList = sysNames;
		this.sysBudgets = budgets;

	}

	/**
	 * @param year
	 * @param totalYears
	 * @param replacementPercent
	 */
	public void setOptimizationConstants(Integer year, Integer totalYears, Map<String, Double> systemCostPenalty, int totalSystemCount, boolean limit){		
		this.totalYears = totalYears;
		this.year = year;
		this.systemCostPenalty = systemCostPenalty;
		this.totalSystemCount = totalSystemCount;
		this.limit = limit;
	}

	/**
	 * @param table
	 * @param keyIdx
	 * @param valueIdx
	 * @return
	 */
	public Map<String, List<String>> mapSetup(List<Object[]> table, Integer keyIdx, Integer valueIdx){

		Map<String, List<String>> returnMap = new HashMap<String, List<String>>();
		for(Object[] row: table){
			String key = row[keyIdx].toString();
			String value = row[valueIdx].toString();
			if(returnMap.keySet().contains(key)){
				returnMap.get(key).add(value);					
			}else{
				List<String> newList = new ArrayList<String>();
				newList.add(value);
				returnMap.put(key, newList);
			}
		}
		return returnMap;
	}

	public double[] getResults(){
		return this.results;
	}

	public List<String> getKeptSystems(){
		return this.keptSystems;
	}

	//	public static void main(String args[]){
	//
	//		PropertyConfigurator.configure("log4j.prop");
	//
	//		List<Object[]> myList = new ArrayList<Object[]>();
	//
	//		Object[] r1 = new Object[]{"S1", "B1"};
	//		myList.add(r1);
	//
	//		Object[] r2 = new Object[]{"S1", "B2"};
	//		myList.add(r2);
	//
	//		Object[] r3 = new Object[]{"S2", "B1"};
	//		myList.add(r3);
	//
	//		Object[] r4 = new Object[]{"S3", "B2"};
	//		myList.add(r4);
	//
	//		Object[] r5 = new Object[]{"S3", "B1"};
	//		myList.add(r5);
	//
	//		Map<String, Integer> map = new HashMap<String, Integer>();
	//		map.put("S1", 0);
	//		map.put("S2", 1);
	//		map.put("S3", 2);
	//
	//		String[] systems = new String[]{"S1", "S2", "S3"};
	//		Double[] buds = new Double[]{10., 5., 20.};
	//
	//		BLUSystemOptimizer ps = new BLUSystemOptimizer();
	//		ps.setSystemData(systems, buds, 20., myList, map);
	//
	//		try {
	//			ps.setupModel();
	//		} catch (LpSolveException e) {
	//			e.printStackTrace();
	//		}
	//
	//		ps.execute();
	//
	//		ps.deleteModel();
	//
	//	}
}

package prerna.ui.components.specific.ousd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import lpsolve.LpSolve;
import lpsolve.LpSolveException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import prerna.algorithm.impl.LPOptimizer;

public class BLUSystemOptimizer extends LPOptimizer{

	protected static final Logger LOGGER = LogManager.getLogger(BLUSystemOptimizer.class.getName());
	
	String[] sysList;
	Double[] sysBudgets;
//	Map<String, Double> sysCost; // this maintenance cost of each system
	Map<String, Integer> sysWave; // this wave of each system
	Map<String, List<String>> bluMap; // this is blu -> list of systems that support this blu
	double maxSysCost;
	int maxWave;
	int sysListSize;
	
	/**
	 * 
	 */
	public BLUSystemOptimizer(){
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
		
		//one variable for each system. simply is the system needed or decommissioned
		for(int i=0; i<sysListSize; i++) {
//			for(j=0; j<bluListSize; j++) {
				solver.setBinary(i + 1, true);
				solver.setColName(i+1, sysList[i]);
//				solver.setVarBranch(i + 1,LpSolve.BRANCH_FLOOR); // TODO: what does this do?
//			}
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

        for(int i=0; i<sysListSize; i++) {

//        	for(j=0; j<siteLength; j++) {
				colno[i] = i + 1;
 				row[i] = maxSysCost * ( maxWave - sysWave.get(sysList[i]) ) + sysBudgets[i];
// 			}
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
		
 		//new constraint for each blu
		Iterator<String> bluIt = bluMap.keySet().iterator();
		
		while(bluIt.hasNext()) {
			String blu = bluIt.next();
			List<String> supportingSysList = bluMap.get(blu);
			System.out.println("Adding constraint for blu " + blu + " with supporting systems " + supportingSysList.toString());

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
				solver.addConstraintex(sysListSize, row, colno, LpSolve.GE, 1);
			} catch (LpSolveException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	

	/**
	 * Executes the optimization.
	 */
	@Override
	public void execute(){

		super.execute();
		
		int index = 0;
		int nConstraints = solver.getNorigRows();
		
		double[] result = new double[sysListSize];
		
		if(solved == LpSolve.OPTIMAL) {
			try {
				double objectiveVal = solver.getObjective();
				System.out.println("objective val::::::::::::::::::: " + objectiveVal);
				
				for(int i = 0; i < sysListSize; i++ ) {
					result[i] = solver.getVarPrimalresult(nConstraints + index + 1);
					System.out.println("result " + i + " is " + result[i]);
					index++;
				}
			} catch(LpSolveException e) {
				LOGGER.error("Unable to get solution. Take no action.");
			}

		} else {
			LOGGER.error("Solution is not optimal. Take no action.");
		}

	}
	
	public void setSystemData(String[] sysNames, Double[] budgets, Double maxBudget, List<Object[]> bluTable, Map<String, Integer> waves){

		sysWave = waves; // this wave of each system
		maxWave = 0;
		for(Integer wave: waves.values()){
			if(wave>maxWave){
				maxWave = wave;
			}
		}
//		sysCost = new HashMap<String, Double> (); // this maintenance cost of each system
		bluMap = new HashMap<String, List<String>> (); // this is blu -> list of systems that support this blu
//		this.maxSysCost = -1; 
		
//		List<String> array = new ArrayList<String>();
		for(Object[] row: bluTable){
//			if(!array.contains(row[0])){
//				array.add((String) row[0]);
//				if(row[2]!=null){
//					sysCost.put((String)row[0], (Double)row[2]);
//					if((Double)row[2] > maxSysCost){
//						maxSysCost = (Double)row[2];
//					}
//				}
//			}
			if(row[1] != null){
				List<String> fillingSys = bluMap.get((String) row[1]);
				if(fillingSys == null){
					fillingSys = new ArrayList<String>();
					bluMap.put((String) row[1], fillingSys);
				}
				fillingSys.add((String) row[0]);
			}
		}
		this.sysListSize = sysNames.length;
		this.sysList = sysNames;
		this.maxSysCost = maxBudget;
		this.sysBudgets = budgets;
//		this.sysList = array.toArray(this.sysList);
	}

	public static void main(String args[]){

		PropertyConfigurator.configure("log4j.prop");
		
		List<Object[]> myList = new ArrayList<Object[]>();
		
		String[] names = new String[]{"System", "BLU"};
		
		Object[] r1 = new Object[]{"S1", "B1"};
		myList.add(r1);
		
		Object[] r2 = new Object[]{"S1", "B2"};
		myList.add(r2);
		
		Object[] r3 = new Object[]{"S2", "B1"};
		myList.add(r3);
		
		Object[] r4 = new Object[]{"S3", "B2"};
		myList.add(r4);
		
		Object[] r5 = new Object[]{"S3", "B1"};
		myList.add(r5);
		
		Map<String, Integer> map = new HashMap<String, Integer>();
		map.put("S1", 0);
		map.put("S2", 1);
		map.put("S3", 2);
		
		String[] systems = new String[]{"S1", "S2", "S3"};
		Double[] buds = new Double[]{10., 5., 20.};
		
		BLUSystemOptimizer ps = new BLUSystemOptimizer();
		ps.setSystemData(systems, buds, 20., myList, map);
		
		try {
			ps.setupModel();
		} catch (LpSolveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		ps.execute();
		
		ps.deleteModel();
		
		
		
		
		
		
		
		
		
	}
}

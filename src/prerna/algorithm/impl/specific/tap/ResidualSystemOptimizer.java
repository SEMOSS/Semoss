package prerna.algorithm.impl.specific.tap;

import java.util.ArrayList;

import lpsolve.LpSolve;
import lpsolve.LpSolveException;

import org.apache.log4j.Logger;

import prerna.algorithm.impl.LPOptimizer;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.specific.tap.SysOptPlaySheet;

public class ResidualSystemOptimizer extends LPOptimizer{
	
	SysOptPlaySheet playSheet=null;
	
	protected Logger logger = Logger.getLogger(getClass());
	public ArrayList<String> sysList;
	ArrayList<String> dataList;
	ArrayList<String> bluList;
	ArrayList<String> regionList;
	
	//a_ip, b_iq, c_ip
	int[][] systemDataMatrix;
	int[][] systemBLUMatrix;
	double[][] systemCostOfDataMatrix;
	int[][] systemRegionMatrix;
	
	//cM_i, cDM_i, s_i
	double[] systemCostOfMaintenance;
	double[] systemCostOfDB;
	double[] systemNumOfSites;
	double[] systemRequired;
	
	//Ap, Bq
	int[][] dataRegionSORSystemExists;
	int[][] bluRegionProviderExists;
	
	//x_i
	public double[] systemIsModernized;
	int ret=0;
	double roi=0.0;
	public double denomCurrentMaintenance = 0.0;
	public double numTransformationTotal = 0.0;
	public double numMaintenanceTotal = 0.0;
	double percentOfPilot = 0.20;
	
	public String errorMessage="";
//	boolean includeRegionalization = false;
	
	public ResidualSystemOptimizer(){
		
	}
	
	public void setPlaySheet(IPlaySheet playSheet)
	{
		this.playSheet = (SysOptPlaySheet)playSheet;
	}

	/**
	 * Gathers data set.
	 */
	
	public void setDataSet(ArrayList<String> sysList, ArrayList<String> dataList, ArrayList<String> bluList,ArrayList<String> regionList,int[][] systemDataMatrix, int[][] systemBLUMatrix, double[][] systemCostOfDataMatrix, int[][] systemRegionMatrix, double[] systemCostOfMaintenance, double[] systemCostOfDB, double[] systemNumOfSites, double[] systemRequired,int[][] dataRegionSORSystemExists, int[][] bluRegionProviderExists) {
		
		this.sysList = sysList;
		this.dataList = dataList;
		this.bluList = bluList;
		this.regionList = regionList;
//		if(regionList!=null)
//			includeRegionalization = true;
		
		this.systemDataMatrix=systemDataMatrix;
		this.systemBLUMatrix=systemBLUMatrix;
		this.systemCostOfDataMatrix=systemCostOfDataMatrix;
		this.systemRegionMatrix = systemRegionMatrix;
		
		//cM_i, cDM_i, s_i
		this.systemCostOfMaintenance=systemCostOfMaintenance;
		this.systemCostOfDB=systemCostOfDB;
		this.systemNumOfSites=systemNumOfSites;
		this.systemRequired = systemRequired;
		
		//Ap, Bq
		this.dataRegionSORSystemExists=dataRegionSORSystemExists;
		this.bluRegionProviderExists=bluRegionProviderExists;
		
	}

	/**
	 * Sets variables in model.
	 */
	@Override
	public void setVariables() throws LpSolveException {
		//solver=null;
		solver = LpSolve.makeLp(0, sysList.size());
		
		if(solver.getLp()==0)
		{
			logger.info("Couldn't construct a new model");
			ret=1;
		}
			
		if(ret == 0) {
	        /* let us name our variables. Not required, but can be useful for debugging */
			for(int sysInd=0;sysInd<sysList.size();sysInd++)
			{
				solver.setColName(sysInd+1, sysList.get(sysInd));
				solver.setBinary(sysInd+1,true);
			}
		}

	}
	
	/**
	 * Sets constraints in the model.
	 */
	@Override
	public void setConstraints() {
		//makes building the model faster if it is done rows by row
		solver.setAddRowmode(true);	
		//adding constraints for data objects
		addRequiredSystemsConstraint(systemRequired);
		addConstraints(systemDataMatrix,systemRegionMatrix,dataRegionSORSystemExists);
		addConstraints(systemBLUMatrix,systemRegionMatrix,bluRegionProviderExists);
		//if(includeRegionalization)
			//addRegionalizationConstraints(systemRegionMatrix);
		//rowmode turned off
		solver.setAddRowmode(false);
	}
	
	public void addRequiredSystemsConstraint(double[] systemRequired)
	{
		try{
			for(int i=0;i<systemRequired.length;i++)
			{
				//add a constraint for each system if it is required (val=1)
				if(systemRequired[i]>=1.0)
				{
					int[] colno = new int[systemRequired.length];
			        double[] row = new double[systemRequired.length];
			        
			        for(int j=0;j<systemRequired.length;j++)
			        {
			        	colno[j] = j+1;
			        	row[j] = 0;
			        	if(i==j)
			        		row[j] = 1;
			        }
			        solver.addConstraintex(systemRequired.length, row,colno,LpSolve.GE,1);
				}
			}
		}catch (LpSolveException e){
			e.printStackTrace();
		}
	}
	
	public void addConstraints(int[][] systemProviderMatrix, int[][] systemRegionMatrix, int[][] constraintMatrix)
	{
		try{
			for(int dataInd=0;dataInd<systemProviderMatrix[0].length;dataInd++)
			{
				for(int regionInd=0;regionInd<systemRegionMatrix[0].length;regionInd++)
				{
					int[] colno = new int[systemProviderMatrix.length];
			        double[] row = new double[systemProviderMatrix.length];
		
			        for(int sysInd=0;sysInd<systemProviderMatrix.length;sysInd++)
			        {
			        	colno[sysInd] = sysInd+1;
			        	row[sysInd] = systemProviderMatrix[sysInd][dataInd]*systemRegionMatrix[sysInd][regionInd];
			        }
			        if(constraintMatrix[dataInd][regionInd]>0)
			        	solver.addConstraintex(systemProviderMatrix.length, row, colno, LpSolve.GE, 1);
			        else
			        	solver.addConstraintex(systemProviderMatrix.length, row, colno, LpSolve.GE, 0);
				}
			}
		}catch (LpSolveException e){
			e.printStackTrace();
		}
	}
	
//	public void addConstraints(int[][] systemRegionMatrix)
//	{
//		try{
//			for(int colInd=0;colInd<systemRegionMatrix[0].length;colInd++)
//			{
//				int[] colno = new int[systemRegionMatrix.length];
//		        double[] row = new double[systemRegionMatrix.length];
//	
//		        for(int sysInd=0;sysInd<systemRegionMatrix.length;sysInd++)
//		        {
//		        	colno[sysInd] = sysInd+1;
//		        	row[sysInd] = systemRegionMatrix[sysInd][colInd] - 1;
//		        }
//	        	solver.addConstraintex(systemRegionMatrix.length, row, colno, LpSolve.EQ, 0);
//			}
//		}catch (LpSolveException e){
//			e.printStackTrace();
//		}
//
//	}

	/**
	 * Sets the function for calculations.
	 */
	@Override
	public void setObjFunction() {
		try{
			int[] colno = new int[systemCostOfMaintenance.length];
	        double[] row = new double[systemCostOfMaintenance.length];

			for(int sysInd=0;sysInd<systemCostOfMaintenance.length;sysInd++)
			{
	        	colno[sysInd] = sysInd+1;
	        	row[sysInd] = systemCostOfMaintenance[sysInd];
	        }
	        solver.setObjFnex(systemCostOfMaintenance.length, row, colno);
			solver.setMinim();
		}catch (LpSolveException e){
			e.printStackTrace();
		}
	}
	
	public void calculateROI()
	{
		double numerator = 0.0;
		for(int sysInd=0;sysInd<sysList.size();sysInd++)
		{
			//adding in maintenance cost if system is not modernized
			double numMaintenance = systemCostOfMaintenance[sysInd] - (systemIsModernized[sysInd]*systemCostOfMaintenance[sysInd]);
			numMaintenanceTotal +=numMaintenance;
			double numTransformation = 0.0;
			for(int dataInd=0;dataInd<dataList.size();dataInd++)
			{
				numTransformation+= (systemDataMatrix[sysInd][dataInd]*systemCostOfDataMatrix[sysInd][dataInd]);
			}
			double numSites = systemNumOfSites[sysInd];
			//first site is pilot and is full cost. each additional site is some percentage of pilot.
			if(numSites>1)
				numTransformation = numTransformation + numTransformation * (numSites-1)*percentOfPilot;
			else
				numTransformation = numTransformation*numSites;
			numTransformationTotal += numTransformation;
			numerator += (numMaintenance - numTransformation);
			denomCurrentMaintenance+=systemCostOfMaintenance[sysInd];
		}
		roi = numerator / denomCurrentMaintenance;
	}
	
	
	public boolean runOpt()
	{
		try {
			setupModel();
			solver.writeLp("model.lp");
			execute();	
			double minObjectiveVal = solver.getObjective();
			System.out.println("Objective Val is : " + minObjectiveVal);
			
			if(minObjectiveVal>1.0*Math.pow(10, 15))
			{
				errorMessage = "No solution can be found for given data and BLU. Please modify the data and BLU selected.";
				return false;
			}
			
			systemIsModernized = new double[sysList.size()];

			solver.getVariables(systemIsModernized);
		
			calculateROI();

			boolean allSystemsKept = true;
			if(playSheet!=null)
			{
				playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\n*Systems to be Modernized...");
				for(int i=0;i<systemIsModernized.length;i++)
					if(systemIsModernized[i]>0)
						playSheet.consoleArea.setText(playSheet.consoleArea.getText()+solver.getColName(i + 1)+", ");
				playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\n*Systems that will not be modernized...");
				for(int i=0;i<systemIsModernized.length;i++)
				{
					if(systemIsModernized[i]<1)
					{
						allSystemsKept = false;
						playSheet.consoleArea.setText(playSheet.consoleArea.getText()+solver.getColName(i + 1)+", ");
					}
				}
				if(allSystemsKept)
				{
					errorMessage = "All systems must be kept to maintain same functionality.";
					return false;
				}
				playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nYearly maintenance if continued as-is: "+denomCurrentMaintenance);
				playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nYearly maintenance of modernized systems only: "+(denomCurrentMaintenance-numMaintenanceTotal));
				playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nOne Time Cost to transform data for all systems: "+numTransformationTotal);
			}
			else
			{
				System.out.println("Systems to be Modernized");
				for(int i=0;i<systemIsModernized.length;i++)
					if(systemIsModernized[i]>0)
						System.out.print(solver.getColName(i + 1)+", ");
				System.out.println("Systems that will not be Modernized");
				for(int i=0;i<systemIsModernized.length;i++)
					if(systemIsModernized[i]<1)
						System.out.print(solver.getColName(i + 1)+", ");
				System.out.println("Yearly maintenance if continued as-is: "+denomCurrentMaintenance);
				System.out.println("Yearly maintenance of modernized systems only: "+(denomCurrentMaintenance-numMaintenanceTotal));
				System.out.println("One Time Cost to transform data for all systems: "+numTransformationTotal);
				System.out.println("Break Even point in years: "+numTransformationTotal/numMaintenanceTotal);
				for(int i=1;i<=10;i++)
					System.out.println("Year "+i+" ROI is "+(numMaintenanceTotal-(numTransformationTotal/i))/denomCurrentMaintenance);
			}
			deleteModel();
			solver=null;
			
		} catch (LpSolveException e) {
			e.printStackTrace();
			return false;
		}
			
		return true;
	}
	
	public double[] getModernizedSystems()
	{
		return systemIsModernized;
	}

}

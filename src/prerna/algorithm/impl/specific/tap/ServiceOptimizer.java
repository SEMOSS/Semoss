/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.algorithm.impl.specific.tap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;

import lpsolve.LpSolve;
import lpsolve.LpSolveException;
import prerna.algorithm.impl.LPOptimizer;
import prerna.ui.components.specific.tap.OptimizationOrganizer;

/**
 * This class is used to optimize calculations for TAP-specific services.
 * Some of the functionalities include imposing constraints and setting budgets.
 */
public class ServiceOptimizer extends LPOptimizer{

	//total number of services
	int totalSerNo;

	//data objects vs the services, 
	public Object[][] icdSerMatrix;
	public Hashtable<String, Double> serCostHash;
	public ArrayList<String> icdLabels;
	public ArrayList<String> serLabels;

	public Object[][] ORIGicdSerMatrix;
	public Hashtable<String, Double> ORIGserCostHash = new Hashtable<String, Double>();
	public ArrayList<String> ORIGicdLabels = new ArrayList<String>();
	public ArrayList<String> ORIGserLabels = new ArrayList<String>();

	ArrayList<String> varColName = new ArrayList<String>();
	double learningConstant;
	public int serStartIdx;

	//cost of maintaining a single ICD
	double icdMtnCost;

	//percentage of build cost to maintain services
	double serMainPct;

	//budget allowance
	double hourlyCost = 150;

	double budget;
	public ArrayList<Double> actualBudgetList = new ArrayList<Double>();//keeps track of how much of the budget was actually used each year

	public ArrayList<Double> objectiveValueList = new ArrayList<Double>();//keeps track of what the obj. func. value was for each year

	ArrayList<ArrayList<String>> yearlyServicesList = new ArrayList<ArrayList<String>>();
	ArrayList<ArrayList<Integer>> yearlyICDList = new ArrayList<ArrayList<Integer>>();

	boolean completion = false;
	/**
	 * Constructor for ServiceOptimizer.
	 * @param 	icdMt	Cost of maintaining a single ICD.
	 * @param 	serMain	Percentage of build cost to maintain services.
	 */
	public ServiceOptimizer(double icdMt, double serMain){
		this.icdMtnCost = icdMt;
		this.serMainPct = serMain;
	}


	/**
	 * Sets the data to be analyzed in calculations.
	 * @param 	organizer	OptimizationOrganizer is used to efficiently run TAP-specific optimizations.
	 */
	public void setData(OptimizationOrganizer organizer) {

		icdSerMatrix = organizer.getICDServiceMatrix();
		if (icdSerMatrix == null || icdSerMatrix.length==0) {
			return;
		}
		icdLabels = organizer.getICDServiceRowNames();
		serLabels = organizer.getICDServiceColNames();
		serCostHash = organizer.getServiceHash();
		ORIGicdSerMatrix = new Object[icdSerMatrix.length][icdSerMatrix[0].length];

		for(int i = 0; i<icdSerMatrix.length; i++) ORIGicdSerMatrix[i] = Arrays.copyOf(icdSerMatrix[i], icdSerMatrix[i].length);

		ORIGicdLabels.addAll(icdLabels);

		ORIGserLabels.addAll(serLabels);

		ORIGserCostHash.putAll(serCostHash);

	}



	/**
	 * Sets variables for the linear programming solver.
	 */
	public void setVariables() throws LpSolveException
	{
		varColName = new ArrayList<String>();
		int totalVar = icdSerMatrix.length + serCostHash.size();
		serStartIdx = icdLabels.size();
		solver=null;
		solver = LpSolve.makeLp(0, totalVar);
		solver.setMipGap(true, .001);
		solver.setBbFloorfirst(1);//its all about the floor first
		solver.setBbDepthlimit(0);
		varColName.addAll(icdLabels);
		Iterator<String> hashIt = serCostHash.keySet().iterator();
		while (hashIt.hasNext())
		{
			varColName.add((String) hashIt.next());
		}

		for (int i = 0; i < varColName.size(); i++)
		{
			solver.setColName(i+1, varColName.get(i));
			solver.setBinary(i+1, true);
		}

	}

	/**
	 * Sets budgets and ICD constraints for optimizations.
	 */
	public void setConstraints()
	{
		createBgtConstraint();
		createICDConstraints();
	}

	/**
	 * Creates budget constraints for optimizations.
	 */
	protected void createBgtConstraint()
	{
		try {
			solver.setAddRowmode(true);
			int[] colno = new int[serCostHash.size()];
			double[] row = new double[serCostHash.size()];
			int colCount = 0;
			for (int i = serStartIdx; i < varColName.size(); i++)
			{
				//i represents the column for each variable set in the beginning
				colno[colCount]=i+1;
				row[colCount]=serCostHash.get(varColName.get(i))/learningConstant;
				colCount++;
			}
			solver.addConstraintex(colCount, row, colno, LpSolve.LE, budget);
		}
		catch (LpSolveException e) {
			e.printStackTrace();
		}
		
	}

	/**
	 * Creates ICD constraints used in optimizations.
	 */
	private void createICDConstraints()
	{
		try {
			solver.setAddRowmode(true);


			for (int matrixICDIdx = 0; matrixICDIdx < icdLabels.size(); matrixICDIdx++)
			{
				//when first getting constraint variables, we do not know how many services are in play so we will first use arraylist
				ArrayList<Integer> colnoAL = new ArrayList<Integer>();
				ArrayList<Double> rowAL = new ArrayList<Double>();
				int colCount = 0;
				for (int matrixSerIdx = 0; matrixSerIdx <icdSerMatrix[0].length;matrixSerIdx++)
				{
					if (icdSerMatrix[matrixICDIdx][matrixSerIdx]!=null &&(Double)icdSerMatrix[matrixICDIdx][matrixSerIdx]>0)
					{
						int serIdx = varColName.indexOf(serLabels.get(matrixSerIdx));
						colnoAL.add(serIdx+1);
						rowAL.add(1.0);
						colCount++;
					}
				}
				//i represents the column for each variable set in the beginning
				colnoAL.add(matrixICDIdx+1);
				rowAL.add(-1.0*colCount);
				colCount++;

				//now we will convert the arraylists into arrays for input into lPsolve constraint function
				int[] colno = new int[colnoAL.size()];
				for (int i=0; i<colnoAL.size();i++)
				{
					colno[i]=colnoAL.get(i);
				}
				double[] row = new double[rowAL.size()];
				for (int i=0; i<rowAL.size();i++)
				{
					row[i]=rowAL.get(i);
				}
				solver.addConstraintex(colCount, row, colno, LpSolve.GE, 0);
			}

		}
		catch (LpSolveException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Writes the objective function used in optimizations.
	 */
	public void setObjFunction() {

		try {
			int[] colno = new int[varColName.size()];
			double[] row = new double[varColName.size()];
			int colCount = 0;
			for (int icdIdx = 0; icdIdx < icdLabels.size(); icdIdx++)
			{
				colno[colCount]=icdIdx+1;
				double icdCount = 0;
				for (int serIdx = 0; serIdx<icdSerMatrix[0].length;serIdx++)
				{
					if (icdSerMatrix[icdIdx][serIdx]!=null && (Double)icdSerMatrix[icdIdx][serIdx]>0)
					{
						icdCount = (Double) icdSerMatrix[icdIdx][serIdx];
						break;
					}
				}
				row[colCount]=1*icdCount*icdMtnCost;
				colCount++;
			}

			for (int serIdx = serStartIdx; serIdx < varColName.size(); serIdx++)
			{
				//get service names
				String serName = varColName.get(serIdx);
				//get service costs
				double serCost = serCostHash.get(serName);
				colno[colCount]=serIdx+1;
				row[colCount]=-serMainPct/learningConstant*serCost;
				colCount++;
			}

			solver.setObjFnex(colCount++, row, colno);
			solver.setMaxim();
		}
		catch (LpSolveException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Runs the linear optimization for services.
	 * Keeps track of how much of the budget was actually used each year and what the objective function value was for each year.
	 * @param 	totalYears			Total number of years service is used.
	 * @param 	learningConstants	Learning constants used in optimization.
	 * @param 	hourlyRate			Hourly cost of service.
	
	 * @return 	Hashtable with budget and objective as keys. */
	public Hashtable<String,ArrayList<Double>> runOpt(int totalYears, double[] learningConstants, double hourlyRate)
	{
		Hashtable<String,ArrayList<Double>> returnHash = new Hashtable<String,ArrayList<Double>>();
		this.hourlyCost = hourlyRate;
		actualBudgetList = new ArrayList<Double>();//keeps track of how much of the budget was actually used each year
		objectiveValueList = new ArrayList<Double>();//keeps track of what the obj. func. value was for each year

		ArrayList<String> impSerList = new ArrayList<String>();
		ArrayList<Integer> impICDList = new ArrayList<Integer>();
		yearlyServicesList = new ArrayList<ArrayList<String>>();
		yearlyICDList= new ArrayList<ArrayList<Integer>>();
		int yearCount = 0;
		while (totalYears>yearCount+1)
		{
			this.learningConstant = learningConstants[yearCount];
			yearCount++;
			try {
				setupModel();
				solver.writeLp("test"+yearCount+".lp");
				execute();	
				if(solver.getObjective()==0.0)
				{
					break; 
				}
				objectiveValueList.add((double) Math.round(solver.getObjective()*hourlyCost));

				//logger.info("Value of objective function: " + solver.getObjective());
				double[] constValues = new double[solver.getNrows()];
				solver.getConstraints(constValues);

				//logger.info("Budget " + constValues[0]);
				actualBudgetList.add((double) Math.round(constValues[0]*hourlyCost));

				ArrayList<String> oldSerLabel = new ArrayList<String>();
				oldSerLabel.addAll(serLabels);
				impSerList = new ArrayList<String>();
				impICDList = new ArrayList<Integer>();
				double[] var = solver.getPtrVariables();
//				String varName;
				for (int i = 0; i < var.length; i++) 
				{
//					varName = solver.getColName(i+1);
					if (var[i]==1 && i<serStartIdx)
					{
						impICDList.add(i);
						icdLabels.remove(solver.getColName(i+1));
						//logger.debug(varName+", Year" +yearCount);
					}
					else if (var[i]==1)
					{
						impSerList.add(solver.getColName(i+1));
						serCostHash.remove(solver.getColName(i+1));
						serLabels.remove(solver.getColName(i+1));
						//logger.debug(varName+", Year" +yearCount);
					}
					//logger.debug("Value of " +varName + " = "+ var[i]);
				}
				Object[][] newMatrix = new Object[icdSerMatrix.length-impICDList.size()][icdSerMatrix[0].length-impSerList.size()];
				int newXIdx=0;
				for (int i = 0;i<icdSerMatrix.length;i++)
				{
					if(!impICDList.contains(i))
					{
						int newYIdx=0;
						for (int j=0;j<icdSerMatrix[0].length;j++)
						{
							String serName = oldSerLabel.get(j);
							if(!impSerList.contains(serName))
							{
								newMatrix[newXIdx][newYIdx]=icdSerMatrix[i][j];
								newYIdx++;
							}
						}
						newXIdx++;
					}
				}
				icdSerMatrix = newMatrix;
			} catch (LpSolveException e) {
				e.printStackTrace();
			}
			yearlyServicesList.add(impSerList);
			yearlyICDList.add(impICDList);

			deleteModel();
			solver = null;
		}
		returnHash.put("budget",actualBudgetList);
		returnHash.put("objective", objectiveValueList);
		return returnHash;
	}

	/**
	 * Sets the budget used in calculations.
	 * @param 	budget	Budget, displayed as a double.
	 */
	public void setBudget(double budget){
		this.budget = budget;
	}

	/**
	 * Resets the service optimizer with the original labels for ICDs, services, and service costs.
	 */
	public void resetServiceOptimizer() {
		icdSerMatrix = new Object[ORIGicdSerMatrix.length][ORIGicdSerMatrix[0].length];

		for(int i = 0; i<ORIGicdSerMatrix.length; i++) {
			icdSerMatrix[i] = Arrays.copyOf(ORIGicdSerMatrix[i], ORIGicdSerMatrix[i].length);
		}

		icdLabels = new ArrayList<String>();
		icdLabels.addAll(ORIGicdLabels);
		serLabels = new ArrayList<String>();
		serLabels.addAll(ORIGserLabels);
		serCostHash = new Hashtable<String,Double>();
		serCostHash.putAll(ORIGserCostHash);
	}
}


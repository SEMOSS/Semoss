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

import lpsolve.LpSolve;
import lpsolve.LpSolveException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.impl.LPOptimizer;

/**
 * Optimizes the systems deployed at each site so that there is not duplicate functionality
 * if possible
 * @author ksmart
 *
 */
public class SysSiteLPSolver extends LPOptimizer{
		
	protected static final Logger LOGGER = LogManager.getLogger(SysSiteLPSolver.class.getName());
	
	//for systems that are not centrally deployed
	private int numNotCentralSystems;
	private int[][] systemDataMatrix, systemBLUMatrix;
	private int[] systemTheater, systemGarrison;

	private Integer[] modArr, decomArr;
	
	private int siteLength;
	private double[][] systemSiteMatrix;	

	private double[] maintenaceCosts, siteMaintenaceCosts, siteDeploymentCosts;
	private double trainingPerc, currentSustainmentCost;
	
	//for systems that are centrally deployed
	private int numCentralSystems;
	private int[][] centralSystemDataMatrix, centralSystemBLUMatrix;
	private int[] centralSystemTheater, centralSystemGarrison;

	private Integer[] centralModArr, centralDecomArr;
	
	private double[] centralSystemMaintenaceCosts;
	
	//created by algorithm
	private double[] interfaceCostArr;
	private int[] dataStillProvidedTheater, dataStillProvidedGarrison;
	private int[] bluStillProvidedTheater, bluStillProvidedGarrison;
	
	private int[][] theaterDataAtSiteMatrix, theaterBLUAtSiteMatrix;
	private int[][] garrisonDataAtSiteMatrix, garrisonBLUAtSiteMatrix;
	
	//input
	private double maxBudget;
	
	private double objectiveVal;
	private double[][] systemSiteResultMatrix;
	private double[] sysKeptArr, centralSysKeptArr;
	private double totalDeploymentCost;
	
	private int numSysKept, numCentralSysKept;

	
	/**
	 * Sets/updates the variables required to run the LP module
	 */
	public void setVariables(int[][] systemDataMatrix, int[][] systemBLUMatrix, double[][] systemSiteMatrix, int[] systemTheater, int[] systemGarrison, Integer[] sysModArr, Integer[] sysDecomArr, double[] maintenaceCosts, double[] siteMaintenaceCosts, double[] siteDeploymentCosts, double[]  interfaceCostArr, int[][] centralSystemDataMatrix,int[][] centralSystemBLUMatrix, int[] centralSystemTheater, int[] centralSystemGarrison, Integer[] centralModArr, Integer[] centralDecomArr, double[] centralSystemMaintenaceCosts, double trainingPerc, double currentSustainmentCost) {
	
		this.systemDataMatrix = systemDataMatrix;
		this.systemBLUMatrix = systemBLUMatrix;
		this.systemSiteMatrix = systemSiteMatrix;
 
		this.systemTheater = systemTheater;
		this.systemGarrison = systemGarrison;

		this.modArr = sysModArr;
		this.decomArr = sysDecomArr;

		this.maintenaceCosts = maintenaceCosts;
		this.siteMaintenaceCosts = siteMaintenaceCosts;
		this.siteDeploymentCosts = siteDeploymentCosts;
		this.interfaceCostArr = interfaceCostArr;

		this.centralSystemDataMatrix = centralSystemDataMatrix;
		this.centralSystemBLUMatrix = centralSystemBLUMatrix;
		
		this.centralSystemTheater = centralSystemTheater;
		this.centralSystemGarrison = centralSystemGarrison;
		
		this.centralModArr = centralModArr;
		this.centralDecomArr = centralDecomArr;
		
		this.centralSystemMaintenaceCosts = centralSystemMaintenaceCosts;

		this.trainingPerc = trainingPerc;
		this.currentSustainmentCost = currentSustainmentCost;
		
		this.numCentralSystems = centralSystemMaintenaceCosts.length;

		this.numNotCentralSystems = systemSiteMatrix.length;
		this.siteLength = systemSiteMatrix[0].length;
		
		calculateFunctionality();
	}
	
	public void setMaxBudget(double maxBudget) {
		this.maxBudget = maxBudget;
	}
	
//	/**
//	 * Sets system site matrix so that the optimizer can be rerun
//	 * @param systemSiteMatrix
//	 */
//	public void updateBudget(double maxBudget) {
//			this.maxBudget = maxBudget;
//			try{
//				
//				if(budgetRow == 0) {
//					LOGGER.info("Readding budget constraint since removed");
//					addBudgetConstraint();
//				}else {
//					solver.setRh(budgetRow, maxBudget);
//				}
//			}catch (LpSolveException e) {
//				e.printStackTrace(); //TODO
//			}
//	}
	
	private void calculateFunctionality() {
		this.dataStillProvidedTheater = calculateFunctionalityStillProvided(systemDataMatrix, centralSystemDataMatrix, systemTheater, centralSystemTheater);
		this.dataStillProvidedGarrison = calculateFunctionalityStillProvided(systemDataMatrix, centralSystemDataMatrix, systemGarrison, centralSystemGarrison);
		
		this.bluStillProvidedTheater = calculateFunctionalityStillProvided(systemBLUMatrix, centralSystemBLUMatrix, systemTheater, centralSystemTheater);
		this.bluStillProvidedGarrison = calculateFunctionalityStillProvided(systemBLUMatrix, centralSystemBLUMatrix, systemGarrison, centralSystemGarrison);
		
		this.theaterDataAtSiteMatrix =calculateFunctionalityAtSite(systemDataMatrix, centralSystemDataMatrix, systemTheater,centralSystemTheater, dataStillProvidedTheater);
		this.garrisonDataAtSiteMatrix =calculateFunctionalityAtSite(systemDataMatrix, centralSystemDataMatrix, systemGarrison,centralSystemGarrison, dataStillProvidedGarrison);
		
		this.theaterBLUAtSiteMatrix = calculateFunctionalityAtSite(systemBLUMatrix, centralSystemBLUMatrix, systemTheater,centralSystemTheater, bluStillProvidedTheater);
		this.garrisonBLUAtSiteMatrix = calculateFunctionalityAtSite(systemBLUMatrix, centralSystemBLUMatrix, systemGarrison,centralSystemGarrison, bluStillProvidedGarrison);
	}
	
	private int[] calculateFunctionalityStillProvided(int[][] systemFuncMatrix, int[][] centralSystemFuncMatrix, int[] systemEnvironment, int[] centralSystemEnvironment) {
		
		int i;
		int j;
		int funcSize = systemFuncMatrix[0].length;
		int numSystems = systemFuncMatrix.length;
		int numCentralSystems = centralSystemFuncMatrix.length;
		
		int[] funcStillProvidedEnvironment = new int[funcSize];
		
		OUTER: for(i=0; i<funcSize; i++) {
			for(j=0; j<numSystems; j++)  {
				if(systemFuncMatrix[j][i] == 1 && systemEnvironment[j] == 1 && (decomArr[j] == null || decomArr[j] == 0)) {
					funcStillProvidedEnvironment[i] = 1;
					continue OUTER;
				}	
			}
			for(j=0; j<numCentralSystems; j++)  {
				if(centralSystemFuncMatrix[j][i] == 1 && centralSystemEnvironment[j] == 1  && (centralDecomArr[j] == null || centralDecomArr[j] == 0)) {
					funcStillProvidedEnvironment[i] = 1;
					continue OUTER;
				}	
			}

			funcStillProvidedEnvironment[i] = 0;
		}
		return funcStillProvidedEnvironment;
	}
	
	/**
	 * Creates the dataAtSite and bluAtSite matricies to see whether functionality exists at a site
	 * @param systemFunctionalityMatrix
	 * @return
	 */
	private int[][] calculateFunctionalityAtSite(int[][] systemFunctionalityMatrix, int[][] centralSystemFunctionalityMatrix, int[] systemEnvironment, int[] centralSystemEnvironment, int[] funcStillProvidedInEnvironment) {
		int i;
		int j;
		int k;
		int functionalityLength = systemFunctionalityMatrix[0].length;
		
		int[][] functionalityAtSiteMatrix = new int[functionalityLength][siteLength];
		
		for(i=0; i<functionalityLength; i++) {
			OUTER: for(j=0; j<siteLength; j++) {
				
				for(k=0; k<numCentralSystems; k++) {
					//if a central system has functionality, then functionality is at site and on to next one
					if(centralSystemEnvironment[k] == 1 && centralSystemFunctionalityMatrix[k][i] == 1 && funcStillProvidedInEnvironment[i] == 1) {
						functionalityAtSiteMatrix[i][j] = 1;
						continue OUTER;
					}
				}
				
				for(k=0; k<numNotCentralSystems; k++) {
					//if the system is in this environment, has functionality, and is at site, then functionality is at site and on to next one
					if(systemEnvironment[k] == 1 && systemFunctionalityMatrix[k][i] == 1 && systemSiteMatrix[k][j] == 1 && funcStillProvidedInEnvironment[i] == 1) {
						functionalityAtSiteMatrix[i][j] = 1;
						continue OUTER;
					}
				}

				//if no matches, then functionality is not there
				functionalityAtSiteMatrix[i][j] = 0;

			}
			
		}
		return functionalityAtSiteMatrix;
	}
	
	/**
	 * Sets up the model for calculations to be performed upon.
	 */

	/**
	 * Sets variables in model.
	 * Makes the new LpSolver with a variable for each system and site pair.
	 * Also names the variables and declares them to be binary.
	 */
	@Override
	public void setVariables() throws LpSolveException { //TODO

		//create solver - one variable for each pair of sys and site combos + one variable for each system to say deployed at any site
		// + one variable for each centrally deployed system to say deployed at all sites
		
//		solver = LpSolve.makeLp(sysLength * siteLength * 7 / 5, sysLength * siteLength + sysLength);
		solver = LpSolve.makeLp(0, numNotCentralSystems * siteLength + numNotCentralSystems + numCentralSystems);
		
        //name variables and make binary
		//one variable for each pair of sys and site combos
		int i;
		int j;
		int index = 0;
		for(i=0; i<numNotCentralSystems; i++) {
			for(j=0; j<siteLength; j++) {
				
				solver.setBinary(index + 1, true);
				solver.setVarBranch(index + 1,LpSolve.BRANCH_FLOOR);
				index++;
			}
		}
		
		//one variable for each system to say whether it is deployed at any site
		for(i=0; i<numNotCentralSystems; i++) {
			solver.setBinary(index + 1, true);
			solver.setVarBranch(index + 1, LpSolve.BRANCH_FLOOR);
			index++;
		}
		
		//one variable for each centrally deployed system to say whether it is deployed at all site
		for(i=0; i<numCentralSystems; i++) {
			solver.setBinary(index + 1, true);
			solver.setVarBranch(index + 1, LpSolve.BRANCH_FLOOR);
			index++;
		}
		
	}
	
	/**
	 * Sets constraints in the model.
	 */
	@Override
	public void setConstraints() {
		try {
			addBudgetConstraint();
			
			//adding constraints for data and blu at each site
			addFunctionalityConstraints(theaterDataAtSiteMatrix, systemDataMatrix, centralSystemDataMatrix, systemTheater, centralSystemTheater);
			addFunctionalityConstraints(garrisonDataAtSiteMatrix, systemDataMatrix, centralSystemDataMatrix, systemGarrison, centralSystemGarrison);
			
			addFunctionalityConstraints(theaterBLUAtSiteMatrix, systemBLUMatrix, centralSystemBLUMatrix, systemTheater, centralSystemTheater);
			addFunctionalityConstraints(garrisonBLUAtSiteMatrix, systemBLUMatrix, centralSystemBLUMatrix,  systemGarrison, centralSystemGarrison);

			addSystemDeployedConstraints();

			addModDecomBounds();

		}catch (LpSolveException e) {
			e.printStackTrace(); //TODO
		}
	}
	
	/**
	 * Adds constraints to ensure functionality remains at each site
	 * For each data/blu AND site pair, adds a new constraint to say if functionality is at a site,
	 * then there must be a system in the solution that is at this site that provides this data/blu
	 * @param systemFunctionalityMatrix
	 * @param functionalityAtSiteMatrix
	 * @throws LpSolveException
	 */
	private void addFunctionalityConstraints(int[][] functionalityAtSiteMatrix, int[][] systemFunctionalityMatrix, int[][] centralSystemFunctionalityMatrix, int[] systemEnvironment, int[] centralSystemEnvironment) throws LpSolveException { //TODO
		int i;
		int j;
		int k;
		int functionalityLength = systemFunctionalityMatrix[0].length;
		
		int index;
 		//new constraint for each data/site pairing
		for(i=0; i<functionalityLength; i++) {
			for(j=0; j<siteLength; j++) {
				
				//determine if data is at a site, if not don't need any constraints				
				if(functionalityAtSiteMatrix[i][j] == 1) {
					
					int[] colno = new int[numNotCentralSystems + numCentralSystems];
			        double[] row = new double[numNotCentralSystems + numCentralSystems];
			        index = 0;
			        for(k=0; k<numNotCentralSystems; k++)
			        {
			        	if(systemEnvironment[k] == 1) {
				        	colno[index] = k * siteLength + j+1;
				        	row[index] = systemFunctionalityMatrix[k][i];
				        	index++;
			        	}
			        }
			        
			        for(k=0; k<numCentralSystems; k++)
			        {
			        	if(centralSystemEnvironment[k] == 1) {
			        		colno[index] = numNotCentralSystems * siteLength + numNotCentralSystems + k + 1;
			        		row[index] = centralSystemFunctionalityMatrix[k][i];
			        		index++;
			        	}
			        }
			        
		        	solver.addConstraintex(numNotCentralSystems + numCentralSystems, row, colno, LpSolve.GE, 1);

				}				
			}
		}
	}
	
	/**
	 * Add constraints to determine whether a system is deployed at any site
	 * @throws LpSolveException
	 */
	private void addSystemDeployedConstraints() throws LpSolveException { //TODO		
		int i;
		int j;

		for(i=0; i<numNotCentralSystems; i++) {
			for(j=0; j<siteLength; j++) {
				int[] colno = new int[2];
				double[] row = new double[2];
				
				colno[0] = i * siteLength + j + 1;
				row[0] = -1.0;
				
				colno[1] = numNotCentralSystems * siteLength + i + 1;
				row[1] = 1.0;

			    solver.addConstraintex(2, row, colno, LpSolve.GE, 0);
			}
        }
	}

	/**
	 * Adds a constraint to ensure deployment cost for systems at new sites is under the specified budget
	 * @param systemFunctionalityMatrix
	 * @param functionalityAtSiteMatrix
	 * @throws LpSolveException
	 */
	private void addBudgetConstraint() throws LpSolveException { //TODO

		int i;
		int j;
		int index = 0;
		
		int[] colno = new int[numNotCentralSystems * siteLength];
        double[] row = new double[numNotCentralSystems * siteLength];
        for(i=0; i<numNotCentralSystems; i++) {
			for(j=0; j<siteLength; j++) {
				colno[index] = i * siteLength + j +1;
				row[index] = (1 - systemSiteMatrix[i][j]) * siteDeploymentCosts[i] + (1+trainingPerc) * interfaceCostArr[i];
				index++;
			}
        }

    	solver.addConstraintex(numNotCentralSystems * siteLength, row, colno, LpSolve.LE, maxBudget);

	}
	
	private void addModDecomBounds() throws LpSolveException{
		int i;
		int j;
		
        for(i=0; i<numNotCentralSystems; i++) {
        	if(modArr[i]!=null && modArr[i] == 1) {
				for(j=0; j<siteLength; j++) {
					if(systemSiteMatrix[i][j]==1) {
						solver.setLowbo(i * siteLength + j +1, 1);
					}
				}
				solver.setLowbo(numNotCentralSystems * siteLength + i + 1, 1);
        	} else if(decomArr[i]!=null && decomArr[i] == 1) {
				for(j=0; j<siteLength; j++) {
					if(systemSiteMatrix[i][j]==1) {
						solver.setUpbo(i * siteLength + j +1, 0);
					}
				}
				solver.setUpbo(numNotCentralSystems * siteLength + i + 1, 0);
        	}
        }
        
        for(i=0; i<numCentralSystems; i++) {
        	if(centralModArr[i]!=null && centralModArr[i] == 1) {
				solver.setLowbo(numNotCentralSystems * (siteLength + 1) + i + 1, 1);
        	} else if(centralDecomArr[i]!=null && centralDecomArr[i] == 1) {
				solver.setUpbo(numNotCentralSystems * (siteLength + 1) + i + 1, 0);
        	}
        }
	}


	/**
	 * Sets the function for calculations.
	 */
	@Override
	public void setObjFunction() {
		int i;
		int j;
		int index = 0;
		
		int[] colno = new int[numNotCentralSystems * siteLength + numNotCentralSystems + numCentralSystems];
        double[] row = new double[numNotCentralSystems * siteLength + numNotCentralSystems + numCentralSystems];

        for(i=0; i<numNotCentralSystems; i++) {

        	for(j=0; j<siteLength; j++) {
				colno[index] = i * siteLength + j+1;
 				row[index] = siteMaintenaceCosts[i];
 				index++;
 			}
        }
        
        for(i=0; i<numNotCentralSystems; i++) {
        	
 			colno[index] = numNotCentralSystems * siteLength + i+1;
			row[index] = maintenaceCosts[i];
			index ++;
			
        }
        
        for(i=0; i<numCentralSystems; i++) {
        	
			colno[index] = numNotCentralSystems * siteLength + numNotCentralSystems + i + 1;
			row[index] = centralSystemMaintenaceCosts[i];
			index++;
			
        }
        
		try{
	        solver.setObjFnex(numNotCentralSystems * siteLength + numNotCentralSystems + numCentralSystems, row, colno);
			solver.setMinim();
		}catch (LpSolveException e){
			e.printStackTrace(); //TODO
		}

	}
	
	/**
	 * TODO this should be used if budget is too small and cant run
	 * @param seconds
	 */
	public void setTimeOut(int seconds) {
		solver.setTimeout(seconds);
	}
	
	/**
	 * Executes the optimization.
	 */
	@Override
	public void execute() {
		try{
			solver.writeLp("model.lp");

			solver.setScaling(LpSolve.SCALE_EXTREME | LpSolve.SCALE_QUADRATIC | LpSolve.SCALE_INTEGERS);

			solver.setPresolve(LpSolve.PRESOLVE_COLS | LpSolve.PRESOLVE_ROWS | LpSolve.PRESOLVE_PROBEFIX | LpSolve.PRESOLVE_LINDEP  | LpSolve.PRESOLVE_PROBEREDUCE , solver.getPresolveloops());
			
			solver.setBbDepthlimit(-1);
			solver.setMipGap(true,maxBudget);

			super.execute();
			
			int i;
			int j;
			int index = 0;
			int nConstraints = solver.getNorigRows();

			systemSiteResultMatrix = new double[numNotCentralSystems][siteLength];
			sysKeptArr = new double[numNotCentralSystems];
			centralSysKeptArr = new double[numCentralSystems];
			
			//if you don't get an output, then everything is just left as is, keep all at all current sites
			if(solved != 0) {
//				if(solved == LpSolve.SUBOPTIMAL)
//					LOGGER.error("SOLUTION IS SUBOPTIMAL");
//				else if(solved == LpSolve.TIMEOUT)
//					LOGGER.error("SOLUTION TIMED OUT");
//				else if(solved == LpSolve.INFEASIBLE)
//	 				LOGGER.error("Solution is infeasible for given budget. Recommend do nothing.");
//				else
//	 				LOGGER.error("SOLVED IS "+solved);
				
				objectiveVal = currentSustainmentCost;
				
				systemSiteResultMatrix = systemSiteMatrix;
				
				numSysKept = numNotCentralSystems;
				for(i = 0; i < numNotCentralSystems; i++ ) {
					sysKeptArr[i] = 1;
				}
				
				numCentralSysKept = numCentralSystems;
				for(i = 0; i < numCentralSystems; i++ ) {
					centralSysKeptArr[i] = 1;
				}
				
				totalDeploymentCost = 0.0;

			} else {
				objectiveVal = solver.getObjective();
				
				for(i = 0; i < numNotCentralSystems; i++ ) {
					for(j = 0; j < siteLength; j++) {
						systemSiteResultMatrix[i][j] = solver.getVarPrimalresult(nConstraints + index + 1);
						index++;
					}
				}
				
				numSysKept = 0;
				for(i = 0; i < numNotCentralSystems; i++ ) {
					sysKeptArr[i] = solver.getVarPrimalresult(nConstraints + index + 1);
					numSysKept += sysKeptArr[i];
					index++;
				}			
				
				numCentralSysKept = 0;
				for(i = 0; i < numCentralSystems; i++ ) {
					centralSysKeptArr[i] = solver.getVarPrimalresult(nConstraints + index + 1);
					numCentralSysKept += centralSysKeptArr[i];
					index++;
				}
				
				totalDeploymentCost = 0.0;
		        for(i=0; i<numNotCentralSystems; i++) {
					for(j=0; j<siteLength; j++) {
						if(systemSiteResultMatrix[i][j] == 1) {
							totalDeploymentCost += (1+trainingPerc) * interfaceCostArr[i];
							if(systemSiteMatrix[i][j] == 0)
								totalDeploymentCost += siteDeploymentCosts[i];
						}
					}
		        }
			}
			
			deleteModel();
		} catch(LpSolveException e) {
			e.printStackTrace(); //TODO
		}
	}
	
	public double[] getSysKeptArr() {
		return sysKeptArr;
	}
	
	public double[] getCentralSysKeptArr() {
		return centralSysKeptArr;
	}
	
	public double getObjectiveVal() {
		return objectiveVal;
	}
	
	public double[][] getSystemSiteResultMatrix() {
		return systemSiteResultMatrix;
	}
	
	public double getTotalDeploymentCost() {
		return totalDeploymentCost;
	}
	
	public int getNumSysKept() {
		return numSysKept;
	}
	
	public int getNumCentralSysKept() {
		return numCentralSysKept;
	}
}

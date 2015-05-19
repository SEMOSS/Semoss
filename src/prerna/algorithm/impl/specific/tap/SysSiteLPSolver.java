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
 * Optimizes the systems deployed at each site so that there is not duplicate systems providing the same functionality
 * Differentiates between theater and garrison systems as well as centrally hosted and local systems
 * @author ksmart
 *
 */
public class SysSiteLPSolver extends LPOptimizer{
		
	protected static final Logger LOGGER = LogManager.getLogger(SysSiteLPSolver.class.getName());
	
	//for local systems
	private int numLocalSystems;
	private int[][] localSystemDataMatrix, localSystemBLUMatrix;
	private int[] localSystemIsTheaterArr, localSystemIsGarrisonArr;
	private Integer[] localSystemIsModArr, localSystemIsDecomArr;
	private double[] localSystemMaintenanceCostArr, localSystemSiteMaintenaceCostArr, localSystemSiteDeploymentCostArr, localSystemSiteInterfaceCostArr;
	
	private int siteLength;
	private double[][] localSystemSiteMatrix;	

	//for centrally deployed systems
	private int numCentralSystems;
	private int[][] centralSystemDataMatrix, centralSystemBLUMatrix;
	private int[] centralSystemIsTheaterArr, centralSystemIsGarrisonArr;
	private Integer[] centralSystemIsModArr, centralSystemIsDecomArr;
	private double[] centralSystemMaintenanceCostArr, centralSystemInterfaceCostArr;
	
	//input
	private double trainingPerc, currentSustainmentCost;
	
	//created by algorithm to show what data/blu in theater/garrison environments will be maintained if forcing decommision
	private int[][] dataStillProvidedInTheaterAtSiteMatrix, dataStillProvidedInGarrisonAtSiteMatrix;
	private int[][] bluStillProvidedInTheaterAtSiteMatrix, bluStillProvidedInGarrisonAtSiteMatrix;
	
	//input
	private double maxBudget;
	
	private int budgetRow = 0;
	
	//results
	private double objectiveVal;//the future sustainment cost
	private double totalDeploymentCost;
	private double[] localSysKeptArr, centralSysKeptArr;
	private double[][] localSystemSiteResultMatrix;
	
	/**
	 * Sets/updates the variables required to run the LP module
	 */
	public void setVariables(int[][] localSystemDataMatrix, int[][] localSystemBLUMatrix, int[] localSystemIsTheaterArr, int[] localSystemIsGarrisonArr, Integer[] localSystemIsModArr, Integer[] localSystemIsDecomArr, double[] localSystemMaintenanceCostArr, double[] localSystemSiteMaintenaceCostArr, double[] localSystemSiteDeploymentCostArr, double[] localSystemSiteInterfaceCostArr, double[][] localSystemSiteMatrix, int[][] centralSystemDataMatrix, int[][] centralSystemBLUMatrix, int[] centralSystemIsTheaterArr, int[] centralSystemIsGarrisonArr, Integer[] centralSystemIsModArr, Integer[] centralSystemIsDecomArr, double[] centralSystemMaintenanceCostArr, double[] centralSystemInterfaceCostArr, double trainingPerc, double currentSustainmentCost) {
	
		this.localSystemDataMatrix = localSystemDataMatrix;
		this.localSystemBLUMatrix = localSystemBLUMatrix;
 
		this.localSystemIsTheaterArr = localSystemIsTheaterArr;
		this.localSystemIsGarrisonArr = localSystemIsGarrisonArr;

		this.localSystemIsModArr = localSystemIsModArr;
		this.localSystemIsDecomArr = localSystemIsDecomArr;

		this.localSystemMaintenanceCostArr = localSystemMaintenanceCostArr;
		this.localSystemSiteMaintenaceCostArr = localSystemSiteMaintenaceCostArr;
		this.localSystemSiteDeploymentCostArr = localSystemSiteDeploymentCostArr;
		this.localSystemSiteInterfaceCostArr = localSystemSiteInterfaceCostArr;

		this.siteLength = localSystemSiteMatrix[0].length;
		this.localSystemSiteMatrix = localSystemSiteMatrix;

		this.centralSystemDataMatrix = centralSystemDataMatrix;
		this.centralSystemBLUMatrix = centralSystemBLUMatrix;
		
		this.centralSystemIsTheaterArr = centralSystemIsTheaterArr;
		this.centralSystemIsGarrisonArr = centralSystemIsGarrisonArr;
		
		this.centralSystemIsModArr = centralSystemIsModArr;
		this.centralSystemIsDecomArr = centralSystemIsDecomArr;
		
		this.centralSystemMaintenanceCostArr = centralSystemMaintenanceCostArr;
		this.centralSystemInterfaceCostArr = centralSystemInterfaceCostArr;

		this.trainingPerc = trainingPerc;
		this.currentSustainmentCost = currentSustainmentCost;
		
		this.numCentralSystems = centralSystemMaintenanceCostArr.length;
		this.numLocalSystems = localSystemMaintenanceCostArr.length;
		
		calculateFunctionality();
	}
	
	public void setMaxBudget(double maxBudget) {
		this.maxBudget = maxBudget;
	}
	
	private void calculateFunctionality() {
		int[] dataStillProvidedInTheaterArr = calculateFunctionalityStillProvided(localSystemDataMatrix, centralSystemDataMatrix, localSystemIsTheaterArr, centralSystemIsTheaterArr);
		int[] dataStillProvidedInGarrisonArr = calculateFunctionalityStillProvided(localSystemDataMatrix, centralSystemDataMatrix, localSystemIsGarrisonArr, centralSystemIsGarrisonArr);
		
		int[] bluStillProvidedInTheaterArr = calculateFunctionalityStillProvided(localSystemBLUMatrix, centralSystemBLUMatrix, localSystemIsTheaterArr, centralSystemIsTheaterArr);
		int[] bluStillProvidedInGarrisonArr = calculateFunctionalityStillProvided(localSystemBLUMatrix, centralSystemBLUMatrix, localSystemIsGarrisonArr, centralSystemIsGarrisonArr);
		
		this.dataStillProvidedInTheaterAtSiteMatrix =calculateFunctionalityAtSite(localSystemDataMatrix, centralSystemDataMatrix, localSystemIsTheaterArr,centralSystemIsTheaterArr, dataStillProvidedInTheaterArr);
		this.dataStillProvidedInGarrisonAtSiteMatrix =calculateFunctionalityAtSite(localSystemDataMatrix, centralSystemDataMatrix, localSystemIsGarrisonArr,centralSystemIsGarrisonArr, dataStillProvidedInGarrisonArr);
		
		this.bluStillProvidedInTheaterAtSiteMatrix = calculateFunctionalityAtSite(localSystemBLUMatrix, centralSystemBLUMatrix, localSystemIsTheaterArr,centralSystemIsTheaterArr, bluStillProvidedInTheaterArr);
		this.bluStillProvidedInGarrisonAtSiteMatrix = calculateFunctionalityAtSite(localSystemBLUMatrix, centralSystemBLUMatrix, localSystemIsGarrisonArr,centralSystemIsGarrisonArr, bluStillProvidedInGarrisonArr);
	}
	
	private int[] calculateFunctionalityStillProvided(int[][] localSystemFuncMatrix, int[][] centralSystemFuncMatrix, int[] localSystemEnvironment, int[] centralSystemEnvironment) {
		
		int i;
		int j;
		int funcSize = localSystemFuncMatrix[0].length;
		
		int[] funcStillProvidedEnvironment = new int[funcSize];
		
		OUTER: for(i=0; i<funcSize; i++) {
			for(j=0; j<numLocalSystems; j++)  {
				if(localSystemFuncMatrix[j][i] == 1 && localSystemEnvironment[j] == 1 && localSystemIsDecomArr[j] == 0) {
					funcStillProvidedEnvironment[i] = 1;
					continue OUTER;
				}	
			}
			for(j=0; j<numCentralSystems; j++)  {
				if(centralSystemFuncMatrix[j][i] == 1 && centralSystemEnvironment[j] == 1  && centralSystemIsDecomArr[j] == 0) {
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
				
				for(k=0; k<numLocalSystems; k++) {
					//if the system is in this environment, has functionality, and is at site, then functionality is at site and on to next one
					if(systemEnvironment[k] == 1 && systemFunctionalityMatrix[k][i] == 1 && localSystemSiteMatrix[k][j] == 1 && funcStillProvidedInEnvironment[i] == 1) {
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
	 * Makes the new LpSolver and sets variables in model.
	 * Variables are: one for each system at every site,
	 * one for each system to say deployed at any site (kept or not),
	 * one for each centrally deployed system to say deployed at all sites (kept or not)
	 * Also declares all variables to be binary and gives a starting point for optimization.
	 */
	@Override
	public void setVariables() throws LpSolveException {

		//make the lp solver with enough variables
		try{
			solver = LpSolve.makeLp(0, numLocalSystems * siteLength + numLocalSystems + numCentralSystems);
		}catch (LpSolveException e) {
			LOGGER.error("Could not instantiate a new LP solver");
			throw new LpSolveException("Could not instantiate a new LP solver");
		}
		
        //name variables and make binary
		try{
			int i;
			int j;
			int index = 0;
			
			//one variable for each pair of sys and site combos
			for(i=0; i<numLocalSystems; i++) {
				for(j=0; j<siteLength; j++) {
					solver.setBinary(index + 1, true);
					solver.setVarBranch(index + 1,LpSolve.BRANCH_FLOOR);
					index++;
				}
			}
			
			//one variable for each system to say whether it is deployed at any site
			for(i=0; i<numLocalSystems; i++) {
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
		}catch (LpSolveException e) {
			LOGGER.error("Could not add variables to LP solver");
			throw new LpSolveException("Could not add variables to LP solver");
		}
	}
	
	/**
	 * Sets constraints in the model.
	 * Functionality constraint: data/blu at any site must be still provided at that site, by the same type of system(theater/garrison)
	 * Local system deployed constraint: if a local system is deployed at any site, it must be kept
	 * Budget constraint: deployment costs and interface costs must be less than the budget specified
	 * Mod/decom bounds: sets bounds for systems that were force modernized or force decommissioned
	 */
	@Override
	public void setConstraints() throws LpSolveException{
			
		long startTime;
		long endTime;
		
		try {
			//adding constraints for data and blu at each site
			startTime = System.currentTimeMillis();
			addFunctionalityConstraints(dataStillProvidedInTheaterAtSiteMatrix, localSystemDataMatrix, centralSystemDataMatrix, localSystemIsTheaterArr, centralSystemIsTheaterArr);
			addFunctionalityConstraints(dataStillProvidedInGarrisonAtSiteMatrix, localSystemDataMatrix, centralSystemDataMatrix, localSystemIsGarrisonArr, centralSystemIsGarrisonArr);
			
			addFunctionalityConstraints(bluStillProvidedInTheaterAtSiteMatrix, localSystemBLUMatrix, centralSystemBLUMatrix, localSystemIsTheaterArr, centralSystemIsTheaterArr);
			addFunctionalityConstraints(bluStillProvidedInGarrisonAtSiteMatrix, localSystemBLUMatrix, centralSystemBLUMatrix,  localSystemIsGarrisonArr, centralSystemIsGarrisonArr);
			endTime = System.currentTimeMillis();
			System.out.println("Time to run add functionality constraint " + (endTime - startTime) / 1000 );
		}catch (LpSolveException e) {
			LOGGER.error("Could not add functionality constraints at each site to LP solver");
			throw new LpSolveException("Could not add functionality constraints at each site to LP solver");
		}
		
		try{
			startTime = System.currentTimeMillis();
			addLocalSystemDeployedConstraints();
			endTime = System.currentTimeMillis();
			System.out.println("Time to run add system deployed constraint " + (endTime - startTime) / 1000 );
		}catch (LpSolveException e) {
			LOGGER.error("Could not add system deployment constraints at each site to LP solver");
			throw new LpSolveException("Could not addcsystem deployment constraints at each site to LP solver");
		}
		
		try {
			startTime = System.currentTimeMillis();
			addBudgetConstraint();
			endTime = System.currentTimeMillis();
			System.out.println("Time to add budget constraint " + (endTime - startTime) / 1000 );
		}catch (LpSolveException e) {
			LOGGER.error("Could not add budget constraint");
		}
		
		try{			
			startTime = System.currentTimeMillis();
			addModDecomBounds();
			endTime = System.currentTimeMillis();
			System.out.println("Time to run add mod/decom bounds " + (endTime - startTime) / 1000 );
		}catch (LpSolveException e) {
			LOGGER.error("Could not add constraints for the manually-selected consolidate or sustain systems to LP solver");
			throw new LpSolveException("Could not add constraints for the manually-selected consolidate or sustain systems to LP solver");
		}

	}
	
	/**
	 * Adds constraints to ensure functionality remains at each site
	 * For each data/blu that exists at a site, this method adds a new constraint
	 * that there must be a system in the solution that is at this site that provides this data/blu
	 * AND that system must match the environment (Theater/garrison) of the original provider
	 * @param functionalityAtSiteMatrix
	 * @param localSystemFunctionalityMatrix
	 * @param centralSystemFunctionalityMatrix
	 * @param localSystemEnvironmentArr
	 * @param centralSystemEnvironmentArr
	 * @throws LpSolveException
	 */
	private void addFunctionalityConstraints(int[][] functionalityAtSiteMatrix, int[][] localSystemFunctionalityMatrix, int[][] centralSystemFunctionalityMatrix, int[] localSystemEnvironmentArr, int[] centralSystemEnvironmentArr) throws LpSolveException {
		int i;
		int j;
		int k;
		int functionalityLength = localSystemFunctionalityMatrix[0].length;
		
		int index;
		int[] colno;
		double[] row;
 		//new constraint for each data/site pairing
		for(i=0; i<functionalityLength; i++) {
			for(j=0; j<siteLength; j++) {
				
				//determine if data is at a site, if not don't need any constraints				
				if(functionalityAtSiteMatrix[i][j] == 1) {
					
					colno = new int[numLocalSystems + numCentralSystems];
			        row = new double[numLocalSystems + numCentralSystems];
			        index = 0;
			        for(k=0; k<numLocalSystems; k++) {
				        colno[index] = k * siteLength + j+1;
				        row[index] = localSystemEnvironmentArr[k] * localSystemFunctionalityMatrix[k][i];
				        index++;
			        }
			        
			        for(k=0; k<numCentralSystems; k++)  {
			        	colno[index] = numLocalSystems * siteLength + numLocalSystems + k + 1;
			        	row[index] = centralSystemEnvironmentArr[k] *  centralSystemFunctionalityMatrix[k][i];
			        	index++;
			        }
			        
		        	solver.addConstraintex(numLocalSystems + numCentralSystems, row, colno, LpSolve.GE, 1);

				}				
			}
		}
	}
	
	/**
	 * Adds constraints to say whether a local system is kept at any site.
	 * If a local system is kept at any site, then the local system kept variable must be 1 or modernized.
	 * @throws LpSolveException
	 */
	private void addLocalSystemDeployedConstraints() throws LpSolveException {		
		int i;
		int j;
		int[] colno = new int[2];
		double[] row = new double[2];
		for(i=0; i<numLocalSystems; i++) {
			
			colno[1] = numLocalSystems * siteLength + i + 1;
			row[1] = 1.0;
			
			for(j=0; j<siteLength; j++) {
				
				colno[0] = i * siteLength + j + 1;
				row[0] = -1.0;

			    solver.addConstraintex(2, row, colno, LpSolve.GE, 0);
			}
        }
	}

	/**
	 * Adds a constraint to ensure deployment cost for systems at new sites 
	 * and interface costs for systems consuming data is under the specified budget
	 * @throws LpSolveException
	 */
	private void addBudgetConstraint() throws LpSolveException {

		int i;
		int j;
		int index = 0;

		int[] colno = new int[numLocalSystems * siteLength + numCentralSystems];
        double[] row = new double[numLocalSystems * siteLength + numCentralSystems];

        for(i=0; i<numLocalSystems; i++) {
			for(j=0; j<siteLength; j++) {
				colno[index] = i * siteLength + j +1;
				row[index] = (1 - localSystemSiteMatrix[i][j]) * localSystemSiteDeploymentCostArr[i] + (1+trainingPerc) * localSystemSiteInterfaceCostArr[i];
				index++;
			}
        }
 
        for(i=0; i<numCentralSystems; i++) {
 			colno[index] = numLocalSystems * (siteLength + 1) + i + 1;
 			row[index] = (1+trainingPerc) * centralSystemInterfaceCostArr[i];
 			index++;
        }
        solver.addConstraintex(numLocalSystems * siteLength + numCentralSystems, row, colno, LpSolve.LE, maxBudget);
    	budgetRow = solver.getNrows();

	}
	
	/**
	 * Adds bounds for systems that were specified for force modernization or force decommission
	 * If a local system is force modernized, then must be kept at all the sites. If force decommissioned, must be decommissioned at all sites.
	 * If central system is force modernized, then it must be kept. If force decommissioned, must be decommissioned. 
	 * @throws LpSolveException
	 */
	private void addModDecomBounds() throws LpSolveException{
		int i;
		int j;
		
        for(i=0; i<numLocalSystems; i++) {
        	if(localSystemIsModArr[i] == 1) {
				for(j=0; j<siteLength; j++) {
					if(localSystemSiteMatrix[i][j]==1) {
						solver.setLowbo(i * siteLength + j +1, 1);
					}
				}
				solver.setLowbo(numLocalSystems * siteLength + i + 1, 1);
        	} else if(localSystemIsDecomArr[i] == 1) {
				for(j=0; j<siteLength; j++) {
					if(localSystemSiteMatrix[i][j]==1) {
						solver.setUpbo(i * siteLength + j +1, 0);
					}
				}
				solver.setUpbo(numLocalSystems * siteLength + i + 1, 0);
        	}
        }
        
        for(i=0; i<numCentralSystems; i++) {
        	if(centralSystemIsModArr[i] == 1) {
				solver.setLowbo(numLocalSystems * (siteLength + 1) + i + 1, 1);
        	} else if(centralSystemIsDecomArr[i] == 1) {
				solver.setUpbo(numLocalSystems * (siteLength + 1) + i + 1, 0);
        	}
        }
	}


	/**
	 * Sets the objection function to minimize sustainment cost.
	 * Sums the sustainment cost for the local systems kept at sites + their central maintenance costs
	 * + the sustainment cost for the centrally hosted systems.
	 */
	@Override
	public void setObjFunction() throws LpSolveException{
		int i;
		int j;
		int index = 0;
		
		int[] colno = new int[numLocalSystems * siteLength + numLocalSystems + numCentralSystems];
        double[] row = new double[numLocalSystems * siteLength + numLocalSystems + numCentralSystems];

        for(i=0; i<numLocalSystems; i++) {

        	for(j=0; j<siteLength; j++) {
				colno[index] = i * siteLength + j+1;
 				row[index] = localSystemSiteMaintenaceCostArr[i];
 				index++;
 			}
        }
        
        for(i=0; i<numLocalSystems; i++) {
        	
 			colno[index] = numLocalSystems * siteLength + i+1;
			row[index] = localSystemMaintenanceCostArr[i];
			index ++;
			
        }
        
        for(i=0; i<numCentralSystems; i++) {
        	
			colno[index] = numLocalSystems * siteLength + numLocalSystems + i + 1;
			row[index] = centralSystemMaintenanceCostArr[i];
			index++;
			
        }
        
		try{
	        solver.setObjFnex(numLocalSystems * siteLength + numLocalSystems + numCentralSystems, row, colno);
			solver.setMinim();
		}catch (LpSolveException e) {
			LOGGER.error("Could not set objective function for LP solver");
			throw new LpSolveException("Could not set objective function for LP solver");
		}

	}
	
	/**
	 * Executes the optimization.
	 */
	@Override
	public void execute(){

		//settings to increase solver performance
		solver.setScaling(LpSolve.SCALE_EXTREME | LpSolve.SCALE_QUADRATIC | LpSolve.SCALE_INTEGERS);
		solver.setPresolve(LpSolve.PRESOLVE_COLS | LpSolve.PRESOLVE_ROWS | LpSolve.PRESOLVE_PROBEFIX | LpSolve.PRESOLVE_LINDEP  | LpSolve.PRESOLVE_PROBEREDUCE , solver.getPresolveloops());
		solver.setBbDepthlimit(-1);
		solver.setMipGap(true,maxBudget);
		
		super.execute();
		
		int i;
		int j;
		int index = 0;
		int nConstraints = solver.getNorigRows();
		
		localSystemSiteResultMatrix = new double[numLocalSystems][siteLength];
		localSysKeptArr = new double[numLocalSystems];
		centralSysKeptArr = new double[numCentralSystems];
		
		if(solved == LpSolve.OPTIMAL) {
			try {
				objectiveVal = solver.getObjective();
				
				for(i = 0; i < numLocalSystems; i++ ) {
					for(j = 0; j < siteLength; j++) {
						localSystemSiteResultMatrix[i][j] = solver.getVarPrimalresult(nConstraints + index + 1);
						index++;
					}
				}
				
				for(i = 0; i < numLocalSystems; i++ ) {
					localSysKeptArr[i] = solver.getVarPrimalresult(nConstraints + index + 1);
					index++;
				}			
				
				for(i = 0; i < numCentralSystems; i++ ) {
					centralSysKeptArr[i] = solver.getVarPrimalresult(nConstraints + index + 1);
					index++;
				}
				
				totalDeploymentCost = solver.getVarPrimalresult(budgetRow);	
			} catch(LpSolveException e) {
				LOGGER.error("Unable to get solution. Take no action.");
				setNoSolution();
			}

		} else {
			LOGGER.error("Solution is not optimal. Take no action.");
			setNoSolution();
		}

	}
	
	private void setNoSolution() {
		
		objectiveVal = currentSustainmentCost;
		
		localSystemSiteResultMatrix = localSystemSiteMatrix;
		
		int i;
		for(i = 0; i < numLocalSystems; i++ ) {
			localSysKeptArr[i] = 1;
		}
		
		for(i = 0; i < numCentralSystems; i++ ) {
			centralSysKeptArr[i] = 1;
		}
		
		totalDeploymentCost = 0.0;
	}
	
	public double[] getLocalSysKeptArr() {
		return localSysKeptArr;
	}
	
	public double[] getCentralSysKeptArr() {
		return centralSysKeptArr;
	}
	
	public double[][] getLocalSystemSiteResultMatrix() {
		return localSystemSiteResultMatrix;
	}
	
	public double getObjectiveVal() {
		return objectiveVal;
	}

	public double getTotalDeploymentCost() {
		return totalDeploymentCost;
	}

}

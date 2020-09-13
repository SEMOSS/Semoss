/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import lpsolve.LpSolve;
import lpsolve.LpSolveException;
import prerna.algorithm.impl.LPOptimizer;
import prerna.util.ArrayUtilityMethods;

/**
 * Performs system rationalization at the site level.
 * Optimizes the subset of systems deployed at each site so that all the same data/blu are provided
 * at all the same sites, by a system of the same environment (theater, garrison, or both).
 * Optimization looks for the minimum future sustainment cost, while ensuring that budget
 * stays under a specified/user input maximum annual budget.
 * @author ksmart
 */
public class SysSiteLPSolver extends LPOptimizer{
		
	protected static final Logger LOGGER = LogManager.getLogger(SysSiteLPSolver.class.getName());
	
	//data stores that hold local data and central system data
	SysSiteOptDataStore localSysData, centralSysData;
	
	//numbers of systems and sites
	private int numLocalSystems, numCentralSystems, numSites;
	
	//input
	private double currentSustainmentCost;
	
	//created by algorithm to show what data/blu in theater/garrison environments will be maintained if forcing decommision
	private int[][] dataStillProvidedInTheaterAtSiteMatrix, dataStillProvidedInGarrisonAtSiteMatrix;
	private int[][] bluStillProvidedInTheaterAtSiteMatrix, bluStillProvidedInGarrisonAtSiteMatrix;
	
	//input
	private double maxBudget;
	
	//results
	private double objectiveVal;//the future sustainment cost
	private double totalDeploymentCost;
	private int[] localSysSustainedArr, centralSysSustainedArr; //TODO should we include ones that werent kept as well?
	private int[][] localSystemSiteResultMatrix;

	/**
	 * Sets/updates the variables required to run the LP module
	 */
	public void setVariables(SysSiteOptDataStore localSysData, SysSiteOptDataStore centralSysData, double currentSustainmentCost) {
	
		this.localSysData = localSysData;
		this.centralSysData = centralSysData;

		this.numSites = localSysData.systemSiteMatrix[0].length;
				
		this.currentSustainmentCost = currentSustainmentCost;
		
		this.numLocalSystems = localSysData.systemDataMatrix.length;
		this.numCentralSystems = centralSysData.systemDataMatrix.length;
		
		calculateFunctionality();
	}
	
	public void setMaxBudget(double maxBudget) {
		this.maxBudget = maxBudget;
	}
	
	private void calculateFunctionality() {
		int[] dataStillProvidedInTheaterArr = calculateFunctionalityStillProvided(localSysData.systemDataMatrix, centralSysData.systemDataMatrix, localSysData.systemTheaterArr, centralSysData.systemTheaterArr);
		int[] dataStillProvidedInGarrisonArr = calculateFunctionalityStillProvided(localSysData.systemDataMatrix, centralSysData.systemDataMatrix, localSysData.systemGarrisonArr, centralSysData.systemGarrisonArr);
		
		int[] bluStillProvidedInTheaterArr = calculateFunctionalityStillProvided(localSysData.systemBLUMatrix, centralSysData.systemBLUMatrix, localSysData.systemTheaterArr, centralSysData.systemTheaterArr);
		int[] bluStillProvidedInGarrisonArr = calculateFunctionalityStillProvided(localSysData.systemBLUMatrix, centralSysData.systemBLUMatrix, localSysData.systemGarrisonArr, centralSysData.systemGarrisonArr);
		
		this.dataStillProvidedInTheaterAtSiteMatrix =calculateFunctionalityAtSite(localSysData.systemDataMatrix, centralSysData.systemDataMatrix, localSysData.systemTheaterArr,centralSysData.systemTheaterArr, dataStillProvidedInTheaterArr);
		this.dataStillProvidedInGarrisonAtSiteMatrix =calculateFunctionalityAtSite(localSysData.systemDataMatrix, centralSysData.systemDataMatrix, localSysData.systemGarrisonArr,centralSysData.systemGarrisonArr, dataStillProvidedInGarrisonArr);
		
		this.bluStillProvidedInTheaterAtSiteMatrix = calculateFunctionalityAtSite(localSysData.systemBLUMatrix, centralSysData.systemBLUMatrix, localSysData.systemTheaterArr,centralSysData.systemTheaterArr, bluStillProvidedInTheaterArr);
		this.bluStillProvidedInGarrisonAtSiteMatrix = calculateFunctionalityAtSite(localSysData.systemBLUMatrix, centralSysData.systemBLUMatrix, localSysData.systemGarrisonArr,centralSysData.systemGarrisonArr, bluStillProvidedInGarrisonArr);
	}
	
	private int[] calculateFunctionalityStillProvided(int[][] localSystemFuncMatrix, int[][] centralSystemFuncMatrix, int[] localSystemEnvironment, int[] centralSystemEnvironment) {
		
		int i;
		int j;
		int funcSize = localSystemFuncMatrix[0].length;
		
		int[] funcStillProvidedEnvironment = new int[funcSize];
		
		OUTER: for(i=0; i<funcSize; i++) {
			for(j=0; j<numLocalSystems; j++)  {
				if(localSystemFuncMatrix[j][i] == 1 && localSystemEnvironment[j] == 1 && localSysData.systemForceDecomArr[j] == 0) {
					funcStillProvidedEnvironment[i] = 1;
					continue OUTER;
				}	
			}
			for(j=0; j<numCentralSystems; j++)  {
				if(centralSystemFuncMatrix[j][i] == 1 && centralSystemEnvironment[j] == 1  && centralSysData.systemForceDecomArr[j] == 0) {
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
		
		int[][] functionalityAtSiteMatrix = new int[functionalityLength][numSites];
		
		for(i=0; i<functionalityLength; i++) {
			OUTER: for(j=0; j<numSites; j++) {
				
				for(k=0; k<numCentralSystems; k++) {
					//if a central system has functionality, then functionality is at site and on to next one
					if(centralSystemEnvironment[k] == 1 && centralSystemFunctionalityMatrix[k][i] == 1 && funcStillProvidedInEnvironment[i] == 1) {
						functionalityAtSiteMatrix[i][j] = 1;
						continue OUTER;
					}
				}
				
				for(k=0; k<numLocalSystems; k++) {
					//if the system is in this environment, has functionality, and is at site, then functionality is at site and on to next one
					if(systemEnvironment[k] == 1 && systemFunctionalityMatrix[k][i] == 1 && localSysData.systemSiteMatrix[k][j] == 1 && funcStillProvidedInEnvironment[i] == 1) {
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
			solver = LpSolve.makeLp(0, numLocalSystems * numSites + numLocalSystems + numCentralSystems);
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
				for(j=0; j<numSites; j++) {
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
			addFunctionalityConstraints(dataStillProvidedInTheaterAtSiteMatrix, localSysData.systemDataMatrix, centralSysData.systemDataMatrix, localSysData.systemTheaterArr, centralSysData.systemTheaterArr);
			addFunctionalityConstraints(dataStillProvidedInGarrisonAtSiteMatrix, localSysData.systemDataMatrix, centralSysData.systemDataMatrix, localSysData.systemGarrisonArr, centralSysData.systemGarrisonArr);
			
			addFunctionalityConstraints(bluStillProvidedInTheaterAtSiteMatrix, localSysData.systemBLUMatrix, centralSysData.systemBLUMatrix, localSysData.systemTheaterArr, centralSysData.systemTheaterArr);
			addFunctionalityConstraints(bluStillProvidedInGarrisonAtSiteMatrix, localSysData.systemBLUMatrix, centralSysData.systemBLUMatrix,  localSysData.systemGarrisonArr, centralSysData.systemGarrisonArr);
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
			for(j=0; j<numSites; j++) {
				
				//determine if data is at a site, if not don't need any constraints				
				if(functionalityAtSiteMatrix[i][j] == 1) {
					
					colno = new int[numLocalSystems + numCentralSystems];
			        row = new double[numLocalSystems + numCentralSystems];
			        index = 0;
			        for(k=0; k<numLocalSystems; k++) {
				        colno[index] = k * numSites + j+1;
				        row[index] = localSystemEnvironmentArr[k] * localSystemFunctionalityMatrix[k][i];
				        index++;
			        }
			        
			        for(k=0; k<numCentralSystems; k++)  {
			        	colno[index] = numLocalSystems * numSites + numLocalSystems + k + 1;
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
			
			colno[1] = numLocalSystems * numSites + i + 1;
			row[1] = 1.0;
			
			for(j=0; j<numSites; j++) {
				
				colno[0] = i * numSites + j + 1;
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

		int[] colno = new int[numLocalSystems * numSites + numCentralSystems];
        double[] row = new double[numLocalSystems * numSites + numCentralSystems];

        double cumTrainingCostToSubtract = 0.0;
        double trainingCostToSubtract = 0.0;

		//for each local system: if system is not currently at the site, but will be there in the future... add the deployment costs, the interface deployment costs for consuming new data AND the user training costs for using a new system
        for(i=0; i<numLocalSystems; i++) {
			for(j=0; j<numSites; j++) {
				trainingCostToSubtract = localSysData.systemSiteMatrix[i][j] * localSysData.systemSingleSiteUserTrainingCostArr[i];
				cumTrainingCostToSubtract += trainingCostToSubtract;
				
				colno[index] = i * numSites + j +1;
				row[index] = (1 - localSysData.systemSiteMatrix[i][j]) * localSysData.systemSingleSiteDeploymentCostArr[i] + localSysData.systemHasUpstreamInterfaceArr[i] * localSysData.systemSingleSiteInterfaceCostArr[i] - trainingCostToSubtract;
				index++;
			}
        }

		//for each central system: add the interface deployment costs for consuming new data AND the user training costs for using a new system
        for(i=0; i<numCentralSystems; i++) {

        	trainingCostToSubtract = centralSysData.systemSingleSiteUserTrainingCostArr[i] * numSites;
        	cumTrainingCostToSubtract += trainingCostToSubtract;
        	
 			colno[index] = numLocalSystems * (numSites + 1) + i + 1;
 			row[index] = centralSysData.systemHasUpstreamInterfaceArr[i] * centralSysData.systemSingleSiteInterfaceCostArr[i] * numSites - trainingCostToSubtract;
 			index++;
        }
        solver.addConstraintex(numLocalSystems * numSites + numCentralSystems, row, colno, LpSolve.LE, maxBudget - cumTrainingCostToSubtract);

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
        	if(localSysData.systemForceModArr[i] == 1) {
				for(j=0; j<numSites; j++) {
					if(localSysData.systemSiteMatrix[i][j]==1) {
						solver.setLowbo(i * numSites + j +1, 1);
					}
				}
				solver.setLowbo(numLocalSystems * numSites + i + 1, 1);
        	} else if(localSysData.systemForceDecomArr[i] == 1) {
				for(j=0; j<numSites; j++) {
					if(localSysData.systemSiteMatrix[i][j]==1) {
						solver.setUpbo(i * numSites + j +1, 0);
					}
				}
				solver.setUpbo(numLocalSystems * numSites + i + 1, 0);
        	}
        }
        
        for(i=0; i<numCentralSystems; i++) {
        	if(centralSysData.systemForceModArr[i] == 1) {
				solver.setLowbo(numLocalSystems * (numSites + 1) + i + 1, 1);
        	} else if(centralSysData.systemForceDecomArr[i] == 1) {
				solver.setUpbo(numLocalSystems * (numSites + 1) + i + 1, 0);
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
		
		int[] colno = new int[numLocalSystems * numSites + numLocalSystems + numCentralSystems];
        double[] row = new double[numLocalSystems * numSites + numLocalSystems + numCentralSystems];

        for(i=0; i<numLocalSystems; i++) {

        	for(j=0; j<numSites; j++) {
				colno[index] = i * numSites + j+1;
 				row[index] = localSysData.systemSingleSiteMaintenanceCostArr[i];
 				index++;
 			}
        }
        
        for(i=0; i<numLocalSystems; i++) {
        	
 			colno[index] = numLocalSystems * numSites + i+1;
			row[index] = localSysData.systemCentralMaintenanceCostArr[i];
			index ++;
			
        }
        
        for(i=0; i<numCentralSystems; i++) {
        	
			colno[index] = numLocalSystems * numSites + numLocalSystems + i + 1;
			row[index] = centralSysData.systemCentralMaintenanceCostArr[i];
			index++;
			
        }
        
		try{
	        solver.setObjFnex(numLocalSystems * numSites + numLocalSystems + numCentralSystems, row, colno);
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
		
		localSystemSiteResultMatrix = new int[numLocalSystems][numSites];
		localSysSustainedArr = new int[numLocalSystems];
		centralSysSustainedArr = new int[numCentralSystems];
		
		if(solved == LpSolve.OPTIMAL) {
			try {
				objectiveVal = solver.getObjective();
				
				for(i = 0; i < numLocalSystems; i++ ) {
					for(j = 0; j < numSites; j++) {
						localSystemSiteResultMatrix[i][j] = (int) solver.getVarPrimalresult(nConstraints + index + 1);
						index++;
					}
				}
				
				index = 0;
				for(i = 0; i < numLocalSystems; i++ ) {
					if(solver.getVarPrimalresult(nConstraints + numLocalSystems * numSites + i + 1) == 1.0) {
						localSysSustainedArr[index] = i;
						index++;
					}
				}
				
				localSysSustainedArr = ArrayUtilityMethods.truncateArray(localSysSustainedArr,index-1);
				
				index = 0;
				for(i = 0; i < numCentralSystems; i++ ) {
					if(solver.getVarPrimalresult(nConstraints + numLocalSystems * numSites + numLocalSystems + i + 1) == 1.0) {
						centralSysSustainedArr[index] = i;
						index++;
					}
				}

				centralSysSustainedArr = ArrayUtilityMethods.truncateArray(centralSysSustainedArr,index-1);
				
				
				totalDeploymentCost = 0.0;
		        for(i=0; i<numLocalSystems; i++) {
					for(j=0; j<numSites; j++) {
						totalDeploymentCost += (localSysData.systemSingleSiteDeploymentCostArr[i] * (1 - localSysData.systemSiteMatrix[i][j]) + localSysData.systemHasUpstreamInterfaceArr[i] * localSysData.systemSingleSiteInterfaceCostArr[i]) * localSystemSiteResultMatrix[i][j] + localSysData.systemSingleSiteUserTrainingCostArr[i] * localSysData.systemSiteMatrix[i][j] * (1 - localSystemSiteResultMatrix[i][j]) ;
					}
		        }
		 
		        for(i=0; i<numCentralSystems; i++) {
		        	if(ArrayUtilityMethods.arrayContainsValue(centralSysSustainedArr, i))
		        		totalDeploymentCost += centralSysData.systemHasUpstreamInterfaceArr[i] * centralSysData.systemSingleSiteInterfaceCostArr[i] * numSites;
		        	else 
		        		totalDeploymentCost += centralSysData.systemSingleSiteUserTrainingCostArr[i] * numSites;
		        }
		        
				//totalDeploymentCost = solver.getVarPrimalresult(budgetRow);	
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
		
		localSystemSiteResultMatrix = localSysData.systemSiteMatrix;
		
		int i;
		for(i = 0; i < numLocalSystems; i++ ) {
			localSysSustainedArr[i] = i;
		}
		
		for(i = 0; i < numCentralSystems; i++ ) {
			centralSysSustainedArr[i] = i;
		}
		
		totalDeploymentCost = 0.0;
	}
	
	public int[] getLocalSysSustainedArr() {
		return localSysSustainedArr;
	}
	
	public int[] getCentralSysSustainedArr() {
		return centralSysSustainedArr;
	}
	
	public int[][] getLocalSystemSiteResultMatrix() {
		return localSystemSiteResultMatrix;
	}
	
	public double getObjectiveVal() {
		return objectiveVal;
	}

	public double getTotalDeploymentCost() {
		return totalDeploymentCost;
	}

}

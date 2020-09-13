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
 * For a system, optimizes the list of systems that can replace it in order to obtain the smallest subset of systems.
 * Primarily used in portfolio rationalization to identify sustained systems that will replace the functionality of consolidated systems.
 * @author ksmart
 */
public class SysReplacementLPSolver extends LPOptimizer{
		
	protected static final Logger LOGGER = LogManager.getLogger(SysReplacementLPSolver.class.getName());
	
	//for consolidated system that needs to be replaced
	private int[] consolSysDataArr, consolSysBLUArr, consolSysSiteArr;
	private int consolSysIsTheater, consolSysIsGarrison;	
	
	//for sustained local systems that are options for replacing consolidated system
	private int numLocalSystems;
	private int[][] localSystemDataMatrix, localSystemBLUMatrix, localSystemSiteMatrix;
	private int[] localSystemIsTheaterArr, localSystemIsGarrisonArr;
	private double[] localSysMaintenanceCostArr;

	//for sustained centrally deployed systems that are options for replacing consolidated system
	private int numCentralSystems;
	private int[][] centralSystemDataMatrix, centralSystemBLUMatrix;
	private int[] centralSystemIsTheaterArr, centralSystemIsGarrisonArr;
	private double[] centralSysMaintenanceCostArr;
		
	//indicies of local systems and central systems that optimal and sufficient to replace the consolidated system
	private double objectiveVal;
	private int[] localSysReplacementArr, centralSysReplacementArr;
	
	/**
	 * Sets/updates the consolidated system variables required to run the LP module
	 * @param consolSysDataArr		Array representing all data objects. 1 if consolidated system provides, 0 if it does not
	 * @param consolSysBLUArr		Array representing all blu. 1 if consolidated system provides, 0 if it does not
	 * @param consolSysSiteArr		Array representing all sites. 1 if consolidated system deployed there, 0 if it is not
	 * @param consolSysIsTheater	Int representing if consolidated system functions in theater environment. 1 if so, 0 if not
	 * @param consolSysIsGarrison	Int representing if consolidated system functions in garrison environment. 1 if so, 0 if not
	 */
	public void setConsolidatedSystemVariables(int[] consolSysDataArr, int[] consolSysBLUArr, int[] consolSysSiteArr, int consolSysIsTheater, int consolSysIsGarrison) {
		this.consolSysDataArr = consolSysDataArr;
		this.consolSysBLUArr = consolSysBLUArr;
		this.consolSysSiteArr = consolSysSiteArr;
		this.consolSysIsTheater = consolSysIsTheater;
		this.consolSysIsGarrison = consolSysIsGarrison;
	}

	/**
	 * Sets/updates the sustained system variables required to run the LP module
	 * @param localSystemDataMatrix			Matrix for all sustained local systems and all data objects. 1 if system provides data, 0 if it does not
	 * @param localSystemBLUMatrix			Matrix for all sustained local systems and all blu. 1 if system provides blu, 0 if it does not
	 * @param localSystemSiteMatrix			Matrix for all sustained local systems and all sites. 1 if system deployed there, 0 if it is not
	 * @param localSystemIsTheaterArr		Array for all sustained local systems representing if they function in a theater environment. 1 if so, 0 if not.
	 * @param localSystemIsGarrisonArr		Array for all sustained local systems representing if they function in a garrison environment. 1 if so, 0 if not.
	 * @param localSysMaintenanceCostArr	Array for all sustained local systems's sustainment costs.
	 * @param centralSystemDataMatrix		Matrix for all sustained central systems and all data objects. 1 if system provides data, 0 if it does not
	 * @param centralSystemBLUMatrix		Matrix for all sustained central systems and all blu. 1 if system provides blu, 0 if it does not
	 * @param centralSystemIsTheaterArr		Array for all sustained central systems representing if they function in a theater environment. 1 if so, 0 if not.
	 * @param centralSystemIsGarrisonArr	Array for all sustained central systems representing if they function in a garrison environment. 1 if so, 0 if not.
	 * @param centralSysMaintenanceCost		Array for all sustained central systems's sustainment costs.
	 */
	public void setSustainedSystemVariables(int[][] localSystemDataMatrix, int[][] localSystemBLUMatrix, int[][] localSystemSiteMatrix, int[] localSystemIsTheaterArr, int[] localSystemIsGarrisonArr, double[] localSysMaintenanceCostArr, int[][] centralSystemDataMatrix, int[][] centralSystemBLUMatrix, int[] centralSystemIsTheaterArr, int[] centralSystemIsGarrisonArr, double[] centralSysMaintenanceCost) {
	
		this.localSystemDataMatrix = localSystemDataMatrix;
		this.localSystemBLUMatrix = localSystemBLUMatrix;
		this.localSystemSiteMatrix = localSystemSiteMatrix;
 
		this.localSystemIsTheaterArr = localSystemIsTheaterArr;
		this.localSystemIsGarrisonArr = localSystemIsGarrisonArr;
		this.localSysMaintenanceCostArr = localSysMaintenanceCostArr;

		this.numLocalSystems = localSystemDataMatrix.length;

		this.centralSystemDataMatrix = centralSystemDataMatrix;
		this.centralSystemBLUMatrix = centralSystemBLUMatrix;
		
		this.centralSystemIsTheaterArr = centralSystemIsTheaterArr;
		this.centralSystemIsGarrisonArr = centralSystemIsGarrisonArr;
		this.centralSysMaintenanceCostArr = centralSysMaintenanceCost;
		
		this.numCentralSystems = centralSystemDataMatrix.length;
		
	}

	/**
	 * Makes the new LpSolver and sets variables in model.
	 * One variable for each sustained system (both local and central)
	 */
	@Override
	public void setVariables() throws LpSolveException {

		//make the lp solver with variables for each sustained system ( all possible replacements)
		try{
			solver = LpSolve.makeLp(0, numLocalSystems + numCentralSystems);
		}catch (LpSolveException e) {
			LOGGER.error("Could not instantiate a new LP solver");
			throw new LpSolveException("Could not instantiate a new LP solver");
		}
		
        //make all variables binary and set a starting value to speed up solver
		try{
			int index;
			for(index=0; index<numLocalSystems + numCentralSystems; index++) {
				solver.setBinary(index + 1, true);
				solver.setVarBranch(index + 1,LpSolve.BRANCH_FLOOR);
			}

		}catch (LpSolveException e) {
			LOGGER.error("Could not add variables to LP solver");
			throw new LpSolveException("Could not add variables to LP solver");
		}
	}
	
	/**
	 * Sets constraints in the model.
	 * Data/BLU constraint: systems chosen for replacement must keep same data/BLU at every site that the consolidated system originally provided
	 * Must also make sure that the replacing systems provide the same garrison/theater capabilities
	 */
	@Override
	public void setConstraints() throws LpSolveException{
					
		try {
			//if consolidated system functions in theater, add constraints that all of its data/blu must be provided in theater
			if(consolSysIsTheater == 1) {
				addFunctionalityConstraints(consolSysDataArr, localSystemDataMatrix, centralSystemDataMatrix, localSystemIsTheaterArr, centralSystemIsTheaterArr);
				addFunctionalityConstraints(consolSysBLUArr, localSystemBLUMatrix, centralSystemBLUMatrix, localSystemIsTheaterArr, centralSystemIsTheaterArr);
			}
			
			//if consolidated system functions in garrison, add constraints that all of its data/blu must be provided in garrison
			if(consolSysIsGarrison == 1) {
				addFunctionalityConstraints(consolSysDataArr, localSystemDataMatrix, centralSystemDataMatrix, localSystemIsGarrisonArr, centralSystemIsGarrisonArr);
				addFunctionalityConstraints(consolSysBLUArr, localSystemBLUMatrix, centralSystemBLUMatrix, localSystemIsGarrisonArr, centralSystemIsGarrisonArr);
			}

		}catch (LpSolveException e) {
			LOGGER.error("Could not add functionality constraints at each site to LP solver");
			throw new LpSolveException("Could not add functionality constraints at each site to LP solver");
		}
		
	}
	
	/**
	 * Adds constraints to ensure that all of consolidated system's data and BLU is preserved at each of its sites
	 * For each data/blu the consolidated system provides and at every site the consolidated system is deployed at,
	 * this method adds a new constraint that there must be a system in the solution (sustained/replacing system)
	 * that is at this site that provides this data/blu AND that system must match the environment (Theater/garrison) of the original provider.
	 * @param consolSysFunctionalityArr			Array representing either all data objects or all blus. 1 if consolidated system provides, 0 if it does not
	 * @param localSystemFunctionalityMatrix	Matrix for all sustained/potential replacement local systems and either all data objects or all blus. 1 if system provides data, 0 if it does not	
	 * @param centralSystemFunctionalityMatrix	Matrix for all sustained/potential replacement central systems and either all data objects or all blus. 1 if system provides data, 0 if it does not
	 * @param localSystemEnvironmentArr			Array for all sustained/potential replacement local systems representing if they function in a certain (either garrison OR theater) environment. 1 if so, 0 if not.
	 * @param centralSystemEnvironmentArr		Array for all sustained/potential replacement central systems  representing if they function in a certain (either garrison OR theater) environment. 1 if so, 0 if not.
	 * @throws LpSolveException					Throws exception if any of the constraints cannot be added to the solver.
	 */
	private void addFunctionalityConstraints(int[] consolSysFunctionArr, int[][] localSystemFunctionMatrix, int[][] centralSystemFunctionMatrix, int[] localSystemEnvironmentArr, int[] centralSystemEnvironmentArr) throws LpSolveException {

		int i; // data/blu index
		int j; // site index
		int k; // system index
		int numFunctions = consolSysFunctionArr.length;
		int numSites = consolSysSiteArr.length;
		int index;
		int[] colno;
		double[] row;
 		//new constraint for each data/blu and site pairing
		for(i=0; i<numFunctions; i++) {
			for(j=0; j<numSites; j++) {
				
				//determine if consolidated system is at site and provides the data/blu				
				if(consolSysFunctionArr[i] * consolSysSiteArr[j] == 1) {
					
					//if it does provide data/blu at site, then need to add a constraint saying some replacing system must provide
					colno = new int[numLocalSystems + numCentralSystems];
			        row = new double[numLocalSystems + numCentralSystems];
			        index = 0;
			        for(k=0; k<numLocalSystems; k++) {
				        colno[index] = k + 1;
				        row[index] = localSystemFunctionMatrix[k][i] * localSystemSiteMatrix[k][j] * localSystemEnvironmentArr[k];
				        index++;
			        }
			        
			        for(k=0; k<numCentralSystems; k++)  {
			        	colno[index] = numLocalSystems + k + 1;
				        row[index] = centralSystemFunctionMatrix[k][i] * centralSystemEnvironmentArr[k];
			        	index++;
			        }
			        
		        	solver.addConstraintex(numLocalSystems + numCentralSystems, row, colno, LpSolve.GE, 1);

				}				
			}
		}
	}
	
	/**
	 * Sets the objective function for optimizing the list of replacing systems.
	 * Minimizes the number of systems needed to replace the consolidated system.
	 * Slight preference given to more similar and cheaper replacing systems.
	 */
	@Override
	public void setObjFunction() throws LpSolveException{
	
		//sum up total maintenance cost
		double totalMaintenanceCost = SysOptUtilityMethods.sumRow(localSysMaintenanceCostArr) + SysOptUtilityMethods.sumRow(centralSysMaintenanceCostArr);
		
		//calculate dissimilarity for each replacing system compared to the consolidated system
		//iterate through data provided by consolidated,
		//if sustained system does not provide, add 1.
		//store for sustained system
		//sum up total dissimilarity
		double[] localSysDissimilarScore = new double[numLocalSystems];
		double[] centralSysDissimilarScore = new double[numCentralSystems];
		double totalDissimilarScore = calculateDissimilarity(localSysDissimilarScore, localSystemDataMatrix, localSystemBLUMatrix, localSystemIsTheaterArr, localSystemIsGarrisonArr);
		totalDissimilarScore += calculateDissimilarity(centralSysDissimilarScore, centralSystemDataMatrix, centralSystemBLUMatrix, centralSystemIsTheaterArr, centralSystemIsGarrisonArr);
		
		//build the objective function. Coefficients for each system are 1 + dissimilar score / total dissimilar score * (1 + maintenance cost / total maintenance cost)
		int[] colno = new int[numLocalSystems + numCentralSystems];
        double[] row = new double[numLocalSystems + numCentralSystems];
		int index = 0;
		
		int i;
		//adding coefficients for the local systems based on their dissimilarity and maintenance cost
        for(i=0; i<numLocalSystems; i++) {
        	
 			colno[index] = i + 1;
			row[index] = (1.0 + (localSysDissimilarScore[i] / totalDissimilarScore) * (1.0 + (localSysMaintenanceCostArr[i] / totalMaintenanceCost)));
			index ++;
        }

		//adding coefficients for the central systems based on their dissimilarity and maintenance cost
        for(i=0; i<numCentralSystems; i++) {
        	
			colno[index] = numLocalSystems + i + 1;
			row[index] = (1.0 + (centralSysDissimilarScore[i] / totalDissimilarScore) * (1.0 + (centralSysMaintenanceCostArr[i] / totalMaintenanceCost)));
			index++;
        }
        
		try{
			//setting the objective function and selecting minimization
	        solver.setObjFnex(numLocalSystems + numCentralSystems, row, colno);
			solver.setMinim();
		}catch (LpSolveException e) {
			LOGGER.error("Could not set objective function for LP solver");
			throw new LpSolveException("Could not set objective function for LP solver");
		}

	}
	
	/**
	 * Calculates the dissimilarity between the consolidated system and the sustained/possible replacement systems.
	 * Dissimilarity is measured by counting the number of data/blu provided by the consolidated system
	 * that are NOT provided by the sustained/possible replacement system.
	 * @param dissimilarScoreArr		Array of dissimilarity scores between the consolidated system and each possible replacement system
	 * @param sustainedSystemDataMatrix Matrix for all sustained systems and all data objects. 1 if system provides data object, 0 if it does not
	 * @param sustainedSystemBLUMatrix	Matrix for all sustained systems and all blu. 1 if system provides blu, 0 if it does not
	 * @param sustainedSysIsTheater		Array for all sustained systems representing if they function in a theater environment. 1 if so, 0 if not.
	 * @param sustainedSysIsGarrison	Array for all sustained systems representing if they function in a garrison environment. 1 if so, 0 if not.
	 * @return 							double representing the summed dissimilar score for all possible replacement systems 
	 */
	private double calculateDissimilarity(double[] dissimilarScoreArr, int[][] sustainedSystemDataMatrix, int[][] sustainedSystemBLUMatrix, int[] sustainedSysIsTheater, int[] sustainedSysIsGarrison) {
		int i;
		int j;
		int numData = consolSysDataArr.length;
		int numBLU = consolSysBLUArr.length;
		int numSystems = sustainedSystemDataMatrix.length;
		double totalDissimilarScore = 0;

		for(i=0; i<numSystems; i++) {
			double simScore = 0;
			for(j=0; j<numData; j++) {
				//if consolidated provides the data and replacement system provides the data
				if(consolSysDataArr[j] == 1 && sustainedSystemDataMatrix[i][j]==1) {
					simScore ++;
				}
			}
			
			for(j=0; j<numBLU; j++) {
				//if consolidated provides the blu and replacement system provides the blu
				if(consolSysBLUArr[j] == 1 && sustainedSystemBLUMatrix[i][j]==1) {
					simScore ++;
				}
			}
			
			double numProvided = SysOptUtilityMethods.sumRow(consolSysDataArr) + SysOptUtilityMethods.sumRow(consolSysBLUArr) + SysOptUtilityMethods.sumRow(sustainedSystemDataMatrix[i]) + SysOptUtilityMethods.sumRow(sustainedSystemBLUMatrix[i]) - simScore;
			
			simScore = simScore / numProvided;
			
			//account for theater and garrison environments
			
			simScore = ( consolSysIsGarrison * sustainedSysIsGarrison[i] * .5 + consolSysIsTheater * sustainedSysIsTheater[i] * .5 ) * simScore;
			
			dissimilarScoreArr[i] = 1 - simScore;
			totalDissimilarScore += 1 - simScore;
		}

//		double totalDissimilarScore = 0;
//				
//		for(i=0; i<numSystems; i++) {
//			int dissimScore = 0;
//			for(j=0; j<numData; j++) {
//				//if consolidated provides the data
//				//AND either replacement system doesnt or replacement system has it but not same environment
//				if(consolSysDataArr[j] == 1) {
//					if(consolSysIsTheater == 1) {
//						if(sustainedSystemDataMatrix[i][j]==0 || sustainedSysIsTheater[i] == 0)
//							dissimScore ++;
//					}
//					if(consolSysIsGarrison == 1) {
//						if(sustainedSystemDataMatrix[i][j]==0 || sustainedSysIsGarrison[i] == 0)
//							dissimScore ++;
//					}
//					
//				}
//			}
//			
//			for(j=0; j<numBLU; j++) {
//				//if consolidated provides the blu
//				//AND either replacement system doesnt or replacement system has it but not same environment
//				if(consolSysBLUArr[j] == 1) {
//					if(consolSysIsTheater == 1) {
//						if(sustainedSystemBLUMatrix[i][j]==0 || sustainedSysIsTheater[i] == 0)
//							dissimScore ++;
//					}
//					if(consolSysIsGarrison == 1) {
//						if(sustainedSystemBLUMatrix[i][j]==0 || sustainedSysIsGarrison[i] == 0)
//							dissimScore ++;
//					}
//					
//				}
//			}
//			dissimilarScoreArr[i] = dissimScore;
//			totalDissimilarScore += dissimScore;
//		}
		return totalDissimilarScore;
	}
	
	/**
	 * Executes the optimization.
	 * Determines if the solution was optimal.
	 * If optimal, creates lists of the indicies of the local and central systems that are optimal replacements.
	 * If not optimal or unable to obtain solution, sets the replacement lists to be all systems.
	 */
	@Override
	public void execute(){
	
		super.execute();
		
		int i;
		int index = 0;
		int nConstraints = solver.getNorigRows();
		
		localSysReplacementArr = new int[numLocalSystems];
		centralSysReplacementArr = new int[numCentralSystems];
		
		if(solved == LpSolve.OPTIMAL) {
			try {
				objectiveVal = solver.getObjective();
				
				index = 0;
				for(i = 0; i < numLocalSystems; i++ ) {
					if(solver.getVarPrimalresult(nConstraints + i + 1) == 1.0) {
						localSysReplacementArr[index] = i;
						index++;
					}
				}
				
				localSysReplacementArr = ArrayUtilityMethods.truncateArray(localSysReplacementArr, index - 1);

				index = 0;
				for(i = 0; i < numCentralSystems; i++ ) {
					if(solver.getVarPrimalresult(nConstraints + numLocalSystems + i + 1) == 1.0) {
						centralSysReplacementArr[index] = i;
						index++;
					}
				}
				
				centralSysReplacementArr = ArrayUtilityMethods.truncateArray(centralSysReplacementArr, index - 1);
				
			} catch(LpSolveException e) {
				LOGGER.error("Unable to get solution. Take no action.");
				setNoSolution();
			}

		} else {
			LOGGER.error("Solution is not optimal. Take no action.");
			setNoSolution();
		}

	}
	
	/**
	 * If no solution can be found,
	 * then assume that all replacement options are needed.
	 */
	private void setNoSolution() {
		objectiveVal = numLocalSystems + numCentralSystems;
		int i;
		for(i = 0; i < numLocalSystems; i++ ) {
			localSysReplacementArr[i] = 1;
		}
		
		for(i = 0; i < numCentralSystems; i++ ) {
			centralSysReplacementArr[i] = 1;
		}
	}
	
	/**
	 * Objective value is fairly meaningless for this equation, 
	 * but can give a VERY rough estimate on how many systems needed to replace
	 * @return double representing the objective value
	 */
	public double getObjectiveVal() {
		return objectiveVal;
	}
	
	/**
	 * Indicies of the replacing local systems in the optimal solution
	 * @return Array containing indicies of all replacing systems that are local
	 */
	public int[] getLocalSysReplacementArr() {
		return localSysReplacementArr;
	}
	
	/**
	 * Indicies of all replacing central systems in the optimal solution
	 * @return Array containing indicies of all replacing systems that are central
	 */
	public int[] getCentralSysReplacementArr() {
		return centralSysReplacementArr;
	}
}

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

import java.util.ArrayList;

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

	private double[] maintenaceCosts;
	private double[] siteMaintenaceCosts;
	private double[] siteDeploymentCosts;
	
	//for systems that are centrally deployed
	private int numCentralSystems;
	private int[][] centralSystemDataMatrix, centralSystemBLUMatrix;
	private int[] centralSystemTheater, centralSystemGarrison;

	private Integer[] centralModArr, centralDecomArr;
	
	private double[] centralSystemMaintenaceCosts;
	
	//created by algorithm
	private int[] dataStillProvidedTheater, dataStillProvidedGarrison;
	private int[] bluStillProvidedTheater, bluStillProvidedGarrison;
	
	private int[][] theaterDataAtSiteMatrix, theaterBLUAtSiteMatrix;
	private int[][] garrisonDataAtSiteMatrix, garrisonBLUAtSiteMatrix;
	
	//input
	private double maxBudget;
	
	//row pertaining to the budget constraint to make updating faster
	private int budgetRow;
	
	private double objectiveVal;
	private double[][] systemSiteResultMatrix;
	private double[] sysKeptArr;
	private double[] centralSysKeptArr;
	private double totalDeploymentCost;
	
	//TODO delete after testing
	ArrayList<String> sysList, centralSysList, dataList, bluList, siteList;

	/**
	 * Instantiates the SystemDeploymentOptimizer
	 */
	public SysSiteLPSolver(int[][] systemDataMatrix, int[][] systemBLUMatrix, double[][] systemSiteMatrix, int[] systemTheater, int[] systemGarrison, Integer[] sysModArr, Integer[] sysDecomArr, double[] maintenaceCosts, double[] siteMaintenaceCosts, double[] siteDeploymentCosts,int[][] centralSystemDataMatrix,int[][] centralSystemBLUMatrix, int[] centralSystemTheater, int[] centralSystemGarrison, Integer[] centralModArr, Integer[] centralDecomArr, double[] centralSystemMaintenaceCosts, double maxBudget) {

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

		this.centralSystemDataMatrix = centralSystemDataMatrix;
		this.centralSystemBLUMatrix = centralSystemBLUMatrix;
		
		this.centralSystemTheater = centralSystemTheater;
		this.centralSystemGarrison = centralSystemGarrison;
		
		this.centralModArr = centralModArr;
		this.centralDecomArr = centralDecomArr;
		
		this.centralSystemMaintenaceCosts = centralSystemMaintenaceCosts;
		
		this.maxBudget = maxBudget;
		
		this.numCentralSystems = centralSystemMaintenaceCosts.length;

		this.numNotCentralSystems = systemSiteMatrix.length;
		this.siteLength = systemSiteMatrix[0].length;

		this.dataStillProvidedTheater = calculateFunctionalityStillProvided(systemDataMatrix, centralSystemDataMatrix, systemTheater, centralSystemTheater);
		this.dataStillProvidedGarrison = calculateFunctionalityStillProvided(systemDataMatrix, centralSystemDataMatrix, systemGarrison, centralSystemGarrison);
		
		this.bluStillProvidedTheater = calculateFunctionalityStillProvided(systemBLUMatrix, centralSystemBLUMatrix, systemTheater, centralSystemTheater);
		this.bluStillProvidedGarrison = calculateFunctionalityStillProvided(systemBLUMatrix, centralSystemBLUMatrix, systemGarrison, centralSystemGarrison);
		
		this.theaterDataAtSiteMatrix =calculateFunctionalityAtSite(systemDataMatrix, centralSystemDataMatrix, systemTheater,centralSystemTheater, dataStillProvidedTheater);
		this.garrisonDataAtSiteMatrix =calculateFunctionalityAtSite(systemDataMatrix, centralSystemDataMatrix, systemGarrison,centralSystemGarrison, dataStillProvidedGarrison);
		
		this.theaterBLUAtSiteMatrix = calculateFunctionalityAtSite(systemBLUMatrix, centralSystemBLUMatrix, systemTheater,centralSystemTheater, bluStillProvidedTheater);
		this.garrisonBLUAtSiteMatrix = calculateFunctionalityAtSite(systemBLUMatrix, centralSystemBLUMatrix, systemGarrison,centralSystemGarrison, bluStillProvidedGarrison);
	}
	
	public void setVariableNames(ArrayList<String> sysList, ArrayList<String> centralSysList, ArrayList<String> dataList, ArrayList<String> bluList, ArrayList<String> siteList) {

		this.sysList = sysList;
		this.centralSysList = centralSysList;
		this.dataList = dataList;
		this.bluList = bluList;
		this.siteList = siteList;
	}
	
	
	/**
	 * Sets system site matrix so that the optimizer can be rerun
	 * @param systemSiteMatrix
	 */
	public void updateBudget(double maxBudget) {
//		if(this.maxBudget != maxBudget) {
			this.maxBudget = maxBudget;
			try{
				
				if(budgetRow == 0) {
					LOGGER.error("BUDGET ROW IS ZERO");
				}
				solver.setRh(budgetRow, maxBudget);
			//	updateBounds();
				
			}catch (LpSolveException e) {
				e.printStackTrace(); //TODO
			}
//		}
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
				if(systemFuncMatrix[j][i] == 1 && systemEnvironment[j] == 1 && decomArr[j] == null) {
					funcStillProvidedEnvironment[i] = 1;
					continue OUTER;
				}	
			}
			for(j=0; j<numCentralSystems; j++)  {
				if(centralSystemFuncMatrix[j][i] == 1 && centralSystemEnvironment[j] == 1  && centralDecomArr[j] == null) {
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
	private int[][] calculateFunctionalityAtSite(int[][] systemFunctionalityMatrix, int[][] centralSystemFunctionalityMatrix, int[] systemEnvironment, int[] centralSystemEnvironment, int[] funcStillProvided) {
		int i;
		int j;
		int k;
		int functionalityLength = systemFunctionalityMatrix[0].length;
		
		int[][] functionalityAtSiteMatrix = new int[functionalityLength][siteLength];
		
		for(i=0; i<functionalityLength; i++) {
			NEXT: for(j=0; j<siteLength; j++) {
				
				for(k=0; k<numNotCentralSystems; k++) {
					//if the system is in this environment, has functionality, and is at site, then functionality is at site and on to next one
					if(systemEnvironment[k] == 1 && systemFunctionalityMatrix[k][i] == 1 && systemSiteMatrix[k][j] == 1 && funcStillProvided[i] == 1) {
						functionalityAtSiteMatrix[i][j] = 1;
						continue NEXT;
					}
				}
				
				for(k=0; k<numCentralSystems; k++) {
					//if a central system has functionality, then functionality is at site and on to next one
					if(centralSystemEnvironment[k] == 1 && centralSystemFunctionalityMatrix[k][i] == 1 && funcStillProvided[i] == 1) {
						functionalityAtSiteMatrix[i][j] = 1;
						continue NEXT;
					}
				}

				//if no matches, then functionality is not there
				functionalityAtSiteMatrix[i][j] = 0;

			}
			
		}
		return functionalityAtSiteMatrix;
	}

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
				solver.setColName(index + 1, sysList.get(i)+"-"+siteList.get(j));
				index++;
//				if(systemSiteMatrix[i][j]==1.0)
					solver.setVarBranch(index,LpSolve.BRANCH_FLOOR);
//				else
//					solver.setVarBranch(index,LpSolve.BRANCH_FLOOR);
			}
		}
		
		//one variable for each system to say whether it is deployed at any site
		for(i=0; i<numNotCentralSystems; i++) {
			solver.setBinary(index + 1, true);
			solver.setColName(index + 1, sysList.get(i));
			index++;
			solver.setVarBranch(index, LpSolve.BRANCH_FLOOR);
		}
		
		//one variable for each centrally deployed system to say whether it is deployed at all site
		for(i=0; i<numCentralSystems; i++) {
			solver.setBinary(index + 1, true);
			solver.setColName(index + 1, centralSysList.get(i));
			index++;
			solver.setVarBranch(index, LpSolve.BRANCH_FLOOR);
		}
		
	}
	
	/**
	 * Sets constraints in the model.
	 */
	@Override
	public void setConstraints() {
		try {
			//makes building the model faster if it is done rows by row
			long startTime;
			long endTime1;

//			solver.resizeLp(sysLength * siteLength * 7 / 5, sysLength * siteLength + sysLength);


			startTime = System.currentTimeMillis();			
			addBudgetConstraint();
			endTime1 = System.currentTimeMillis();
//			System.out.println("Time to create budget constraint " + (endTime1 - startTime) / 1000 );
			
			startTime = System.currentTimeMillis();			
			//adding constraints for washingtodata and blu at each site
			addFunctionalityConstraints(theaterDataAtSiteMatrix, systemDataMatrix, centralSystemDataMatrix, systemTheater, centralSystemTheater);
			addFunctionalityConstraints(garrisonDataAtSiteMatrix, systemDataMatrix, centralSystemDataMatrix, systemGarrison, centralSystemGarrison);
			
			addFunctionalityConstraints(theaterBLUAtSiteMatrix, systemBLUMatrix, centralSystemBLUMatrix, systemGarrison, centralSystemGarrison);
			addFunctionalityConstraints(garrisonBLUAtSiteMatrix, systemBLUMatrix, centralSystemBLUMatrix,  systemGarrison, centralSystemGarrison);
			endTime1 = System.currentTimeMillis();
//			System.out.println("Time to create functionality constraints " + (endTime1 - startTime) / 1000 );
			
//			//we estimated the number of rows necessary. if there isnt enough space, we need to resize
//			if(solver.getNrows() - currRowInd < sysLength * siteLength + 1) {
//				solver.resizeLp(currRowInd + sysLength * siteLength + 1, solver.getNcolumns());
//			}
			
			startTime = System.currentTimeMillis();			
			addSystemDeployedConstraints();
			endTime1 = System.currentTimeMillis();
//			System.out.println("Time to create deployment constraints " + (endTime1 - startTime) / 1000 );
			
			//addBudgetBounds();
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
		
		int count;
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
		        	
//					//added
//					count = 0;
//			        for(k=0; k<sysLength; k++)
//			        	if(systemFunctionalityMatrix[k][i] == 1)
//			        		count++;
//		
//					int[] colno = new int[count];
//			        double[] row = new double[count];
//			        index = 0;
//			        for(k=0; k<sysLength; k++) {
//			        	if(systemFunctionalityMatrix[k][i] == 1) {
//				        	colno[index] = k * siteLength + j+1;
//				        	row[index] = 1.0;
//				        	index++;
//			        	}
//			        }
////					solver.setRowex(currRowInd, count, row, colno);
////					solver.setRh(currRowInd, 1);
////					solver.setConstrType(currRowInd, LpSolve.GE);
////					currRowInd++;
//
//		        	solver.addConstraintex(count, row, colno, LpSolve.GE,1);
//					//added

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

//				solver.setRowex(currRowInd, 2, row, colno);
//				solver.setRh(currRowInd, 0);
//				solver.setConstrType(currRowInd, LpSolve.GE);
//				currRowInd++;
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
//		int i;
//		int j;
//
//		int count = 0;
//        for(i=0; i<sysLength; i++) {
//			for(j=0; j<siteLength; j++) {
//				if(systemSiteMatrix[i][j]==0) {
//					count++;
//				}
//			}
//        }
//        
//        int index = 0;
//		int[] colno = new int[count];
//        double[] row = new double[count];
//        for(i=0; i<sysLength; i++) {
//			for(j=0; j<siteLength; j++) {
//				if(systemSiteMatrix[i][j]==0) {
//					colno[index] = i * siteLength + j +1 ;
//				    row[index] = siteDeploymentCosts[i] / scalingFactor;
//				    index++;
//				}
//			}
//        }
//    	solver.addConstraintex(count, row, colno, LpSolve.LE, maxBudget/scalingFactor);

		int i;
		int j;
		int index = 0;
		
		int[] colno = new int[numNotCentralSystems * siteLength];
        double[] row = new double[numNotCentralSystems * siteLength];
        for(i=0; i<numNotCentralSystems; i++) {
			for(j=0; j<siteLength; j++) {
				if(systemSiteMatrix[i][j]==0) {
					
//					if(siteDeploymentCosts[i] <= maxBudget) {
						colno[index] = i * siteLength + j +1;
						row[index] = siteDeploymentCosts[i];
						index++;
//					}
				}
			}
        }

    	solver.addConstraintex(numNotCentralSystems * siteLength + numNotCentralSystems + numCentralSystems, row, colno, LpSolve.LE, maxBudget);
    	budgetRow = solver.getNorigRows();

//		solver.setRowex(currRowInd, sysLength * siteLength, row, colno);
//		solver.setRh(currRowInd, maxBudget/scalingFactor);
//		solver.setConstrType(currRowInd, LpSolve.LE);
//		currRowInd++;

	}
	
//	private void updateBounds() throws LpSolveException{
//		int i;
//		int j;
//		int oldIndex = 0;
//		int newIndex = 0;
//		
//        for(i=0; i<numNotCentralSystems; i++) {
//			for(j=0; j<siteLength; j++) {
//				oldIndex = i * siteLength + j +1;
//				newIndex = solver.getLpIndex(oldIndex);
//				System.out.println("old var " + oldIndex + " new index " + newIndex + ": lower " + solver.getLowbo(newIndex)+ " upper " + solver.getUpbo(newIndex));
//				if(newIndex !=0)
//					solver.setBounds(newIndex, 0, 1);
//			}
//			oldIndex = numNotCentralSystems * siteLength + i + 1;
//			newIndex = solver.getLpIndex(oldIndex);
//			System.out.println("old var " + oldIndex + " new index " + newIndex + ": lower " + solver.getLowbo(newIndex)+ " upper " + solver.getUpbo(newIndex));
//			if(newIndex !=0)
//				solver.setBounds(newIndex, 0, 1);
//         }
//        
//        for(i=0; i<numCentralSystems; i++) {
//
//			oldIndex = numNotCentralSystems * (siteLength + 1) + i + 1;
//			newIndex = solver.getLpIndex(oldIndex);
//			System.out.println("old var " + oldIndex + " new index " + newIndex + ": lower " + solver.getLowbo(newIndex)+ " upper " + solver.getUpbo(newIndex));
//			if(newIndex !=0)
//				solver.setBounds(newIndex, 0, 1);
//        }
//        addBudgetBounds();
//        addModDecomBounds();
//
//	}
	
//	private void addBudgetBounds() throws LpSolveException{
//		int i;
//		int j;
//		
//        for(i=0; i<numNotCentralSystems; i++) {
//        	if(siteDeploymentCosts[i] > maxBudget) {
//				for(j=0; j<siteLength; j++) {
//					if(systemSiteMatrix[i][j]==0) {
//						solver.setUpbo(i * siteLength + j +1, 0);//cost to deploy is greater than budget, 0 for all sites system is not currently at
//					}
//				}
//        	}
//        }
//	}
	
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
			
//			System.out.println("orig rows" + solver.getNorigRows());
			
			solver.writeLp("model.lp");
//			System.out.println("row index " + currRowInd);

//			System.out.println("Presolve Loops..." + solver.getPresolveloops());
//			
			solver.setScaling(LpSolve.SCALE_EXTREME | LpSolve.SCALE_QUADRATIC | LpSolve.SCALE_INTEGERS);
//			solver.setScaling(LpSolve.SCALE_GEOMETRIC | LpSolve.SCALE_EQUILIBRATE | LpSolve.SCALE_INTEGERS);
////			solver.setScaling(LpSolve.SCALE_GEOMETRIC | LpSolve.SCALE_QUADRATIC | LpSolve.SCALE_INTEGERS);
//
			solver.setPresolve(LpSolve.PRESOLVE_COLS | LpSolve.PRESOLVE_ROWS | LpSolve.PRESOLVE_PROBEFIX | LpSolve.PRESOLVE_LINDEP  | LpSolve.PRESOLVE_PROBEREDUCE , solver.getPresolveloops());

//			solver.setPresolve(LpSolve.PRESOLVE_COLS | LpSolve.PRESOLVE_ROWS | LpSolve.PRESOLVE_PROBEFIX | LpSolve.PRESOLVE_LINDEP | LpSolve.PRESOLVE_ELIMEQ2, solver.getPresolveloops());
//			solver.setPresolve(solver.PRESOLVE_COLS | solver.PRESOLVE_ROWS | solver.PRESOLVE_LINDEP | solver.PRESOLVE_SOS | solver.PRESOLVE_REDUCEMIP | solver.PRESOLVE_KNAPSACK | solver.PRESOLVE_ELIMEQ2 |  solver.PRESOLVE_IMPLIEDFREE | solver.PRESOLVE_REDUCEGCD | solver.PRESOLVE_PROBEFIX | solver.PRESOLVE_PROBEREDUCE | solver.PRESOLVE_ROWDOMINATE | solver.PRESOLVE_COLDOMINATE | solver.PRESOLVE_MERGEROWS | solver.PRESOLVE_COLFIXDUAL | solver.PRESOLVE_BOUNDS | solver.PRESOLVE_DUALS | solver.PRESOLVE_SENSDUALS, solver.getPresolveloops());
//
//			solver.setPivoting(LpSolve.PRICER_DANTZIG | LpSolve.PRICE_PARTIAL);
//			solver.setPivoting(LpSolve.PRICER_DANTZIG | LpSolve.PRICE_ADAPTIVE );
//			
//			solver.setAntiDegen(LpSolve.ANTIDEGEN_STALLING | LpSolve.ANTIDEGEN_FIXEDVARS);
//			solver.setBbFloorfirst(LpSolve.BRANCH_AUTOMATIC);
//			solver.setImprove(LpSolve.IMPROVE_DUALFEAS | LpSolve.IMPROVE_THETAGAP | LpSolve.IMPROVE_BBSIMPLEX);
//			solver.guessBasis();
//			solver.setBasis(), arg1);
//			solver.setEpsb(1.0e-5);
//			solver.setPreferdual(true);
//
			
			solver.setBbDepthlimit(-1);
			solver.setMipGap(true,maxBudget);

			System.out.println("Starting execute...");
			super.execute();
			System.out.println("Finished execute...");
			
			if(solved == LpSolve.SUBOPTIMAL)
				LOGGER.error("SOLUTION IS SUBOPTIMAL");
			if(solved == LpSolve.TIMEOUT)
				LOGGER.error("SOLUTION TIMED OUT");
			if(solved != 0)
 				LOGGER.error("SOLVED IS "+solved);
			
			System.out.println("orig cols " + solver.getNorigColumns() + " now cols "+solver.getNcolumns());
			System.out.println("orig rows " + solver.getNorigRows() + " now rows "+solver.getNrows());

			objectiveVal = solver.getObjective();
			
			budgetRow = solver.getLpIndex(budgetRow);
			
			int i;
			int j;

			int nConstraints = solver.getNorigRows();
//			System.out.println("row index " + currRowInd);
//			System.out.println("orig cols " + solver.getNorigColumns() + " now cols "+solver.getNcolumns());
//			System.out.println("orig rows " + solver.getNorigRows() + " now rows "+solver.getNrows());

			
			systemSiteResultMatrix = new double[numNotCentralSystems][siteLength];
			sysKeptArr = new double[numNotCentralSystems];
			centralSysKeptArr = new double[numCentralSystems];
			
			int index = 0;
			for(i = 0; i < numNotCentralSystems; i++ ) {
				for(j = 0; j < siteLength; j++) {
					systemSiteResultMatrix[i][j] = solver.getVarPrimalresult(nConstraints + index + 1);
					index++;
				}
			}
			
			for(i = 0; i < numNotCentralSystems; i++ ) {
				sysKeptArr[i] = solver.getVarPrimalresult(nConstraints + index + 1);
				index++;
			}			
			
			for(i = 0; i < numCentralSystems; i++ ) {
				centralSysKeptArr[i] = solver.getVarPrimalresult(nConstraints + index + 1);
				index++;
			}
			
			totalDeploymentCost = 0.0;
	        for(i=0; i<numNotCentralSystems; i++) {
				for(j=0; j<siteLength; j++) {
					if(systemSiteMatrix[i][j] == 0 && systemSiteResultMatrix[i][j] == 1) {
						totalDeploymentCost += siteDeploymentCosts[i];
					}
				}
	        }
			
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
	
	public Boolean isOptimalSolution() {
		return solved == 0;
	}
}

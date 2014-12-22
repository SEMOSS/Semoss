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
import java.util.Hashtable;

import lpsolve.LpSolve;
import lpsolve.LpSolveException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.impl.LPOptimizer;

	public class ResidualSystemBudgetOptMaxSavings extends LPOptimizer{
		
		protected static final Logger logger = LogManager.getLogger(ResidualSystemBudgetOptMaxSavings.class.getName());
		ArrayList<String> sysList;
		ArrayList<String> dataList;
		ArrayList<String> bluList;
		
		//a_ip, b_iq, c_ip
		int[][] systemDataMatrix;
		int[][] systemBLUMatrix;
		double[][] systemCostOfDataMatrix;
		
		//cM_i, cDM_i, s_i
		double[] systemCostOfMaintenance;
		double[] systemCostOfDB;
		double[] systemNumOfSites;
		
		//Ap, Bq
		int[] dataSORSystemExists;
		int[] bluProviderExists;
		
		//x_i
		double[] systemIsModernized;
		
		int ret=0;
		double roi=0.0;
		double denomCurrentMaintenance = 0.0;
		double numTransformationTotal = 0.0;
		double numMaintenanceTotal = 0.0;
		double percentOfPilot = 0.20;
		
		public ResidualSystemBudgetOptMaxSavings(){
			
		}
		
		//set data, set variables,set constraints, setobjfunction
		

		/**
		 * Gathers data set.
		 */
		
		public void setDataSet(ArrayList<String> sysList, ArrayList<String> dataList, ArrayList<String> bluList,int[][] systemDataMatrix, int[][] systemBLUMatrix, double[][] systemCostOfDataMatrix, double[] systemCostOfMaintenance, double[] systemCostOfDB, double[] systemNumOfSites, int[] dataSORSystemExists, int[] bluProviderExists) {
			
			this.sysList = sysList;
			this.dataList = dataList;
			this.bluList = bluList;
			
			this.systemDataMatrix=systemDataMatrix;
			this.systemBLUMatrix=systemBLUMatrix;
			this.systemCostOfDataMatrix=systemCostOfDataMatrix;
			
			//cM_i, cDM_i, s_i
			this.systemCostOfMaintenance=systemCostOfMaintenance;
			this.systemCostOfDB=systemCostOfDB;
			this.systemNumOfSites=systemNumOfSites;
			
			//Ap, Bq
			this.dataSORSystemExists=dataSORSystemExists;
			this.bluProviderExists=bluProviderExists;
			
		}
		
		public void setModernizedSystemList(double[] systemIsModernized)
		{
			this.systemIsModernized = systemIsModernized;
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
			addConstraints(systemDataMatrix,dataSORSystemExists);
			addConstraints(systemBLUMatrix,bluProviderExists);
			//rowmode turned off
			solver.setAddRowmode(false);
		}
		
		public void addConstraints(int[][] systemProviderMatrix, int[] constraintMatrix)
		{
			try{
				for(int colInd=0;colInd<systemProviderMatrix[0].length;colInd++)
				{
					int[] colno = new int[systemProviderMatrix.length];
			        double[] row = new double[systemProviderMatrix.length];
		
			        for(int sysInd=0;sysInd<systemProviderMatrix.length;sysInd++)
			        {
			        	colno[sysInd] = sysInd+1;
			        	row[sysInd] = systemProviderMatrix[sysInd][colInd];
			        }
			        solver.addConstraintex(systemProviderMatrix.length, row, colno, LpSolve.GE, constraintMatrix[colInd]);
				}
			}catch (LpSolveException e){
				e.printStackTrace();
			}

		}

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
				if(sysInd>1)
					numTransformation = numTransformation + numTransformation * (numSites-1)*percentOfPilot;
				else
					numTransformation = numTransformation*numSites;
				numTransformationTotal += numTransformation;
				numerator += (numMaintenance - numTransformation);
				denomCurrentMaintenance+=systemCostOfMaintenance[sysInd];
			}
			roi = numerator / denomCurrentMaintenance;
		}
		
		
		public Hashtable runOpt()
		{
			Hashtable returnHash = new Hashtable();
			try {
//				fillSysDataBLULists();
//				gatherDataSet();
				setupModel();
				solver.writeLp("model.lp");
				execute();	
				double minObjectiveVal = solver.getObjective();
				System.out.println("Objective Val is : " + minObjectiveVal);
				
				systemIsModernized = new double[sysList.size()];

				solver.getVariables(systemIsModernized);
				for(int i=0;i<systemIsModernized.length;i++)
					if(systemIsModernized[i]>0)
						System.out.print(solver.getColName(i + 1)+", ");
				
				calculateROI();

				System.out.println("Yearly maintenance if continued as-is: "+denomCurrentMaintenance);
				System.out.println("Yearly maintenance of modernized systems only: "+(denomCurrentMaintenance-numMaintenanceTotal));
				System.out.println("One Time Cost to transform data for all systems: "+numTransformationTotal);
				System.out.println("Break Even point in years: "+numTransformationTotal/numMaintenanceTotal);

				for(int i=1;i<=10;i++)
					System.out.println("Year "+i+" ROI is "+(numMaintenanceTotal-(numTransformationTotal/i))/denomCurrentMaintenance);
				
				deleteModel();
				solver=null;
				
			} catch (LpSolveException e) {
				e.printStackTrace();
			}
				
			return returnHash;
		}
		
		public double[] getModernizedSystems()
		{
			return systemIsModernized;
		}

}


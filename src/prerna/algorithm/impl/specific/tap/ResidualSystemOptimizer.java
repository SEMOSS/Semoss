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
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.specific.tap.SysOptPlaySheet;

public class ResidualSystemOptimizer extends LPOptimizer{
	
	SysOptPlaySheet playSheet=null;
	
	protected static final Logger logger = LogManager.getLogger(ResidualSystemOptimizer.class.getName());
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
	double[] systemModernize;
	double[] systemDecommission;
	
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
	
	public void setPlaySheet(IPlaySheet playSheet)
	{
		this.playSheet = (SysOptPlaySheet)playSheet;
	}

	/**
	 * Gathers data set.
	 */
	public void setDataSet(ArrayList<String> sysList, ArrayList<String> dataList, ArrayList<String> bluList,ArrayList<String> regionList,int[][] systemDataMatrix, int[][] systemBLUMatrix, double[][] systemCostOfDataMatrix, int[][] systemRegionMatrix, double[] systemCostOfMaintenance, double[] systemCostOfDB, double[] systemNumOfSites, double[] systemModernize, double[] systemDecommission,int[][] dataRegionSORSystemExists, int[][] bluRegionProviderExists) {
		
		this.sysList = sysList;
		this.dataList = dataList;
		this.bluList = bluList;
		this.regionList = regionList;
		
		this.systemDataMatrix=systemDataMatrix;
		this.systemBLUMatrix=systemBLUMatrix;
		this.systemCostOfDataMatrix=systemCostOfDataMatrix;
		this.systemRegionMatrix = systemRegionMatrix;
		
		//cM_i, cDM_i, s_i
		this.systemCostOfMaintenance=systemCostOfMaintenance;
		this.systemCostOfDB=systemCostOfDB;
		this.systemNumOfSites=systemNumOfSites;
		this.systemModernize = systemModernize;
		this.systemDecommission = systemDecommission;
		
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
		addRequiredSystemsConstraint(systemModernize);
		addDecommissionSystemsConstraint(systemDecommission);
		addConstraints(systemDataMatrix,systemRegionMatrix,dataRegionSORSystemExists);
		addConstraints(systemBLUMatrix,systemRegionMatrix,bluRegionProviderExists);
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
	public void addDecommissionSystemsConstraint(double[] systemRequired)
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
			        solver.addConstraintex(systemRequired.length, row,colno,LpSolve.LE,0);
				}
			}
		}catch (LpSolveException e){
			e.printStackTrace();
		}
	}
	
	private void addConstraints(int[][] systemProviderMatrix, int[][] systemRegionMatrix, int[][] constraintMatrix)
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
			logger.info("Objective Val is : " + minObjectiveVal);
			
			if(minObjectiveVal>1.0*Math.pow(10, 15))
			{
				errorMessage = "No solution can be found for given systems, data, and blu. Please modify the data and blu selected or the manually set systems for modernization and decommission.";
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
				logger.info("Systems to be Modernized");
				String logOut = "";
				for(int i=0;i<systemIsModernized.length;i++)
					if(systemIsModernized[i]>0)
						logOut+=solver.getColName(i + 1)+", ";
				logger.info(logOut);
				logger.info("Systems that will not be Modernized");
				logOut = "";
				for(int i=0;i<systemIsModernized.length;i++)
					if(systemIsModernized[i]<1)
						logOut+=solver.getColName(i + 1)+", ";
				logger.info(logOut);
				logger.info("Yearly maintenance if continued as-is: "+denomCurrentMaintenance);
				logger.info("Yearly maintenance of modernized systems only: "+(denomCurrentMaintenance-numMaintenanceTotal));
				logger.info("One Time Cost to transform data for all systems: "+numTransformationTotal);
				logger.info("Break Even point in years: "+numTransformationTotal/numMaintenanceTotal);
				for(int i=1;i<=10;i++)
					logger.info("Year "+i+" ROI is "+(numMaintenanceTotal-(numTransformationTotal/i))/denomCurrentMaintenance);
			}
			deleteModel();
			solver=null;
			
		} catch (LpSolveException e) {
			e.printStackTrace();
			return false;
		}
			
		return true;
	}
	public int countModernized() {
		int numModernized=0;
		for(int i=0;i<systemIsModernized.length;i++) {
			if(systemIsModernized[i]>0)
				numModernized++;
		}
		return numModernized;
	}
	
	public ArrayList<Integer> getModernizedIndicies() {
		ArrayList<Integer> indicies = new ArrayList<Integer>();
		for(int i=0;i<systemIsModernized.length;i++) {
			if(systemIsModernized[i]>0)
				indicies.add(i);
		}
		return indicies;
	}
	
	public ArrayList<Integer> getDecommissionedIndicies() {
		ArrayList<Integer> indicies = new ArrayList<Integer>();
		for(int i=0;i<systemIsModernized.length;i++) {
			if(systemIsModernized[i]<1)
				indicies.add(i);
		}
		return indicies;
	}
}

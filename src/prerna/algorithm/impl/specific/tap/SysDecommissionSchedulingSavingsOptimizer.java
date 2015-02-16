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
import java.util.Hashtable;

import javax.swing.JTextArea;

import lpsolve.LpSolve;
import lpsolve.LpSolveException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.impl.LPOptimizer;

	public class SysDecommissionSchedulingSavingsOptimizer extends LPOptimizer{
		
		protected static final Logger logger = LogManager.getLogger(SysDecommissionSchedulingSavingsOptimizer.class.getName());
		
		double budget;
		int numYears;
		double percentOfPilot;
		double serMainPerc;
		double numBudgetYearData = 5;
		
		ArrayList<String> sysList;
		ArrayList<String> sysListLeftOver;
		
		//max number of sites that may be transformed during year t
		Hashtable<String,Integer> sysToSiteCount;
		Hashtable<String,Integer> sysToSiteCountLeftOver;
		Hashtable<String, Double> sysToWorkVolHashPerSite;
//	    Hashtable<String, Double> sysToSustainmentCost;
	    Hashtable<String, Double[]> sysToBudget;

		int currYear;
		int totalInvestment;
//		int totalSavings;
		double[] siteIsTransformed;
		
		//matrix of yit, where system i is row and year t is col
		//1 if first site for system is transformed in year t
		ArrayList<Double[]> sysNumSitesMatrix;
		ArrayList<Double[]> sysInvestCostMatrix;
		ArrayList<Double[]> sysSavingsMatrix;

		ArrayList<Double> yearInvestment;
//		ArrayList<Double> yearInvestmentSustainment;
		ArrayList<Double> yearSavings;
//		ArrayList<Double> cummulativeYearSavings;

		JTextArea consoleArea;
		int ret=0;
		
		public SysDecommissionSchedulingSavingsOptimizer(){
			
		}
		
		//set data, set variables,set constraints, setobjfunction
		

		/**
		 * Gathers data set.
		 */
		public void setDataSet(JTextArea consoleArea,ArrayList<String> sysList,Hashtable<String,Integer> sysToSiteCount,Hashtable<String, Double> sysToWorkVolHashPerSite,Hashtable<String, Double[]> sysToBudget,double budget, int numYears, double percentOfPilot,double serMainPerc) {
			this.consoleArea = consoleArea;
			this.sysList = sysList;
			this.sysToSiteCount = sysToSiteCount;
			this.sysToWorkVolHashPerSite = sysToWorkVolHashPerSite;
		    this.sysToBudget = sysToBudget;
			this.budget = budget;
			this.numYears = numYears;
			this.percentOfPilot = percentOfPilot;
			this.serMainPerc = serMainPerc;
		}
		
		public void setBudget(double budget) {
			this.budget = budget;
		}
		
		public double getSerMainPerc() {
			return serMainPerc;
		}
		public int getTotalInvestment() {
			return totalInvestment;
		}

		public ArrayList<Double[]> getSysNumSitesMatrix(){
			return sysNumSitesMatrix;
		}
		public ArrayList<Double[]> getSysInvestCostMatrix(){
			return sysInvestCostMatrix;
		}
		public ArrayList<Double[]> getSysSavingsMatrix(){
			return sysSavingsMatrix;
		}
		
		
		public ArrayList<Double> getYearInvestment(){
			return yearInvestment;
		}
		public ArrayList<Double> getYearSavings(){
			return yearSavings;
		}

		public void addTextToConsole(String text) {
			consoleArea.setText(consoleArea.getText()+text);
		}
		
		/**
		 * Sets variables in model.
		 */
		@Override
		public void setVariables() throws LpSolveException {
			//variable for every y and x (for a given year)
			solver = LpSolve.makeLp(0, sysListLeftOver.size());
			
			if(solver.getLp()==0) {
				logger.info("Couldn't construct a new model");
				ret=1;
			}
				
			if(ret == 0) {
		        /* let us name our variables. Not required, but can be useful for debugging */
				for(int sysInd=0;sysInd<sysListLeftOver.size();sysInd++)
				{
					String sysName = "";
					sysName = sysListLeftOver.get(sysInd);
					solver.setColName(sysInd+1, sysName);
					solver.setInt(sysInd+1,true);
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
			addBudgetConstraint();
			addSiteConstraints();
			addBoundsConstraints();
			//rowmode turned off
			solver.setAddRowmode(false);
		}
		
		public void addBudgetConstraint()
		{
			try{
				int[] colno = new int[sysListLeftOver.size()];
		        double[] row = new double[sysListLeftOver.size()];
		        
				for(int sysInd=0;sysInd<sysListLeftOver.size();sysInd++) {	
			        colno[sysInd] = sysInd+1;
			        String sys = sysListLeftOver.get(sysInd);
			        double workVol = sysToWorkVolHashPerSite.get(sys);
			        row[sysInd] = ( workVol + ((sysToSiteCount.get(sys) - 1) * workVol * percentOfPilot )) / sysToSiteCount.get(sys);
				}

			    solver.addConstraintex(sysListLeftOver.size(), row, colno, LpSolve.LE, budget);
			    
			}catch (LpSolveException e){
				e.printStackTrace();
			}
		}
		
		public void addSiteConstraints()
		{
			try{
				for(int sysInd=0;sysInd<sysListLeftOver.size();sysInd++) {
					String sys = sysListLeftOver.get(sysInd);
					int[] colno = new int[sysListLeftOver.size()];
			        double[] row = new double[sysListLeftOver.size()];
			        
					for(int ind=0;ind<sysListLeftOver.size();ind++) {	
				        colno[ind] = ind+1;
				        row[ind] = 0;
					}
					
					row[sysInd] = 1;
				    solver.addConstraintex(sysListLeftOver.size(), row, colno, LpSolve.LE, sysToSiteCountLeftOver.get(sys));
				}
			}catch (LpSolveException e){
				e.printStackTrace();
			}
		}
		public void addBoundsConstraints()
		{
			try{
				for(int sysInd=0;sysInd<sysListLeftOver.size();sysInd++) {
					int[] colno = new int[sysListLeftOver.size()];
			        double[] row = new double[sysListLeftOver.size()];
			        
					for(int ind=0;ind<sysListLeftOver.size();ind++) {	
				        colno[ind] = ind+1;
				        row[ind] = 0;
					}
					
					row[sysInd] = 1;
				    solver.addConstraintex(sysListLeftOver.size(), row, colno, LpSolve.GE, 0);
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
				int[] colno = new int[sysListLeftOver.size()];
		        double[] row = new double[sysListLeftOver.size()];

				for(int sysInd=0;sysInd<sysListLeftOver.size();sysInd++) {
					String sys = sysListLeftOver.get(sysInd);
		        	colno[sysInd] = sysInd+1;
			        double workVol = sysToWorkVolHashPerSite.get(sys);
			        double sysBudget = 0.0;
			        if(currYear<sysToBudget.get(sys).length)
			        	sysBudget = sysToBudget.get(sys)[currYear];
			        else
			        	sysBudget = sysToBudget.get(sys)[sysToBudget.get(sys).length-1];
			        double numerator = (sysBudget - serMainPerc*( workVol + ((sysToSiteCount.get(sys) - 1) * workVol * percentOfPilot )));
			        if(numerator<0)
			        	row[sysInd] = -1* numerator / sysToSiteCount.get(sys);
			        else
			        	row[sysInd] =  numerator / sysToSiteCount.get(sys);
			        	
		        }

		        solver.setObjFnex(sysListLeftOver.size(), row, colno);
				solver.setMaxim();
			}catch (LpSolveException e){
				e.printStackTrace();
			}
		}
		
		public double calcInvestmentForSystemForCurrYear() {
			
			addTextToConsole("\nNumber of sites transformed and cost for each system are:");
			double invest=0.0;
			for(int sysInd=0;sysInd<sysListLeftOver.size();sysInd++) {
				String sys = sysListLeftOver.get(sysInd);
				int sysMasterInd = sysList.indexOf(sys);
				double workVol = sysToWorkVolHashPerSite.get(sys);
				double sites = sysToSiteCount.get(sys);
				double sysInvest = ( workVol + ((sites - 1) * workVol * percentOfPilot )) / sites * sysNumSitesMatrix.get(currYear)[sysMasterInd];
				sysInvestCostMatrix.get(currYear)[sysMasterInd] = sysInvest;
				addTextToConsole(" "+sys+": for "+sysNumSitesMatrix.get(currYear)[sysMasterInd]+" sites the cost is "+sysInvest+",");
		        invest +=  sysInvest;
			}
			return invest;
		}
		
		public double calcSavingsForSystemForCurrYear() {
			double savings=0.0;
			for(int sysInd=0;sysInd<sysList.size();sysInd++)
			{
				String sys = sysList.get(sysInd);
				//int sysMasterInd = sysList.indexOf(sys);
		        double workVol = sysToWorkVolHashPerSite.get(sys);
		        double sysBudget = 0.0;
		        if(currYear<sysToBudget.get(sys).length)
		        	sysBudget = sysToBudget.get(sys)[currYear];
		        else
		        	sysBudget = sysToBudget.get(sys)[sysToBudget.get(sys).length-1];
		        double numSitesTransformed = 0.0;
		        for(int i=0;i<currYear;i++)
		        	numSitesTransformed+=sysNumSitesMatrix.get(i)[sysInd];
		        double sysSavings = (sysBudget - serMainPerc*( workVol + ((sysToSiteCount.get(sys) - 1) * workVol * percentOfPilot )))/sysToSiteCount.get(sys)*numSitesTransformed;
				sysSavingsMatrix.get(currYear)[sysInd] = sysSavings;
		        savings += sysSavings;
			}
			return savings;
		}
		
		public void adjustSitesFromPrevYear() {
			ArrayList<String> sysToRemove = new ArrayList<String>();
			for(int sysInd=0;sysInd<sysListLeftOver.size();sysInd++)
			{
				String sys = sysListLeftOver.get(sysInd);
				int sysMasterInd = sysList.indexOf(sys);
				double sites = sysNumSitesMatrix.get(currYear-1)[sysMasterInd];
				int sitesAsInt = (int)sites;
				int prevSitesLeft = sysToSiteCountLeftOver.get(sys);
				if(sitesAsInt>0) {
					sysToSiteCountLeftOver.put(sys, prevSitesLeft - sitesAsInt);
				}
				if(prevSitesLeft - sitesAsInt==0)
					sysToRemove.add(sys);
			}
			for(String sys : sysToRemove)
				sysListLeftOver.remove(sys);
		}
		
		public int runScheduling()
		{
			currYear = 0;
			totalInvestment = 0;
			
			sysListLeftOver = deepCopy(sysList);
			
			sysNumSitesMatrix = new ArrayList<Double[]>();//createMatrix(sysList.size(),numYears);
			sysNumSitesMatrix.add(createArray(sysList.size()));

			sysInvestCostMatrix = new ArrayList<Double[]>();//createMatrix(sysList.size(),numYears);
			sysInvestCostMatrix.add(createArray(sysList.size()));
			
			sysSavingsMatrix = new ArrayList<Double[]>();//createMatrix(sysList.size(),numYears);
			sysSavingsMatrix.add(createArray(sysList.size()));
			
			sysToSiteCountLeftOver = new Hashtable<String,Integer>();
			
			yearInvestment = new ArrayList<Double>();
			yearSavings = new ArrayList<Double>();
			//do i need to remove any systems that dont have site data? right now assuming one site
			
			for(String sys : sysList)
			{
				if(!sysToSiteCount.containsKey(sys))
					sysToSiteCount.put(sys,1);
				if(!sysToWorkVolHashPerSite.containsKey(sys))
					sysToWorkVolHashPerSite.put(sys,0.0);
				if(!sysToBudget.containsKey(sys))
				{
					Double[] fyBudgetRow = new Double[]{0.1,0.1,0.1,0.1,0.1};
					sysToBudget.put(sys,fyBudgetRow);
				}
				for(int i=0;i<numBudgetYearData;i++)
				{
					if(sysToBudget.get(sys)[i]==null)
						sysToBudget.get(sys)[i]=0.1;
					if(sysToBudget.get(sys)[i]==0.0)
						sysToBudget.get(sys)[i] = 0.1;
				}
				
				sysToSiteCountLeftOver.put(sys, sysToSiteCount.get(sys));
			}
			
			while(currYear<numYears)
			{
				runOpt();
				double investment = calcInvestmentForSystemForCurrYear();
				totalInvestment +=investment;
				yearInvestment.add(investment);
				double savings = calcSavingsForSystemForCurrYear();
				yearSavings.add(savings);
				currYear++;
				adjustSitesFromPrevYear();
				if(allSitesTransformed())
					return currYear;
				sysNumSitesMatrix.add(createArray(sysList.size()));
				sysInvestCostMatrix.add(createArray(sysList.size()));
				sysSavingsMatrix.add(createArray(sysList.size()));
			}
			currYear++;
			currYear++;
			return currYear;
		}
		
		public boolean allSitesTransformed()
		{
			for(String sys : sysToSiteCountLeftOver.keySet())
				if(sysToSiteCountLeftOver.get(sys)>0)
					return false;
			return true;
		}
		
		public Double[] createArray(int rows)
		{
			Double[] matrix = new Double[rows];
			for(int row=0;row<rows;row++)
					matrix[row] = 0.0;
			return matrix;
		}
		public void runOpt()
		{
			try {
				gatherDataSet();
				setupModel();
				solver.writeLp("model.lp");
				execute();
				double maxObjectiveVal = solver.getObjective();
				addTextToConsole("\nYear is: "+(currYear+1));
				addTextToConsole("\nObjective Val is: " + maxObjectiveVal);
				siteIsTransformed = new double[sysListLeftOver.size()];

				solver.getVariables(siteIsTransformed);
				for(int i=0;i<sysListLeftOver.size();i++)
				{
					String sys = sysListLeftOver.get(i);
					int sysMasterInd = sysList.indexOf(sys);
					sysNumSitesMatrix.get(currYear)[sysMasterInd] = siteIsTransformed[i];
				}
						
				deleteModel();
				solver=null;
				
			} catch (LpSolveException e) {
				e.printStackTrace();
			}

		}
		
		public ArrayList<String> deepCopy(ArrayList<String> list)
		{
			ArrayList<String> copy = new ArrayList<String>();
			for(String entry : list)
				copy.add(entry);
			return copy;
		}

}


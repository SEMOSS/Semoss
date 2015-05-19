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
package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;
import java.util.ArrayList;

import javax.swing.JComponent;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.impl.specific.tap.SysOptUtilityMethods;
import prerna.algorithm.impl.specific.tap.SysSiteOptimizer;
import prerna.engine.api.IEngine;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.specific.tap.SysSiteOptPlaySheet;
import prerna.ui.helpers.AlgorithmRunner;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SysSiteOptBtnListener implements IChakraListener {
	
	private static final Logger LOGGER = LogManager.getLogger(SysOptBtnListener.class.getName());
	private SysSiteOptPlaySheet playSheet;
	
	private int maxYears;
	private double maxBudget;
	private double infRate, disRate;
	private double trainingPerc;
	private double hourlyRate;
	private double relConvergence, absConvergence;
	private int noOfPts;
	private SysSiteOptimizer optimizer;
	
	/**
	 * Method actionPerformed.
	 * @param arg0 ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		LOGGER.info("Optimization Button Pushed");
		boolean validInputs = setVariables();
		
		if (validInputs)
		{	
			IEngine siteEngine = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Site_Data");
			IEngine costEngine = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Cost_Data");
			
			if(siteEngine == null || costEngine == null) {
				Utility.showError("Cannot find required databases. Make sure you have all: " + "TAP_Cost_Data and TAP_Site_Data");
				return;
			}
			
			ArrayList<String> sysList = playSheet.sysSelectPanel.getSelectedSystems();
			ArrayList<String> sysModList = playSheet.systemModernizePanel.getSelectedSystems();
			ArrayList<String> sysDecomList = playSheet.systemDecomissionPanel.getSelectedSystems();
			
			//check to make sure user did not put the same system in the mod list and the decom list... may not be issue on web depending how UI is implemented
			ArrayList<String> duplicates = SysOptUtilityMethods.inBothLists(sysModList, sysDecomList);
			if (!duplicates.isEmpty()) {
				Utility.showError("There is at least one system on the manually modernize and manually decommission. Please resolve the lists for systems: " + duplicates.toString());
				return;
			}
			
			//actually running the algorithm
			optimizer = new SysSiteOptimizer();
			optimizer.setEngines(playSheet.engine, costEngine, siteEngine); //likely hr core and tap site
			optimizer.setVariables(maxBudget, maxYears, infRate, disRate, trainingPerc, hourlyRate, noOfPts, relConvergence, absConvergence); //budget per year, the number of years, infl rate, discount rate, training perc, number of points
			optimizer.setUseDHMSMFunctionality(playSheet.useDHMSMFuncCheckbox.isSelected()); //whether the data objects will come from the list of systems or the dhmsm provided capabilities
			optimizer.setOptimizationType(playSheet.getOptType()); //eventually will be Savings, ROI, or IRR
			optimizer.setIsOptimizeBudget(playSheet.optimizeBudgetCheckbox.isSelected()); //true means that we are looking for optimal budget. false means that we are running LPSolve just for the single budget input
			optimizer.setSysList(sysList, sysModList, sysDecomList); //list of all systems to use in analysis, force modernize and force decommision. Decommision is not implemented yet
			optimizer.setPlaySheet(playSheet);

			AlgorithmRunner runner = new AlgorithmRunner(optimizer);
			Thread playThread = new Thread(runner);
			playThread.start();

		}
	}
	
	/**
	 * Method setOptPlaySheet.
	 * @param sheet SerOptPlaySheet
	 */
	public void setOptPlaySheet(SysSiteOptPlaySheet sheet)
	{
		this.playSheet = sheet;
	}
	/**
	 * Method setVariables.
	 * @return boolean 
	 */
	public boolean setVariables()
	{
		int failCount = 0;
		String failStr = "The following inputs are not valid: \n\n";
		
		int userMaxYears = Integer.parseInt(playSheet.yearField.getText());
		if(userMaxYears<1){
			failStr = failStr+"Maximum Number of Years must be greater than 0\n";
			failCount++;
		}
		else 
			this.maxYears = userMaxYears;
		
		//convert to milions
		double userMaxBudget = Double.parseDouble(playSheet.maxBudgetField.getText()) * 1000000;
		if(userMaxBudget<0){
			failStr = failStr+"Maximum Annual Budget must not be negative\n";
			failCount++;
		}
		else 
			this.maxBudget = userMaxBudget;

		int userNoPts = Integer.parseInt(playSheet.startingPtsField.getText());
		if(userNoPts<1 ){
			failStr = failStr+"Number of Starting Points must be greater than 0\n";
			failCount++;
		}
		else 
			this.noOfPts = userNoPts;
		
		//convert to decimal
		double trainingPerc =Double.parseDouble(playSheet.trainingPercField.getText())/100;
		if(trainingPerc<0 || trainingPerc > 1){
			failStr = failStr+"Training Percentage must be between 0 and 100 inclusive\n";
			failCount++;
		}
		else 
			this.trainingPerc = trainingPerc;


		double hourlyRate = Double.parseDouble(playSheet.hourlyRateField.getText());
		if(hourlyRate<1 ){
			failStr = failStr+"Hourly Rate must be greater than 0\n";
			failCount++;
		}
		else 
			this.hourlyRate = hourlyRate;

		//convert to decimal
		this.infRate = Double.parseDouble(playSheet.infRateField.getText())/100;
		this.disRate = Double.parseDouble(playSheet.disRateField.getText())/100;

		//convert to decimal
		this.relConvergence = Double.parseDouble(playSheet.relConvergenceField.getText())/100;
		this.absConvergence = Double.parseDouble(playSheet.absConvergenceField.getText())/100;
		
		if(failCount>0){
			failStr = failStr + "\nPlease adjust the inputs and try again";
			Utility.showError(failStr);
			return false;
		}
		else
			return true;

	}

	/**
	 * Override method from IChakraListener
	 * @param view JComponent
	 */
	@Override
	public void setView(JComponent view) {
		
	}

	
}
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
import javax.swing.JFrame;
import javax.swing.JOptionPane;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.impl.specific.tap.SysIRROptimizer;
import prerna.algorithm.impl.specific.tap.SysNetSavingsOptimizer;
import prerna.algorithm.impl.specific.tap.SysOptUtilityMethods;
import prerna.algorithm.impl.specific.tap.SysROIOptimizer;
import prerna.algorithm.impl.specific.tap.UnivariateSysOptimizer;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.specific.tap.SysOptPlaySheet;
import prerna.ui.helpers.AlgorithmRunner;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SysOptBtnListener implements IChakraListener {
	
	private static final Logger LOGGER = LogManager.getLogger(SysOptBtnListener.class.getName());
	private SysOptPlaySheet playSheet;
	
	private int maxYears;
	private double serMainPerc;
	private int noOfPts;
	private double minBudget;
	private double maxBudget;
	private double hourlyCost;
	private double iniLC;
	private int scdLT;
	private double scdLC;
	private double attRate;
	private double hireRate;
	private double infRate;
	private double disRate;
	private UnivariateSysOptimizer optimizer;
	
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
			ArrayList<String> sysList = playSheet.systemSelectPanel.getSelectedSystems();

			if(sysList.size() < 2) {
				JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
				Object[] buttons = {"Cancel Calculation", "Continue with All Systems"};
				int response = JOptionPane.showOptionDialog(playPane, "At least two systems must be selected to run algorithm. Would you like to proceed with all systems?", 
					"Warning", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE, null, buttons, buttons[1]);

				if (response == 1){
					playSheet.systemSelectPanel.selectAllSystems();
					sysList = playSheet.systemSelectPanel.getSelectedSystems();
					
				}else {
					return;
				}
			}
			
			ArrayList<String> sysModList = playSheet.systemModernizePanel.getSelectedSystems();
			ArrayList<String> sysDecList = playSheet.systemDecomissionPanel.getSelectedSystems();
			
			ArrayList<String> duplicates = SysOptUtilityMethods.inBothLists(sysModList, sysDecList);
			if (!duplicates.isEmpty()) {
				Utility.showError("There is at least one system on the manually modernize and manually decommission. Please resolve the lists for systems: " + duplicates.toString());
				return;
			}

			playSheet.progressBar.setVisible(true);
			playSheet.progressBar.setIndeterminate(true);
			playSheet.progressBar.setStringPainted(true);
			
			if(playSheet.rdbtnProfit.isSelected()) this.optimizer = new SysNetSavingsOptimizer();
			else if(playSheet.rdbtnROI.isSelected()) this.optimizer = new SysROIOptimizer();
			else if(playSheet.rdbtnIRR.isSelected()) this.optimizer = new SysIRROptimizer();

			optimizer.setPlaySheet(playSheet);
			optimizer.setProgressBar(playSheet.progressBar);
			optimizer.setConsoleArea(playSheet.consoleArea);
			optimizer.setVariables(maxYears, 0.0, serMainPerc, attRate,hireRate,infRate, disRate,noOfPts, minBudget,maxBudget,hourlyCost,  iniLC, scdLT, scdLC); //dont need an interface cost so set to 0.0
			optimizer.setSelectDropDowns(sysList, playSheet.systemSelectPanel.isTheaterCheckBoxSelected(), playSheet.systemSelectPanel.isGarrisonCheckBoxSelected(), playSheet.capabilitySelectPanel.getSelectedCapabilities(), playSheet.dataBLUSelectPanel.getSelectedData(), playSheet.dataBLUSelectPanel.getSelectedBLU(), sysModList, sysDecList, playSheet.includeRegionalizationCheckbox.isSelected(), playSheet.garrTheaterCheckbox.isSelected());

			AlgorithmRunner runner = new AlgorithmRunner(optimizer);
			Thread playThread = new Thread(runner);
			playThread.start();

		}
	}
	
	/**
	 * Method setOptPlaySheet.
	 * @param sheet SerOptPlaySheet
	 */
	public void setOptPlaySheet(SysOptPlaySheet sheet)
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
		
		double userSerMainPerc = Double.parseDouble(playSheet.mtnPctgField.getText())/100;
		if(userSerMainPerc<0){
			failStr = failStr+"Annual Service Sustainment Percentage must not be negative\n";
			failCount++;
		}
		else 
			this.serMainPerc = userSerMainPerc;
		
		double userMinBudget = Double.parseDouble(playSheet.minBudgetField.getText());
		if(userMinBudget<0){
			failStr = failStr+"Minimum Annual Budget must not be negative\n";
			failCount++;
		}
		else 
			this.minBudget = userMinBudget;
		
		double userMaxBudget = Double.parseDouble(playSheet.maxBudgetField.getText());
		if(userMaxBudget<0){
			failStr = failStr+"Maximum Annual Budget must not be negative\n";
			failCount++;
		}
		else 
			this.maxBudget = userMaxBudget;
		
		double userHourlyCost =Double.parseDouble(playSheet.hourlyRateField.getText());
		if(userHourlyCost<0){
			failStr = failStr+"Hourly Build Cost Rate must not be negative\n";
			failCount++;
		}
		else 
			this.hourlyCost = userHourlyCost;
		
		if(minBudget>maxBudget){
			failStr = failStr+"Maximum Annual Budget must be greater than or equal to Minimum Annual Budget\n";
			failCount++;
		}
		
		double userAttrition =Double.parseDouble(playSheet.attRateField.getText())/100;
		if(userAttrition<0 || userAttrition > 1){
			failStr = failStr+"Attrition Rate must be between 0 and 100 inclusive\n";
			failCount++;
		}
		else 
			this.attRate = userAttrition;
		
		double userHire =Double.parseDouble(playSheet.hireRateField.getText())/100;
		if(userHire<0){
			failStr = failStr+"Hiring Rate must not be negative\n";
			failCount++;
		}
		else 
			this.hireRate = userHire;
		if(userAttrition-userHire == 1){
			this.hireRate=0.000000000001;
		}
		
		double userInitLC = Double.parseDouble(playSheet.iniLearningCnstField.getText());
		if(userInitLC<0 || userInitLC>=1){
			failStr = failStr+"Experience Level at year 0 must be greater than or equal to 0 and less than 1\n";
			failCount++;
		}
		else 
			this.iniLC = userInitLC;

		double userSecondLC = Double.parseDouble(playSheet.scdLearningCnstField.getText());
		if(userSecondLC<0 || userSecondLC>=1){
			failStr = failStr+"Second Experience Level must be greater than or equal to 0 and less than 1\n";
			failCount++;
		}
		else 
			this.scdLC = userSecondLC;

		int userSecondLT = Integer.parseInt(playSheet.scdLearningTimeField.getText());
		if(userSecondLT<0 ){
			failStr = failStr+"Second Experience Level year must be greater than 0\n";
			failCount++;
		}
		else 
			this.scdLT = userSecondLT;

		int userNoPts = Integer.parseInt(playSheet.startingPtsField.getText());
		if(userNoPts<1 ){
			failStr = failStr+"Number of Starting Points must be greater than 0\n";
			failCount++;
		}
		else 
			this.noOfPts = userNoPts;
		

		this.infRate = Double.parseDouble(playSheet.infRateField.getText())/100;
		this.disRate = Double.parseDouble(playSheet.disRateField.getText())/100;
		
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
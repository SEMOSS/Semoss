/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;
import java.util.ArrayList;

import javax.swing.JComponent;
import javax.swing.JTextArea;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.IAlgorithm;
import prerna.algorithm.impl.specific.tap.SysDecommissionScheduleOptimizer;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.specific.tap.DHMSMSysDecommissionSchedulingPlaySheet;
import prerna.ui.helpers.AlgorithmRunner;
import prerna.util.Utility;

/**
 */
public class DHMSMSysDecomissionSchedulingBtnListener implements IChakraListener {
	
	static final Logger logger = LogManager.getLogger(DHMSMSysDecomissionSchedulingBtnListener.class.getName());

	DHMSMSysDecommissionSchedulingPlaySheet playSheet;
	JTextArea consoleArea;
	
	int maxYears;
	double serMainPerc;
	double minBudget;
	double maxBudget;
	IAlgorithm optimizer;
	
	/**
	 * Method actionPerformed.
	 * @param arg0 ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		logger.info("Optimization Button Pushed");
		boolean validInputs = setVariables();
		if (validInputs)
		{
			this.optimizer = new SysDecommissionScheduleOptimizer();//change type of optimizer
			((SysDecommissionScheduleOptimizer)optimizer).setVariables(maxYears,serMainPerc,minBudget,maxBudget);
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
	public void setOptPlaySheet(DHMSMSysDecommissionSchedulingPlaySheet sheet)
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
				
		failStr = specificSetVariablesString(failStr);
		failCount = specificSetVariablesCount(failCount);
		
		if(failCount>0){
			failStr = failStr + "\nPlease adjust the inputs and try again";
			Utility.showError(failStr);
			return false;
		}
		else
			return true;
	}
	
	public String specificSetVariablesString(String failStr)
	{
		return failStr;
	}
	
	public Integer specificSetVariablesCount(int failCount)
	{
		return failCount;
	}
	
	
	/**
	 * Override method from IChakraListener
	 * @param view JComponent
	 */
	@Override
	public void setView(JComponent view) {
		
	}

}
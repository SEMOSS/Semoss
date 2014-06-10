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

import org.apache.log4j.Logger;

import prerna.algorithm.impl.specific.tap.SysIRROptimizer;
import prerna.algorithm.impl.specific.tap.SysNetSavingsOptimizer;
import prerna.algorithm.impl.specific.tap.SysROIOptimizer;
import prerna.ui.components.specific.tap.SysOptPlaySheet;
import prerna.ui.helpers.AlgorithmRunner;

/**
 */
public class SysOptBtnListener extends SerOptBtnListener {
	
	Logger logger = Logger.getLogger(getClass());
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
			playSheet.progressBar.setVisible(true);
			playSheet.progressBar.setIndeterminate(true);
			playSheet.progressBar.setStringPainted(true);
			playSheet.progressBar.setString("Collecting Data");
			
			if(playSheet.rdbtnProfit.isSelected()) this.optimizer = new SysNetSavingsOptimizer();
			else if(playSheet.rdbtnROI.isSelected()) this.optimizer = new SysROIOptimizer();
			else if(((SysOptPlaySheet)playSheet).rdbtnIRR.isSelected()) this.optimizer = new SysIRROptimizer();

			optimizer.setPlaySheet(playSheet);
			((SysNetSavingsOptimizer)optimizer).setVariables(maxYears, 0.0, serMainPerc, attRate,hireRate,infRate, disRate,noOfPts, minBudget,maxBudget,hourlyCost,  iniLC, scdLT, scdLC); //dont need an interface cost so set to 0.0
			((SysNetSavingsOptimizer)optimizer).setSelectDropDowns(((SysOptPlaySheet)playSheet).sysSelectDropDown,((SysOptPlaySheet)playSheet).capSelectDropDown,((SysOptPlaySheet)playSheet).dataSelectDropDown,((SysOptPlaySheet)playSheet).bluSelectDropDown,((SysOptPlaySheet)playSheet).showSystemSelectBtn.isSelected(),((SysOptPlaySheet)playSheet).updateDataBLUPanelButton.isSelected(),false);

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
	@Override
	public String specificSetVariablesString(String failStr)
	{
		return failStr;
	}
	@Override
	public Integer specificSetVariablesCount(int failCount)
	{
		return failCount;
	}
	
	
	
}
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
package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.impl.specific.tap.SysIRROptimizer;
import prerna.algorithm.impl.specific.tap.SysNetSavingsOptimizer;
import prerna.algorithm.impl.specific.tap.SysROIOptimizer;
import prerna.algorithm.impl.specific.tap.UnivariateSysOptimizer;
import prerna.ui.components.specific.tap.SysOptPlaySheet;
import prerna.ui.helpers.AlgorithmRunner;

/**
 */
public class SysOptBtnListener extends SerOptBtnListener {
	
	static final Logger logger = LogManager.getLogger(SysOptBtnListener.class.getName());
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
			((UnivariateSysOptimizer)optimizer).setVariables(maxYears, 0.0, serMainPerc, attRate,hireRate,infRate, disRate,noOfPts, minBudget,maxBudget,hourlyCost,  iniLC, scdLT, scdLC); //dont need an interface cost so set to 0.0
			((UnivariateSysOptimizer)optimizer).setSelectDropDowns(((SysOptPlaySheet)playSheet).systemSelectPanel,((SysOptPlaySheet)playSheet).capabilitySelectPanel,((SysOptPlaySheet)playSheet).dataBLUSelectPanel,((SysOptPlaySheet)playSheet).systemModernizePanel,((SysOptPlaySheet)playSheet).systemDecomissionPanel,((SysOptPlaySheet)playSheet).includeRegionalizationCheckbox.isSelected(),((SysOptPlaySheet)playSheet).garrTheaterCheckbox.isSelected());

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
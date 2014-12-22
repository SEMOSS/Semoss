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
import java.util.ArrayList;

import javax.swing.JComponent;
import javax.swing.JTextArea;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.IAlgorithm;
import prerna.algorithm.impl.specific.tap.ProfitOptimizer;
import prerna.algorithm.impl.specific.tap.ROIOptimizer;
import prerna.algorithm.impl.specific.tap.RecoupOptimizer;
import prerna.algorithm.impl.specific.tap.UnivariateSvcOptimizer;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.specific.tap.SerOptPlaySheet;
import prerna.ui.helpers.AlgorithmRunner;
import prerna.util.Utility;

/**
 */
public class SerOptBtnListener implements IChakraListener {
	
	static final Logger logger = LogManager.getLogger(SerOptBtnListener.class.getName());

	SerOptPlaySheet playSheet;
	JTextArea consoleArea;
	
	int maxYears;
	double interfaceCost;
	double serMainPerc;
	int noOfPts;
	double minBudget;
	double maxBudget;
	double hourlyCost;
	double iniLC;
	int scdLT;
	double scdLC;
	double attRate;
	double hireRate;
	double infRate;
	double disRate;
	IAlgorithm optimizer;
	
	/**
	 * Method actionPerformed.
	 * @param arg0 ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		logger.info("Optimization Button Pushed");
		boolean validInputs = setVariables();
		if(!playSheet.sysSpecComboBox.getSelectedItem().toString().equals("System Specific") && validInputs){
			if(playSheet.rdbtnProfit.isSelected()) this.optimizer = new ProfitOptimizer();
			else if(playSheet.rdbtnROI.isSelected()) this.optimizer = new ROIOptimizer();
			else if(playSheet.rdbtnBreakeven.isSelected()) this.optimizer = new RecoupOptimizer();
			((UnivariateSvcOptimizer)optimizer).setVariables(maxYears, interfaceCost, serMainPerc, attRate,hireRate,infRate, disRate,noOfPts, minBudget,maxBudget,hourlyCost,  iniLC, scdLT, scdLC);
			optimizer.setPlaySheet(playSheet);
			AlgorithmRunner runner = new AlgorithmRunner(optimizer);
			Thread playThread = new Thread(runner);
			playThread.start();
		}
		else if (validInputs)
		{
			ArrayList sysArrayList =  (ArrayList) playSheet.sysSelect.list.getSelectedValuesList();
			String[] sysArray = (String[]) sysArrayList.toArray(new String[0]);
			if(playSheet.rdbtnProfit.isSelected()) this.optimizer = new ProfitOptimizer();
			else if(playSheet.rdbtnROI.isSelected()) this.optimizer = new ROIOptimizer();
			else if(playSheet.rdbtnBreakeven.isSelected()) this.optimizer = new RecoupOptimizer();
			((UnivariateSvcOptimizer)optimizer).setVariables(maxYears, interfaceCost, serMainPerc, attRate,hireRate,infRate, disRate,noOfPts, minBudget,maxBudget,hourlyCost,  iniLC, scdLT, scdLC);
			optimizer.setPlaySheet(playSheet);
			AlgorithmRunner runner = new AlgorithmRunner(optimizer);
			((UnivariateSvcOptimizer)optimizer).setSystems(sysArray);
			Thread playThread = new Thread(runner);
			playThread.start();
		}

		
	}
		
	/**
	 * Method setOptPlaySheet.
	 * @param sheet SerOptPlaySheet
	 */
	public void setOptPlaySheet(SerOptPlaySheet sheet)
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
		double interfaceCost = Double.parseDouble(playSheet.icdSusField.getText());
		if(interfaceCost<0){
			failStr = failStr+"Annual Interface Sustainment Cost must not be negative\n";
		}
		else 
			this.interfaceCost = interfaceCost;
		return failStr;
	}
	
	public Integer specificSetVariablesCount(int failCount)
	{
		double interfaceCost = Double.parseDouble(playSheet.icdSusField.getText());
		if(interfaceCost<0){
			failCount++;
		}
		else 
			this.interfaceCost = interfaceCost;
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
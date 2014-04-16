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

import org.apache.log4j.Logger;

import prerna.algorithm.impl.specific.tap.SysNetSavingsOptimizer;
import prerna.algorithm.impl.specific.tap.SysROIOptimizer;
import prerna.algorithm.impl.specific.tap.UnivariateSvcOptimizer;
import prerna.ui.components.specific.tap.SysOptPlaySheet;
import prerna.ui.helpers.AlgorithmRunner;

/**
 */
public class SysOptBtnListener extends SerOptBtnListener {
	
	Logger logger = Logger.getLogger(getClass());

	String sysQuery, dataQuery, bluQuery;
	
	public void updateProg()
	{

		playSheet.progressBar.revalidate();
	}
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
			
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nSystem Query...");
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\n"+sysQuery);
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nData Query...");
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\n"+dataQuery);	
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nBLU Query...");
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\n"+bluQuery);
			
			if(playSheet.rdbtnProfit.isSelected()) this.optimizer = new SysNetSavingsOptimizer();
			else if(playSheet.rdbtnROI.isSelected()) this.optimizer = new SysROIOptimizer();

			((SysNetSavingsOptimizer)optimizer).setQueries(sysQuery, dataQuery, bluQuery);
			String selectedCapOption = ((SysOptPlaySheet)playSheet).capSelectComboBox.getSelectedItem().toString();
			if(selectedCapOption.contains("Individual"))
			{
				ArrayList capArrayList =  (ArrayList) ((SysOptPlaySheet)playSheet).capSelect.list.getSelectedValuesList();
				((SysNetSavingsOptimizer)optimizer).addCapBindings(capArrayList);
				//add bindings to data query and blu query based on individually selected
			}
			((UnivariateSvcOptimizer)optimizer).setVariables(maxYears, 0.0, serMainPerc, attRate,hireRate,infRate, disRate,noOfPts, minBudget,maxBudget,hourlyCost,  iniLC, scdLT, scdLC); //dont need an interface cost so set to 0.0
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
	public void setOptPlaySheet(SysOptPlaySheet sheet)
	{
		this.playSheet = sheet;
	}
	
	public String specificSetVariablesString(String failStr)
	{
		String sysQuery = ((SysOptPlaySheet)playSheet).sysSelectQueryField.getText();
		if(!sysQuery.contains("SELECT")){
			failStr = failStr+"System Select Query must be a select query\n";
		}
		else 
			this.sysQuery = sysQuery;
		String dataQuery = ((SysOptPlaySheet)playSheet).dataSelectQueryField.getText();
		if(!dataQuery.contains("SELECT")){
			failStr = failStr+"Data Select Query must be a select query\n";
		}
		else 
			this.dataQuery = dataQuery;
		String bluQuery = ((SysOptPlaySheet)playSheet).bluSelectQueryField.getText();
		if(!bluQuery.contains("SELECT")){
			failStr = failStr+"BLU Select Query must be a select query\n";
		}
		else 
			this.bluQuery = bluQuery;
		return failStr;
	}
	
	public Integer specificSetVariablesCount(int failCount)
	{
		String sysQuery = ((SysOptPlaySheet)playSheet).sysSelectQueryField.getText();
		if(!sysQuery.contains("SELECT")){
			failCount++;
		}
		else 
			this.sysQuery = sysQuery;
		String dataQuery = ((SysOptPlaySheet)playSheet).dataSelectQueryField.getText();
		if(!dataQuery.contains("SELECT")){
			failCount++;
		}
		else 
			this.dataQuery = dataQuery;
		String bluQuery = ((SysOptPlaySheet)playSheet).bluSelectQueryField.getText();
		if(!bluQuery.contains("SELECT")){
			failCount++;
		}
		else 
			this.bluQuery = bluQuery;
		return failCount;
	}
	
	
	
}
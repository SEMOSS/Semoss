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

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.JToggleButton;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.GridScrollPane;
import prerna.ui.components.specific.tap.DHMSMDeploymentStrategyPlaySheet;
import prerna.ui.components.specific.tap.DHMSMIntegrationSavingsPerFiscalYearProcessor;
import prerna.ui.components.specific.tap.SerOptPlaySheet;
import prerna.util.Utility;

/**
 */
public class DHMSMDeploymentStrategySysBarChartListener implements ActionListener {

	private static final Logger LOGGER = LogManager.getLogger(DHMSMDeploymentStrategySysBarChartListener.class.getName());

	private DHMSMDeploymentStrategyPlaySheet ps;
	private JTextArea consoleArea;
	
	private ArrayList<String> selectedSysList;
	private ArrayList<Object []> savingsList;
	private String[] sysSavingsHeaders;

	/**
	 * Method actionPerformed.
	 * @param arg0 ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		LOGGER.info("Create System Bar Chart Button Pushed");
		
		consoleArea = ps.consoleArea;

		//get selected systems and get the overall yearly savings
		selectedSysList = ps.systemSelectBarChartPanel.sysSelectDropDown.getSelectedValues();
		savingsList = ps.getSystemYearlySavings();
		sysSavingsHeaders = ps.getSysSavingsHeaders();
		
		//create whatever filtered structure is needed
		//draw it
		
		Hashtable dataHash = createData();
		ps.sysSavingsChart.callIt(dataHash);
	}
	
	/**
	 * Method setPlaySheet.
	 * @param sheet DHMSMDeploymentStrategyPlaySheet
	 */
	public void setPlaySheet(DHMSMDeploymentStrategyPlaySheet ps)
	{
		this.ps = ps;
	}
	
	public Hashtable createData() {
			int[] xAxisValues = new int[sysSavingsHeaders.length - 2];
			for (int i=1;i<sysSavingsHeaders.length - 1;i++) {
				String year = sysSavingsHeaders[i];
				if(year.contains("FY"))
					year = year.substring(2);
				xAxisValues[i-1]=Integer.parseInt(year);
			}
			Hashtable barChartHash = new Hashtable();
			Hashtable seriesHash = new Hashtable();
			Hashtable colorHash = new Hashtable();
			barChartHash.put("type",  "column");
			barChartHash.put("stack", "normal");
			barChartHash.put("title",  "Decommission savings");
			barChartHash.put("yAxisTitle", "Savings");
			barChartHash.put("xAxisTitle", "Fiscal Year");
			barChartHash.put("xAxis", xAxisValues);
			
			for(String system : selectedSysList) {
				//for each system in our list,
				//find the index of the system
				//create a new double[] for that system
				int sysIndex = findSystem(system);
				if(sysIndex == -1) {
					LOGGER.error("Could not find savings data for system "+system);
					consoleArea.setText(consoleArea.getText()+"\nCould not find savings data for system "+system);
				}
				double[] systemSavings = new double[sysSavingsHeaders.length - 2];
				for(int i=1;i<sysSavingsHeaders.length - 1;i++) {
					String value = (String)savingsList.get(sysIndex)[i];
					if(value.contains("No")) {
						LOGGER.info("No cost info for system "+system);
						consoleArea.setText(consoleArea.getText()+"\nNo cost info for system "+system);
					} else {
						if(value.startsWith("$ "))
							value = value.substring(2);
						value = value.replaceAll(",","");
						value = value.replaceAll("\\*","");
						systemSavings[i-1] = Double.parseDouble(value);
					}
				}
				seriesHash.put(system + " Savings", systemSavings);
			}
			
			//colorHash.put("SOA Service Spending", "#4572A7");
			barChartHash.put("dataSeries",  seriesHash);
			barChartHash.put("colorSeries", colorHash);
			return barChartHash;

	}
	
	public int findSystem(String system) {
		for(int i=0;i<savingsList.size(); i++) {
			Object[] row = savingsList.get(i);
			if(((String)row[0]).equals(system))
				return i;
		}
		return -1;
	}
}
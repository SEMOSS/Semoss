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

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.JToggleButton;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.teamdev.jxbrowser.chromium.JSValue;

import prerna.ui.components.GridScrollPane;
import prerna.ui.components.specific.tap.DHMSMDeploymentStrategyPlaySheet;
import prerna.ui.components.specific.tap.DHMSMDeploymentStrategyProcessor;
import prerna.ui.components.specific.tap.DHMSMIntegrationSavingsPerFiscalYearProcessor;
import prerna.util.Utility;

/**
 */
public class DHMSMDeploymentStrategyRunBtnListener implements ActionListener {

	private static final Logger LOGGER = LogManager.getLogger(DHMSMDeploymentStrategyRunBtnListener.class.getName());

	private static final String YEAR_ERROR = "field is not an integer between 00 and 99, inclusive.";
	private static final String QUARTER_ERROR = "field is not an integer between 1 and 4, inclusive.";
	private static final String NON_INT_ERROR = "field contains a non-integer value.";
	private DHMSMDeploymentStrategyPlaySheet ps;
	private JTextArea consoleArea;

	private Hashtable<String, List<String>> regionWaveHash;
	private ArrayList<String> waveOrder;
	private HashMap<String, String[]> waveStartEndDate;

	/**
	 * Method actionPerformed.
	 * @param arg0 ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		LOGGER.info("Run Deployment Strategy Button Pushed");
		consoleArea = ps.consoleArea;
		DHMSMDeploymentStrategyProcessor deploymentStrategyProcessor = new DHMSMDeploymentStrategyProcessor(regionWaveHash, waveOrder, waveStartEndDate, consoleArea);
		
		JToggleButton selectRegionTimesButton = ps.getSelectRegionTimesButton();
		deploymentStrategyProcessor.setRegionsList(ps.getRegionsList());

		if(!selectRegionTimesButton.isSelected()) {
			//grab the original values for deployment schedule
			int oBeginQuarter = ps.getqBeginDefault();
			int oBeginYear = ps.getyBeginDefault() - 2000;
			int oEndQuarter = ps.getqEndDefault();
			int oEndYear = ps.getyEndDefault() - 2000;

			//pull from begin / end and fill the regions accordingly
			int beginQuarter = getInteger(ps.getQBeginField(), ps.getQBeginField().getName());
			int beginYear = getInteger(ps.getYBeginField(), ps.getYBeginField().getName());
			int endQuarter = getInteger(ps.getQEndField(), ps.getQEndField().getName());
			int endYear = getInteger(ps.getYEndField(), ps.getYEndField().getName());
			if(beginQuarter<0 || beginYear<0 || endQuarter<0 || endYear<0) {
				Utility.showError("Cannot read fields. Please check the Console tab for more information");
				return;
			}
			if(!validQuarter(beginQuarter, ps.getQBeginField().getName()) || !validQuarter(endQuarter, ps.getQEndField().getName()) || !validYear(beginYear, ps.getYBeginField().getName())  || !validYear(endYear, ps.getYEndField().getName()) ) {
				Utility.showError("Cannot read fields. Please check the Console tab for more information");
				return;
			}

			// if no change to deployment values, run default schedule
			if(oBeginQuarter == beginQuarter && oBeginYear == beginYear && oEndQuarter == endQuarter && oEndYear == endYear) {
				LOGGER.info("Using original deployment schedule");
			} else {
				deploymentStrategyProcessor.runDefaultSchedule(beginQuarter, beginYear, endQuarter, endYear);
			}
		} else {
			//grab the original values for the deployment schedule
			Hashtable<String, Integer> oBeginQuarterFieldRegionList = ps.getqBeginDefaultHash();
			Hashtable<String, Integer> oBeginYearFieldRegionList = ps.getyBeginDefaultHash();
			Hashtable<String, Integer> oEndQuarterFieldRegionList = ps.getqEndDefaultHash();
			Hashtable<String, Integer> oEndYearFieldRegionList = ps.getyEndDefaultHash();
			
			//pull from region list
			//check if region textfields are valid
			//add them to list of regions
			Hashtable<String,JTextField> beginQuarterFieldRegionList = ps.getQBeginFieldHash();
			Hashtable<String,JTextField> beginYearFieldRegionList = ps.getYBeginFieldHash();
			Hashtable<String,JTextField> endQuarterFieldRegionList = ps.getQEndFieldHash();
			Hashtable<String,JTextField> endYearFieldRegionList = ps.getYEndFieldHash();
			
			//if no changes to schedule, run default values
			ArrayList<String> regionsList = ps.getRegionsList();
			boolean noChange = true;
			for(int i = 0; i < regionsList.size(); i++) {
				String region = regionsList.get(i);
				int oBeginQuarter = oBeginQuarterFieldRegionList.get(region);
				int oBeginYear = oBeginYearFieldRegionList.get(region) - 2000;
				int oEndQuarter = oEndQuarterFieldRegionList.get(region);
				int oEndYear = oEndYearFieldRegionList.get(region) - 2000;
				
				int beginQuarter = getInteger(beginQuarterFieldRegionList.get(region), beginQuarterFieldRegionList.get(region).getName());
				int beginYear = getInteger(beginYearFieldRegionList.get(region), beginYearFieldRegionList.get(region).getName());
				int endQuarter = getInteger(endQuarterFieldRegionList.get(region), endQuarterFieldRegionList.get(region).getName());
				int endYear = getInteger(endYearFieldRegionList.get(region), endYearFieldRegionList.get(region).getName());
				
				if(beginQuarter<0 || beginYear<0 || endQuarter<0 || endYear<0) {
					Utility.showError("Cannot read fields. Please check the Console tab for more information");
					return;
				}
				if(!validQuarter(beginQuarter, beginQuarterFieldRegionList.get(region).getName()) || !validQuarter(endQuarter, endQuarterFieldRegionList.get(region).getName()) || !validYear(beginYear, endQuarterFieldRegionList.get(region).getName())  || !validYear(endYear, endYearFieldRegionList.get(region).getName()) ) {
					Utility.showError("Cannot read fields. Please check the Console tab for more information");
					return;
				}
				
				if(oBeginQuarter == beginQuarter && oBeginYear == beginYear && oEndQuarter == endQuarter && oEndYear == endYear) {
					// do nothing
				} else {
					noChange = false;
					break;
				}
			}
			
			if(noChange) {
				LOGGER.info("Using original deployment schedule");
			} else {
				for(int i=0;i<regionsList.size();i++) {
					String region = regionsList.get(i);
					int beginQuarter = getInteger(beginQuarterFieldRegionList.get(region), beginQuarterFieldRegionList.get(region).getName());
					int beginYear = getInteger(beginYearFieldRegionList.get(region), beginYearFieldRegionList.get(region).getName());
					int endQuarter = getInteger(endQuarterFieldRegionList.get(region), endQuarterFieldRegionList.get(region).getName());
					int endYear = getInteger(endYearFieldRegionList.get(region), endYearFieldRegionList.get(region).getName());
					if(beginQuarter<0 || beginYear<0 || endQuarter<0 || endYear<0) {
						Utility.showError("Cannot read fields. Please check the Console tab for more information");
						return;
					}
					if(!validQuarter(beginQuarter, beginQuarterFieldRegionList.get(region).getName()) || !validQuarter(endQuarter, endQuarterFieldRegionList.get(region).getName()) || !validYear(beginYear, endQuarterFieldRegionList.get(region).getName())  || !validYear(endYear, endYearFieldRegionList.get(region).getName()) ) {
						Utility.showError("Cannot read fields. Please check the Console tab for more information");
						return;
					}

					deploymentStrategyProcessor.runRegionTimesSchedule(beginQuarter, beginYear, endQuarter, endYear, region);
				}
				
				String region = regionsList.get(regionsList.size() - 1);
				int endYear = getInteger(endYearFieldRegionList.get(region), endYearFieldRegionList.get(region).getName());
				int endQuarter = getInteger(endQuarterFieldRegionList.get(region), endQuarterFieldRegionList.get(region).getName());
				deploymentStrategyProcessor.updateRegionTimesSchedule(endYear, endQuarter);
			}
		}

		ArrayList<Object[]> systemList = new ArrayList<Object[]>();
		String[] sysNames = null;
		ArrayList<Object[]> siteList = new ArrayList<Object[]>();
		String[] siteNames = null;
		HashMap<String, String[]> waveStartEndHash = new HashMap<String, String[]>();
		waveStartEndHash = deploymentStrategyProcessor.getWaveStartEndHash();
		
		DHMSMIntegrationSavingsPerFiscalYearProcessor processor = new DHMSMIntegrationSavingsPerFiscalYearProcessor();
		boolean success = true;
		try {
			processor.runSupportQueries();
		} catch(NullPointerException ex) {
			Utility.showError(ex.getMessage());
		}
		if(success) {
			processor.runMainQuery("");
			if(!waveStartEndHash.isEmpty()) {
				processor.setWaveStartEndDate(waveStartEndHash);
			}
			processor.generateSavingsData();
			processor.processSystemData();
			systemList = processor.getSystemOutputList();	
			sysNames = processor.getSysNames();
			//*****Displays data for system analysis tab
			displayListOnTab(sysNames, systemList, ps.overallAlysPanel);
		}
		//TODO: figure out why what obj is being changed causing me to run twice to make sure numbers match
		success = true;
		processor = new DHMSMIntegrationSavingsPerFiscalYearProcessor();
		try {
			processor.runSupportQueries();
		} catch(NullPointerException ex) {
			Utility.showError(ex.getMessage());
		}
		if(success) {
			processor.runMainQuery("");
			if(!waveStartEndHash.isEmpty()) {
				processor.setWaveStartEndDate(waveStartEndHash);
			}
			processor.generateSavingsData();
			processor.processSiteData();
			siteList = processor.getSiteOutputList();	
			siteNames = processor.getSiteNames();
			//*****Displays data for site analysis tab
			displayListOnTab(siteNames, siteList, ps.siteAnalysisPanel);
		}
		//print out waveinfo
		for(String s : waveOrder) {
			String[] x = waveStartEndHash.get(s);
			System.out.println(s + " : " + x[0] + ", " + x[1]);
		}
		
		//setting data for bar chart tab
		ps.setSystemYearlySavings(systemList);
		ps.setSysSavingsHeaders(sysNames);
		Set<String> allSystemsList = processor.getAllSystems();
		Vector sysVector = new Vector(allSystemsList);
		Collections.sort(sysVector);
		sysVector.remove("Total");
		sysVector.add(0,"Total");
		ps.systemSelectBarChartPanel.sysSelectDropDown.resetList(sysVector);
		ps.systemSelectBarChartPanel.setVisible(true);
		ps.sysSavingsChart.setVisible(true);
		ps.runSysBarChartBtn.setVisible(true);
		
		//getting data for the deployment map
		HashMap<String, String> lastWaveForEachSystem = processor.getLastWaveForEachSystem();
		HashMap<String, String> firstWaveForEachSystem = processor.getFirstWaveForEachSystem();		
		HashMap<String, String> lastWaveForSitesAndFloatersInMultipleWavesHash = processor.getLastWaveForSitesAndFloatersInMultipleWavesHash();
		HashMap<String, String> firstWaveForSitesAndFloatersInMultipleWavesHash = processor.getFirstWaveForSitesAndFloatersInMultipleWavesHash();
		HashMap<String, ArrayList<String>> systemsForSiteHash = processor.getSystemsForSiteHash();
		HashMap<String, HashMap<String, Double>> siteLocationHash = processor.getSiteLocationHash();
		HashMap<String, List<String>> waveForSites = processor.getWaveForSites();

		Hashtable<Integer, Object> dataHash = new Hashtable<Integer, Object>();
		dataHash = deploymentStrategyProcessor.calculateDeploymentMapData(systemList, sysNames, siteList, siteNames, firstWaveForEachSystem, lastWaveForEachSystem,
				firstWaveForSitesAndFloatersInMultipleWavesHash, lastWaveForSitesAndFloatersInMultipleWavesHash, systemsForSiteHash, siteLocationHash, waveForSites);
		
		Hashtable allData = new Hashtable();
		allData.put("data", dataHash);	
		allData.put("label", "savings");
		// execute method to restart values when different deployment schedule is initiated
		JSValue val = ps.sysMap.browser.executeJavaScriptAndReturnValue("refresh();");
		//*****Sends data for map visualization to the JS file
		ps.sysMap.callIt(allData);
	}
	
	public Hashtable generateJSONData(boolean defaultValuesSelected) {
		Hashtable aggregateDataHash = new Hashtable();
		LOGGER.info("Run Deployment Strategy Button Pushed");
		consoleArea = ps.consoleArea;
		DHMSMDeploymentStrategyProcessor deploymentStrategyProcessor = new DHMSMDeploymentStrategyProcessor(regionWaveHash, waveOrder, waveStartEndDate, consoleArea);
		
		deploymentStrategyProcessor.setRegionsList(ps.getRegionsList());

		if(defaultValuesSelected) {
			//grab the original values for deployment schedule
			int oBeginQuarter = ps.getqBeginDefault(); //%%% need to pass in defaults
			int oBeginYear = ps.getyBeginDefault() - 2000; //%%% need to pass in defaults
			int oEndQuarter = ps.getqEndDefault(); //%%% need to pass in defaults
			int oEndYear = ps.getyEndDefault() - 2000; //%%% need to pass in defaults

			//pull from begin / end and fill the regions accordingly
			int beginQuarter = getInteger(ps.getQBeginField(), ps.getQBeginField().getName()); //%%% need to pass in defaults
			int beginYear = getInteger(ps.getYBeginField(), ps.getYBeginField().getName()); //%%% need to pass in defaults
			int endQuarter = getInteger(ps.getQEndField(), ps.getQEndField().getName()); //%%% need to pass in defaults
			int endYear = getInteger(ps.getYEndField(), ps.getYEndField().getName()); //%%% need to pass in defaults
			if(beginQuarter<0 || beginYear<0 || endQuarter<0 || endYear<0) {
				Utility.showError("Cannot read fields. Please check the Console tab for more information");
				return aggregateDataHash;
			}
			//%%% need to pass in defaults
			if(!validQuarter(beginQuarter, ps.getQBeginField().getName()) || !validQuarter(endQuarter, ps.getQEndField().getName()) || !validYear(beginYear, ps.getYBeginField().getName())  || !validYear(endYear, ps.getYEndField().getName()) ) {
				Utility.showError("Cannot read fields. Please check the Console tab for more information");
				return aggregateDataHash;
			}

			// if no change to deployment values, run default schedule
			if(oBeginQuarter == beginQuarter && oBeginYear == beginYear && oEndQuarter == endQuarter && oEndYear == endYear) {
				LOGGER.info("Using original deployment schedule");
			} else {
				deploymentStrategyProcessor.runDefaultSchedule(beginQuarter, beginYear, endQuarter, endYear);
			}
		} else {
			//grab the original values for the deployment schedule
			//%%% need to pass in defaults
			Hashtable<String, Integer> oBeginQuarterFieldRegionList = ps.getqBeginDefaultHash();
			Hashtable<String, Integer> oBeginYearFieldRegionList = ps.getyBeginDefaultHash();
			Hashtable<String, Integer> oEndQuarterFieldRegionList = ps.getqEndDefaultHash();
			Hashtable<String, Integer> oEndYearFieldRegionList = ps.getyEndDefaultHash();
			
			//pull from region list
			//check if region textfields are valid
			//add them to list of regions
			//%%% need to pass in defaults
			Hashtable<String,JTextField> beginQuarterFieldRegionList = ps.getQBeginFieldHash();
			Hashtable<String,JTextField> beginYearFieldRegionList = ps.getYBeginFieldHash();
			Hashtable<String,JTextField> endQuarterFieldRegionList = ps.getQEndFieldHash();
			Hashtable<String,JTextField> endYearFieldRegionList = ps.getYEndFieldHash();
			
			//if no changes to schedule, run default values
			//check default values against new web values
			ArrayList<String> regionsList = ps.getRegionsList();
			boolean noChange = true;
			for(int i = 0; i < regionsList.size(); i++) {
				String region = regionsList.get(i);
				int oBeginQuarter = oBeginQuarterFieldRegionList.get(region);
				int oBeginYear = oBeginYearFieldRegionList.get(region) - 2000;
				int oEndQuarter = oEndQuarterFieldRegionList.get(region);
				int oEndYear = oEndYearFieldRegionList.get(region) - 2000;
				
				int beginQuarter = getInteger(beginQuarterFieldRegionList.get(region), beginQuarterFieldRegionList.get(region).getName());
				int beginYear = getInteger(beginYearFieldRegionList.get(region), beginYearFieldRegionList.get(region).getName());
				int endQuarter = getInteger(endQuarterFieldRegionList.get(region), endQuarterFieldRegionList.get(region).getName());
				int endYear = getInteger(endYearFieldRegionList.get(region), endYearFieldRegionList.get(region).getName());
				
				if(beginQuarter<0 || beginYear<0 || endQuarter<0 || endYear<0) {
					Utility.showError("Cannot read fields. Please check the Console tab for more information");
					return aggregateDataHash;
				}
				if(!validQuarter(beginQuarter, beginQuarterFieldRegionList.get(region).getName()) || !validQuarter(endQuarter, endQuarterFieldRegionList.get(region).getName()) || !validYear(beginYear, endQuarterFieldRegionList.get(region).getName())  || !validYear(endYear, endYearFieldRegionList.get(region).getName()) ) {
					Utility.showError("Cannot read fields. Please check the Console tab for more information");
					return aggregateDataHash;
				}
				
				if(oBeginQuarter == beginQuarter && oBeginYear == beginYear && oEndQuarter == endQuarter && oEndYear == endYear) {
					// do nothing
				} else {
					noChange = false;
					break;
				}
			}
			
			if(noChange) {
				LOGGER.info("Using original deployment schedule");
			} else {
				//run processing with new web values
				for(int i=0;i<regionsList.size();i++) {
					String region = regionsList.get(i);
					int beginQuarter = getInteger(beginQuarterFieldRegionList.get(region), beginQuarterFieldRegionList.get(region).getName());
					int beginYear = getInteger(beginYearFieldRegionList.get(region), beginYearFieldRegionList.get(region).getName());
					int endQuarter = getInteger(endQuarterFieldRegionList.get(region), endQuarterFieldRegionList.get(region).getName());
					int endYear = getInteger(endYearFieldRegionList.get(region), endYearFieldRegionList.get(region).getName());
					if(beginQuarter<0 || beginYear<0 || endQuarter<0 || endYear<0) {
						Utility.showError("Cannot read fields. Please check the Console tab for more information");
						return aggregateDataHash;
					}
					if(!validQuarter(beginQuarter, beginQuarterFieldRegionList.get(region).getName()) || !validQuarter(endQuarter, endQuarterFieldRegionList.get(region).getName()) || !validYear(beginYear, endQuarterFieldRegionList.get(region).getName())  || !validYear(endYear, endYearFieldRegionList.get(region).getName()) ) {
						Utility.showError("Cannot read fields. Please check the Console tab for more information");
						return aggregateDataHash;
					}

					deploymentStrategyProcessor.runRegionTimesSchedule(beginQuarter, beginYear, endQuarter, endYear, region);
				}
				
				String region = regionsList.get(regionsList.size() - 1);
				int endYear = getInteger(endYearFieldRegionList.get(region), endYearFieldRegionList.get(region).getName());
				int endQuarter = getInteger(endQuarterFieldRegionList.get(region), endQuarterFieldRegionList.get(region).getName());
				deploymentStrategyProcessor.updateRegionTimesSchedule(endYear, endQuarter);
			}
		}

		ArrayList<Object[]> systemList = new ArrayList<Object[]>();
		String[] sysNames = null;
		ArrayList<Object[]> siteList = new ArrayList<Object[]>();
		String[] siteNames = null;
		HashMap<String, String[]> waveStartEndHash = new HashMap<String, String[]>();
		waveStartEndHash = deploymentStrategyProcessor.getWaveStartEndHash();
		
		DHMSMIntegrationSavingsPerFiscalYearProcessor processor = new DHMSMIntegrationSavingsPerFiscalYearProcessor();
		//Setting up Data for the aggregate hash
		Hashtable systemAnalysisHash = new Hashtable();
		Hashtable siteAnalysisHash = new Hashtable();
		
		boolean success = true;
		try {
			processor.runSupportQueries();
		} catch(NullPointerException ex) {
			Utility.showError(ex.getMessage());
		}
		if(success) {
			processor.runMainQuery("");
			if(!waveStartEndHash.isEmpty()) {
				processor.setWaveStartEndDate(waveStartEndHash);
			}
			processor.generateSavingsData();
			processor.processSystemData();
			systemList = processor.getSystemOutputList();	
			sysNames = processor.getSysNames();
			//*****Send Data to system Hash
			systemAnalysisHash.put("sysNames", sysNames);
			systemAnalysisHash.put("systemList", systemList);
		}
		//TODO: figure out why what obj is being changed causing me to run twice to make sure numbers match
		success = true;
		processor = new DHMSMIntegrationSavingsPerFiscalYearProcessor();
		try {
			processor.runSupportQueries();
		} catch(NullPointerException ex) {
			Utility.showError(ex.getMessage());
		}
		if(success) {
			processor.runMainQuery("");
			if(!waveStartEndHash.isEmpty()) {
				processor.setWaveStartEndDate(waveStartEndHash);
			}
			processor.generateSavingsData();
			processor.processSiteData();
			siteList = processor.getSiteOutputList();	
			siteNames = processor.getSiteNames();
			//*****Send Data to site Hash
			siteAnalysisHash.put("siteNames", siteNames);
			siteAnalysisHash.put("siteList", siteList);
		}
				
		//getting data for the deployment map
		HashMap<String, String> lastWaveForEachSystem = processor.getLastWaveForEachSystem();
		HashMap<String, String> firstWaveForEachSystem = processor.getFirstWaveForEachSystem();		
		HashMap<String, String> lastWaveForSitesAndFloatersInMultipleWavesHash = processor.getLastWaveForSitesAndFloatersInMultipleWavesHash();
		HashMap<String, String> firstWaveForSitesAndFloatersInMultipleWavesHash = processor.getFirstWaveForSitesAndFloatersInMultipleWavesHash();
		HashMap<String, ArrayList<String>> systemsForSiteHash = processor.getSystemsForSiteHash();
		HashMap<String, HashMap<String, Double>> siteLocationHash = processor.getSiteLocationHash();
		HashMap<String, List<String>> waveForSites = processor.getWaveForSites();

		Hashtable<Integer, Object> dataHash = new Hashtable<Integer, Object>();
		dataHash = deploymentStrategyProcessor.calculateDeploymentMapData(systemList, sysNames, siteList, siteNames, firstWaveForEachSystem, lastWaveForEachSystem,
				firstWaveForSitesAndFloatersInMultipleWavesHash, lastWaveForSitesAndFloatersInMultipleWavesHash, systemsForSiteHash, siteLocationHash, waveForSites);
		
		Hashtable allData = new Hashtable();
		allData.put("data", dataHash);	
		allData.put("label", "savings");
		
		//populate data into aggregate data hash
		aggregateDataHash.put("mapData", allData);
		aggregateDataHash.put("systemAnalysisData", systemAnalysisHash);
		aggregateDataHash.put("siteAnalysisData", siteAnalysisHash);
		
		return aggregateDataHash;
	}

	public void displayListOnTab(String[] colNames,ArrayList <Object []> list, JPanel panel) {
		GridScrollPane pane = new GridScrollPane(colNames, list);
		panel.removeAll();
		GridBagLayout gridBagLayout = new GridBagLayout();
		gridBagLayout.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gridBagLayout.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		panel.setLayout(gridBagLayout);
		GridBagConstraints gbc_panel_1_1 = new GridBagConstraints();
		gbc_panel_1_1.insets = new Insets(0, 0, 5, 5);
		gbc_panel_1_1.fill = GridBagConstraints.BOTH;
		gbc_panel_1_1.gridx = 0;
		gbc_panel_1_1.gridy = 0;
		panel.add(pane, gbc_panel_1_1);
		panel.repaint();
	}


	/**
	 * Method setPlaySheet.
	 * @param sheet DHMSMDeploymentStrategyPlaySheet
	 */
	public void setPlaySheet(DHMSMDeploymentStrategyPlaySheet ps)
	{
		this.ps = ps;
	}
	
	/**
	 * Gets the integer in a textfield.
	 * if it does not contain an integer, throws an error.
	 * @param field
	 * @return
	 */
	private int getInteger(JTextField field, String fieldName) {
		String q = field.getText();
		try{
			int qInt = Integer.parseInt(q);
			return qInt;
		}catch (RuntimeException e) {
			consoleArea.setText(consoleArea.getText()+"\n"+fieldName+" "+NON_INT_ERROR);
			LOGGER.error(fieldName+" "+NON_INT_ERROR);
			return -1;
		}
	}

	/**
	 * Determines if the quarter is valid, between 1 and 4 inclusive
	 * @param quarter
	 * @param fieldName
	 * @return
	 */
	private Boolean validQuarter(int quarter, String fieldName) {
		if(quarter < 1 || quarter > 4) {
			consoleArea.setText(consoleArea.getText()+"\n"+fieldName+" "+QUARTER_ERROR);
			LOGGER.error(fieldName+" "+QUARTER_ERROR);
			return false;
		}
		return true;
	}

	/**
	 * Determines if the year is valid, between 0 and 99 inclusive
	 * @param year
	 * @param fieldName
	 * @return
	 */
	private Boolean validYear(int year, String fieldName) {
		if(year < 0 || year > 99) {
			consoleArea.setText(consoleArea.getText()+"\n"+fieldName+" "+YEAR_ERROR);
			LOGGER.error(fieldName+" "+YEAR_ERROR);
			return false;
		}
		return true;
	}

	public int findSystem(ArrayList<Object []> savingsList,String system) {
		for(int i=0;i<savingsList.size(); i++) {
			Object[] row = savingsList.get(i);
			if(((String)row[0]).equals(system))
				return i;
		}
		return -1;
	}
	
	public Hashtable<String, List<String>> getRegionWaveHash() {
		return regionWaveHash;
	}

	public void setRegionWaveHash(Hashtable<String, List<String>> regionWaveHash) {
		this.regionWaveHash = regionWaveHash;
	}

	public void setWaveOrder(ArrayList<String> waveOrder) {
		this.waveOrder = waveOrder;
	}

	public ArrayList<String> getWaveOrder() {
		return waveOrder;
	}

	public void setWaveStartEndDate(HashMap<String, String[]> waveStartEndDate) {
		this.waveStartEndDate = waveStartEndDate;
	}

	public HashMap<String, String[]> setWaveStartEndDate() {
		return waveStartEndDate;
	}
}
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

		HashMap<String, String[]> waveStartEndHash = new HashMap<String, String[]>();

		consoleArea = ps.consoleArea;
		JToggleButton selectRegionTimesButton = ps.getSelectRegionTimesButton();
		ArrayList<String> regionsList = ps.getRegionsList();

		if(!selectRegionTimesButton.isSelected()) {
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
			//TODO: add logic for wave start/end dates
		} else {
			//pull from region list
			//check if region textfields are valid
			//add them to list of regions
			Hashtable<String,JTextField> beginQuarterFieldRegionList = ps.getQBeginFieldHash();
			Hashtable<String,JTextField> beginYearFieldRegionList = ps.getYBeginFieldHash();
			Hashtable<String,JTextField> endQuarterFieldRegionList = ps.getQEndFieldHash();
			Hashtable<String,JTextField> endYearFieldRegionList = ps.getYEndFieldHash();

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

				List<String> wavesInRegion = regionWaveHash.get(region);
				// calculate distance in number of quarters
				int distanceInQuarters = Math.abs(beginQuarter - endQuarter);
				if(distanceInQuarters == 0) {
					distanceInQuarters += 4 * (endYear - beginYear);
				} else {
					distanceInQuarters += 4 * (endYear - (beginYear+1)); // +1 for when less than a year different, but in two different FYs
				}
				double numQuartersPerWave = distanceInQuarters/wavesInRegion.size();

				double currQuarter = beginQuarter;
				int currYear = beginYear;
				for(String wave : waveOrder) {
					if(wavesInRegion.contains(wave)) {
						String[] date = new String[2];
						date[0] = "Q" + ((int) Math.ceil(currQuarter)) + "FY20" + currYear;
						if(numQuartersPerWave > 4) {
							int yearsPassed = (int) Math.floor(numQuartersPerWave / 4);
							double quartersPassed = numQuartersPerWave % 4;
							currYear += yearsPassed;
							currQuarter += quartersPassed;
						} else if(currQuarter + numQuartersPerWave > 4) {
							currQuarter = ((currQuarter + numQuartersPerWave) - 4);
							currYear += 1;
						} else {
							currQuarter += numQuartersPerWave;
						}
						date[1] = "Q" + ((int) Math.ceil(currQuarter)) + "FY20" + currYear;
						waveStartEndHash.put(wave, date);
					}
				}
			}
		}

		DHMSMIntegrationSavingsPerFiscalYearProcessor processor = new DHMSMIntegrationSavingsPerFiscalYearProcessor();
		processor.runSupportQueries();
		processor.runMainQuery("");
		if(!waveStartEndHash.isEmpty()) {
			processor.setWaveStartEndDate(waveStartEndHash);
		}
		processor.generateSavingsData();
		processor.processSystemData();
		ArrayList<Object[]> systemList = processor.getSystemOutputList();	
		String[] sysNames = processor.getSysNames();
		displayListOnTab(sysNames, systemList, ps.overallAlysPanel);
		
		//TODO: figure out why what obj is being changed causing me to run twice to make sure numbers match
		processor = new DHMSMIntegrationSavingsPerFiscalYearProcessor();
		processor.runSupportQueries();
		processor.runMainQuery("");
		if(!waveStartEndHash.isEmpty()) {
			processor.setWaveStartEndDate(waveStartEndHash);
		}
		processor.generateSavingsData();
		processor.processSiteData();
		ArrayList<Object[]> siteList = processor.getSiteOutputList();	
		String[] siteNames = processor.getSiteNames();
		displayListOnTab(siteNames, siteList, ps.siteAnalysisPanel);
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
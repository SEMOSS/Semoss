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
				int distanceInQuarters = 0;
				if(beginYear != endYear) {
					if(beginQuarter > endQuarter) {
						distanceInQuarters = (Math.abs(beginYear - endYear)*4) - Math.abs(beginQuarter - endQuarter);
					} else {
						distanceInQuarters = (Math.abs(beginYear - endYear)*4) + Math.abs(beginQuarter - endQuarter);
					}
				} else {
					distanceInQuarters = Math.abs(beginQuarter - endQuarter);
				}
				
				waveStartEndHash.put("IOC", waveStartEndDate.get("IOC"));
				
				double numQuartersPerWave = (double) distanceInQuarters / (waveOrder.size()-1);
				
				double currQuarter = beginQuarter;
				int currYear = beginYear;
				for(int i=1;i<regionsList.size();i++) {
					String region = regionsList.get(i);
					List<String> wavesInRegion = regionWaveHash.get(region);

					for(String wave : waveOrder) {
						if(wavesInRegion.contains(wave)) {
							String[] date = new String[2];
							date[0] = "Q" + ((int) Math.ceil(currQuarter)) + "FY20" + currYear;

							if(numQuartersPerWave > 4) {
								int yearsPassed = (int) Math.floor(numQuartersPerWave / 4);
								double quartersPassed = numQuartersPerWave % 4;
								currYear += yearsPassed;
								if(currQuarter + quartersPassed > 12) {
									currQuarter = ((currQuarter + quartersPassed) - 4);
									currYear += 1;
								}
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
				
				String[] checkLastDate = waveStartEndHash.get(waveOrder.get(waveOrder.size()-1));
				if(!checkLastDate[1].equals("Q".concat(endQuarter + "").concat("FY20").concat(endYear + ""))) {
					LOGGER.info("Alter last date");
					checkLastDate[1] = "Q".concat(endQuarter + "").concat("FY20").concat(endYear + "");
					waveStartEndHash.put(waveOrder.get(waveOrder.size()-1), checkLastDate);
				}
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
	
					List<String> wavesInRegion = regionWaveHash.get(region);
					// calculate distance in number of quarters
					int distanceInQuarters = 0;
					if(beginYear != endYear) {
						if(beginQuarter > endQuarter) {
							distanceInQuarters = (Math.abs(beginYear - endYear)*4) - Math.abs(beginQuarter - endQuarter);
						} else {
							distanceInQuarters = (Math.abs(beginYear - endYear)*4) + Math.abs(beginQuarter - endQuarter);
						}
					} else {
						distanceInQuarters = Math.abs(beginQuarter - endQuarter);
					}
					
					double numQuartersPerWave = (double) distanceInQuarters/wavesInRegion.size();
	
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
								if(currQuarter + quartersPassed > 4) {
									currQuarter = ((currQuarter + quartersPassed) - 4);
									currYear += 1;
								}
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
				
				String region = regionsList.get(regionsList.size() - 1);
				int endYear = getInteger(endYearFieldRegionList.get(region), endYearFieldRegionList.get(region).getName());
				int endQuarter = getInteger(endQuarterFieldRegionList.get(region), endQuarterFieldRegionList.get(region).getName());
				String[] checkLastDate = waveStartEndHash.get(waveOrder.get(waveOrder.size()-1));
				if(!checkLastDate[1].equals("Q".concat(endQuarter + "").concat("FY20").concat(endYear + ""))) {
					LOGGER.info("Alter last date");
					checkLastDate[1] = "Q".concat(endQuarter + "").concat("FY20").concat(endYear + "");
					waveStartEndHash.put(waveOrder.get(waveOrder.size()-1), checkLastDate);
				}
			}
		}

		ArrayList<Object[]> systemList = new ArrayList<Object[]>();
		String[] sysNames = null;
		ArrayList<Object[]> siteList = new ArrayList<Object[]>();
		String[] siteNames = null;
		
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
		
		//setting data for the deployment map
		Hashtable<Integer, Object> dataHash = new Hashtable<Integer, Object>();
		HashMap<String, String> lastWaveForEachSystem = processor.getLastWaveForEachSystem();
		HashMap<String, String> firstWaveForEachSystem = processor.getFirstWaveForEachSystem();

		for(int sysIndex=0;sysIndex<systemList.size();sysIndex++) {
			Object[] row = systemList.get(sysIndex);
			String sys = (String)row[0];
			String startWave = firstWaveForEachSystem.get(sys);
			String endWave = lastWaveForEachSystem.get(sys);
			int startYear;
			int endYear;
			if(startWave==null || endWave == null || startWave.equals("") || endWave.equals("")) {
				LOGGER.error("No wave info for system "+sys);
				consoleArea.setText(consoleArea.getText()+"\nNo wave info for system "+sys);
				startYear = 3000;
				endYear = Integer.parseInt("20"+sysNames[sysNames.length - 3].substring(2));
			} else {
				String[] startWaveDate = waveStartEndHash.get(startWave);
				String[] endWaveDate = waveStartEndHash.get(endWave);
				
				startYear = Integer.parseInt(startWaveDate[0].substring(4));
				endYear = Integer.parseInt(endWaveDate[1].substring(4));
			}
			for(int i=1;i<sysNames.length - 1;i++) {
				int year = Integer.parseInt("20"+sysNames[i].substring(2));
				String status = "Not Started";
				if(year>=startYear)
					status = "In Progress";
				if(year>endYear)
					status = "Decommissioned";
				double savings = 0.0;
				String value = (String)systemList.get(sysIndex)[i];
				if(value.contains("No")) {
					LOGGER.info("No cost info for system "+sys);
					consoleArea.setText(consoleArea.getText()+"\nNo cost info for system "+sys);
				} else {
					if(value.startsWith("$ "))
						value = value.substring(2);
					value = value.replaceAll(",","");
					value = value.replaceAll("\\*","");
					savings = Double.parseDouble(value);
				}
				Hashtable<String, Object> sysElement = new Hashtable<String, Object>();
				sysElement.put("AggregatedStatus",status);
				sysElement.put("TCostSystem", savings);
				
				if(dataHash.containsKey(year)) {
					Hashtable<String, Hashtable> yearHash = (Hashtable<String, Hashtable>)dataHash.get(year);
					Hashtable<String, Hashtable> systemHash = (Hashtable<String, Hashtable>)yearHash.get("system");
					systemHash.put(sys, sysElement);
				}else {
					Hashtable<String, Hashtable> yearHash = new Hashtable<String, Hashtable>();
					Hashtable<String, Hashtable> systemHash = new Hashtable<String, Hashtable>();
					systemHash.put(sys, sysElement);
					yearHash.put("system", systemHash);
					dataHash.put(year, yearHash);
				}
			}
		}
		
		HashMap<String, String> lastWaveForSitesAndFloatersInMultipleWavesHash = processor.getLastWaveForSitesAndFloatersInMultipleWavesHash();
		HashMap<String, String> firstWaveForSitesAndFloatersInMultipleWavesHash= processor.getFirstWaveForSitesAndFloatersInMultipleWavesHash();
		HashMap<String, ArrayList<String>> systemsForSiteHash = processor.getSystemsForSiteHash();
		HashMap<String, HashMap<String, Double>> siteLocationHash = processor.getSiteLocationHash();
		HashMap<String, List<String>> waveForSites = processor.getWaveForSites();

		for(int siteIndex=0;siteIndex<siteList.size();siteIndex++) {
			Object[] row = siteList.get(siteIndex);
			String site = (String)row[0];
			String startWave = firstWaveForSitesAndFloatersInMultipleWavesHash.get(site);
			String endWave = lastWaveForSitesAndFloatersInMultipleWavesHash.get(site);
			List<String> startWaves = waveForSites.get(site);
			List<String> endWaves = waveForSites.get(site);
			if((startWave==null || endWave == null) && (startWaves ==null || endWaves ==null)) {
				LOGGER.error("No wave info for site "+site);
				consoleArea.setText(consoleArea.getText()+"\nNo wave info for site "+site);
			} else {
				if((startWave==null || endWave == null)) {
					startWave = startWaves.get(0);
					endWave = endWaves.get(0);
				}
			
				String[] startWaveDate = waveStartEndHash.get(startWave);
				String[] endWaveDate = waveStartEndHash.get(endWave);
				
				int startYear = Integer.parseInt(startWaveDate[0].substring(4));
				int endYear = Integer.parseInt(endWaveDate[1].substring(4));
				
				for(int i=1;i<siteNames.length - 1;i++) {
					int year = Integer.parseInt("20"+siteNames[i].substring(2));
					String status = "Not Started";
					if(year>=startYear)
						status = "In Progress";
					if(year>endYear)
						status = "Decommissioned";
					double savings = 0.0;
					String value = (String)siteList.get(siteIndex)[i];
					if(value.contains("No")) {
						LOGGER.info("No cost info for site "+site);
						consoleArea.setText(consoleArea.getText()+"\nNo cost info for site "+site);
					} else {
						if(value.startsWith("$ "))
							value = value.substring(2);
						value = value.replaceAll(",","");
						value = value.replaceAll("\\*","");
						savings = Double.parseDouble(value);
					}
					HashMap<String, Double> latLongHash = siteLocationHash.get(site);
					if(latLongHash==null) {
						LOGGER.info("No lat or long info for site "+site);
						consoleArea.setText(consoleArea.getText()+"\nNo lat or long info for site "+site);
					} else {
						double latVal = latLongHash.get("Lat");
						double longVal = latLongHash.get("Long");
						ArrayList<String> systemsForSite = systemsForSiteHash.get(site);
						Hashtable<String,Hashtable> systemHash = new Hashtable<String,Hashtable>();
						for(String system : systemsForSite) {
							systemHash.put(system,new Hashtable());
						}
						
						Hashtable<String, Object> siteElement = new Hashtable<String, Object>();
						siteElement.put("Lat", latVal);
						siteElement.put("Long", longVal);
						siteElement.put("Status",status);
						siteElement.put("TCostSite", savings);
						siteElement.put("SystemForSite", systemHash);
						//TODO only systems that are included in my list?
						Hashtable<String, Hashtable> yearHash = (Hashtable<String, Hashtable>)dataHash.get(year);
						if(yearHash.containsKey("site")) {
							Hashtable<String, Hashtable> siteHash = (Hashtable<String, Hashtable>)yearHash.get("site");
							siteHash.put(site, siteElement);
						}else {
							Hashtable<String, Hashtable> siteHash = new Hashtable<String, Hashtable>();
							siteHash.put(site, siteElement);
							yearHash.put("site", siteHash);
						}
					}
				}
			}
		}
		
		Hashtable allData = new Hashtable();
		allData.put("data", dataHash);
		allData.put("label", "savings");
		// execute method to restart values when different deployment schedule is initiated
		JSValue val = ps.sysMap.browser.executeJavaScriptAndReturnValue("refresh();");
		ps.sysMap.callIt(allData);
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
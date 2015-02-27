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
package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

import javax.swing.JTextArea;
import javax.swing.JTextField;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.main.listener.specific.tap.DHMSMDeploymentStrategyRunBtnListener;
import prerna.util.Utility;

public class DHMSMDeploymentStrategyProcessor {
	
	public DHMSMDeploymentStrategyProcessor(
			Hashtable<String, List<String>> regionWaveHash,
			ArrayList<String> waveOrder,
			HashMap<String, String[]> waveStartEndDate,
			JTextArea consoleArea) {
		super();
		this.regionWaveHash = regionWaveHash;
		this.waveOrder = waveOrder;
		this.waveStartEndDate = waveStartEndDate;
		this.consoleArea = consoleArea;
	}

	private static final Logger LOGGER = LogManager.getLogger(DHMSMDeploymentStrategyProcessor.class.getName());
	
	private static final String YEAR_ERROR = "field is not an integer between 00 and 99, inclusive.";
	private static final String QUARTER_ERROR = "field is not an integer between 1 and 4, inclusive.";
	private static final String NON_INT_ERROR = "field contains a non-integer value.";
	
	private Hashtable<String, List<String>> regionWaveHash;
	private ArrayList<String> waveOrder;
	private HashMap<String, String[]> waveStartEndDate;
	private JTextArea consoleArea;
	
	private HashMap<String, String[]> waveStartEndHash = new HashMap<String, String[]>();
	private ArrayList<String> regionsList;
	
	public ArrayList<String> getRegionsList() {
		return regionsList;
	}

	public void setRegionsList(ArrayList<String> regionsList) {
		this.regionsList = regionsList;
	}

	public HashMap<String, String[]> getWaveStartEndHash() {
		return waveStartEndHash;
	}

	public void setWaveStartEndHash(HashMap<String, String[]> waveStartEndHash) {
		this.waveStartEndHash = waveStartEndHash;
	}
	
	public void runDefaultSchedule(int beginQuarter, int beginYear, int endQuarter, int endYear) {
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
	
	public void runRegionTimesSchedule(int beginQuarter, int beginYear, int endQuarter, int endYear, String region) {
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
	
	public void updateRegionTimesSchedule(int endYear, int endQuarter) {
		String[] checkLastDate = waveStartEndHash.get(waveOrder.get(waveOrder.size()-1));
		if(!checkLastDate[1].equals("Q".concat(endQuarter + "").concat("FY20").concat(endYear + ""))) {
			LOGGER.info("Alter last date");
			checkLastDate[1] = "Q".concat(endQuarter + "").concat("FY20").concat(endYear + "");
			waveStartEndHash.put(waveOrder.get(waveOrder.size()-1), checkLastDate);
		}
	}

	public Hashtable<Integer, Object> calculateDeploymentMapData(ArrayList<Object[]> systemList, String[] sysNames, ArrayList<Object[]> siteList, String[] siteNames,
			HashMap<String, String> firstWaveForEachSystem, HashMap<String, String> lastWaveForEachSystem, HashMap<String, String> firstWaveForSitesAndFloatersInMultipleWavesHash,
			HashMap<String, String> lastWaveForSitesAndFloatersInMultipleWavesHash, HashMap<String, ArrayList<String>> systemsForSiteHash, HashMap<String, HashMap<String, Double>> siteLocationHash, HashMap<String, List<String>> waveForSites) {
		Hashtable<Integer, Object> dataHash = new Hashtable<Integer, Object>();		

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
		return dataHash;
	}
	
	
}

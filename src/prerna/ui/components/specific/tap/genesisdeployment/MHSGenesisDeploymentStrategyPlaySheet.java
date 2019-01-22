/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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
package prerna.ui.components.specific.tap.genesisdeployment;


import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ds.h2.H2Frame;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.test.TestUtilityMethods;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.specific.tap.DHMSMDeploymentHelper;
import prerna.ui.components.specific.tap.InputPanelPlaySheet;
import prerna.util.DIHelper;


/**
 * This is the playsheet used exclusively for TAP service optimization.
 */
@SuppressWarnings("serial")
public class MHSGenesisDeploymentStrategyPlaySheet extends InputPanelPlaySheet {

		
	private static final Logger LOGGER = LogManager.getLogger(MhsGenesisDeploymentSavingsProcessor.class.getName());

	// store the system deployment savings over time
	private H2Frame systemDeploymentSavings;
	private String[] systemDeploymentSavingsHeaders;

	// store the s deployment savings over time
	private H2Frame siteDeploymentSavings;

	
	// frame to keep track of waves to sites to systems to fys containing inflated costs
	protected H2Frame mainSustainmentFrame;
	// frame to keep track of systems to sits to fys containing the inflated costs
	// only for systems which have site level cost information
	private H2Frame systemSiteSustainmentFrame;

	// need to get the wave start and end
	private Map<String, String[]> waveStartEndDate;
	
	//used for lat and long
	private HashMap<String, HashMap<String, Double>> siteLocationHash;

	private int numColumns;
	private double percentRealized;
	
	public MHSGenesisDeploymentStrategyPlaySheet(){
		super();
		overallAnalysisTitle = "System Analysis";
		titleText = "Set Deployment Time Frame";
		MhsGenesisDeploymentSavingsProcessor processor = new MhsGenesisDeploymentSavingsProcessor();
		processor.runSupportQueries();

		// frame to keep track of waves to sites to systems to fys containing inflated costs
		this.mainSustainmentFrame = processor.getMainSustainmentFrame();
		// frame to keep track of systems to sits to fys containing the inflated costs
		// only for systems which have site level cost information
		this.systemSiteSustainmentFrame = processor.getSystemSiteSustainmentFrame();
		this.waveStartEndDate = processor.getWaveStartEndDate();
		this.numColumns = processor.getNumColumns();
		this.percentRealized = processor.getPercentRealized();
		MhsGenesisSiteDeploymentSavingsPlaySheet sitePS = new MhsGenesisSiteDeploymentSavingsPlaySheet(mainSustainmentFrame, systemSiteSustainmentFrame, processor);
		sitePS.processDataMakerComponent(null);
		siteDeploymentSavings = sitePS.getSiteDeploymentSavings();

		// need to get the wave start and end

		this.siteLocationHash= DHMSMDeploymentHelper.getSiteLocation(processor.getTapSite());
	}

	@Override
	public void processDataMakerComponent(DataMakerComponent component) {
		// generate the necessary data
		generateSystemDeploymentSavings();
	}


	@Override
	public Hashtable getDataMakerOutput(String... selectors) {
		//TODO: finish this
		Hashtable returnHash = new Hashtable();

		// Site Analysis Data
		Hashtable siteAnalysis = new Hashtable();
		ArrayList<String> siteQueryHeaders = createSiteQueryHeaders();
		siteAnalysis.put("data", siteDataList(siteQueryHeaders));
		siteAnalysis.put("headers", headerReturnData(siteQueryHeaders));
		returnHash.put("siteAnalysisData", siteAnalysis);

		// System Analysis Data
		Hashtable systemAnalysisData = new Hashtable();
		ArrayList<String> systemHeaders = createSytemHeaders();
		systemAnalysisData.put("data", systemDataList(systemHeaders));
		systemAnalysisData.put("headers", headerReturnData(systemHeaders));
		returnHash.put("systemAnalysisData", systemAnalysisData);

		// map data
		returnHash.put("mapData", creatMapData());

		Hashtable finalReturnHash = new Hashtable();
		finalReturnHash.put("data", returnHash);

		return finalReturnHash;
	}

	private Hashtable creatMapData() {
		Hashtable mapData = new Hashtable();
		mapData.put("label", "savings");
		Hashtable data = new Hashtable();

		ArrayList<String> fysQueryString = new ArrayList<String>(); // FY15
		ArrayList<String> fysLabel = new ArrayList<String>(); // 2015
		for (int i = 15; i < (15+this.numColumns); i++) {
			fysQueryString.add("FY" + i);
			fysLabel.add("20" + i);
		}

		// add fiscal year to data
		for (int j = 0; j < fysLabel.size(); j++) {
			String fiscalYear = fysQueryString.get(j);
			Hashtable fysData = new Hashtable();
			
			// Site
			fysData.put("site", createSiteListData(fiscalYear, fysLabel.get(j)));
			
			// System
			fysData.put("system", createSystemListData(fiscalYear, fysLabel.get(j) ));
			
			data.put(fysLabel.get(j), fysData);
		}

		mapData.put("data", data);		
		return mapData;
	}

	private Hashtable createSystemListData(String fiscalYear, String fiscalLabel) {
		String mapSystemQuery = "SELECT DISTINCT System, Last_Wave_For_System, " + fiscalYear + "  FROM "
				+ mainSustainmentFrame.getName();
		ResultSet mapSystemQS = mainSustainmentFrame.execQuery(mapSystemQuery);
		Hashtable systemListData = new Hashtable();

		try {
			String system = "";
			String lastWaveForSystem = "";
			double value = 0;
			while (mapSystemQS.next()) {
				Hashtable systemData = new Hashtable();

				system = mapSystemQS.getString(1);
				lastWaveForSystem = mapSystemQS.getString(2);
				value = 0;
				
				// aggregated status
				String status = "Not Started";
				String[] waveDate = waveStartEndDate.get(lastWaveForSystem);
				boolean total = false;
				if (waveDate != null) {
					String[] startTime = waveDate[0].split("FY");
					int startYear = Integer.parseInt(startTime[1]);
					String[] endTime = waveDate[1].split("FY");
					int endYear = Integer.parseInt(endTime[1]);
					int year = Integer.parseInt((String) fiscalLabel);
					if (year >= startYear)
						status = "In Progress";
					if (year > endYear) {
						status = "Decommissioned";
						total = true;
					}
					if (total) {
						if (fiscalYear.equals("FY26")) {
							fiscalYear = "Total";
						}
						String systemSavingsQuery = "SELECT DISTINCT System," + fiscalYear + " FROM "
								+ systemDeploymentSavings.getName() + " WHERE System = \'" + system + "\';";
						ResultSet systemSavingsRS = systemDeploymentSavings.execQuery(systemSavingsQuery);
						try {
							while (systemSavingsRS.next()) {
								value = systemSavingsRS.getDouble(2);
							}
							systemSavingsRS.close();

						} catch (Exception e) {

						}
					}

				}

				DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance();
				symbols.setGroupingSeparator(',');
				NumberFormat formatter = new DecimalFormat("'$' ###,##0.00", symbols);
				systemData.put("TCostSystem", formatter.format(value));
				systemData.put("AggregatedStatus", status);
				systemListData.put(system, systemData);
			}
			mapSystemQS.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
		return systemListData;
	}

	private Hashtable createSiteListData(String fiscalYear, String fiscalLabel) {
		String mapSiteQuery = "SELECT DISTINCT HostSiteAndFloater, Last_Wave_For_Site, " + fiscalYear
				+ ", Wave FROM " + mainSustainmentFrame.getName();
		ResultSet mapSiteQS = mainSustainmentFrame.execQuery(mapSiteQuery);
		Hashtable siteListData = new Hashtable();
		try {
			String site = "";
			double value = 0;
			while (mapSiteQS.next()) {
				site = mapSiteQS.getString(1);
				String lastWaveForSite = mapSiteQS.getString(2);
				value = 0;
				
				//get Wave
				if(lastWaveForSite.equals("-1")) {
					lastWaveForSite = mapSiteQS.getString(4);
				}
				boolean total = false;
				if (site != null) {
					Hashtable siteData = new Hashtable();
					String status = "Not Started";
					String[] startDate = waveStartEndDate.get(lastWaveForSite);
					if (startDate != null) {
						String[] startTime = startDate[0].split("FY");
						int startYear = Integer.parseInt(startTime[1]);
						String[] endTime = startDate[1].split("FY");
						int endYear = Integer.parseInt(endTime[1]);
						int year = Integer.parseInt((String) fiscalLabel);
						// TODO
						if (year >= startYear)
							status = "In Progress";
						if (year > endYear) {
							status = "Decommissioned";
							total = true;
						} 
					}
					
					siteData.put("Status", status);

					HashMap<String, Double> latLongHash = siteLocationHash.get(site);
					double latVal = 0;
					double longVal = 0;
					if (latLongHash != null) {
						latVal = latLongHash.get("Lat");
						longVal = latLongHash.get("Long");
					}

					siteData.put("Lat", latVal);
					siteData.put("Long", longVal);

					if (total) {
						if (fiscalYear.equals("FY26")) {
							fiscalYear = "Total";
						}
						String siteSavingsQuery = "SELECT DISTINCT Site," + fiscalYear + " FROM "
								+ siteDeploymentSavings.getName() + " WHERE Site = \'" + site + "\';";
						ResultSet siteSavingsRS = siteDeploymentSavings.execQuery(siteSavingsQuery);

						try {
							while (siteSavingsRS.next()) {
								value = siteSavingsRS.getDouble(2);
							}

						} catch (Exception e) {

						}

					}
					DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance();
					symbols.setGroupingSeparator(',');
					NumberFormat formatter = new DecimalFormat(" ###,##0.00", symbols);
					
					siteData.put("TCostSite", formatter.format(value));
					siteListData.put(site, siteData);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return siteListData;
	}

	private ArrayList systemDataList(ArrayList<String> systemHeaders) {
		
		String systemQuery = "SELECT ";
		for(int i = 0; i < systemHeaders.size(); i++) {
			systemQuery += systemHeaders.get(i) + ", ";
		}
		//remove extra , at the end
		systemQuery = systemQuery.substring(0, systemQuery.length()-2).toString();
		systemQuery +=" FROM " + systemDeploymentSavings.getName() + ";";
		
		LOGGER.info("Executing query for system data from systemDeploymentSavings" + systemQuery);

		ResultSet systemRS = systemDeploymentSavings.execQuery(systemQuery.toString());
		ArrayList systemDataList = new ArrayList();
		DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance();
		symbols.setGroupingSeparator(',');
		NumberFormat formatter = new DecimalFormat("'$' ###,##0.00", symbols);
		try {
			while (systemRS.next()) {
				Hashtable data = new Hashtable();
				for (int i = 0; i < systemHeaders.size(); i++) {
					if (i == 0) {
						String string = systemRS.getString(i+1);
						//TODO HOSTSItie-Floater
						data.put(systemHeaders.get(i), string);
					}
					else {
						double value = systemRS.getDouble(i+1);
						data.put(systemHeaders.get(i), formatter.format(value));
					}
				}
				systemDataList.add(data);
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
		return systemDataList;
	}

	private ArrayList<String> createSytemHeaders() {
		ArrayList systemHeaders = new ArrayList<>();
		systemHeaders.add("System");
		for(int i = 15; i < (15+this.numColumns); i++) {
			systemHeaders.add("FY"+i);
		}
		systemHeaders.add("Total");
		return systemHeaders;
	}

	private ArrayList headerReturnData(ArrayList<String> headers) {
		ArrayList headerData = new ArrayList();
		for(int i = 0; i< headers.size(); i++) {
			Hashtable data = new Hashtable();
			data.put("filteredTitle", headers.get(i)); 
			//filter
			Hashtable filter = new Hashtable();
			filter.put(headers.get(i), "");
			data.put("filter", filter);
			data.put("title", headers.get(i));
			headerData.add(data);
		}		
		return headerData;
	}

	private ArrayList<String> createSiteQueryHeaders() {
		ArrayList siteHeaders = new ArrayList<>();
		siteHeaders.add("Site");
		for(int i = 15; i < (15+this.numColumns); i++) {
			siteHeaders.add("FY"+i);
		}
		siteHeaders.add("Total");
		return siteHeaders;
	}

	private ArrayList siteDataList(ArrayList<String> siteHeaders) {
		String siteQuery = "SELECT ";
		
		//ignore fy26
		for(int i = 0; i < siteHeaders.size(); i++) {
			siteQuery += siteHeaders.get(i) + ", ";
		}
		//remove extra , at the end
		siteQuery = siteQuery.substring(0, siteQuery.length()-2).toString();
		siteQuery +=" FROM " + siteDeploymentSavings.getName() + ";";
		
		ResultSet siteRS = siteDeploymentSavings.execQuery(siteQuery.toString());
		LOGGER.info("Executing query for site data from siteDeploymentSavings" + siteQuery);
		ArrayList siteDataList = new ArrayList();
		DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance();
		symbols.setGroupingSeparator(',');
		NumberFormat formatter = new DecimalFormat("'$' ###,##0.00", symbols);
		try {
			while (siteRS.next()) {
				Hashtable data = new Hashtable();
				for (int i = 0; i < siteHeaders.size(); i++) {
					if (i == 0) {
						String site = siteRS.getString(i+1);
						//TODO HOSTSItie-Floater
						data.put(siteHeaders.get(i), site);
					} else {
						double value = siteRS.getDouble(i+1);
						data.put(siteHeaders.get(i), formatter.format(value));
					}
				}
				siteDataList.add(data);
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return siteDataList;
	}

	/**
	 * Calculate the cost of system deployment savings
	 */
	public void generateSystemDeploymentSavings() {
		// okay, create the frame that will hold
		// the information regarding the system
		// deployment savings
		// so each row represents a system and its savings
		// across each FY
		this.systemDeploymentSavingsHeaders = new String[numColumns+1];
		String[] dataTypes = new String[numColumns+1];
		this.systemDeploymentSavingsHeaders[0] = "System";
		dataTypes[0] = "String";
		for(int i = 0; i < numColumns; i++) {
			this.systemDeploymentSavingsHeaders[i+1] = "FY" + (15 + i);
			dataTypes[i+1] = "Number";
		}
		this.systemDeploymentSavings = new H2Frame();
		
		this.systemDeploymentSavings.addNewColumn(this.systemDeploymentSavingsHeaders, dataTypes, this.systemDeploymentSavings.getName());

		// just going to go through and add all the different type of savings
		addLocallyDeployedSystemSavings();
		addGloballyDeployedSystemSavings();
		addSiteSpecificSystemSavings();
		addFixedSustainmentCostForAllLocallyDeployedSystems();
		
		// this will add the column and row totals
		MhsGenesisDeploymentSavingsProcessor.calculateColAndRowTotals(this.systemDeploymentSavings, this.systemDeploymentSavingsHeaders);

		// iterate through the results for testing
		// iterate through the results for testing
		// iterate through the results for testing
		// iterate through the results for testing
		// iterate through the results for testing
//		LOGGER.info("Testing data...");
//		Iterator<Object[]> it = this.systemDeploymentSavings.iterator();
//		System.out.println(">>> " + Arrays.toString( headers ) );
//		try{
//			PrintWriter writer = new PrintWriter("C:\\Users\\rramirezjimenez\\Desktop\\Datasets\\SAVINGS_SystemView.csv", "UTF-8");
//			for(Object val : headers) {
//				writer.print(val + ",");
//			}
//			writer.print("\n");
//			while(it.hasNext()) {
//				Object[] values = it.next();
//				for(Object val : values) {
//					writer.print(val + ",");
//				}
//				writer.print("\n");
//				System.out.println(">>> " + Arrays.toString( values ) );
//			}
//			writer.close();
//		} catch (IOException e) {
//			// do something
//		}
//		LOGGER.info("Done iterating through system deployment savings data");
	}
	
	private void addLocallyDeployedSystemSavings() {
		LOGGER.info("START ::: Add locally deployed systems where we do not have site specific costs...");
		// >>>>>>>>>>>>>>>>>>>>>>>
		// this section will go ahead and add all the systems
		// which are locally deployed and we do not have site level cost for
		
		// query will be to get all system rows and produce the FY inflated savings / num sites
		// and we will add the appropriate filters such that we are only getting the rows desired
		StringBuilder baseSystemsCostQuery = new StringBuilder("SELECT SYSTEM ");
		for(int i = 0; i < numColumns; i++) {
			baseSystemsCostQuery.append(", SUM( FY" + (15+i) + " / NUM_SITES ) ");
		}
		baseSystemsCostQuery.append(" FROM ").append(mainSustainmentFrame.getName());
		baseSystemsCostQuery.append(" WHERE Central_Deployment='FALSE' AND Site_Specific='FALSE' "); 

		for(String wave : waveStartEndDate.keySet()) {
			String[] startDate = waveStartEndDate.get(wave);
			String[] endTime = startDate[1].split("FY");
			String endYear = "FY" + endTime[1].substring(2);

			// so we have the wave
			// just query to get the cost information
			// for all the systems in the wave
			StringBuilder locallyDeployedSystemCost = new StringBuilder(baseSystemsCostQuery.toString());
			locallyDeployedSystemCost.append(" AND WAVE = '").append(wave).append("' AND ( Last_Wave_For_Site = '-1' OR Last_Wave_For_Site = '" + wave + "') ");
			locallyDeployedSystemCost.append(" GROUP BY SYSTEM;");
			updateSystemCostValues(locallyDeployedSystemCost.toString(), endYear, this.percentRealized);
		}
		LOGGER.info("DONE ::: Finsihed with locally deployed systems where we do not have site specific costs...");
	}
	
	private void addGloballyDeployedSystemSavings() {
		LOGGER.info("START ::: Add globally deployed systems...");
		// >>>>>>>>>>>>>>>>>>>>>>>
		// this section will add the globally deployed systems
		// they by definition get added at the last year
		// again, note that we need to account for the additional year for CHCS
		
		StringBuilder globallyDeployedSystemCostSavings = new StringBuilder("SELECT SYSTEM ");
		for(int i = 0; i < numColumns; i++) {
			globallyDeployedSystemCostSavings.append(", SUM(FY" + (15+i) + " / NUM_SITES) ");
		}
		globallyDeployedSystemCostSavings.append(" FROM ").append(mainSustainmentFrame.getName());
		globallyDeployedSystemCostSavings.append(" WHERE Central_Deployment='TRUE' GROUP BY SYSTEM;");
		
		// the cost savings for all of these systems will occur on final year of deployment schedule 
		updateSystemCostValues(globallyDeployedSystemCostSavings.toString(), "FY" + (15+numColumns-3), 1.0);
		LOGGER.info("DONE ::: Finsihed with globally deployed systems...");
	}
	
	private void addSiteSpecificSystemSavings() {
		LOGGER.info("START ::: Add systems where we have site specific costs...");
		// >>>>>>>>>>>>>>>>>>>>>>>
		// this section will add the site specific sustainment costs
		// base query below is to determine which sites are in each wave
		// as this requires 2 separate queries
		StringBuilder baseSitesInWaveQuery = new StringBuilder("SELECT DISTINCT HostSiteAndFloater FROM ").append(mainSustainmentFrame.getName());
		// get all the systems and their cost
		StringBuilder baseSiteSystemSpecificCost = new StringBuilder("SELECT SYSTEM ");
		for(int i = 0; i < numColumns; i++) {
			baseSiteSystemSpecificCost.append(", SUM(FY" + (15+i) + ")");
		}
		baseSiteSystemSpecificCost.append(" FROM ").append(systemSiteSustainmentFrame.getName());
		
		for(String wave : waveStartEndDate.keySet()) {
			String[] startDate = waveStartEndDate.get(wave);
			String[] endTime = startDate[1].split("FY");
			String endYear = "FY" + endTime[1].substring(2);

			// got to query for the list of sites in this wave
			// this will be used as a filter for the second query to run
			StringBuilder sitesInWaveQuery = new StringBuilder(baseSitesInWaveQuery.toString()).append(" WHERE WAVE = '").append(wave).append("'")
					.append(" AND ( Last_Wave_For_Site = '-1' OR Last_Wave_For_Site = '" + wave + "') ");			
			// create a filter string for the site system specific cost query
			StringBuilder siteSystemFilter = new StringBuilder(" WHERE SITE IN (");
			ResultSet rs = mainSustainmentFrame.execQuery(sitesInWaveQuery.toString());
			try {
				if(rs.next()) {
					siteSystemFilter.append("'").append(rs.getString(1)).append("'");
				}
				while(rs.next()) {
					siteSystemFilter.append(",'").append(rs.getString(1)).append("'");
				}
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		
			siteSystemFilter.append(")");
			
			// so we have the list of sites to filter to in this wave
			// append that to the base site system specific cost query and then update the values
			StringBuilder siteSystemSpecificCost = new StringBuilder(baseSiteSystemSpecificCost.toString()).append(siteSystemFilter.toString()).append(" GROUP BY SYSTEM;");
			// update the base site specific costs
			// this is a different method only because chcs has year of savings pushed by 2 years
			updateSiteSpecificSystemCostValues(siteSystemSpecificCost.toString(), endYear);
		}
		LOGGER.info("DONE ::: Finsihed with systems where we have site specific costs...");
	}
	
	private void addFixedSustainmentCostForAllLocallyDeployedSystems() {
		LOGGER.info("START ::: Add fixed sustainment cost for systems where we do not have site specific costs...");
		// >>>>>>>>>>>>>>>>>>>>>>>
		// add the fixed amount savings
		// get the query to get the sum for all the values group by system
		StringBuilder baseSystemFixedCostQuery = new StringBuilder("SELECT SYSTEM ");
		for(int i = 0; i < numColumns; i++) {
			baseSystemFixedCostQuery.append(", SUM(FY" + (15+i) + "/NUM_SITES) ");
		}
		String endYear = "FY" + (15 + numColumns-3);
		StringBuilder systemFixedCostQuery = new StringBuilder(baseSystemFixedCostQuery.toString());
		systemFixedCostQuery.append(" FROM ").append(mainSustainmentFrame.getName())
					.append(" WHERE Central_Deployment='FALSE' GROUP BY SYSTEM;");
		updateSystemFixedCostValues(systemFixedCostQuery.toString(), endYear);
		
		LOGGER.info("DONE ::: Finsihed with fixed sustainment cost for systems where we do not have site specific costs...");
	}

	/**
	 * 
	 * @param query
	 * @param headers
	 * @param endYear
	 */
	private void updateSystemCostValues(String query, String endYear, double percentRealized) {
		ResultSet systemData = mainSustainmentFrame.execQuery(query);
		try {
			// then iterate add up all the data
			while(systemData.next()) {
				// get the system
				String system = systemData.getString(1);

				// iterate through and get the row data
				double[] newValues = new double[numColumns];
				for(int i = 1; i <= numColumns; i++) {
					newValues[i-1] = systemData.getDouble(i+1) * percentRealized;
				}
				
				// this will handle the specific system and values and perform the consolidation
				// if the system is already stored in the frame
				MhsGenesisDeploymentSavingsProcessor.updateCostValues(this.systemDeploymentSavings, system, newValues, endYear, this.systemDeploymentSavingsHeaders);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param query
	 * @param endYear
	 */
	private void updateSiteSpecificSystemCostValues(String query, String endYear) {
		ResultSet systemData = systemSiteSustainmentFrame.execQuery(query);
		try {
			// then iterate add up all the data
			while(systemData.next()) {
				// get the system
				String system = systemData.getString(1);
				// iterate through and get the row data
				double[] newValues = new double[numColumns];
				for(int i = 1; i <= numColumns; i++) {
					newValues[i-1] = systemData.getDouble(i+1);
				}
				
				// this will handle the specific system and values and perform the consolidation
				// if the system is already stored in the frame
				// since CHCS is the annoying thing we need to update
				// perform the check here
				String modEndYear = endYear;
				if(system.equals("CHCS")) {
					modEndYear = "FY" + ( Integer.parseInt(endYear.substring(2)) + 1 );
				}
				MhsGenesisDeploymentSavingsProcessor.updateCostValues(systemDeploymentSavings, system, newValues, modEndYear, this.systemDeploymentSavingsHeaders);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param query
	 * @param headers
	 * @param endYear
	 */
	private void updateSystemFixedCostValues(String query, String endYear) {
		ResultSet systemData = mainSustainmentFrame.execQuery(query);
		try {
			// then iterate add up all the data
			while(systemData.next()) {
				// get the system
				String system = systemData.getString(1);

				// iterate through and get the row data
				double[] newValues = new double[numColumns];
				for(int i = 1; i <= numColumns; i++) {
					newValues[i-1] = systemData.getDouble(i+1) * (1.0 - percentRealized);
				}
				
				String modEndYear = endYear;
				if(system.equals("CHCS")) {
					modEndYear = "FY" + ( Integer.parseInt(endYear.substring(2)) + 1 );
				}
				MhsGenesisDeploymentSavingsProcessor.updateCostValues(systemDeploymentSavings, system, newValues, modEndYear, this.systemDeploymentSavingsHeaders);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}



	
	
	
	
	
	


	/////////////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////// MAIN METHOD ////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * Testing method
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		TestUtilityMethods.loadDIHelper();

		String engineProp = "C:\\Users\\rramirezjimenez\\pksl\\Semoss\\db\\TAP_Core_Data.smss";
		IEngine coreEngine = new BigDataEngine();
		coreEngine.setEngineId("TAP_Core_Data");
		coreEngine.openDB(engineProp);
		coreEngine.setEngineId("TAP_Core_Data");
		DIHelper.getInstance().setLocalProperty("TAP_Core_Data", coreEngine);

		engineProp = "C:\\Users\\rramirezjimenez\\pksl\\Semoss\\db\\TAP_Site_Data.smss";
		coreEngine = new BigDataEngine();
		coreEngine.setEngineId("TAP_Site_Data");
		coreEngine.openDB(engineProp);
		coreEngine.setEngineId("TAP_Site_Data");
		DIHelper.getInstance().setLocalProperty("TAP_Site_Data", coreEngine);

		engineProp = "C:\\Users\\rramirezjimenez\\pksl\\Semoss\\db\\TAP_Portfolio.smss";
		coreEngine = new BigDataEngine();
		coreEngine.setEngineId("TAP_Portfolio");
		coreEngine.openDB(engineProp);
		coreEngine.setEngineId("TAP_Portfolio");
		DIHelper.getInstance().setLocalProperty("TAP_Portfolio", coreEngine);

		MhsGenesisDeploymentSavingsProcessor processor = new MhsGenesisDeploymentSavingsProcessor();
		MHSGenesisDeploymentStrategyPlaySheet ps = new MHSGenesisDeploymentStrategyPlaySheet();
		ps.processDataMakerComponent(null);
		ps.getDataMakerOutput("");
	}


}

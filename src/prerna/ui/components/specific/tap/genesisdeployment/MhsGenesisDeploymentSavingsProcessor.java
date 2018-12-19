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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.h2.H2Frame;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.specific.tap.DHMSMDeploymentHelper;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

public class MhsGenesisDeploymentSavingsProcessor {

	private static final Logger LOGGER = LogManager.getLogger(MhsGenesisDeploymentSavingsProcessor.class.getName());

	protected IEngine tapPortfolio;
	protected IEngine tapSite;
	protected IEngine tapCore;

	protected String hpSystemFilterStr = null;

	// frame to keep track of waves to sites to systems to fys containing inflated costs
	protected H2Frame mainSustainmentFrame;
	// frame to keep track of systems to sits to fys containing the inflated costs
	// only for systems which have site level cost information
	protected H2Frame systemSiteSustainmentFrame;

	// need to get the wave start and end
	protected Map<String, String[]> waveStartEndDate;
	// need to keep track of last wave for each site
	protected Map<String, String> lastWaveForEachSystem;

	protected int minYear;
	protected int maxYear;
	protected int numColumns;
	protected double[] inflationArr;
	protected final double percentRealized = .18;

	/**
	 * Update existing cost values or insert new values
	 * @param frame						The frame where we are adding values to
	 * @param instance					The specific system or site
	 * @param newValues					The new values we are adding
	 * @param endYear					The endYear which these values should be applied to
	 * @param headers					The headers for the frame
	 */
	public static void updateCostValues(H2Frame frame, String instance, double[] newValues, String endYear, String[] headers) {
		try {
			String mainColName = headers[0];

			// see if system/site already exists in table
			// if it does, append these values
			// if it doesn't, insert
			int numColumns = newValues.length;
			double[] oldValues = new double[numColumns];
			boolean foundExisting = false;;
			if(!frame.isEmpty()) {
				String curSystemDeploymentSavingsQuery = "SELECT * FROM " + frame.getTableName() + " WHERE " + mainColName + " = '" + instance + "'";
				ResultSet curSavingRs = frame.execQuery(curSystemDeploymentSavingsQuery);

				if(curSavingRs.next()) {
					foundExisting = true;
					for(int i = 0; i < numColumns; i++) {
						oldValues[i] = curSavingRs.getDouble(headers[i+1]);
					}
				}
			}
			// need to figure out based on the wave
			// which indices in updateValues to add to oldValues
			// shift the index by -1 to account for the system column in headers
			// but, the savings are 1 year after the deployment year
			// which would result in shifting the index by +1
			// so it cancels out
			int fyIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(headers, endYear);

			// based on if the system/site exist
			// do a insert or a update query
			if(foundExisting) {
				String[] updateHeaders = new String[numColumns-fyIndex];
				Object[] updateValues = new Object[numColumns-fyIndex];
				for(int updateIndex = fyIndex; updateIndex < numColumns; updateIndex++) {
					// ugh, again account for the system/site column in headers...
					updateHeaders[updateIndex-fyIndex] = headers[updateIndex+1];
					updateValues[updateIndex-fyIndex] = newValues[updateIndex] + oldValues[updateIndex];
				}

				// need to do an update
				// got to make a query
				PreparedStatement updatePs = frame.createUpdatePreparedStatement(updateHeaders, new String[]{mainColName});
				for(int updateIndex = 0; updateIndex < updateValues.length; updateIndex++) {
					updatePs.setObject(updateIndex+1, updateValues[updateIndex]);
				}
				updatePs.setObject(updateValues.length+1, instance);
				updatePs.addBatch();
				updatePs.executeBatch();
			} else {
				// just add the system to the list of values
				// and do an add row
				Object[] newValuesWithInstance = new Object[numColumns+1];
				newValuesWithInstance[0] = instance;
				for(int i = 1; i <= fyIndex; i++) {
					newValuesWithInstance[i] = 0;
				}
				for(int i = fyIndex; i < numColumns; i++) {
					newValuesWithInstance[i+1] = newValues[i];
				}
				frame.addRow(newValuesWithInstance, headers);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	// method to append the column and row totals
	public static void calculateColAndRowTotals(H2Frame frame, String[] headers) {
		// assumption that first column is either systems or sites
		
		String mainColName = headers[0];
		
		// first, calculate the row totals
		StringBuilder rowTotal = new StringBuilder("SELECT ").append(headers[0]).append(",").append(headers[1]);
		for(int i = 2; i < headers.length; i++) {
			rowTotal.append("+").append(headers[i]);
		}
		rowTotal.append(" FROM ").append(frame.getTableName());
		
		String[] newHeaders = new String[2];
		String[] dataTypes = new String[2];
		newHeaders[0] = mainColName;
		newHeaders[1] = "Total";
		dataTypes[0] = SemossDataType.STRING.toString();
		dataTypes[1] = SemossDataType.DOUBLE.toString();

		// we will merge the new headers into our existing frame
		frame.addNewColumn(newHeaders, dataTypes, frame.getTableName());

		ResultSet it = frame.execQuery(rowTotal.toString());
		try {
			// create an update statement to set the systems which are centrally deployed to true
			PreparedStatement updatePs = frame.createUpdatePreparedStatement(new String[]{"Total"}, new String[]{mainColName});
			while(it.next()) {
				updatePs.setObject(1, it.getDouble(2));
				updatePs.setObject(2, it.getString(1));
				updatePs.addBatch();
			}
			updatePs.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		// now calculate the column totals
		StringBuilder colTotals = new StringBuilder("SELECT SUM(").append(headers[1]).append(")");
		for(int i = 2; i < headers.length; i++) {
			colTotals.append(", SUM(").append(headers[i]).append(")");
		}
		colTotals.append(" FROM ").append(frame.getTableName());
		it = frame.execQuery(colTotals.toString());
		Object[] colTotalArr = new Object[headers.length];
		colTotalArr[0] = "Total";
		try {
			while(it.next()) {
				for(int i = 1; i < headers.length; i++) {
					colTotalArr[i] = it.getObject(i);
				}
			}
			frame.addRow(colTotalArr, headers);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}

	/////////////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////// Data Gathering Section ///////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////////

	public void runSupportQueries() {
		this.tapPortfolio = (IEngine) Utility.getEngine(MasterDatabaseUtility.testEngineIdIfAlias("TAP_Portfolio"));
		this.tapSite = (IEngine) Utility.getEngine(MasterDatabaseUtility.testEngineIdIfAlias("TAP_Site_Data"));
		this.tapCore = (IEngine) Utility.getEngine(MasterDatabaseUtility.testEngineIdIfAlias("TAP_Core_Data"));

		this.waveStartEndDate = DHMSMDeploymentHelper.getWaveStartAndEndDate(tapSite);
		// calculate the wave start/end dates
		this.minYear = 3000;
		this.maxYear = 0;
		for(String wave : this.waveStartEndDate.keySet()) {
			String[] startDate = this.waveStartEndDate.get(wave);
			String startTime[] = startDate[0].split("FY");
			String endTime[] = startDate[1].split("FY");
			String startYear = startTime[1];
			String endYear = endTime[1];
			int startYearAsNum = Integer.parseInt(startYear);
			int endYearAsNum = Integer.parseInt(endYear);
			if(endYearAsNum > this.maxYear) {
				this.maxYear = endYearAsNum;
			} else if(startYearAsNum < this.minYear) {
				this.minYear = startYearAsNum;
			}
		}
		// costs gains are typically realized a year after, except for centrally distributed systems
		this.numColumns = this.maxYear - this.minYear + 2;
		if(this.numColumns < 4) {
			this.numColumns = 4;
		}

		// TODO: REALLY ANNOYING LOGIC!!!!
		// New business rule now requires that CHCS cost savings to be extended by 2 years instead of 1
		// pushing back the fy an additional year
		this.numColumns+=2;

		this.inflationArr = new double[numColumns+1];
		List<Double> inflationValues = DHMSMDeploymentHelper.getInflationRate(tapSite);
		for(int i = 0; i < numColumns + 1; i++) {
			if(i < inflationValues.size()) {
				this.inflationArr[i] = inflationValues.get(i);
			} else {
				this.inflationArr[i] = this.inflationArr[i-1] + .02;
			}
		}

		// create the wave to site to system frame
		this.mainSustainmentFrame = createWavesSiteSystemFrame();

		// append system to fy's to sustainment budget information
		// account for the inflation array here so that it is easier later on
		this.mainSustainmentFrame = appendSystemSustainmentInfo(this.mainSustainmentFrame, this.inflationArr);

		// append the system count
		this.mainSustainmentFrame = appendSystemSiteCount(this.mainSustainmentFrame);

		// append if system is centrally deployed
		this.mainSustainmentFrame = appendCentralDeployment(this.mainSustainmentFrame);

		// create the system to site to fy's to sys-site cost
		createSystemSiteSustainmentInfo(this.inflationArr);

		// append if system is site specific or not
		this.mainSustainmentFrame = appendSiteSpecific(this.mainSustainmentFrame, this.systemSiteSustainmentFrame);

		// append to determine if wave-site combination is the last one
		this.mainSustainmentFrame = appendLastWaveForSite(this.mainSustainmentFrame);

		// append the last wave for each system
		this.mainSustainmentFrame = appendLastWaveForSystem(this.mainSustainmentFrame);

//		Iterator<IHeadersDataRow> it = this.mainSustainmentFrame.iterator();
//		//Printing to csv
//		try{
//			PrintWriter writer = new PrintWriter("C:\\Users\\mahkhalil\\Desktop\\Datasets\\mainSustainmentTable.csv", "UTF-8");
//			String[] headers = this.mainSustainmentFrame.getColumnHeaders();
//			for(Object val : headers) {
//				writer.print(val + ",");
//			}
//			writer.print("\n");
//			while(it.hasNext()) {
//				Object[] values = it.next().getValues();
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
//		LOGGER.info("Done iterating through system sustainment data");
//		
//		it = this.systemSiteSustainmentFrame.iterator();
//
//		try{
//			PrintWriter writer = new PrintWriter("C:\\Users\\mahkhalil\\Desktop\\Datasets\\siteSpecificSustainment.csv", "UTF-8");
//			String[] headers = this.systemSiteSustainmentFrame.getColumnHeaders();
//			for(Object val : headers) {
//				writer.print(val + ",");
//			}
//			writer.print("\n");
//			while(it.hasNext()) {
//				Object[] values = it.next().getValues();
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
//		LOGGER.info("Done iterating through system sustainment data");
	}

	private H2Frame appendLastWaveForSystem(H2Frame mainSustainmentFrame) {
		List<String> waveOrder = DHMSMDeploymentHelper.getWaveOrder(tapSite);

		Map<String, String> lastWaveForSystem = DHMSMDeploymentHelper.getLastWaveForEachSystem(tapSite, waveOrder);

		// need to create the metadata for the new column
		String[] headers = new String[2];
		String[] dataTypes = new String[2];
		headers[0] = "System";
		headers[1] = "Last_Wave_For_System";
		dataTypes[0] = SemossDataType.STRING.toString();
		dataTypes[1] = SemossDataType.STRING.toString();

		// we will merge the new headers into our existing frame
		mainSustainmentFrame.addNewColumn(headers, dataTypes, mainSustainmentFrame.getTableName());

		try {
			// create an update statement to set the systems last wave
			PreparedStatement updatePs = mainSustainmentFrame.createUpdatePreparedStatement(new String[]{"Last_Wave_For_System"}, new String[]{"System"});
			for(String system : lastWaveForSystem.keySet()) {
				updatePs.setObject(1, lastWaveForSystem.get(system));
				updatePs.setObject(2, system);
				updatePs.addBatch();
			}
			updatePs.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return mainSustainmentFrame;	
	}

	private H2Frame appendLastWaveForSite(H2Frame mainSustainmentFrame) {
		List<String> waveOrder = DHMSMDeploymentHelper.getWaveOrder(tapSite);

		Map<String, List<String>> sitesInMultipleWavesHash = DHMSMDeploymentHelper.getSitesAndMultipleWaves(tapSite);
		Map<String, List<String>> floaterWaveList = DHMSMDeploymentHelper.getFloatersAndWaves(tapSite);
		Map<String, String> lastWaveForInput = DHMSMDeploymentHelper.determineLastWaveForInput(waveOrder, sitesInMultipleWavesHash);
		lastWaveForInput.putAll(DHMSMDeploymentHelper.determineLastWaveForInput(waveOrder, floaterWaveList));

		// need to create the metadata for the new column
		String[] headers = new String[2];
		String[] dataTypes = new String[2];
		headers[0] = "HostSiteAndFloater";
		headers[1] = "Last_Wave_For_Site";
		dataTypes[0] = SemossDataType.STRING.toString();
		dataTypes[1] = SemossDataType.STRING.toString();

		// we will merge the new headers into our existing frame
		mainSustainmentFrame.addNewColumn(headers, dataTypes, mainSustainmentFrame.getTableName());

		try {
			// create an update statement to set all the values to false as default
			PreparedStatement updatePs = mainSustainmentFrame.createUpdatePreparedStatement(new String[]{"Last_Wave_For_Site"}, new String[]{});
			updatePs.setObject(1, "-1");
			updatePs.addBatch();
			updatePs.executeBatch();

			// create an update statement to set the systems which are centrally deployed to true
			updatePs = mainSustainmentFrame.createUpdatePreparedStatement(new String[]{"Last_Wave_For_Site"}, new String[]{"HostSiteAndFloater"});
			for(String siteOrFloater : lastWaveForInput.keySet()) {
				updatePs.setObject(1, lastWaveForInput.get(siteOrFloater));
				updatePs.setObject(2, siteOrFloater);
				updatePs.addBatch();
			}
			updatePs.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return mainSustainmentFrame;	
	}

	private H2Frame appendSiteSpecific(H2Frame mainSustainmentFrame, H2Frame systemSiteSustainmentFrame) {
		// need to create the metadata for the new column
		String[] headers = new String[2];
		String[] dataTypes = new String[2];
		headers[0] = "System";
		headers[1] = "Site_Specific";
		dataTypes[0] = SemossDataType.STRING.toString();
		dataTypes[1] = SemossDataType.STRING.toString();

		// we will merge the new headers into our existing frame
		mainSustainmentFrame.addNewColumn(headers, dataTypes, mainSustainmentFrame.getTableName());

		try {
			// create an update statement to set all the values to false as default
			PreparedStatement updatePs = mainSustainmentFrame.createUpdatePreparedStatement(new String[]{"Site_Specific"}, new String[]{});
			updatePs.setObject(1, "FALSE");
			updatePs.addBatch();
			updatePs.executeBatch();

			StringBuilder siteSpecificSystems = new StringBuilder();
			siteSpecificSystems.append("SELECT DISTINCT System FROM " + systemSiteSustainmentFrame.getTableName());

			// get an iterator for the systems which we have site specific data
			ResultSet siteSpecificSystemsIterator = systemSiteSustainmentFrame.execQuery(siteSpecificSystems.toString());

			// create an update statement to set the systems which are centrally deployed to true
			updatePs = mainSustainmentFrame.createUpdatePreparedStatement(new String[]{"Site_Specific"}, new String[]{"System"});
			while(siteSpecificSystemsIterator.next()) {
				updatePs.setObject(1, "TRUE");
				updatePs.setObject(2, siteSpecificSystemsIterator.getObject(1));
				updatePs.addBatch();
			}
			updatePs.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return mainSustainmentFrame;
	}

	private H2Frame appendCentralDeployment(H2Frame mainSustainmentFrame) {
		// need to create the metadata for the new column
		String[] headers = new String[2];
		String[] dataTypes = new String[2];
		headers[0] = "System";
		headers[1] = "Central_Deployment";
		dataTypes[0] = SemossDataType.STRING.toString();
		dataTypes[1] = SemossDataType.STRING.toString();

		// we will merge the new headers into our existing frame
		mainSustainmentFrame.addNewColumn(headers, dataTypes, mainSustainmentFrame.getTableName());

		StringBuilder centralDeployedSystems = new StringBuilder();
		centralDeployedSystems.append("SELECT DISTINCT ?System "
				+ "WHERE "
				+ "{ "
				+ "{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} "
				+ "{?System <http://semoss.org/ontologies/Relation/Contains/CentralDeployment> 'Y'} "
				+ "} ");
		centralDeployedSystems.append( getHPSystemFilterString() );

		// get an iterator for the new data
		IRawSelectWrapper rawIterator = WrapperManager.getInstance().getRawWrapper(tapCore, centralDeployedSystems.toString());

		try {
			// create an update statement to set all the values to false as default
			PreparedStatement updatePs = mainSustainmentFrame.createUpdatePreparedStatement(new String[]{"Central_Deployment"}, new String[]{});
			updatePs.setObject(1, "FALSE");
			updatePs.addBatch();
			updatePs.executeBatch();

			// create an update statement to set the systems which are centrally deployed to true
			updatePs = mainSustainmentFrame.createUpdatePreparedStatement(new String[]{"Central_Deployment"}, new String[]{"System"});
			while(rawIterator.hasNext()) {
				Object[] values = rawIterator.next().getValues();
				updatePs.setObject(1, "TRUE");
				updatePs.setObject(2, values[0]);
				updatePs.addBatch();
			}
			updatePs.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return mainSustainmentFrame;
	}

	private H2Frame appendSystemSiteCount(H2Frame mainSustainmentFrame) {
		// need to create the metadata for the new column
		String[] headers = new String[2];
		String[] dataTypes = new String[2];
		headers[0] = "System";
		headers[1] = "Num_Sites";
		dataTypes[0] = SemossDataType.STRING.toString();
		dataTypes[1] = SemossDataType.DOUBLE.toString();

		// we will merge the new headers into our existing frame
		mainSustainmentFrame.addNewColumn(headers, dataTypes, mainSustainmentFrame.getTableName());

		StringBuilder sysNumSitesQuery = new StringBuilder();
		sysNumSitesQuery.append("SELECT ?System (COUNT(?HostSite) AS ?NumSites) "
				+ "WHERE "
				+ "{ "
				+ "{?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInstallation>} "
				+ "{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} "
				+ "{?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemInstallation} "
				+ "{?HostSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Installation>} "
				+ "{?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?HostSite} "
				+ "} GROUP BY ?System ");
		sysNumSitesQuery.append( getHPSystemFilterString() );

		// get an iterator for the new data
		IRawSelectWrapper rawIterator = WrapperManager.getInstance().getRawWrapper(tapSite, sysNumSitesQuery.toString());
		// create an update statement
		PreparedStatement updatePs = mainSustainmentFrame.createUpdatePreparedStatement(new String[]{"Num_Sites"}, new String[]{"System"});
		try {
			while(rawIterator.hasNext()) {
				Object[] values = rawIterator.next().getValues();
				updatePs.setObject(1, values[1]);
				updatePs.setObject(2, values[0]);
				updatePs.addBatch();
			}
			updatePs.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return mainSustainmentFrame;
	}

	/**
	 * Get the system sustainment info
	 * @param waveSiteSystemSustainmentFrame 
	 * @param inflationArr
	 */
	private H2Frame appendSystemSustainmentInfo(H2Frame mainSustainmentFrame, double[] inflationArr) {
		LOGGER.info("Running query to get the systems and their costs");

		IEngine tapPortfolio = Utility.getEngine(MasterDatabaseUtility.testEngineIdIfAlias("TAP_Portfolio"));
		String systemSustainmentBudgetQuery = "SELECT DISTINCT ?System ?FY (SUM(?Cost) AS ?Cost) WHERE { "
				+ "BIND(<http://health.mil/ontologies/Concept/GLTag/Grand_Total> AS ?OMTag) "
				+ "{?OMTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag>} "
				+ "{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} "
				+ "{?SystemBudget <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemBudgetGLItem>} "
				+ "{?System <http://semoss.org/ontologies/Relation/Has> ?SystemBudget} "
				+ "{?SystemBudget <http://semoss.org/ontologies/Relation/TaggedBy> ?OMTag} "
				+ "{?SystemBudget <http://semoss.org/ontologies/Relation/Contains/Cost> ?Cost} "
				+ "{?FY <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag>} "
				+ "{?SystemBudget <http://semoss.org/ontologies/Relation/OccursIn> ?FY} "
				+ "} GROUP BY ?System ?FY ORDER BY ?System";
		StringBuilder systemSustainmentBudgetBuilder = new StringBuilder(systemSustainmentBudgetQuery);
		systemSustainmentBudgetBuilder.append(getHPSystemFilterString());
		Iterator<IHeadersDataRow> rawWrapper = WrapperManager.getInstance().getRawWrapper(tapPortfolio, systemSustainmentBudgetBuilder.toString());

		// okay, just going to insert into a temp h2frame
		// this will add everything as system, fy, and cost
		// then i will do a series of queries to fill a new frame
		// that will instead be system, fy1, fy2, .. fyn, and have the cost as the cell value
		String[] tempHeaders = new String[]{"System", "FY", "Cost"};
		Map<String, SemossDataType> tempDataType = new Hashtable<String, SemossDataType>();
		tempDataType.put("System", SemossDataType.STRING);
		tempDataType.put("FY", SemossDataType.STRING);
		tempDataType.put("Cost", SemossDataType.DOUBLE);
		H2Frame tempFrame = new H2Frame();
		tempFrame.addNewColumn(tempHeaders, new String[] {"String", "String", "Number"}, tempFrame.getTableName());
		tempFrame.addRowsViaIterator(rawWrapper, tempDataType);			

		int numFYs = inflationArr.length;
		int systemSustainmentFrameSize = numFYs+1;

		// get the list of all the systems we have cost data for
		String tempFrameName = tempFrame.getTableName();
		ResultSet rs = tempFrame.execQuery("SELECT DISTINCT System FROM " + tempFrameName);
		List<String> systemsWithCostList = new Vector<String>();
		try {
			while(rs.next()) {
				systemsWithCostList.add(rs.getString(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		// ASSUMPTION ::: We will only consider FY above 15
		// based on writing this, the schedule wont have any savings till fy18
		// so think this is fine
		String[] headers = new String[systemSustainmentFrameSize];
		String[] newColumnsForFrame = new String[systemSustainmentFrameSize - 1];
		String[] dataTypes = new String[systemSustainmentFrameSize];
		headers[0] = "System";
		dataTypes[0] = "String";

		// add new columns to h2 frame
		for (int i = 0; i < numFYs; i++) {
			headers[i + 1] = "FY" + (15 + i);
			newColumnsForFrame[i] = headers[i + 1];
			dataTypes[i + 1] = "Number";
		}

		// we will add the new headers into our existing frame
		mainSustainmentFrame.addNewColumn(headers, dataTypes, mainSustainmentFrame.getTableName());

		PreparedStatement updatePs = mainSustainmentFrame.createUpdatePreparedStatement(newColumnsForFrame, new String[] { "System" });

		try {
			// loop through and create the appropriate query on the tempframe for each system
			// and fill in the missing values using the inflation arr before inserting
			final String baseSysQuery = "SELECT * FROM " + tempFrameName + " WHERE SYSTEM = ";

			int numSystems = systemsWithCostList.size();
			for (int sysIndex = 0; sysIndex < numSystems; sysIndex++) {
				String systemName = systemsWithCostList.get(sysIndex);
				LOGGER.info("Running query for system = " + systemName);
				// this array will contain the values we want to insert into the 
				// system cost frame
				Object[] sysCostValues = new Object[systemSustainmentFrameSize];
				sysCostValues[0] = systemName;
				String sysQuery = baseSysQuery + "'" + systemName + "';";
				rs = tempFrame.execQuery(sysQuery);

				// loop through the values and assign the appropriate values in the positions
				while(rs.next()) {
					String fy = rs.getString(2);
					double cost = rs.getDouble(3);

					// get the correct position to set in the
					// sysCostValues array
					int position = -1;
					switch(fy) {
					case "FY15" : position = 1; break;
					case "FY16" : position = 2; break;
					case "FY17" : position = 3; break;
					case "FY18" : position = 4; break;
					case "FY19" : position = 5; break;
					case "FY20" : position = 6; break;
					case "FY21" : position = 7; break;
					case "FY22" : position = 8; break;
					case "FY23" : position = 9; break;
					case "FY24" : position = 10; break;
					case "FY25" : position = 11; break;
					case "FY26" : position = 12; break;
					case "FY27" : position = 13; break;
					case "FY28" : position = 14; break;
					case "FY29" : position = 15; break;
					case "FY30" : position = 16; break;
					case "FY31" : position = 17; break;
					case "FY32" : position = 18; break;
					}

					// if anything older than FY15, just ignore it
					if(position == -1 || position >= systemSustainmentFrameSize) {
						continue;
					}

					// assign the value in the array
					sysCostValues[position] = cost;
				}
				// close the connection as we are making quite a few of them
				rs.close();

				// all the values in the array have been filled based on
				// what sits in the cost databases
				// now we will fill based on the inflation values that we have
				// set values in array at index+1 since index 0 is system name
				for(int inflationIndex = 0; inflationIndex < numFYs; inflationIndex++) {
					// only need to update the values that we do not have
					if(sysCostValues[inflationIndex+1] == null || ((Number) sysCostValues[inflationIndex+1]).intValue() == 0) {
						// we got missing data!
						if(inflationIndex == 0) {
							// well, no cost info here, you are out of luck
							sysCostValues[inflationIndex+1] = 0;
						} else {
							double previousFySysValue = ((Number) sysCostValues[inflationIndex]).doubleValue();
							double savingsIn2015 = previousFySysValue / inflationArr[inflationIndex-1];
							double inflatedSavings = savingsIn2015 * inflationArr[inflationIndex];
							sysCostValues[inflationIndex+1] = inflatedSavings;
						}
					}
				}

				// okay, we are done accounting for inflation
				// now we just need to update the row
				for(int index = 1; index < sysCostValues.length; index++) {
					updatePs.setObject(index, sysCostValues[index]);
				}
				// set the join column being the system
				updatePs.setObject(sysCostValues.length, sysCostValues[0]);
				updatePs.addBatch();
			}

			// execute the update
			updatePs.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		tempFrame.close();
		
		return mainSustainmentFrame;
	}

	/**
	 * Generate the wave to site to system frame
	 * @param query
	 */
	private H2Frame createWavesSiteSystemFrame() {
		LOGGER.info("Running query to get the waves, sites, and systems");

		// string to get the wave, site, and system
		// note we append a limit on the systems we care about
		String waveSiteSystemQuery = "SELECT DISTINCT ?Wave ?HostSiteAndFloater ?System WHERE { "
				+ "{?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} "
				+ "{ "
				+ "{?HostSiteAndFloater <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Installation>} "
				+ "{?Wave <http://semoss.org/ontologies/Relation/Contains> ?HostSiteAndFloater} "
				+ "{?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInstallation>} "
				+ "{?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?HostSiteAndFloater} "
				+ "{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} "
				+ "{?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite} "
				+ "} "
				+ "UNION "
				+ "{ "
				+ "{?HostSiteAndFloater <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Floater>} "
				+ "{?HostSiteAndFloater <http://semoss.org/ontologies/Relation/Supports> ?Wave} "
				+ "{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} "
				+ "{?HostSiteAndFloater <http://semoss.org/ontologies/Relation/Supports> ?System} "
				+ "} "
				+ "} ORDER BY ?Wave";
		StringBuilder waveSiteSystemBuilder = new StringBuilder(waveSiteSystemQuery);
		waveSiteSystemBuilder.append(getHPSystemFilterString());

		// create a new frame
		String[] waveSiteSystemHeaders = new String[]{"Wave","HostSiteAndFloater","System"};
		H2Frame mainSustainmentFrame2 = new H2Frame(waveSiteSystemHeaders);
		// execute the query
		IEngine tapSite = Utility.getEngine(MasterDatabaseUtility.testEngineIdIfAlias("TAP_Site_Data"));
		Map<String, SemossDataType> dataTypes = new Hashtable<String, SemossDataType>();
		dataTypes.put("Wave", SemossDataType.STRING);
		dataTypes.put("HostSiteAndFloater", SemossDataType.STRING);
		dataTypes.put("System", SemossDataType.STRING);
		IRawSelectWrapper rawWrapper = WrapperManager.getInstance().getRawWrapper(tapSite, waveSiteSystemBuilder.toString());
		// add the data into the frame
		mainSustainmentFrame2.addRowsViaIterator(rawWrapper, dataTypes);

		LOGGER.info("Done creating wavesSiteSystemFrame");
		return mainSustainmentFrame2;
	}

	/**
	 * Get the filtered list of hp systems we want to use for this analysis
	 * 
	 * @return
	 */
	private String getHPSystemFilterString() {
		if(this.hpSystemFilterStr == null) {
			final String systemFilterQuery = "SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?System <http://semoss.org/ontologies/Relation/Contains/Device> 'N'} {?System <http://semoss.org/ontologies/Relation/Contains/Disposition> 'High'}{?System <http://semoss.org/ontologies/Relation/Contains/Review_Status> ?Review_Status}FILTER (?Review_Status in('FAC_Approved','FCLG_Approved')) }";
			// this will run and get the list of system with the requierd set of property values
			IEngine tapCore = Utility.getEngine(MasterDatabaseUtility.testEngineIdIfAlias("TAP_Core_Data"));
			Iterator<IHeadersDataRow> rawWrapper = WrapperManager.getInstance().getRawWrapper(tapCore, systemFilterQuery);
			StringBuilder waveSiteSystemBuilder = new StringBuilder(" BINDINGS ?System {");
			while(rawWrapper.hasNext()) {
				waveSiteSystemBuilder.append("(<http://health.mil/ontologies/Concept/System/").append(rawWrapper.next().getValues()[0]).append(">)");
			}
			waveSiteSystemBuilder.append("}");
			this.hpSystemFilterStr = waveSiteSystemBuilder.toString();
		}
		return this.hpSystemFilterStr;
	}

	/**
	 * Create the system site sustainment 
	 * @param inflationArr
	 */
	private void createSystemSiteSustainmentInfo(double[] inflationArr) {
		if(this.systemSiteSustainmentFrame == null) {
			LOGGER.info("Running query to get the systems and their costs");

			int numFYs = inflationArr.length;
			int systemSustainmentFrameSize = numFYs+2;
			// ASSUMPTION ::: We will only consider FY above 15
			// based on writing this, the schedule wont have any savings till fy18
			// so think this is fine
			String[] headers = new String[systemSustainmentFrameSize];
			String[] dataTypes = new String[systemSustainmentFrameSize];
			headers[0] = "System";
			headers[1] = "Site";
			dataTypes[0] = "String";
			dataTypes[1] = "String";
			for (int i = 0; i < numFYs; i++) {
				headers[i + 2] = "FY" + (15 + i);
				dataTypes[i + 2] = "Number";
			}
			this.systemSiteSustainmentFrame = new H2Frame();
			this.systemSiteSustainmentFrame.addNewColumn(headers, dataTypes, this.systemSiteSustainmentFrame.getTableName());

			IEngine tapPortfolio = Utility.getEngine(MasterDatabaseUtility.testEngineIdIfAlias("TAP_Portfolio"));
			String[] systemSiteSustainmentQueryArr = new String[2];
			systemSiteSustainmentQueryArr[0] = "SELECT DISTINCT ?System ?Site ?Cost ?FYTag WHERE { "
					+ "{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} "
					+ "{?SysSiteGLItem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemSiteSupportGLItem>} "
					+ "{?System <http://semoss.org/ontologies/Relation/Has> ?SysSiteGLItem} "
					+ "{?Site <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>} "
					+ "{?Site <http://semoss.org/ontologies/Relation/Has> ?SysSiteGLItem} "
					+ "{?SysSiteGLItem <http://semoss.org/ontologies/Relation/Contains/Cost> ?Cost} "
					+ "{?FYTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag>} "
					+ "{?SysSiteGLItem <http://semoss.org/ontologies/Relation/OccursIn> ?FYTag} "
					+ "} ORDER BY ?System ?Site";
			systemSiteSustainmentQueryArr[1] = "SELECT DISTINCT ?System ?Site ?Cost ?FYTag WHERE { "
					+ "{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} "
					+ "{?FloaterGLItem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FloaterGLItem>} "
					+ "{?System <http://semoss.org/ontologies/Relation/Has> ?FloaterGLItem} "
					+ "{?Site <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Floater>} "
					+ "{?Site <http://semoss.org/ontologies/Relation/Has> ?FloaterGLItem} "
					+ "{?FloaterGLItem <http://semoss.org/ontologies/Relation/Contains/Cost> ?Cost} "
					+ "{?FYTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FYTag>} "
					+ "{?FloaterGLItem <http://semoss.org/ontologies/Relation/OccursIn> ?FYTag} "
					+ "} ORDER BY ?System ?Site";

			for(String query : systemSiteSustainmentQueryArr) {
				Iterator<IHeadersDataRow> rawWrapper = WrapperManager.getInstance().getRawWrapper(tapPortfolio, query);

				// okay, just going to insert into a temp h2frame
				// this will add everything as system, site, fy, and cost
				// then i will do a series of queries to fill a new frame
				// that will instead be system, site, fy1, fy2, .. fyn, and have the cost as the cell value
				String[] tempHeaders = new String[]{"System", "Site", "FYTag", "Cost"};
				Map<String, SemossDataType> tempDataType = new Hashtable<String, SemossDataType>();
				tempDataType.put("System", SemossDataType.STRING);
				tempDataType.put("Site", SemossDataType.STRING);
				tempDataType.put("FYTag", SemossDataType.STRING);
				tempDataType.put("Cost", SemossDataType.DOUBLE);
				H2Frame tempFrame = new H2Frame(tempHeaders);
				tempFrame.addRowsViaIterator(rawWrapper, tempDataType);			
	
				// get the list of all the systems we have cost data for
				String tempFrameName = tempFrame.getTableName();
				ResultSet rs = tempFrame.execQuery("SELECT DISTINCT System, Site FROM " + tempFrameName);
				List<String> systemsSiteComboList = new Vector<String>();
				try {
					while(rs.next()) {
						systemsSiteComboList.add(rs.getString(1) + "+++" + rs.getString(2));
					}
				} catch (SQLException e) {
					e.printStackTrace();
				} finally {
					try {
						rs.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}

				try {
					// loop through and create the appropriate query on the tempframe for each system
					// and fill in the missing values using the inflation arr before inserting
					final String baseSysQuery = "SELECT System, Site, FYTag, Cost FROM " + tempFrameName + " WHERE ";

					int numSysSiteCombos = systemsSiteComboList.size();
					for(int sysIndex = 0; sysIndex < numSysSiteCombos; sysIndex++) {
						String[] systemSiteCombo = systemsSiteComboList.get(sysIndex).split("\\+\\+\\+");
						String systemName = systemSiteCombo[0];
						String siteName = systemSiteCombo[1];
						LOGGER.info("Running query for system = " + systemName + " and site = " + siteName);
						// this array will contain the values we want to insert into the 
						// system cost frame
						Object[] sysCostValues = new Object[systemSustainmentFrameSize];
						sysCostValues[0] = systemName;
						sysCostValues[1] = siteName;
						String sysQuery = baseSysQuery + " SYSTEM = '" + systemName + "' AND SITE ='" + siteName + "';";
						rs = tempFrame.execQuery(sysQuery);

						// loop through the values and assign the appropriate values in the positions
						while(rs.next()) {
							String fy = rs.getString(3);
							double cost = rs.getDouble(4);

							// get the correct position to set in the
							// sysCostValues array
							int position = -1;
							switch(fy) {
							case "FY15" : position = 2; break;
							case "FY16" : position = 3; break;
							case "FY17" : position = 4; break;
							case "FY18" : position = 5; break;
							case "FY19" : position = 6; break;
							case "FY20" : position = 7; break;
							case "FY21" : position = 8; break;
							case "FY22" : position = 9; break;
							case "FY23" : position = 10; break;
							case "FY24" : position = 11; break;
							case "FY25" : position = 12; break;
							case "FY26" : position = 13; break;
							case "FY27" : position = 14; break;
							case "FY28" : position = 15; break;
							case "FY29" : position = 16; break;
							case "FY30" : position = 17; break;
							case "FY31" : position = 18; break;
							case "FY32" : position = 19; break;
							}

							// if anything older than FY15, just ignore it
							if(position == -1 || position >= systemSustainmentFrameSize) {
								continue;
							}

							// assign the value in the array
							sysCostValues[position] = cost;
						}
						// close the connection as we are making quite a few of them
						rs.close();

						// all the values in the array have been filled based on
						// what sits in the cost databases
						// now we will fill based on the inflation values that we have
						// set values in array at index+1 since index 0 is system name
						for(int inflationIndex = 0; inflationIndex < numFYs; inflationIndex++) {
							// only need to update the values that we do not have
							if(sysCostValues[inflationIndex+2] == null || ((Number) sysCostValues[inflationIndex+2]).intValue() == 0) {
								// we got missing data!
								if(inflationIndex == 0) {
									// well, no cost info here, you are out of luck
									sysCostValues[inflationIndex+2] = 0.0;
								} else {
									double previousFySysValue = ((Number) sysCostValues[inflationIndex+1]).doubleValue();
									double savingsIn2015 = previousFySysValue / inflationArr[inflationIndex-1];
									double inflatedSavings = savingsIn2015 * inflationArr[inflationIndex];
									sysCostValues[inflationIndex+2] = inflatedSavings;
								}
							}
						}

						// okay, we are done accounting for inflation
						// now we just need to insert the row
						this.systemSiteSustainmentFrame.addRow(sysCostValues, headers);
					}

					// drop the intermediary temp frame
					tempFrame.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}

			// iterate through the results for testing
			//			LOGGER.info("Testing data...");
			//			Iterator<Object[]> it = this.systemSiteSustainmentFrame.iterator();
			//			while(it.hasNext()) {
			//				System.out.println(">>> " + Arrays.toString( it.next() ) );
			//			}
			//			LOGGER.info("Done iterating through system sustainment data");
		}
	}

	public H2Frame getMainSustainmentFrame() {
		return mainSustainmentFrame;
	}

	public H2Frame getSystemSiteSustainmentFrame() {
		return systemSiteSustainmentFrame;
	}

	public Map<String, String[]> getWaveStartEndDate() {
		return waveStartEndDate;
	}

	public int getNumColumns() {
		return numColumns;
	}

	public double getPercentRealized() {
		return percentRealized;
	}

	public IEngine getTapSite() {
		return tapSite;
	}

}
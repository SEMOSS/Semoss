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

import java.awt.GridBagConstraints;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;

import javax.swing.JButton;

import prerna.engine.api.IEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Heat Map that shows how much a System Supports a Capability
 */
public class SysCapSimHeatMapSheet extends SimilarityHeatMapSheet {

	private final String hrCoreDB = "HR_Core";
	private String fileLoc;
	private IEngine coreDB = (IEngine) DIHelper.getInstance().getLocalProp(hrCoreDB);
	private final String capabilityQuery = "SELECT DISTINCT ?Capability WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>}}";
	private final String businessProcessQuery = "SELECT DISTINCT ?BusinessProcess WHERE {{?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>}}";	
	String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
	private ArrayList<String> systemNames;
	private ArrayList<String> capabilityNames;
	private JButton excelExportButton;
	
	/**
	 * Constructor for CapSimHeatMapSheet.
	 */
	public SysCapSimHeatMapSheet() {
		super();	
	}	
	
	public void createData() {
		capabilityNames = new ArrayList<String>();
		systemNames = new ArrayList<String>();
		addPanel();
		SimilarityFunctions sdf = new SimilarityFunctions();	
		SysBPCapInsertProcessor processor = new SysBPCapInsertProcessor(0.0, 0.0, "AND");
		String comparisonType = this.query;
		logger.info("Creating " + comparisonType + " to System Heat Map.");
		
		if (comparisonType.equals("Capability"))
		{
			allHash.put("xAxisTitle", "Capability");
			updateProgressBar("10%...Getting " + comparisonType + " list for evaluation", 10);
			comparisonObjectList = sdf.createComparisonObjectList(hrCoreDB, capabilityQuery);
			setComparisonObjectTypes("Capability", "System");
			updateProgressBar("35%...Querying Data", 35);
			processor.genStorageInformation(coreDB, comparisonType);
		} else if (comparisonType.equals("BusinessProcess"))
		{
			allHash.put("xAxisTitle", "BusinessProcess");
			updateProgressBar("10%...Getting " + comparisonType + " list for evaluation", 10);
			comparisonObjectList = sdf.createComparisonObjectList(hrCoreDB, businessProcessQuery);
			setComparisonObjectTypes("BusinessProcess", "System");
			updateProgressBar("35%...Querying Data", 35);
			processor.genStorageInformation(coreDB, comparisonType);
		}

		sdf.setComparisonObjectList(comparisonObjectList);
		HashMap getData = new HashMap();
		
		Hashtable resultHash = new Hashtable();
		resultHash.putAll(processor.storageHash);
		
		// comparisonObjectList contains every capability, even the ones not shown on heatmap
		// causing the program to crash due to too many system capability combinations
		for (String capabilityKey : processor.storageHash.get("Data").keySet())
		{
			capabilityNames.add(capabilityKey);
			for (String systemKey : processor.storageHash.get("Data").get(capabilityKey).keySet())
			{
				if (!systemNames.contains(systemKey))
					systemNames.add(systemKey);
			}
		}
		for (String capabilityKey : processor.storageHash.get("BLU").keySet())
		{
			if (!capabilityNames.contains(capabilityKey))
				capabilityNames.add(capabilityKey);
			for (String systemKey : processor.storageHash.get("BLU").get(capabilityKey).keySet())
			{
				if (!systemNames.contains(systemKey))
					systemNames.add(systemKey);
			}
		}

		updateProgressBar("50%...Evaluating Data Objects Created for a " + comparisonType, 50);
		getData = (HashMap) resultHash.get(processor.DATAC);
		Hashtable dataCScoreHash = convertMapToTable(getData);
		logger.info("Finished Data Objects Created Processing.");
		dataCScoreHash = processHashForCharting(dataCScoreHash);		
		
		/*updateProgressBar("60%...Evaluating Data Objects Read for a Capability", 60);
		Hashtable dataRScoreHash = (Hashtable) resultHash.get(processor.DATAR);
		logger.info("Finished Data Objects Read Processing.");
		dataRScoreHash = processHashForCharting(dataRScoreHash);*/		
		
		updateProgressBar("70%...Evaluating Business Logic Provided for a " + comparisonType, 70);
		getData.clear();
		getData = (HashMap) resultHash.get(processor.BLU);
		Hashtable bluScoreHash = convertMapToTable(getData);
		logger.info("Finished Business Logic Provided Processing.");
		bluScoreHash = processHashForCharting(bluScoreHash);		
			
		updateProgressBar("80%...Creating Heat Map Visualization", 80);
	
		paramDataHash.put("Data_Objects_Created", dataCScoreHash);
		//paramDataHash.put("Data_Objects_Read", dataRScoreHash);
		paramDataHash.put("Business_Logic_Provided", bluScoreHash);
		
		allHash.put("title",  "Systems Support " + comparisonType);		
		allHash.put("yAxisTitle", "System");
		allHash.put("value", "Score");
		allHash.put("sysDup", false);
	}
	
	private Hashtable convertMapToTable(HashMap entry) {
		Hashtable returnHash = new Hashtable();
		for(Object key: entry.keySet()){
			HashMap innerMap = new HashMap();
			Hashtable returnInnerTable = new Hashtable();
			innerMap = (HashMap) entry.get(key);
			for(Object innerKey : innerMap.keySet()) {
				returnInnerTable.put(innerKey, innerMap.get(innerKey));
			}
			returnHash.put(key, returnInnerTable);
		}
		
		return returnHash;
	}
	
	@Override
	public void createControlPanel()
	{
		super.createControlPanel();
		addExportExcelButton(5);
		
		this.controlPanel.setPlaySheet(this);
	}
	
	public void addExportExcelButton(int gridXidx)
	{
		excelExportButton = new JButton("Export to Excel");
		
		// SimHeatMapExcelExportListener excelExportListener = new SimHeatMapExcelExportListener();
		excelExportButton.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				writeToFile();
				Utility.showMessage("Export successful: " + fileLoc);
			}
		});
		excelExportButton.setToolTipText("Export percentage data to excel");
		
		GridBagConstraints gbc_excelExportButton = new GridBagConstraints();
		gbc_excelExportButton.insets = new Insets(10, 0, 0, 5);
		gbc_excelExportButton.anchor = GridBagConstraints.WEST;
		gbc_excelExportButton.gridx = gridXidx;
		gbc_excelExportButton.gridy = 0;
		controlPanel.add(excelExportButton, gbc_excelExportButton);
	}

	/**
	 * Create the report file name and location, and call the writer to write the report for the specified system Create the
	 * location for the fact sheet report template
	 * 
	 * @param databaseName
	 *            String containing the system name to produce the fact sheet
	 * @param systemDataHash
	 *            Hashtable containing the results for the query for the specified system
	 */
	public void writeToFile()
	{
		SysCapSimHeatMapWriter writer = new SysCapSimHeatMapWriter();
		String folder = System.getProperty("file.separator") + "export" + System.getProperty("file.separator") + "Reports"
				+ System.getProperty("file.separator");
		String writeFileName = "SystemCapabilityHeatMap_for_" + coreDB.getEngineName().replaceAll(":", "") + "_"
				+ DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(new Date()).replace(":", "").replaceAll(" ", "_")
				+ ".xlsx";
		
		fileLoc = workingDir + folder + writeFileName;
		String templateFileName = "HeatMap_Template.xlsx";
		String templateLoc = workingDir + folder + templateFileName;
		logger.info(fileLoc);
		
		writer.exportExcelPercent(fileLoc, templateLoc, capabilityNames, systemNames, paramDataHash);
	}
}

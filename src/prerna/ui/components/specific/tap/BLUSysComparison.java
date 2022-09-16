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
package prerna.ui.components.specific.tap;

import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.beans.PropertyVetoException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.swing.JPanel;

import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.main.listener.impl.BrowserPlaySheetListener;

/**
 * Heat Map that shows what gaps in data exist for systems to suppor their BLUs
 */
public class BLUSysComparison extends SimilarityHeatMapSheet {
	private static final long serialVersionUID = 1L;

	transient List<String> systemNamesList = new ArrayList<>();
	private static final String MASTER_QUERY = "SELECT DISTINCT ?System ?BLU ?Data (IF (BOUND (?Provide), 'Needed and Present', IF( BOUND(?ICD), 'Needed and Present', 'Needed but not Present')) AS ?Status) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} OPTIONAL{ { {?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface>;} {?Consume <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} {?ICD ?Consume ?System} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?Payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?ICD ?Payload ?Data} } UNION { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?Provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?System ?Provide ?Data} } } { SELECT DISTINCT ?System ?BLU ?Data WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} {?Provide2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?System ?Provide2 ?BLU} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?Requires <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Requires>;} {?BLU ?Requires ?Data} } } }";
	private static final String SYSTEM_KEY = "System";
	private static final String BLU_KEY = "BLU";
	private static final String DATA_KEY = "Data";
	private static final String STATUS_KEY = "Status";
	private static final String STATUS_TRUE_KEY = "Needed and Present";
	private static final String VALUE_STRING = "Score";
	private static final String KEY_STRING = "key";
	private static final String BLU_DATA = "BLU-Data";
	SysToBLUDataGapsPlaySheet playSheet;
	transient List<Object[]> list;
	String[] names;

	@Override
	public List<Object[]> getList() {
		return this.list;
	}

	@Override
	public String[] getNames() {
		return this.names;
	}

	/**
	 * Constructor for CapSimHeatMapSheet.
	 */
	public BLUSysComparison() {
		super();
	}

	@Override
	public void addPanel() {
//		browser = new Browser();
//		browserView = new BrowserView(browser);
		try {
			JPanel mainPanel = new JPanel();
			BrowserPlaySheetListener psListener = new BrowserPlaySheetListener();
			this.addInternalFrameListener(psListener);
			this.setContentPane(mainPanel);
			GridBagLayout gblMainPanel = new GridBagLayout();
			gblMainPanel.columnWidths = new int[] { 0, 0 };
			gblMainPanel.rowHeights = new int[] { 0, 0 };
			gblMainPanel.columnWeights = new double[] { 1.0, Double.MIN_VALUE };
			gblMainPanel.rowWeights = new double[] { 1.0, Double.MIN_VALUE };
			mainPanel.setLayout(gblMainPanel);

			JPanel panel = new JPanel();
			panel.setLayout(new BorderLayout());
//			panel.add(browserView, BorderLayout.CENTER);
			GridBagConstraints gbcScrollPane = new GridBagConstraints();
			gbcScrollPane.fill = GridBagConstraints.BOTH;
			gbcScrollPane.gridx = 0;
			gbcScrollPane.gridy = 0;
			mainPanel.add(panel, gbcScrollPane);

			updateProgressBar("0%...Preprocessing", 0);
			resetProgressBar();
			JPanel barPanel = new JPanel();

			GridBagConstraints gbcBarPanel = new GridBagConstraints();
			gbcBarPanel.fill = GridBagConstraints.BOTH;
			gbcBarPanel.gridx = 0;
			gbcBarPanel.gridy = 1;
			mainPanel.add(barPanel, gbcBarPanel);
			barPanel.setLayout(new BorderLayout(0, 0));
			barPanel.add(jBar, BorderLayout.CENTER);

			playSheet.HeatPanel.add(this);

			this.setVisible(true);
			this.setSelected(false);
			this.setSelected(true);
			logger.debug("Added the main pane");
		} catch (PropertyVetoException e) {
			logger.debug(e.getStackTrace());
		}
	}

	public void createData(List<String> bluNames, List<String> sysNames) {

		addPanel();
		String comparisonType = "System-BLU Comparison";
		logger.info("Creating " + comparisonType + " Heat Map.");
		updateProgressBar("10%...Getting " + comparisonType + " list for evaluation", 10);

		ISelectWrapper mainWrapper = WrapperManager.getInstance().getSWrapper(this.engine, MASTER_QUERY);
		names = mainWrapper.getVariables();
		processWrapper(mainWrapper, bluNames, sysNames);
		averageAdder();

		// Creating keyHash from paramDataHash
		updateProgressBar("70%...Evaluating Business Logic Provided for a " + comparisonType, 70);

		logger.info(systemNamesList);

		allHash.put("xAxisTitle", "BLU");
		allHash.put("title", "Systems Support " + comparisonType);
		allHash.put("yAxisTitle", "System");
		allHash.put("value", "Score");
		allHash.put("sysDup", false);
	}

	private void processWrapper(ISelectWrapper sjw, List<String> bluNames, List<String> sysNames){

		Hashtable<String, Hashtable<String, Object>> bluDataHash = new Hashtable<>();

		try {
			while (sjw.hasNext()) {
				ISelectStatement sjss = sjw.next();

				Hashtable<String, Object> systemBLUHash = new Hashtable<>();
				Hashtable<String, Object> keySystemBLUHash = new Hashtable<>();
				Hashtable<String, Double> innerDataHash = new Hashtable<>();
				String systemTemp = sjss.getVar(SYSTEM_KEY) + "";
				String bluTemp = sjss.getVar(BLU_KEY) + "";
				String dataTemp = "";
				String statusTemp = "";
				String sysBLUTemp = "";
				Double dataValueTemp = 0.0;

				if (bluNames.contains(sjss.getVar(BLU_KEY)) && sysNames.contains(sjss.getVar(SYSTEM_KEY))) {
					dataTemp = sjss.getVar(DATA_KEY) + "";
					statusTemp = sjss.getVar(STATUS_KEY) + "";

					if (statusTemp.equals(STATUS_TRUE_KEY)) {
						dataValueTemp = 100.0;
					}

					sysBLUTemp = systemTemp + "-" + bluTemp;

					if (!bluDataHash.containsKey(sysBLUTemp)) {
						innerDataHash.put(dataTemp, dataValueTemp);
						systemBLUHash.put("System", systemTemp);
						systemBLUHash.put("BLU", bluTemp);
						systemBLUHash.put("Data", innerDataHash);
						bluDataHash.put(sysBLUTemp, systemBLUHash);

						// for keyHash
						keySystemBLUHash.put("System", systemTemp);
						keySystemBLUHash.put("BLU", bluTemp);
						keyHash.put(sysBLUTemp, keySystemBLUHash);

					} else {
						systemBLUHash = bluDataHash.get(sysBLUTemp);
						innerDataHash = (Hashtable<String, Double>) systemBLUHash.get("Data");
						innerDataHash.put(dataTemp, dataValueTemp);
					}
				}
			}
			paramDataHash.put(BLU_DATA, bluDataHash);

		} catch (RuntimeException e) {
			logger.fatal(e);
		}
	}

	private void averageAdder() {
		Hashtable<String, Hashtable<String, Object>> bluDataHash = paramDataHash.get(BLU_DATA);

		Set<String> sysBLUKeys = bluDataHash.keySet();
		try {
			for (String sysBLUKey : sysBLUKeys) {
				Hashtable<String, Object> systemBLUHash = bluDataHash.get(sysBLUKey);
				Hashtable<String, Double> innerDataHash = (Hashtable<String, Double>) systemBLUHash.get("Data");
				Double numerator = 0.0;
				Double denominator = 0.0;
				Double average = null;

				Set<String> dataKeys = innerDataHash.keySet();
				for (String dataKey : dataKeys) {
					numerator += innerDataHash.get(dataKey);
					denominator++;
				}
				
				if (denominator != 0) {
					average = (numerator / denominator);
				}
				systemBLUHash.put("Score", average);
			}
		} catch (RuntimeException e) {
			logger.fatal(e);
		}
	}

	@Override
	public List retrieveValues(List<String> selectedVars, Map<String, Double> minimumWeights, String key) {
		List<Hashtable> retHash = new ArrayList<>();
		Hashtable<String, Hashtable<String, Object>> bluDataHash = paramDataHash.get(BLU_DATA);
		Hashtable<String, Object> systemBLUHash = bluDataHash.get(key);
		Hashtable<String, Double> innerDataHash = (Hashtable<String, Double>) systemBLUHash.get("Data");
		Set<String> innerDataKeys = innerDataHash.keySet();

		for (String innerDataKey : innerDataKeys) {
			Hashtable newHash = new Hashtable();
			newHash.put(KEY_STRING, innerDataKey);
			newHash.put(VALUE_STRING, innerDataHash.get(innerDataKey));
			retHash.add(newHash);
		}
		return retHash;
	}

	public void setPlaySheet(IPlaySheet playSheet) {
		this.playSheet = (SysToBLUDataGapsPlaySheet) playSheet;
	}
}

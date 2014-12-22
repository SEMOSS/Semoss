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
package prerna.ui.components.specific.tap;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.beans.PropertyVetoException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

import javax.swing.JPanel;
import javax.swing.JSplitPane;
import javax.swing.JTable;

import com.google.gson.Gson;
import com.teamdev.jxbrowser.chromium.BrowserFactory;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.ChartControlPanel;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.main.listener.impl.BrowserPlaySheetListener;
import prerna.util.DIHelper;

/**
 * Heat Map that shows what gaps in data exist for systems to suppor their BLUs
 */
public class BLUSysComparison extends SimilarityHeatMapSheet{

	String hrCoreDB = "HR_Core";
	private IEngine coreDB = (IEngine) DIHelper.getInstance().getLocalProp(hrCoreDB);
	public ArrayList<String> systemNamesList = new ArrayList<String>();
	String masterQuery = "SELECT DISTINCT ?System ?BLU ?Data (IF (BOUND (?Provide), 'Needed and Present', IF( BOUND(?ICD), 'Needed and Present', 'Needed but not Present')) AS ?Status) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} OPTIONAL{ { {?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>;} {?Consume <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} {?ICD ?Consume ?System} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?Payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?ICD ?Payload ?Data} } UNION { {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?Provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?System ?Provide ?Data} } } { SELECT DISTINCT ?System ?BLU ?Data WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} {?Provide2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?System ?Provide2 ?BLU} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?Requires <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Requires>;} {?BLU ?Requires ?Data} } } }";
	String systemKey = "System";
	String BLUKey = "BLU";
	String dataKey = "Data";
	String statusKey = "Status";
	String statusTrueKey = "Needed and Present";
	final String valueString = "Score";
	final String keyString = "key";
	ArrayList<String> BLUNames = new ArrayList<String>();
	ArrayList<String> sysNames = new ArrayList<String>();
	ChartControlPanel controlPanel;
	SysToBLUDataGapsPlaySheet playSheet;


	/**
	 * Constructor for CapSimHeatMapSheet.
	 */
	public BLUSysComparison() {
		super();	
	}	

	@Override
	public void addPanel()
	{
		browser = BrowserFactory.create();
		try {
//			table = new JTable();
			JPanel mainPanel = new JPanel();
//			setWindow();
//			createControlPanel();
			
			BrowserPlaySheetListener psListener = new BrowserPlaySheetListener();
			this.addInternalFrameListener(psListener);
			this.setContentPane(mainPanel);
			GridBagLayout gbl_mainPanel = new GridBagLayout();
			gbl_mainPanel.columnWidths = new int[]{0, 0};
			gbl_mainPanel.rowHeights = new int[]{0, 0};
			gbl_mainPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
			gbl_mainPanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
			mainPanel.setLayout(gbl_mainPanel);
			
			JPanel panel = new JPanel();
			panel.setLayout(new BorderLayout());
			panel.add(browser.getView().getComponent(), BorderLayout.CENTER);
			GridBagConstraints gbc_scrollPane = new GridBagConstraints();
			gbc_scrollPane.fill = GridBagConstraints.BOTH;
			gbc_scrollPane.gridx = 0;
			gbc_scrollPane.gridy = 0;
			mainPanel.add(panel, gbc_scrollPane);

			updateProgressBar("0%...Preprocessing", 0);
			resetProgressBar();
			JPanel barPanel = new JPanel();

			GridBagConstraints gbc_barPanel = new GridBagConstraints();
			gbc_barPanel.fill = GridBagConstraints.BOTH;
			gbc_barPanel.gridx = 0;
			gbc_barPanel.gridy = 1;
			mainPanel.add(barPanel, gbc_barPanel);
			barPanel.setLayout(new BorderLayout(0, 0));
			barPanel.add(jBar, BorderLayout.CENTER);

			playSheet.HeatPanel.add(this);

//			this.pack();
			this.setVisible(true);
			this.setSelected(false);
			this.setSelected(true);
			logger.debug("Added the main pane");
		}
		catch (PropertyVetoException e) {
			e.printStackTrace();
		}
	}
	
	public void createData(ArrayList<String> BLUNames, ArrayList<String> sysNames) {

		addPanel();
		SimilarityFunctions simfns = new SimilarityFunctions();	
		String comparisonType = "System-BLU Comparison";
		logger.info("Creating " + comparisonType + " Heat Map.");
		updateProgressBar("10%...Getting " + comparisonType + " list for evaluation", 10);
		
		//Creating hashtable from main query 
		SesameJenaSelectWrapper mainWrapper = new SesameJenaSelectWrapper();
		mainWrapper.setQuery(masterQuery);
		mainWrapper.setEngine(coreDB);
		mainWrapper.executeQuery();
		names = mainWrapper.getVariables();
		processWrapper(mainWrapper, names, BLUNames, sysNames);
		averageAdder();
		
		//Creating keyHash from paramDataHash
		updateProgressBar("70%...Evaluating Business Logic Provided for a " + comparisonType, 70);		
		
		logger.info(systemNamesList);

		allHash.put("xAxisTitle", "BLU");
		allHash.put("title",  "Systems Support " + comparisonType);
		allHash.put("yAxisTitle", "System");
		allHash.put("value", "Score");
		allHash.put("sysDup", false);
		
	}
	
	private void processWrapper(SesameJenaSelectWrapper sjw, String[] names, ArrayList<String> BLUNames, ArrayList<String> sysNames){
		
		Hashtable<String, Hashtable<String, Object>> BLUDataHash = new Hashtable<String, Hashtable<String, Object>>();
		try {
			while(sjw.hasNext())
			{
				SesameJenaSelectStatement sjss = sjw.next();
				
				Hashtable<String, Object> systemBLUHash = new Hashtable<String, Object>();
				Hashtable<String, Object> keySystemBLUHash = new Hashtable<String, Object>();
				Hashtable<String, Double> innerDataHash = new Hashtable<String, Double>();
				String systemTemp = sjss.getVar(systemKey) + "";
				String BLUTemp = sjss.getVar(BLUKey) + "";
				String dataTemp = "";
				String statusTemp = "";
				String sysBLUTemp = "";
				Double dataValueTemp = 0.0;
				
				if(BLUNames.contains(sjss.getVar(BLUKey)) && sysNames.contains(sjss.getVar(systemKey))){
					dataTemp = sjss.getVar(dataKey) + "";
					statusTemp = sjss.getVar(statusKey) + "";
				
					if(statusTemp.equals(statusTrueKey)){
						dataValueTemp = 100.0;
					}
					
					sysBLUTemp = systemTemp + "-" + BLUTemp;
					
					if(!BLUDataHash.containsKey(sysBLUTemp)){
						innerDataHash.put(dataTemp, dataValueTemp);
						systemBLUHash.put("System", systemTemp);
						systemBLUHash.put("BLU", BLUTemp);
						systemBLUHash.put("Data", innerDataHash);
						BLUDataHash.put(sysBLUTemp, systemBLUHash);
						
						//for keyHash
						keySystemBLUHash.put("System", systemTemp);
						keySystemBLUHash.put("BLU", BLUTemp);
						keyHash.put(sysBLUTemp, keySystemBLUHash);

					}
					else{
						systemBLUHash = (Hashtable<String, Object>) BLUDataHash.get(sysBLUTemp);
						innerDataHash = (Hashtable<String, Double>) systemBLUHash.get("Data");
						innerDataHash.put(dataTemp, dataValueTemp);
					}
				}
			}
			paramDataHash.put("BLU-Data", BLUDataHash);

		} 
		catch (RuntimeException e) {
			logger.fatal(e);
		}	
	}
	
	private void averageAdder(){
		Hashtable<String, Hashtable<String, Object>> BLUDataHash = new Hashtable<String, Hashtable<String, Object>>();
		
		BLUDataHash = (Hashtable<String, Hashtable<String, Object>>) paramDataHash.get("BLU-Data"); 
		
		Set<String> sysBLUKeys = BLUDataHash.keySet();
		try {		
			for(String sysBLUKey: sysBLUKeys){
				Hashtable<String, Object> systemBLUHash = new Hashtable<String, Object>();
				Hashtable<String, Double> innerDataHash = new Hashtable<String, Double>();	
				
				systemBLUHash = (Hashtable<String, Object>) BLUDataHash.get(sysBLUKey);
				innerDataHash = (Hashtable<String, Double>) systemBLUHash.get("Data");
	
				Double numerator = 0.0;
				Double denominator = 0.0;
				Set<String> dataKeys = innerDataHash.keySet();
				for(String dataKey: dataKeys){
					numerator += innerDataHash.get(dataKey);
					denominator++;
				}
				Double average = (double) (numerator/denominator);
				systemBLUHash.put("Score", average);	
			}
		} catch (RuntimeException e) {
			logger.fatal(e);
		}
	}

	public ArrayList retrieveValues(ArrayList<String> selectedVars, Hashtable<String, Double>minimumWeights, String key){
		ArrayList<Hashtable> retHash = new ArrayList<Hashtable>();
		Hashtable<String, Hashtable<String, Object>> BLUDataHash = new Hashtable<String, Hashtable<String, Object>>();
		BLUDataHash = (Hashtable<String, Hashtable<String, Object>>) paramDataHash.get("BLU-Data");
		Hashtable<String, Object> systemBLUHash = new Hashtable<String, Object>();		
		systemBLUHash = (Hashtable<String, Object>) BLUDataHash.get(key);
		Hashtable<String, Double> innerDataHash = new Hashtable<String, Double>();	
		innerDataHash = (Hashtable<String, Double>) systemBLUHash.get("Data");
		Set<String> innerDataKeys = innerDataHash.keySet();
		for(String innerDataKey: innerDataKeys){
			Hashtable newHash = new Hashtable();
			newHash.put(keyString, innerDataKey);
			newHash.put(valueString, innerDataHash.get(innerDataKey));
			retHash.add(newHash);
		}			
		return retHash;
	}
	
	public void setPlaySheet(IPlaySheet playSheet) {
		this.playSheet = (SysToBLUDataGapsPlaySheet) playSheet;
	}
}



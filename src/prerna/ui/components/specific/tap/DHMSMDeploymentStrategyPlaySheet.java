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

import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.JToggleButton;
import javax.swing.border.BevelBorder;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.main.listener.specific.tap.DHMSMDeploymentStrategyRestoreDefaultsListener;
import prerna.ui.main.listener.specific.tap.DHMSMDeploymentStrategyRunBtnListener;
import prerna.ui.main.listener.specific.tap.DHMSMDeploymentStrategySetRegionListener;
import prerna.ui.swing.custom.CustomButton;
import prerna.ui.swing.custom.ToggleButton;
import prerna.util.DIHelper;
import prerna.util.Utility;
import aurelienribon.ui.css.Style;


/**
 * This is the playsheet used exclusively for TAP service optimization.
 */
@SuppressWarnings("serial")
public class DHMSMDeploymentStrategyPlaySheet extends InputPanelPlaySheet{
	private static final Logger LOGGER = LogManager.getLogger(DHMSMDeploymentStrategyPlaySheet.class.getName());

	//components for single begin/end date of deployment
	private JPanel timePanel;
	//begin/end quarter/year fields with user entries for deployment
	private JTextField qBeginField, yBeginField, qEndField, yEndField;
	//default begin/end quarter/year for deployment.
	private int qBeginDefault, yBeginDefault, qEndDefault, yEndDefault;

	//toggle to select specific region begin/end dates
	private JToggleButton selectRegionTimesButton;

	//list of regions
	private ArrayList<String> regionsList;
	//waves in each region and their order
	private Hashtable<String, List<String>> regionWaveHash;
	private ArrayList<String> waveOrder;
	//start and end of each wave
	private HashMap<String, String[]> waveStartEndDate;
	
	//components for specific region begin/end dates
	private JPanel regionTimePanel;
	//hashtables holding the begin/end quarters/years fields with user entries for each region.
	private Hashtable<String,JTextField> qBeginFieldHash, yBeginFieldHash, qEndFieldHash, yEndFieldHash;
	//hashtables with default begin/end quarters/years for each region.
	private Hashtable<String,Integer> qBeginDefaultHash, yBeginDefaultHash, qEndDefaultHash, yEndDefaultHash;

	//button to restore defaults
	private JButton restoreDefaultsButton;
	
	//button to run algorithm
	private JButton runButton;
	
	//display tabs
	public JPanel siteAnalysisPanel = new JPanel();

	public DHMSMDeploymentStrategyPlaySheet(){
		super();
		overallAnalysisTitle = "System Analysis";
		titleText = "Set Deployment Time Frame";
	}

	/**
	 * Sets up the Param panel at the top of the split pane
	 */
	public void createGenericParamPanel()
	{
		queryRegions();

		if(regionsList.isEmpty()) {
			Utility.showError("Cannot find regions in TAP Site");
		}

		super.createGenericParamPanel();

		timePanel = new JPanel();
		GridBagConstraints gbc_timePanel = new GridBagConstraints();
		gbc_timePanel.gridx = 1;
		gbc_timePanel.gridy = 1;
		ctlPanel.add(timePanel, gbc_timePanel);
		GridBagLayout gbl_timePanel = new GridBagLayout();
		gbl_timePanel.columnWidths = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0 };
		gbl_timePanel.rowHeights = new int[] { 0, 0, 0, 0};
		gbl_timePanel.columnWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE };
		gbl_timePanel.rowWeights = new double[] { 0.0, 0.0, 0.0, 1.0, Double.MIN_VALUE };
		timePanel.setLayout(gbl_timePanel);

		regionTimePanel = new JPanel();
		regionTimePanel.setBorder(new BevelBorder(BevelBorder.LOWERED, null, null, null, null));
		regionTimePanel.setVisible(false);
		GridBagConstraints gbc_regionTimePanel = new GridBagConstraints();
		gbc_regionTimePanel.gridx = 2;
		gbc_regionTimePanel.gridy = 0;
		gbc_regionTimePanel.gridheight = 2;
		ctlPanel.add(regionTimePanel, gbc_regionTimePanel);
		GridBagLayout gbl_regionTimePanel = new GridBagLayout();
		gbl_regionTimePanel.columnWidths = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0 };
		gbl_regionTimePanel.rowHeights = new int[] { 0, 0, 0, 0, 0, 0, 0 };
		gbl_regionTimePanel.columnWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE };
		gbl_regionTimePanel.rowWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE };
		regionTimePanel.setLayout(gbl_regionTimePanel);

		// begin deployment
		JLabel lblDeployment1 = new JLabel("Deployment");
		lblDeployment1.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_lblDeployment1 = new GridBagConstraints();
		gbc_lblDeployment1.anchor = GridBagConstraints.WEST;
		gbc_lblDeployment1.insets = new Insets(0, 0, 5, 10);
		gbc_lblDeployment1.gridx = 0;
		gbc_lblDeployment1.gridy = 1;
		timePanel.add(lblDeployment1, gbc_lblDeployment1);

		JLabel lblBeginDeployment = new JLabel("Begins in ");
		lblBeginDeployment.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblBeginDeployment = new GridBagConstraints();
		gbc_lblBeginDeployment.anchor = GridBagConstraints.WEST;
		gbc_lblBeginDeployment.insets = new Insets(0, 0, 5, 10);
		gbc_lblBeginDeployment.gridx = 1;
		gbc_lblBeginDeployment.gridy = 1;
		timePanel.add(lblBeginDeployment, gbc_lblBeginDeployment);

		JLabel lblbeginQuarter = new JLabel("Q");
		lblbeginQuarter.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblbeginQuarter = new GridBagConstraints();
		gbc_lblbeginQuarter.anchor = GridBagConstraints.WEST;
		gbc_lblbeginQuarter.insets = new Insets(0, 0, 5, 0);
		gbc_lblbeginQuarter.gridx = 2;
		gbc_lblbeginQuarter.gridy = 1;
		timePanel.add(lblbeginQuarter, gbc_lblbeginQuarter);

		qBeginField = new JTextField();
		qBeginField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		qBeginField.setText("1");
		qBeginField.setColumns(1);
		qBeginField.setName("Deployment begins quarter");

		GridBagConstraints gbc_beginQuarterField = new GridBagConstraints();
		gbc_beginQuarterField.anchor = GridBagConstraints.WEST;
		gbc_beginQuarterField.insets = new Insets(0, 0, 5, 10);
		gbc_beginQuarterField.gridx = 3;
		gbc_beginQuarterField.gridy = 1;
		timePanel.add(qBeginField, gbc_beginQuarterField);

		JLabel lblBeginYear = new JLabel("FY 20");
		lblBeginYear.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblBeginYear = new GridBagConstraints();
		gbc_lblBeginYear.anchor = GridBagConstraints.WEST;
		gbc_lblBeginYear.insets = new Insets(0, 0, 5, 0);
		gbc_lblBeginYear.gridx = 4;
		gbc_lblBeginYear.gridy = 1;
		timePanel.add(lblBeginYear, gbc_lblBeginYear);

		yBeginField = new JTextField();
		yBeginField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		yBeginField.setText("15");
		yBeginField.setColumns(2);
		yBeginField.setName("Deployment begins year");

		GridBagConstraints gbc_beginYearField = new GridBagConstraints();
		gbc_beginYearField.anchor = GridBagConstraints.WEST;
		gbc_beginYearField.insets = new Insets(0, 0, 5, 10);
		gbc_beginYearField.gridx = 5;
		gbc_beginYearField.gridy = 1;
		timePanel.add(yBeginField, gbc_beginYearField);

		//end deployment
		JLabel lblDeployment2 = new JLabel("Deployment");
		lblDeployment2.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_lblDeployment2 = new GridBagConstraints();
		gbc_lblDeployment2.anchor = GridBagConstraints.WEST;
		gbc_lblDeployment2.insets = new Insets(0, 0, 5, 10);
		gbc_lblDeployment2.gridx = 0;
		gbc_lblDeployment2.gridy = 2;
		timePanel.add(lblDeployment2, gbc_lblDeployment2);

		JLabel lblEndDeployment = new JLabel("Ends in ");
		lblEndDeployment.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblEndDeployment = new GridBagConstraints();
		gbc_lblEndDeployment.anchor = GridBagConstraints.WEST;
		gbc_lblEndDeployment.insets = new Insets(0, 0, 5, 10);
		gbc_lblEndDeployment.gridx = 1;
		gbc_lblEndDeployment.gridy = 2;
		timePanel.add(lblEndDeployment, gbc_lblEndDeployment);

		JLabel lblEndQuarter = new JLabel("Q");
		lblEndQuarter.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblEndQuarter = new GridBagConstraints();
		gbc_lblEndQuarter.anchor = GridBagConstraints.WEST;
		gbc_lblEndQuarter.insets = new Insets(0, 0, 5, 0);
		gbc_lblEndQuarter.gridx = 2;
		gbc_lblEndQuarter.gridy = 2;
		timePanel.add(lblEndQuarter, gbc_lblEndQuarter);

		qEndField = new JTextField();
		qEndField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		qEndField.setText("1");
		qEndField.setColumns(1);
		qEndField.setName("Deployment ends quarter");

		GridBagConstraints gbc_endQuarterField = new GridBagConstraints();
		gbc_endQuarterField.anchor = GridBagConstraints.WEST;
		gbc_endQuarterField.insets = new Insets(0, 0, 5, 10);
		gbc_endQuarterField.gridx = 3;
		gbc_endQuarterField.gridy = 2;
		timePanel.add(qEndField, gbc_endQuarterField);		

		JLabel lblEndYear = new JLabel("FY 20");
		lblEndYear.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblEndYear = new GridBagConstraints();
		gbc_lblEndYear.anchor = GridBagConstraints.WEST;
		gbc_lblEndYear.insets = new Insets(0, 0, 5, 0);
		gbc_lblEndYear.gridx = 4;
		gbc_lblEndYear.gridy = 2;
		timePanel.add(lblEndYear, gbc_lblEndYear);

		yEndField = new JTextField();
		yEndField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		yEndField.setText("21");
		yEndField.setColumns(2);
		yEndField.setName("Deployment ends year");

		GridBagConstraints gbc_endYearField = new GridBagConstraints();
		gbc_endYearField.anchor = GridBagConstraints.WEST;
		gbc_endYearField.insets = new Insets(0, 0, 5, 10);
		gbc_endYearField.gridx = 5;
		gbc_endYearField.gridy = 2;
		timePanel.add(yEndField, gbc_endYearField);

		selectRegionTimesButton = new ToggleButton("Set deployment times by region");
		selectRegionTimesButton.setName("selectRegionTimesButton");
		selectRegionTimesButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		Style.registerTargetClassName(selectRegionTimesButton,  ".toggleButton");

		GridBagConstraints gbc_selectRegionTimesButton = new GridBagConstraints();
		gbc_selectRegionTimesButton.anchor = GridBagConstraints.NORTH;
		gbc_selectRegionTimesButton.gridwidth = 6;
		gbc_selectRegionTimesButton.fill = GridBagConstraints.HORIZONTAL;
		gbc_selectRegionTimesButton.insets = new Insets(0, 0, 5, 5);
		gbc_selectRegionTimesButton.gridx = 0;
		gbc_selectRegionTimesButton.gridy = 3;
		timePanel.add(selectRegionTimesButton, gbc_selectRegionTimesButton);

		//listener to show region panel
		DHMSMDeploymentStrategySetRegionListener setRegLis = new DHMSMDeploymentStrategySetRegionListener();
		setRegLis.setPlaySheet(this);
		selectRegionTimesButton.addActionListener(setRegLis);
		
		//select by region panel
		qBeginFieldHash = new Hashtable<String,JTextField>();
		yBeginFieldHash = new Hashtable<String,JTextField>();
		qEndFieldHash = new Hashtable<String,JTextField>();
		yEndFieldHash = new Hashtable<String,JTextField>();

		//add in the regions labels and fields to the region panel
		for(String region : regionsList) {
			addRegion(region);
		}
		
		setDefaults();
		
		//restore defaults
		restoreDefaultsButton = new CustomButton("Restore defaults");
		restoreDefaultsButton.setName("restoreDefaultsButton");
		restoreDefaultsButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		Style.registerTargetClassName(restoreDefaultsButton,  ".toggleButton");

		GridBagConstraints gbc_restoreDefaultsButton = new GridBagConstraints();
		gbc_restoreDefaultsButton.anchor = GridBagConstraints.NORTH;
		gbc_restoreDefaultsButton.gridwidth = 6;
		gbc_restoreDefaultsButton.fill = GridBagConstraints.HORIZONTAL;
		gbc_restoreDefaultsButton.insets = new Insets(0, 0, 5, 5);
		gbc_restoreDefaultsButton.gridx = 0;
		gbc_restoreDefaultsButton.gridy = 4;
		timePanel.add(restoreDefaultsButton, gbc_restoreDefaultsButton);

		//listener to show region panel
		DHMSMDeploymentStrategyRestoreDefaultsListener restoreDefaultsLis = new DHMSMDeploymentStrategyRestoreDefaultsListener();
		restoreDefaultsLis.setPlaySheet(this);
		restoreDefaultsButton.addActionListener(restoreDefaultsLis);
		
		runButton = new CustomButton("Create deployment strategy");
		runButton.setName("runButton");
		runButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		Style.registerTargetClassName(runButton,  ".createBtn");

		GridBagConstraints gbc_runButton = new GridBagConstraints();
		gbc_runButton.anchor = GridBagConstraints.NORTH;
		gbc_runButton.gridwidth = 6;
		gbc_runButton.fill = GridBagConstraints.HORIZONTAL;
		gbc_runButton.insets = new Insets(20, 0, 5, 5);
		gbc_runButton.gridx = 0;
		gbc_runButton.gridy = 5;
		timePanel.add(runButton, gbc_runButton);

		DHMSMDeploymentStrategyRunBtnListener runList = new DHMSMDeploymentStrategyRunBtnListener();
		runList.setRegionWaveHash(regionWaveHash);
		runList.setWaveOrder(waveOrder);
		runList.setWaveStartEndDate(waveStartEndDate);
		runList.setPlaySheet(this);
		runButton.addActionListener(runList);
	}
	
	@Override
	public void createGenericDisplayPanel() {
		super.createGenericDisplayPanel();
		siteAnalysisPanel = new JPanel();
		tabbedPane.insertTab("Site Analysis", null, siteAnalysisPanel, null,1);
		GridBagLayout gbl_siteAnalysisPanel = new GridBagLayout();
		gbl_siteAnalysisPanel.columnWidths = new int[]{0, 0};
		gbl_siteAnalysisPanel.rowHeights = new int[]{0, 0};
		gbl_siteAnalysisPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_siteAnalysisPanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		siteAnalysisPanel.setLayout(gbl_siteAnalysisPanel);
	}

	public void showSelectRegionTimesPanel(Boolean show) {
		if(show) {
			regionTimePanel.setVisible(true);
			qBeginField.setEnabled(false);
			yBeginField.setEnabled(false);
			qEndField.setEnabled(false);
			yEndField.setEnabled(false);

		}else {
			regionTimePanel.setVisible(false);
			qBeginField.setEnabled(true);
			yBeginField.setEnabled(true);
			qEndField.setEnabled(true);
			yEndField.setEnabled(true);
		}
	}

	/**
	 * makes a list of all the regions that will need to be included.
	 * returns them in order of deployment
	 * filters out IOC.
	 */
	private void queryRegions() {
		regionsList = new ArrayList<String>();
		qBeginDefaultHash = new Hashtable<String,Integer>();
		yBeginDefaultHash = new Hashtable<String,Integer>();
		qEndDefaultHash = new Hashtable<String,Integer>();
		yEndDefaultHash = new Hashtable<String,Integer>();	
		
		IEngine siteEngine = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Site_Data");
		//query is written to pull wave so that i can determine in what order the regions are deployed in
		String regionQuery = "SELECT DISTINCT ?Region ?Wave ?BeginQ ?BeginY ?EndQ ?EndY WHERE {{?Region <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Region>}{?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?BeginYQ <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year-Quarter>} {?EndYQ <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year-Quarter>}{?BeginQ <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Quarter>}{?BeginY <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year>}{?EndQ <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Quarter>}{?EndY <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Year>}{?Region <http://semoss.org/ontologies/Relation/Deploys> ?Wave}{?Wave <http://semoss.org/ontologies/Relation/BeginsOn> ?BeginYQ}{?BeginYQ  <http://semoss.org/ontologies/Relation/has> ?BeginY}{?BeginYQ  <http://semoss.org/ontologies/Relation/has> ?BeginQ}{?Wave <http://semoss.org/ontologies/Relation/EndsOn> ?EndYQ}{?EndYQ  <http://semoss.org/ontologies/Relation/has> ?EndY}{?EndYQ  <http://semoss.org/ontologies/Relation/has> ?EndQ}}";
		SesameJenaSelectWrapper wrapper = Utility.processQuery(siteEngine,regionQuery);


		regionWaveHash = new Hashtable<String, List<String>>();
		String[] names = wrapper.getVariables();

			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.next();
				String region = (String) sjss.getVar(names[0]);
				String waveString = (String) sjss.getVar(names[1]);

				List<String> waveValues;
				if(regionWaveHash.containsKey(region)) {
					waveValues = regionWaveHash.get(region);
					waveValues.add(waveString);
				} else {
					waveValues = new ArrayList<String>();
					waveValues.add(waveString);
					regionWaveHash.put(region, waveValues);
				}
				try{
					int beginQ = Integer.parseInt((String) sjss.getVar(names[2]));
					int beginY = Integer.parseInt((String) sjss.getVar(names[3]));
					int endQ = Integer.parseInt((String) sjss.getVar(names[4]));
					int endY = Integer.parseInt((String) sjss.getVar(names[5]));
	
					//if current begin val is earlier than what is saved, save current
					if(qBeginDefaultHash.containsKey(region) && yBeginDefaultHash.containsKey(region)) {
						int earlyBeginQ = qBeginDefaultHash.get(region);
						int earlyBeginY = yBeginDefaultHash.get(region);
						if(compareTo(earlyBeginQ,earlyBeginY,beginQ,beginY)<0) {
							qBeginDefaultHash.put(region,beginQ);
							yBeginDefaultHash.put(region,beginY);
						}
					} else{
						qBeginDefaultHash.put(region,beginQ);
						yBeginDefaultHash.put(region,beginY);
					}
					
					//if current end val is later than what is saved, save current
					if(qEndDefaultHash.containsKey(region) && yEndDefaultHash.containsKey(region)) {
						int lateEndQ = qEndDefaultHash.get(region);
						int lateEndY = yEndDefaultHash.get(region);
						if(compareTo(lateEndQ,lateEndY,endQ,endY)>0) {
							qEndDefaultHash.put(region,endQ);
							yEndDefaultHash.put(region,endY);
						}
					} else{
						qEndDefaultHash.put(region,endQ);
						yEndDefaultHash.put(region,endY);
					}
				}catch(Exception e) {
					LOGGER.error("Could not add region "+region+" wave "+waveString);
				}
			}

		for(String region : regionWaveHash.keySet()) {
			//if(!region.toUpperCase().equals("IOC")) {
			int beginQuarter =qBeginDefaultHash.get(region);
			int beginYear =yBeginDefaultHash.get(region);
			if(regionsList.size()==0)
				regionsList.add(region);
			else {
				int i=0;
				Boolean added = false;
				while(i<regionsList.size()) {
					String regionI = regionsList.get(i);
					int iBeginQuarter =qBeginDefaultHash.get(regionI);
					int iBeginYear =yBeginDefaultHash.get(regionI);
					//if the region to be added begins before the one at index i, add it
					if(!added&&compareTo(beginQuarter,beginYear,iBeginQuarter,iBeginYear)>=1) {
						regionsList.add(i,region);
						added = true;
					}
					i++;
				}
				if(!added) {
					regionsList.add(region);
				}
			}
		}
		
		qBeginDefault = qBeginDefaultHash.get(regionsList.get(0));
		yBeginDefault = yBeginDefaultHash.get(regionsList.get(0));
		qEndDefault = qEndDefaultHash.get(regionsList.get(0));
		yEndDefault = yEndDefaultHash.get(regionsList.get(0));
		
		for(String region : regionsList) {
				int beginQ = qBeginDefaultHash.get(region);
				int beginY = yBeginDefaultHash.get(region);
				int endQ = qEndDefaultHash.get(region);
				int endY = yEndDefaultHash.get(region);
				if(compareTo(qBeginDefault,yBeginDefault,beginQ,beginY)<0) {
					qBeginDefault = beginQ;
					yBeginDefault = beginY;
				}
				if(compareTo(qEndDefault,yEndDefault,endQ,endY)>0) {
					qEndDefault = endQ;
					yEndDefault = endY;
				}
		}
		
		waveOrder = DHMSMDeploymentHelper.getWaveOrder(siteEngine);
		waveStartEndDate = DHMSMDeploymentHelper.getWaveStartAndEndDate(siteEngine);
	}

	/**
	 * compares two dates to see which comes earlier.
	 * if the first date is before, returns 1
	 * if the same date, returns 0
	 * if the first date is after, returns -1
	 * @param firstQ
	 * @param firstY
	 * @param secondQ
	 * @param secondY
	 * @return
	 */
	private int compareTo(int firstQ, int firstY, int secondQ, int secondY) {
		if(firstY<secondY) {
			return 1;
		} else if(firstY==secondY){
			if(firstQ<secondQ) {
				return 1;
			}else if(firstQ==secondQ) {
				return 0;
			}else if(firstQ > secondY) {
				return -1;
			}
		}
		return -1;
	}
	
	private void addRegion(String region) {
		int y = qBeginFieldHash.size();

		JLabel lblRegion = new JLabel("Region "+region);
		lblRegion.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_lblRegion = new GridBagConstraints();
		gbc_lblRegion.anchor = GridBagConstraints.WEST;
		gbc_lblRegion.insets = new Insets(0, 0, 5, 10);
		gbc_lblRegion.gridx = 0;
		gbc_lblRegion.gridy = y+1;
		regionTimePanel.add(lblRegion, gbc_lblRegion);

		JLabel lblBeginRegion = new JLabel("Begins in ");
		lblBeginRegion.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblBeginRegion = new GridBagConstraints();
		gbc_lblBeginRegion.anchor = GridBagConstraints.WEST;
		gbc_lblBeginRegion.insets = new Insets(0, 0, 5, 10);
		gbc_lblBeginRegion.gridx = 1;
		gbc_lblBeginRegion.gridy = y+1;
		regionTimePanel.add(lblBeginRegion, gbc_lblBeginRegion);

		JLabel lblbeginQuarter = new JLabel("Q");
		lblbeginQuarter.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblbeginQuarter = new GridBagConstraints();
		gbc_lblbeginQuarter.anchor = GridBagConstraints.WEST;
		gbc_lblbeginQuarter.insets = new Insets(0, 0, 5, 0);
		gbc_lblbeginQuarter.gridx = 2;
		gbc_lblbeginQuarter.gridy = y+1;
		regionTimePanel.add(lblbeginQuarter, gbc_lblbeginQuarter);

		JTextField beginQuarterField = new JTextField();
		beginQuarterField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		beginQuarterField.setColumns(1);
		beginQuarterField.setName(region+" begins quarter");

		GridBagConstraints gbc_beginQuarterField = new GridBagConstraints();
		gbc_beginQuarterField.anchor = GridBagConstraints.WEST;
		gbc_beginQuarterField.insets = new Insets(0, 0, 5, 10);
		gbc_beginQuarterField.gridx = 3;
		gbc_beginQuarterField.gridy = y+1;
		regionTimePanel.add(beginQuarterField, gbc_beginQuarterField);

		JLabel lblBeginYear = new JLabel("FY 20");
		lblBeginYear.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblBeginYear = new GridBagConstraints();
		gbc_lblBeginYear.anchor = GridBagConstraints.WEST;
		gbc_lblBeginYear.insets = new Insets(0, 0, 5, 0);
		gbc_lblBeginYear.gridx = 4;
		gbc_lblBeginYear.gridy = y+1;
		regionTimePanel.add(lblBeginYear, gbc_lblBeginYear);

		JTextField beginYearField = new JTextField();
		beginYearField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		beginYearField.setColumns(2);
		beginYearField.setName(region+" begins year");

		GridBagConstraints gbc_beginYearField = new GridBagConstraints();
		gbc_beginYearField.anchor = GridBagConstraints.WEST;
		gbc_beginYearField.insets = new Insets(0, 0, 5, 20);
		gbc_beginYearField.gridx = 5;
		gbc_beginYearField.gridy = y+1;
		regionTimePanel.add(beginYearField, gbc_beginYearField);

		//end deployment
		JLabel lblEndDeployment = new JLabel("Ends in");
		lblEndDeployment.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblEndDeployment = new GridBagConstraints();
		gbc_lblEndDeployment.anchor = GridBagConstraints.WEST;
		gbc_lblEndDeployment.insets = new Insets(0, 0, 5, 10);
		gbc_lblEndDeployment.gridx = 6;
		gbc_lblEndDeployment.gridy = y+1;
		regionTimePanel.add(lblEndDeployment, gbc_lblEndDeployment);

		JLabel lblEndQuarter = new JLabel("Q");
		lblEndQuarter.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblEndQuarter = new GridBagConstraints();
		gbc_lblEndQuarter.anchor = GridBagConstraints.WEST;
		gbc_lblEndQuarter.insets = new Insets(0, 0, 5, 0);
		gbc_lblEndQuarter.gridx = 7;
		gbc_lblEndQuarter.gridy = y+1;
		regionTimePanel.add(lblEndQuarter, gbc_lblEndQuarter);

		JTextField endQuarterField = new JTextField();
		endQuarterField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		endQuarterField.setColumns(1);
		endQuarterField.setName(region+" ends quarter");

		GridBagConstraints gbc_endQuarterField = new GridBagConstraints();
		gbc_endQuarterField.anchor = GridBagConstraints.WEST;
		gbc_endQuarterField.insets = new Insets(0, 0, 5, 10);
		gbc_endQuarterField.gridx = 8;
		gbc_endQuarterField.gridy = y+1;
		regionTimePanel.add(endQuarterField, gbc_endQuarterField);		

		JLabel lblEndYear = new JLabel("FY 20");
		lblEndYear.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblEndYear = new GridBagConstraints();
		gbc_lblEndYear.anchor = GridBagConstraints.WEST;
		gbc_lblEndYear.insets = new Insets(0, 0, 5, 0);
		gbc_lblEndYear.gridx = 9;
		gbc_lblEndYear.gridy = y+1;
		regionTimePanel.add(lblEndYear, gbc_lblEndYear);

		JTextField endYearField = new JTextField();
		endYearField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		endYearField.setColumns(2);
		endYearField.setName(region+" ends year");

		GridBagConstraints gbc_endYearField = new GridBagConstraints();
		gbc_endYearField.anchor = GridBagConstraints.WEST;
		gbc_endYearField.insets = new Insets(0, 0, 5, 10);
		gbc_endYearField.gridx = 10;
		gbc_endYearField.gridy = y+1;
		regionTimePanel.add(endYearField, gbc_endYearField);

		qBeginFieldHash.put(region,beginQuarterField);
		yBeginFieldHash.put(region,beginYearField);
		qEndFieldHash.put(region,endQuarterField);
		yEndFieldHash.put(region,endYearField);

	}

	public void setDefaults() {
		qBeginField.setText("" + qBeginDefault);
		yBeginField.setText(("" + yBeginDefault).substring(2));
		qEndField.setText("" + qEndDefault);
		yEndField.setText(("" + yEndDefault).substring(2));
		
		for(String region : regionsList) {
			String beginQ = "" + qBeginDefaultHash.get(region);
			String beginY = "" + yBeginDefaultHash.get(region);
			String endQ = "" + qEndDefaultHash.get(region);
			String endY = "" + yEndDefaultHash.get(region);
			qBeginFieldHash.get(region).setText(beginQ);
			qEndFieldHash.get(region).setText(endQ);
			yBeginFieldHash.get(region).setText(beginY.substring(2));
			yEndFieldHash.get(region).setText(endY.substring(2));
	
		}
	}
	
	public JToggleButton getSelectRegionTimesButton() {
		return selectRegionTimesButton;
	}

	public JTextField getQBeginField() {
		return qBeginField;
	}

	public JTextField getYBeginField() {
		return yBeginField;
	}

	public JTextField getQEndField() {
		return qEndField;
	}

	public JTextField getYEndField() {
		return yEndField;
	}

	public ArrayList<String> getRegionsList() {
		return regionsList;
	}

	public Hashtable<String,JTextField> getQBeginFieldHash() {
		return qBeginFieldHash;
	}

	public Hashtable<String,JTextField> getYBeginFieldHash() {
		return yBeginFieldHash;
	}

	public Hashtable<String,JTextField> getQEndFieldHash() {
		return qEndFieldHash;
	}

	public Hashtable<String,JTextField> getYEndFieldHash() {
		return yEndFieldHash;
	}

	public Hashtable<String,Integer> getQBeginDefaultHash() {
		return qBeginDefaultHash;
	}
	public Hashtable<String,Integer> getYBeginDefaultHash() {
		return yBeginDefaultHash;
	}
	public Hashtable<String,Integer> getQEndDefaultHash() {
		return qEndDefaultHash;
	}
	public Hashtable<String,Integer> getYEndDefaultHash() {
		return yEndDefaultHash;
	}
}

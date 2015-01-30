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

	//param panel components
	//begin/end deployment fields
	private JPanel timePanel;
	private JTextField beginQuarterField, beginYearField;
	private JTextField endQuarterField, endYearField;

	//toggle to select specific region times
	private JToggleButton selectRegionTimesButton;

	//list of regions
	private ArrayList<String> regionsList;
	private Hashtable<String, List<String>> regionWaveHash;
	private ArrayList<String> waveOrder;
	private JPanel regionTimePanel;
	private ArrayList<JTextField> beginQuarterFieldRegionList, beginYearFieldRegionList;
	private ArrayList<JTextField> endQuarterFieldRegionList, endYearFieldRegionList;

	//button to run algorithm
	private JButton runButton;


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

		titleLbl.setText("Set Deployment Time Frame");

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

		beginQuarterField = new JTextField();
		beginQuarterField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		beginQuarterField.setText("1");
		beginQuarterField.setColumns(1);
		beginQuarterField.setName("Deployment begins quarter");

		GridBagConstraints gbc_beginQuarterField = new GridBagConstraints();
		gbc_beginQuarterField.anchor = GridBagConstraints.WEST;
		gbc_beginQuarterField.insets = new Insets(0, 0, 5, 10);
		gbc_beginQuarterField.gridx = 3;
		gbc_beginQuarterField.gridy = 1;
		timePanel.add(beginQuarterField, gbc_beginQuarterField);

		JLabel lblBeginYear = new JLabel("FY 20");
		lblBeginYear.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblBeginYear = new GridBagConstraints();
		gbc_lblBeginYear.anchor = GridBagConstraints.WEST;
		gbc_lblBeginYear.insets = new Insets(0, 0, 5, 0);
		gbc_lblBeginYear.gridx = 4;
		gbc_lblBeginYear.gridy = 1;
		timePanel.add(lblBeginYear, gbc_lblBeginYear);

		beginYearField = new JTextField();
		beginYearField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		beginYearField.setText("15");
		beginYearField.setColumns(2);
		beginYearField.setName("Deployment begins year");

		GridBagConstraints gbc_beginYearField = new GridBagConstraints();
		gbc_beginYearField.anchor = GridBagConstraints.WEST;
		gbc_beginYearField.insets = new Insets(0, 0, 5, 10);
		gbc_beginYearField.gridx = 5;
		gbc_beginYearField.gridy = 1;
		timePanel.add(beginYearField, gbc_beginYearField);

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

		endQuarterField = new JTextField();
		endQuarterField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		endQuarterField.setText("1");
		endQuarterField.setColumns(1);
		endQuarterField.setName("Deployment ends quarter");

		GridBagConstraints gbc_endQuarterField = new GridBagConstraints();
		gbc_endQuarterField.anchor = GridBagConstraints.WEST;
		gbc_endQuarterField.insets = new Insets(0, 0, 5, 10);
		gbc_endQuarterField.gridx = 3;
		gbc_endQuarterField.gridy = 2;
		timePanel.add(endQuarterField, gbc_endQuarterField);		

		JLabel lblEndYear = new JLabel("FY 20");
		lblEndYear.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblEndYear = new GridBagConstraints();
		gbc_lblEndYear.anchor = GridBagConstraints.WEST;
		gbc_lblEndYear.insets = new Insets(0, 0, 5, 0);
		gbc_lblEndYear.gridx = 4;
		gbc_lblEndYear.gridy = 2;
		timePanel.add(lblEndYear, gbc_lblEndYear);

		endYearField = new JTextField();
		endYearField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		endYearField.setText("21");
		endYearField.setColumns(2);
		endYearField.setName("Deployment ends year");

		GridBagConstraints gbc_endYearField = new GridBagConstraints();
		gbc_endYearField.anchor = GridBagConstraints.WEST;
		gbc_endYearField.insets = new Insets(0, 0, 5, 10);
		gbc_endYearField.gridx = 5;
		gbc_endYearField.gridy = 2;
		timePanel.add(endYearField, gbc_endYearField);

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
		beginQuarterFieldRegionList = new ArrayList<JTextField>();
		beginYearFieldRegionList = new ArrayList<JTextField>();
		endQuarterFieldRegionList = new ArrayList<JTextField>();
		endYearFieldRegionList = new ArrayList<JTextField>();

		//add in the regions labels and fields to the region panel
		for(String region : regionsList) {
			addRegion(region);
		}

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
		gbc_runButton.gridy = 4;
		timePanel.add(runButton, gbc_runButton);

		DHMSMDeploymentStrategyRunBtnListener runList = new DHMSMDeploymentStrategyRunBtnListener();
		runList.setRegionWaveHash(regionWaveHash);
		runList.setWaveOrder(waveOrder);
		runList.setPlaySheet(this);
		runButton.addActionListener(runList);
	}

	public void showSelectRegionTimesPanel(Boolean show) {
		if(show) {
			regionTimePanel.setVisible(true);
			beginQuarterField.setEnabled(false);
			beginYearField.setEnabled(false);
			endQuarterField.setEnabled(false);
			endYearField.setEnabled(false);

		}else {
			regionTimePanel.setVisible(false);
			beginQuarterField.setEnabled(true);
			beginYearField.setEnabled(true);
			endQuarterField.setEnabled(true);
			endYearField.setEnabled(true);
		}
	}

	/**
	 * makes a list of all the regions that will need to be included.
	 * returns them in order of deployment
	 * filters out IOC.
	 */
	private void queryRegions() {
		regionsList = new ArrayList<String>();
		IEngine siteEngine = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Site_Data");
		//query is written to pull wave so that i can determine in what order the regions are deployed in
		String regionQuery = "SELECT DISTINCT ?Region ?Wave WHERE {{?Region <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Region>}{?Wave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Wave>} {?Region <http://semoss.org/ontologies/Relation/Deploys> ?Wave}}";
		SesameJenaSelectWrapper wrapper = Utility.processQuery(siteEngine,regionQuery);

		Hashtable<Integer, String> waveRegionHash = new Hashtable<Integer, String>();
		regionWaveHash = new Hashtable<String, List<String>>();
		String[] names = wrapper.getVariables();
		try {
			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.next();
				String region = (String) sjss.getVar(names[0]);
				String wave = (String) sjss.getVar(names[1]);

				List<String> waveValues;
				if(regionWaveHash.containsKey(region)) {
					waveValues = regionWaveHash.get(region);
					waveValues.add(wave);
				} else {
					waveValues = new ArrayList<String>();
					waveValues.add(wave);
					regionWaveHash.put(region, waveValues);
				}
				try{
					int waveInt = Integer.parseInt(wave);
					waveRegionHash.put(waveInt, region);
				}catch(Exception e) {
					LOGGER.error("Could not add wave "+wave+" because it is not an integer");
				}
			}
		} catch (RuntimeException e) {
			LOGGER.fatal(e);
		}
		int iteration = 0;
		int countFound = 0;
		while(countFound<waveRegionHash.size()) {
			if(waveRegionHash.containsKey(iteration)) {
				String region = waveRegionHash.get(iteration);
				if(!regionsList.contains(region))
					regionsList.add(region);
				countFound++;
			}
			iteration++;
		}
		
		waveOrder = DHMSMDeploymentHelper.getWaveOrder(siteEngine);
	}

	private void addRegion(String region) {
		int y = beginQuarterFieldRegionList.size();

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
		beginQuarterField.setText("1");
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
		beginYearField.setText("15");
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
		endQuarterField.setText("1");
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
		endYearField.setText("21");
		endYearField.setColumns(2);
		endYearField.setName(region+" ends year");

		GridBagConstraints gbc_endYearField = new GridBagConstraints();
		gbc_endYearField.anchor = GridBagConstraints.WEST;
		gbc_endYearField.insets = new Insets(0, 0, 5, 10);
		gbc_endYearField.gridx = 10;
		gbc_endYearField.gridy = y+1;
		regionTimePanel.add(endYearField, gbc_endYearField);

		beginQuarterFieldRegionList.add(beginQuarterField);
		beginYearFieldRegionList.add(beginYearField);
		endQuarterFieldRegionList.add(endQuarterField);
		endYearFieldRegionList.add(endYearField);

	}

	public JToggleButton getSelectRegionTimesButton() {
		return selectRegionTimesButton;
	}

	public JTextField getBeginQuarterField() {
		return beginQuarterField;
	}

	public JTextField getBeginYearField() {
		return beginYearField;
	}

	public JTextField getEndQuarterField() {
		return endQuarterField;
	}

	public JTextField getEndYearField() {
		return endYearField;
	}

	public ArrayList<String> getRegionsList() {
		return regionsList;
	}

	public ArrayList<JTextField> getBeginQuarterFieldRegionList() {
		return beginQuarterFieldRegionList;
	}

	public ArrayList<JTextField> getBeginYearFieldRegionList() {
		return beginYearFieldRegionList;
	}

	public ArrayList<JTextField> getEndQuarterFieldRegionList() {
		return endQuarterFieldRegionList;
	}

	public ArrayList<JTextField> getEndYearFieldRegionList() {
		return endYearFieldRegionList;
	}
}

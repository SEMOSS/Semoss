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
package prerna.ui.components.playsheets;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.beans.PropertyVetoException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JSlider;
import javax.swing.JTabbedPane;
import javax.swing.JTextField;
import javax.swing.JToggleButton;
import javax.swing.border.BevelBorder;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.similarity.DatasetSimilarity;
import prerna.math.BarChart;
import prerna.om.SEMOSSParam;
import prerna.ui.components.BrowserGraphPanel;
import prerna.ui.components.NewScrollBarUI;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.main.listener.impl.ClassificationSelectionListener;
import prerna.ui.main.listener.impl.ClusterTabSelectionListener;
import prerna.ui.main.listener.impl.NumberOfClustersSelectionListener;
import prerna.ui.main.listener.impl.RunAlgorithmListener;
import prerna.ui.main.listener.impl.RunDrillDownListener;
import prerna.ui.main.listener.impl.SelectAlgorithmListener;
import prerna.ui.main.listener.impl.SelectCheckboxesListener;
import prerna.ui.main.listener.impl.ShowDrillDownListener;
import prerna.ui.swing.custom.CustomButton;
import prerna.ui.swing.custom.ToggleButton;
import prerna.util.CSSApplication;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;
import prerna.util.Utility;
import aurelienribon.ui.css.Style;

public class ClassifyClusterPlaySheet extends BasicProcessingPlaySheet{

	private static final Logger LOGGER = LogManager.getLogger(ClassifyClusterPlaySheet.class.getName());
	
	private JTabbedPane jTab;
	private Hashtable<String, IPlaySheet> playSheetHash = new Hashtable<String,IPlaySheet>();
	private JPanel variableSelectorPanel;

	//independent variable panel
	private Double[] entropyArr;
	private ArrayList<JCheckBox> ivCheckboxes;
	private ArrayList<JLabel> entropyLabels;
	private ArrayList<JLabel> accuracyLabels;
	private ArrayList<JLabel> precisionLabels;
	//independent variable panel - select all
	private JCheckBox checkboxSelectAllIVs;
	private SelectCheckboxesListener selectAllIVsList;
	public JPanel indVariablesPanel;
	
	//data set similarity chart
	private BrowserGraphPanel simBarChartPanel;
	private Hashtable<String, Object> simBarChartHash;
	
	private String checkboxName = "checkBox";
	
	//cluster/classify/outlier selection drop down
	private JComboBox<String> algorithmComboBox;
	//run buttons
	private JButton runAlgorithm, runDrillDown;
	
	//cluster panel components
	private JPanel clusterPanel;
	private JLabel lblSelectNumClusters;
	private JComboBox<String> selectNumClustersComboBox;
	private final String automaticallySelectNumClustersText = "Automatically select number of clusters";
	private final String manuallySelectNumClustersText = "Manually set number of clusters";
	private JTextField selectNumClustersTextField;
	
	//classification panel components
	private JPanel classifyPanel;
	private JComboBox<String> classificationMethodComboBox;
	private JComboBox<String> classComboBox;
	private JLabel lblSelectClass, lblSelectClassMethod;
	
	//outlier panel components
	private JPanel outlierPanel;
	private JLabel lblEnterKNeighbors;
	private JSlider enterKNeighborsSlider;
	
	//association learning panel combonents
	private JPanel associationLearningPanel;
	private JLabel lblEnterNumRules, lblEnterMinSupport, lblEnterMaxSupport, lblEnterConfInterval;
	private JTextField enterNumRulesTextField, enterMinSupportTextField, enterMaxSupportTextField, enterConfIntervalTextField;
	
	//matrix regression panel components
	private JPanel matrixRegPanel;
	private JComboBox<String> matrixDepVarComboBox;
	private JLabel lblSelectDepVar;
	
	//self organizing map components
	private JPanel somPanel;
	private JLabel lblEnterR0, lblEnterL0, lblEnterTau, lblEnterMaxIt;
	private JTextField enterR0TextField, enterL0TextField, enterTauTextField, enterMaxItTextField; 
	private int som_height, som_length;
	
	//drill down panel components
	private JToggleButton showDrillDownBtn;
	private JPanel drillDownPanel;
	private JLabel lblDrillDownSelectTab;
	private JComboBox<String> drillDownTabSelectorComboBox;
	//drill down panel cluster checkboxes
	private JPanel clusterCheckBoxPanel;
	private JCheckBox checkboxSelectAllClusters;
	private SelectCheckboxesListener selectAllClustersList;
	private ArrayList<JCheckBox> clusterCheckboxes = new ArrayList<JCheckBox>();
	
	public int instanceIndex = 0;
	public String[] columnHeaders;
	public boolean[] isNumeric;
	public List<String> numericalPropNames;
	public List<String> categoricalPropNames;
	
	@Override
	public void createData() {
		if(dataFrame == null || dataFrame.isEmpty())
			super.createData();
		
		columnHeaders = dataFrame.getColumnHeaders();
		isNumeric = dataFrame.isNumeric();
		numericalPropNames = new ArrayList<String>();
		categoricalPropNames = new ArrayList<String>();
		for(int i = 0; i < columnHeaders.length; i++) {
			if(i == instanceIndex) {
				continue;
			}
			if(isNumeric[i]) {
				numericalPropNames.add(columnHeaders[i]);
			} else {
				categoricalPropNames.add(columnHeaders[i]);
			}
		}
	}
	
	@Override
	public void runAnalytics() {
		if(dataFrame == null || dataFrame.isEmpty())
			return;
		
		entropyArr = dataFrame.getEntropyDensity();
		fillSimBarChartHash(dataFrame, new ArrayList<String>());
	}
	
	public void fillSimBarChartHash(ITableDataFrame data, List<String> skipColumns) {
		//run the algorithms for similarity bar chart to create hash.
		DatasetSimilarity alg = new DatasetSimilarity();
		List<SEMOSSParam> options = alg.getOptions();
		HashMap<String, Object> selectedOptions = new HashMap<String, Object>();
		selectedOptions.put(options.get(0).getName(), instanceIndex); // default of 0 is acceptable
		selectedOptions.put(options.get(2).getName(), skipColumns); // TODO: add skipColumns to all algorithms

		alg.setSelectedOptions(selectedOptions);
		ITableDataFrame simAlgResults = alg.runAlgorithm(dataFrame);
		
		Double[] values = simAlgResults.getColumnAsNumeric(simAlgResults.getColumnHeaders()[1]);
		Hashtable<String, Object>[] bins = null;
		BarChart chart = new BarChart(values, simAlgResults.getColumnHeaders()[1]);
		if(chart.isUseCategoricalForNumericInput()) {
			chart.calculateCategoricalBins("?", true, true);
			chart.generateJSONHashtableCategorical();
			bins = chart.getRetHashForJSON();
		} else {
			chart.generateJSONHashtableNumerical();
			bins = chart.getRetHashForJSON();
		}
		
		simBarChartHash = new Hashtable<String, Object>();
		Object[] binArr = new Object[]{bins};
		simBarChartHash.put("dataSeries", binArr);
		simBarChartHash.put("names", new String[]{dataFrame.getColumnHeaders()[instanceIndex].concat(" Similarity Distribution to Dataset Center")});
	}

	@Override
	public void createView() {	
		if(dataFrame == null || dataFrame.isEmpty()){
			String questionID = getQuestionID();
			QuestionPlaySheetStore.getInstance().remove(questionID);
			if(QuestionPlaySheetStore.getInstance().isEmpty())
			{
				JButton btnShowPlaySheetsList = (JButton) DIHelper.getInstance().getLocalProp(Constants.SHOW_PLAYSHEETS_LIST);
				btnShowPlaySheetsList.setEnabled(false);
			}
			Utility.showError("Query returned no results.");
			return;		
		}
		addPanel();

		if(dataFrame != null){
			gfd.setColumnNames(dataFrame.getColumnHeaders());
			gfd.setDataList(dataFrame.getData());
		}

		updateProgressBar("100%...Selector Panel Complete", 100);
	}
	
	@Override
	public void addPanel() {//TODO add control panel?
		setWindow();
		updateProgressBar("0%...Preprocessing", 0);
		resetProgressBar();
		
		JPanel mainPanel = new JPanel();
		this.setContentPane(mainPanel);
		GridBagLayout gbl_mainPanel = new GridBagLayout();
		gbl_mainPanel.columnWidths = new int[]{0, 0};
		gbl_mainPanel.rowHeights = new int[]{0, 0};
		gbl_mainPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_mainPanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		mainPanel.setLayout(gbl_mainPanel);
		
		JPanel barPanel = new JPanel();
		GridBagConstraints gbc_barPanel = new GridBagConstraints();
		gbc_barPanel.fill = GridBagConstraints.BOTH;
		gbc_barPanel.gridx = 0;
		gbc_barPanel.gridy = 1;
		mainPanel.add(barPanel, gbc_barPanel);
		barPanel.setLayout(new BorderLayout(0, 0));
		barPanel.add(jBar, BorderLayout.CENTER);
		
		//Panel that will be the first tab to select variables
		variableSelectorPanel = new JPanel();
		JScrollPane scrollPane = new JScrollPane(variableSelectorPanel);
		scrollPane.getVerticalScrollBar().setUI(new NewScrollBarUI());
		scrollPane.setAutoscrolls(true);
		
		jTab = new JTabbedPane();
		jTab.add("Variable Selector", scrollPane);
		GridBagConstraints gbc_jTab = new GridBagConstraints();
		gbc_jTab.fill = GridBagConstraints.BOTH;
		gbc_jTab.gridx = 0;
		gbc_jTab.gridy = 0;
		mainPanel.add(jTab, gbc_jTab);

		GridBagLayout gbl_panel = new GridBagLayout();
		gbl_panel.columnWidths = new int[]{0, 0, 0};
		gbl_panel.rowHeights = new int[]{0, 0, 0};
		gbl_panel.columnWeights = new double[]{0.0, 0.0, 1.0};
		gbl_panel.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 1.0};//, 0.0, 0.0, 0.0, 0.0, 1.0};
		variableSelectorPanel.setLayout(gbl_panel);

		JLabel lblDataSetSimilarity = new JLabel();
		lblDataSetSimilarity.setText("Dataset similarity distribution");
		lblDataSetSimilarity.setFont(new Font("Tahoma", Font.BOLD, 16));
		GridBagConstraints gbc_lblDataSetSimilarity = new GridBagConstraints();
		gbc_lblDataSetSimilarity.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_lblDataSetSimilarity.fill = GridBagConstraints.NONE;
		gbc_lblDataSetSimilarity.gridwidth = 3;
		gbc_lblDataSetSimilarity.insets = new Insets(10, 5, 0, 0);
		gbc_lblDataSetSimilarity.gridx = 0;
		gbc_lblDataSetSimilarity.gridy = 0;
		variableSelectorPanel.add(lblDataSetSimilarity, gbc_lblDataSetSimilarity);
		
		addSimBarChart();
		
		JLabel indVariablesLabel = new JLabel();
		indVariablesLabel.setText("Filter parameters");
		indVariablesLabel.setFont(new Font("Tahoma", Font.BOLD, 16));
		GridBagConstraints gbc_indVariablesLabel = new GridBagConstraints();
		gbc_indVariablesLabel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_indVariablesLabel.fill = GridBagConstraints.NONE;
		gbc_indVariablesLabel.insets = new Insets(10, 5, 0, 0);
		gbc_indVariablesLabel.gridx = 0;
		gbc_indVariablesLabel.gridy = 2;
		variableSelectorPanel.add(indVariablesLabel, gbc_indVariablesLabel);
		
		indVariablesPanel = new JPanel();
		GridBagConstraints gbc_indVariablesPanel = new GridBagConstraints();
		gbc_indVariablesPanel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_indVariablesPanel.fill = GridBagConstraints.NONE;
		gbc_indVariablesPanel.gridheight = 3;
		gbc_indVariablesPanel.insets = new Insets(10, 5, 0, 0);
		gbc_indVariablesPanel.gridx = 0;
		gbc_indVariablesPanel.gridy = 3;
		variableSelectorPanel.add(indVariablesPanel, gbc_indVariablesPanel);
		
		fillIndependentVariablePanel(indVariablesPanel);

		JLabel lblSelectAnalysis = new JLabel("Select analysis to perform:");
		lblSelectAnalysis.setFont(new Font("Tahoma", Font.BOLD, 16));
		GridBagConstraints gbc_lblSelectAnalysis = new GridBagConstraints();
		gbc_lblSelectAnalysis.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_lblSelectAnalysis.fill = GridBagConstraints.NONE;
		gbc_lblSelectAnalysis.insets = new Insets(10, 15, 0, 0);
		gbc_lblSelectAnalysis.gridx = 1;
		gbc_lblSelectAnalysis.gridy = 2;
		variableSelectorPanel.add(lblSelectAnalysis, gbc_lblSelectAnalysis);

		algorithmComboBox = new JComboBox<String>();
		algorithmComboBox.setFont(new Font("Tahoma", Font.PLAIN, 11));
		algorithmComboBox.setBackground(Color.GRAY);
		algorithmComboBox.setPreferredSize(new Dimension(150, 25));
		algorithmComboBox.setModel(new DefaultComboBoxModel<String>(new String[] {"Cluster", "Classify","Outliers","Association Learning","Similarity",
				"Predictability","Linear Regression","Numerical Correlation", "Correlation", "Self Organizing Map"}));
		GridBagConstraints gbc_algorithmComboBox = new GridBagConstraints();
		gbc_algorithmComboBox.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_algorithmComboBox.fill = GridBagConstraints.NONE;
		gbc_algorithmComboBox.insets = new Insets(10, 5, 0, 0);
		gbc_algorithmComboBox.gridx = 2;
		gbc_algorithmComboBox.gridy = 2;
		variableSelectorPanel.add(algorithmComboBox, gbc_algorithmComboBox);
		SelectAlgorithmListener algSelectList = new SelectAlgorithmListener();
		algSelectList.setView(this);
		algorithmComboBox.addActionListener(algSelectList);
		
		//add in cluster / classify / outlier panels
		//show the cluster as default
		clusterPanel = new JPanel();
		GridBagConstraints gbc_clusterPanel = new GridBagConstraints();
		gbc_clusterPanel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_clusterPanel.fill = GridBagConstraints.NONE;
		gbc_clusterPanel.gridwidth = 3;
		gbc_clusterPanel.insets = new Insets(5, 15, 0, 0);
		gbc_clusterPanel.gridx = 1;
		gbc_clusterPanel.gridy = 3;
		variableSelectorPanel.add(clusterPanel, gbc_clusterPanel);
		
		classifyPanel = new JPanel();
		GridBagConstraints gbc_classifyPanel = new GridBagConstraints();
		gbc_classifyPanel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_classifyPanel.fill = GridBagConstraints.NONE;
		gbc_classifyPanel.gridwidth = 3;
		gbc_classifyPanel.insets = new Insets(5, 15, 0, 0);
		gbc_classifyPanel.gridx = 1;
		gbc_classifyPanel.gridy = 3;
		variableSelectorPanel.add(classifyPanel, gbc_classifyPanel);
		classifyPanel.setVisible(false);
		
		outlierPanel = new JPanel();
		GridBagConstraints gbc_outlierPanel = new GridBagConstraints();
		gbc_outlierPanel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_outlierPanel.fill = GridBagConstraints.NONE;
		gbc_outlierPanel.gridwidth = 3;
		gbc_outlierPanel.insets = new Insets(5, 15, 0, 0);
		gbc_outlierPanel.gridx = 1;
		gbc_outlierPanel.gridy = 3;
		variableSelectorPanel.add(outlierPanel, gbc_outlierPanel);
		outlierPanel.setVisible(false);
		
		associationLearningPanel = new JPanel();
		GridBagConstraints gbc_frequentSetsPanel = new GridBagConstraints();
		gbc_frequentSetsPanel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_frequentSetsPanel.fill = GridBagConstraints.NONE;
		gbc_frequentSetsPanel.gridwidth = 3;
		gbc_frequentSetsPanel.insets = new Insets(5, 15, 0, 0);
		gbc_frequentSetsPanel.gridx = 1;
		gbc_frequentSetsPanel.gridy = 3;
		variableSelectorPanel.add(associationLearningPanel, gbc_frequentSetsPanel);
		associationLearningPanel.setVisible(false);
		
		matrixRegPanel = new JPanel();
		GridBagConstraints gbc_matrixRegPanel = new GridBagConstraints();
		gbc_matrixRegPanel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_matrixRegPanel.fill = GridBagConstraints.NONE;
		gbc_matrixRegPanel.gridwidth = 3;
		gbc_matrixRegPanel.insets = new Insets(5, 15, 0, 0);
		gbc_matrixRegPanel.gridx = 1;
		gbc_matrixRegPanel.gridy = 3;
		variableSelectorPanel.add(matrixRegPanel, gbc_matrixRegPanel);
		matrixRegPanel.setVisible(false);

		somPanel = new JPanel();
		GridBagConstraints gbc_somPanel = new GridBagConstraints();
		gbc_somPanel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_somPanel.fill = GridBagConstraints.NONE;
		gbc_somPanel.gridwidth = 3;
		gbc_somPanel.insets = new Insets(5, 15, 0, 0);
		gbc_somPanel.gridx = 1;
		gbc_somPanel.gridy = 3;
		variableSelectorPanel.add(somPanel, gbc_somPanel);
		somPanel.setVisible(false);
		
		fillClusterPanel(clusterPanel);
		fillClassifyPanel(classifyPanel);
		fillOutlierPanel(outlierPanel);
		fillFrequentSetsPanel(associationLearningPanel);
		fillMatrixRegPanel(matrixRegPanel);
		fillSOMPanel(somPanel);

		runAlgorithm = new CustomButton("Run Algorithm");
		runAlgorithm.setFont(new Font("Tahoma", Font.BOLD, 11));
		runAlgorithm.setPreferredSize(new Dimension(150, 25));
		GridBagConstraints gbc_runAlgorithm = new GridBagConstraints();
		gbc_runAlgorithm.insets = new Insets(10, 15, 0, 0);
		gbc_runAlgorithm.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_runAlgorithm.fill = GridBagConstraints.NONE;
		gbc_runAlgorithm.gridx = 1;
		gbc_runAlgorithm.gridy = 4;
		variableSelectorPanel.add(runAlgorithm, gbc_runAlgorithm);
		Style.registerTargetClassName(runAlgorithm,  ".createBtn");

		showDrillDownBtn = new ToggleButton("Drill Down on Specific Clusters");
		showDrillDownBtn.setName("showDrillDownBtn");
		showDrillDownBtn.setFont(new Font("Tahoma", Font.BOLD, 11));
		showDrillDownBtn.setPreferredSize(new Dimension(150, 25));
		showDrillDownBtn.setVisible(false);
		Style.registerTargetClassName(showDrillDownBtn,  ".toggleButton");
		
		GridBagConstraints gbc_showDrillDownBtn = new GridBagConstraints();
		gbc_showDrillDownBtn.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_showDrillDownBtn.fill = GridBagConstraints.NONE;
		gbc_showDrillDownBtn.insets = new Insets(10, 15, 0, 0);
		gbc_showDrillDownBtn.gridx = 2;
		gbc_showDrillDownBtn.gridy = 4;
		variableSelectorPanel.add(showDrillDownBtn, gbc_showDrillDownBtn);
		
		drillDownPanel = new JPanel();
		drillDownPanel.setBorder(new BevelBorder(BevelBorder.LOWERED, null, null, null, null));
		
		GridBagConstraints gbc_drillDownPanel = new GridBagConstraints();
		gbc_drillDownPanel.anchor = GridBagConstraints.FIRST_LINE_START;
		//gbc_drillDownPanel.fill = GridBagConstraints.BOTH;
		gbc_drillDownPanel.gridwidth = 2;
		gbc_drillDownPanel.insets = new Insets(15, 5, 0, 0);
		gbc_drillDownPanel.gridx = 1;
		gbc_drillDownPanel.gridy = 5;
		variableSelectorPanel.add(drillDownPanel, gbc_drillDownPanel);
		drillDownPanel.setVisible(false);

		fillDrillDownPanel(drillDownPanel);
		
		RunAlgorithmListener runListener = new RunAlgorithmListener();
		runListener.setView(this);
		runListener.setEntropyArr(entropyArr);
		runAlgorithm.addActionListener(runListener);
		
		ShowDrillDownListener drillDownListener = new ShowDrillDownListener();
		drillDownListener.setView(this);
		showDrillDownBtn.addActionListener(drillDownListener);
		
		ClusterTabSelectionListener drillDownSelectTabListener = new ClusterTabSelectionListener();
		drillDownSelectTabListener.setView(this);
		drillDownTabSelectorComboBox.addActionListener(drillDownSelectTabListener);
		
		RunDrillDownListener runDrillDownListener = new RunDrillDownListener();
		runDrillDownListener.setView(this);
		runDrillDown.addActionListener(runDrillDownListener);

		this.setPreferredSize(new Dimension(1000,750));
		new CSSApplication(getContentPane());
		pane.add(this);
		try {
			this.pack();
			this.setVisible(true);
			simBarChartPanel.callIt(simBarChartHash);			
			this.setSelected(false);
			this.setSelected(true);
		} catch (PropertyVetoException e) {
			LOGGER.error("Exception creating view");
		}
	
	}
	
	public void addSimBarChart() {
		simBarChartPanel = new BrowserGraphPanel("/html/MHS-RDFSemossCharts/app/columnchart.html");
		simBarChartPanel.setPreferredSize(new Dimension(500, 300));
		GridBagConstraints gbc_simBarChartPanel = new GridBagConstraints();
		gbc_simBarChartPanel.fill = GridBagConstraints.BOTH;
		gbc_simBarChartPanel.gridwidth = 3;
		gbc_simBarChartPanel.insets = new Insets(10, 5, 0, 0);
		gbc_simBarChartPanel.gridx = 0;
		gbc_simBarChartPanel.gridy = 1;
		variableSelectorPanel.add(simBarChartPanel, gbc_simBarChartPanel);
	}
	
	public void recreateSimBarChart(ITableDataFrame dataFrame, List<String> skipColumns) {
		BrowserGraphPanel oldSimBarChartPanel = simBarChartPanel;
		fillSimBarChartHash(dataFrame, skipColumns);
		addSimBarChart();
		variableSelectorPanel.remove(oldSimBarChartPanel);
		
		this.pack();
		this.setVisible(true);
		simBarChartPanel.callIt(simBarChartHash);
		try{
			this.setSelected(false);
			this.setSelected(true);
		} catch (PropertyVetoException e) {
			LOGGER.error("Exception creating similarity bar chart");
		}
	}
	
	private void fillIndependentVariablePanel(JPanel indVariablesPanel) {
		GridBagLayout gbl_indVariablesPanel = new GridBagLayout();
		gbl_indVariablesPanel.columnWidths = new int[]{0, 0, 0};
		gbl_indVariablesPanel.rowHeights = new int[]{0, 0, 0};
		gbl_indVariablesPanel.columnWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
		gbl_indVariablesPanel.rowWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
		indVariablesPanel.setLayout(gbl_indVariablesPanel);
		
		//select all, deselect all independent variables
		String checkboxSelectLabel = "Select All Variables";
		checkboxSelectAllIVs = new JCheckBox(checkboxSelectLabel);
		checkboxSelectAllIVs.setName(checkboxSelectLabel+"checkBox");
		checkboxSelectAllIVs.setSelected(true);
		GridBagConstraints gbc_checkboxSelectAllIVs = new GridBagConstraints();
		gbc_checkboxSelectAllIVs.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_checkboxSelectAllIVs.fill = GridBagConstraints.NONE;
		gbc_checkboxSelectAllIVs.insets = new Insets(5, 20, 0, 0);
		gbc_checkboxSelectAllIVs.gridx = 0;
		gbc_checkboxSelectAllIVs.gridy = 0;
		indVariablesPanel.add(checkboxSelectAllIVs, gbc_checkboxSelectAllIVs);

		JLabel includeLabel = new JLabel();
		includeLabel.setText("Variables");
		includeLabel.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_includeLabel = new GridBagConstraints();
		gbc_includeLabel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_includeLabel.fill = GridBagConstraints.NONE;
		gbc_includeLabel.gridwidth = 2;
		gbc_includeLabel.insets = new Insets(5, 20, 0, 0);
		gbc_includeLabel.gridx = 0;
		gbc_includeLabel.gridy = 1;
		indVariablesPanel.add(includeLabel, gbc_includeLabel);
		
		JLabel entropyLabel = new JLabel();
		entropyLabel.setText("Entropy");
		entropyLabel.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_entropyLabel = new GridBagConstraints();
		gbc_entropyLabel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_entropyLabel.fill = GridBagConstraints.NONE;
		gbc_entropyLabel.insets = new Insets(5, 20, 0, 0);
		gbc_entropyLabel.gridx = 2;
		gbc_entropyLabel.gridy = 1;
		indVariablesPanel.add(entropyLabel, gbc_entropyLabel);
		
		JLabel accuracyLabel = new JLabel();
		accuracyLabel.setText("Accuracy");
		accuracyLabel.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_accuracyLabel = new GridBagConstraints();
		gbc_accuracyLabel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_accuracyLabel.fill = GridBagConstraints.NONE;
		gbc_accuracyLabel.insets = new Insets(5, 20, 0, 0);
		gbc_accuracyLabel.gridx = 3;
		gbc_accuracyLabel.gridy = 1;
		indVariablesPanel.add(accuracyLabel, gbc_accuracyLabel);
		
		JLabel precisionLabel = new JLabel();
		precisionLabel.setText("Precision");
		precisionLabel.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_precisionLabel = new GridBagConstraints();
		gbc_precisionLabel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_precisionLabel.fill = GridBagConstraints.NONE;
		gbc_precisionLabel.insets = new Insets(5, 20, 0, 0);
		gbc_precisionLabel.gridx = 4;
		gbc_precisionLabel.gridy = 1;
		indVariablesPanel.add(precisionLabel, gbc_precisionLabel);
		
		ivCheckboxes = new ArrayList<JCheckBox>();
		entropyLabels = new ArrayList<JLabel>();

		DecimalFormat formatter = new DecimalFormat("#0.00");
		
		for(int i = 1; i < columnHeaders.length; i++) {
			if(i == instanceIndex) {
				continue;
			}
			String checkboxLabel = columnHeaders[i];
			JCheckBox checkbox = new JCheckBox(checkboxLabel);
			checkbox.setName(checkboxLabel+checkboxName);
			checkbox.setSelected(true);
			GridBagConstraints gbc_checkbox = new GridBagConstraints();
			gbc_checkbox.anchor = GridBagConstraints.FIRST_LINE_START;
			gbc_checkbox.gridwidth = 2;
			gbc_checkbox.fill = GridBagConstraints.NONE;
			gbc_checkbox.insets = new Insets(5, 20, 0, 0);
			gbc_checkbox.gridx = 0;
			gbc_checkbox.gridy = i+1;
			indVariablesPanel.add(checkbox, gbc_checkbox);
			ivCheckboxes.add(checkbox);
				
			JLabel entropyVal = new JLabel();
			entropyVal.setText(formatter.format(entropyArr[i]));
			GridBagConstraints gbc_entropyVal = new GridBagConstraints();
			gbc_entropyVal.anchor = GridBagConstraints.FIRST_LINE_START;
			gbc_entropyVal.fill = GridBagConstraints.NONE;
			gbc_entropyVal.insets = new Insets(5, 20, 0, 0);
			gbc_entropyVal.gridx = 2;
			gbc_entropyVal.gridy = i+1;
			indVariablesPanel.add(entropyVal, gbc_entropyVal);
			entropyLabels.add(entropyVal);
		}
		
		selectAllIVsList = new SelectCheckboxesListener();
		selectAllIVsList.setCheckboxes(ivCheckboxes);
		checkboxSelectAllIVs.addActionListener(selectAllIVsList);
		
		accuracyLabels = new ArrayList<JLabel>(columnHeaders.length);
		precisionLabels = new ArrayList<JLabel>(columnHeaders.length);
	}
	
	public void fillAccuracyAndPrecision(double[] accuracyArr, double[] precisionArr) {
		DecimalFormat formatter = new DecimalFormat("#0.00");
		
		boolean[] includeColArr = new boolean[ivCheckboxes.size()];
		includeColArr[0] = true; //this is the "title" or "name"
		for(int i = 0; i < ivCheckboxes.size(); i++) {
			JCheckBox checkbox = ivCheckboxes.get(i);
			includeColArr[i] = checkbox.isSelected();
		}

		for(int i = 0; i < columnHeaders.length - 1; i++) {
			if(includeColArr[i]) {
				try {
					// try to remove old values if present
					indVariablesPanel.remove(accuracyLabels.get(i));
					indVariablesPanel.remove(precisionLabels.get(i));
					accuracyLabels.remove(i);
					precisionLabels.remove(i);
				} catch (IndexOutOfBoundsException ex) {
					// do nothing
				}				
				JLabel accuracyVal = new JLabel();
				accuracyVal.setText(formatter.format(accuracyArr[i])+"%");
				GridBagConstraints gbc_accuracyVal = new GridBagConstraints();
				gbc_accuracyVal.anchor = GridBagConstraints.FIRST_LINE_START;
				gbc_accuracyVal.fill = GridBagConstraints.NONE;
				gbc_accuracyVal.insets = new Insets(5, 20, 0, 0);
				gbc_accuracyVal.gridx = 3;
				gbc_accuracyVal.gridy = i+2;
				indVariablesPanel.add(accuracyVal, gbc_accuracyVal);
				accuracyLabels.add(i, accuracyVal);
				
				JLabel precisionVal = new JLabel();
				precisionVal.setText(formatter.format(precisionArr[i])+"%");
				GridBagConstraints gbc_precisionVal = new GridBagConstraints();
				gbc_precisionVal.anchor = GridBagConstraints.FIRST_LINE_START;
				gbc_precisionVal.fill = GridBagConstraints.NONE;
				gbc_precisionVal.insets = new Insets(5, 20, 0, 0);
				gbc_precisionVal.gridx = 4;
				gbc_precisionVal.gridy = i+2;
				indVariablesPanel.add(precisionVal, gbc_precisionVal);
				precisionLabels.add(i, precisionVal);
			} else {
				try {
					accuracyLabels.get(i);
				} catch (IndexOutOfBoundsException ex) {
					// if value isn't present
					JLabel accuracyVal = new JLabel();
					accuracyVal.setText("Not Selected");
					GridBagConstraints gbc_accuracyVal = new GridBagConstraints();
					gbc_accuracyVal.anchor = GridBagConstraints.FIRST_LINE_START;
					gbc_accuracyVal.fill = GridBagConstraints.NONE;
					gbc_accuracyVal.insets = new Insets(5, 20, 0, 0);
					gbc_accuracyVal.gridx = 3;
					gbc_accuracyVal.gridy = i+2;
					indVariablesPanel.add(accuracyVal, gbc_accuracyVal);
					accuracyLabels.add(i, accuracyVal);
					
					JLabel precisionVal = new JLabel();
					precisionVal.setText("Not Selected");
					GridBagConstraints gbc_precisionVal = new GridBagConstraints();
					gbc_precisionVal.anchor = GridBagConstraints.FIRST_LINE_START;
					gbc_precisionVal.fill = GridBagConstraints.NONE;
					gbc_precisionVal.insets = new Insets(5, 20, 0, 0);
					gbc_precisionVal.gridx = 4;
					gbc_precisionVal.gridy = i+2;
					indVariablesPanel.add(precisionVal, gbc_precisionVal);
					precisionLabels.add(i, precisionVal);
				}
			}
		}
		indVariablesPanel.revalidate();
		indVariablesPanel.repaint();
	}
	
	private void fillClusterPanel(JPanel clusterPanel) {
		GridBagLayout gbl_clusterPanel = new GridBagLayout();
		gbl_clusterPanel.columnWidths = new int[]{0, 0, 0};
		gbl_clusterPanel.rowHeights = new int[]{0, 0, 0};
		gbl_clusterPanel.columnWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
		gbl_clusterPanel.rowWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
		clusterPanel.setLayout(gbl_clusterPanel);
		
		lblSelectNumClusters = new JLabel("Select number of clusters:");
		lblSelectNumClusters.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_lblSelectNumClusters = new GridBagConstraints();
		gbc_lblSelectNumClusters.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_lblSelectNumClusters.fill = GridBagConstraints.NONE;
		gbc_lblSelectNumClusters.insets = new Insets(10, 5, 0, 0);
		gbc_lblSelectNumClusters.gridx = 0;
		gbc_lblSelectNumClusters.gridy = 0;
		clusterPanel.add(lblSelectNumClusters, gbc_lblSelectNumClusters);

		selectNumClustersComboBox = new JComboBox<String>();
		selectNumClustersComboBox.setFont(new Font("Tahoma", Font.PLAIN, 11));
		selectNumClustersComboBox.setBackground(Color.GRAY);
		selectNumClustersComboBox.setPreferredSize(new Dimension(225, 25));
		String[] dropDownVals = new String[] {automaticallySelectNumClustersText,manuallySelectNumClustersText};
		selectNumClustersComboBox.setModel(new DefaultComboBoxModel<String>(dropDownVals));
		GridBagConstraints gbc_selectNumClustersComboBox = new GridBagConstraints();
		gbc_selectNumClustersComboBox.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_selectNumClustersComboBox.fill = GridBagConstraints.NONE;
		gbc_selectNumClustersComboBox.insets = new Insets(5, 5, 0, 0);
		gbc_selectNumClustersComboBox.gridx = 1;
		gbc_selectNumClustersComboBox.gridy = 0;
		clusterPanel.add(selectNumClustersComboBox, gbc_selectNumClustersComboBox);
		
		selectNumClustersTextField = new JTextField();
		selectNumClustersTextField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		selectNumClustersTextField.setText("2");
		selectNumClustersTextField.setColumns(4);
		selectNumClustersTextField.setVisible(false);
		GridBagConstraints gbc_selectNumClustersTextField = new GridBagConstraints();
		gbc_selectNumClustersTextField.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_selectNumClustersTextField.fill = GridBagConstraints.NONE;
		gbc_selectNumClustersTextField.insets = new Insets(5, 5, 0, 0);
		gbc_selectNumClustersTextField.gridx = 2;
		gbc_selectNumClustersTextField.gridy = 0;
		clusterPanel.add(selectNumClustersTextField, gbc_selectNumClustersTextField);
		
		NumberOfClustersSelectionListener numClustersSelectList = new NumberOfClustersSelectionListener();
		numClustersSelectList.setView(this);
		selectNumClustersComboBox.addActionListener(numClustersSelectList);
	}
	
	private void fillClassifyPanel(JPanel classifyPanel) {
		
		GridBagLayout gbl_classifyPanel = new GridBagLayout();
		gbl_classifyPanel.columnWidths = new int[]{0, 0, 0};
		gbl_classifyPanel.rowHeights = new int[]{0, 0, 0};
		gbl_classifyPanel.columnWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
		gbl_classifyPanel.rowWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
		classifyPanel.setLayout(gbl_classifyPanel);
		
		lblSelectClass = new JLabel("Select class:");
		lblSelectClass.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_lblSelectClass = new GridBagConstraints();
		gbc_lblSelectClass.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_lblSelectClass.fill = GridBagConstraints.NONE;
		gbc_lblSelectClass.insets = new Insets(10, 5, 0, 0);
		gbc_lblSelectClass.gridx = 0;
		gbc_lblSelectClass.gridy = 0;
		classifyPanel.add(lblSelectClass, gbc_lblSelectClass);

		classComboBox = new JComboBox<String>();
		classComboBox.setName("classComboBox");
		classComboBox.setFont(new Font("Tahoma", Font.PLAIN, 11));
		classComboBox.setBackground(Color.GRAY);
		classComboBox.setPreferredSize(new Dimension(250, 25));
		String[] cols = new String[columnHeaders.length-1];
		
		int counter = 0;
		for(int i = 0; i < columnHeaders.length; i++) {
			if(i == instanceIndex) {
				continue;
			}
			cols[counter] = columnHeaders[i];
			counter++;
		}
		classComboBox.setModel(new DefaultComboBoxModel<String>(cols));
		GridBagConstraints gbc_classComboBox = new GridBagConstraints();
		gbc_classComboBox.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_classComboBox.fill = GridBagConstraints.NONE;
		gbc_classComboBox.insets = new Insets(5, 5, 0, 0);
		gbc_classComboBox.gridx = 1;
		gbc_classComboBox.gridy = 0;
		classifyPanel.add(classComboBox, gbc_classComboBox);
		ClassificationSelectionListener classSelectList = new ClassificationSelectionListener();
		classSelectList.setView(this);
		classComboBox.addActionListener(classSelectList);
		
		lblSelectClassMethod = new JLabel("Select classification Method:");
		lblSelectClassMethod.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_lblSelectClassMethod = new GridBagConstraints();
		gbc_lblSelectClassMethod.anchor = GridBagConstraints.FIRST_LINE_END;
		gbc_lblSelectClassMethod.fill = GridBagConstraints.NONE;
		gbc_lblSelectClassMethod.insets = new Insets(10, 5, 0, 0);
		gbc_lblSelectClassMethod.gridx = 0;
		gbc_lblSelectClassMethod.gridy = 1;
		classifyPanel.add(lblSelectClassMethod, gbc_lblSelectClassMethod);

		classificationMethodComboBox = new JComboBox<String>();
		classificationMethodComboBox.setFont(new Font("Tahoma", Font.PLAIN, 11));
		classificationMethodComboBox.setBackground(Color.GRAY);
		classificationMethodComboBox.setPreferredSize(new Dimension(250, 25));
		String[] classTypes = new String[] {"J48","J48GRAFT","SIMPLECART","REPTREE","BFTREE"};
		classificationMethodComboBox.setModel(new DefaultComboBoxModel<String>(classTypes));
		GridBagConstraints gbc_classificationMethodComboBox = new GridBagConstraints();
		gbc_classificationMethodComboBox.anchor = GridBagConstraints.FIRST_LINE_END;
		gbc_classificationMethodComboBox.fill = GridBagConstraints.NONE;
		gbc_classificationMethodComboBox.insets = new Insets(5, 5, 0, 0);
		gbc_classificationMethodComboBox.gridx = 1;
		gbc_classificationMethodComboBox.gridy = 1;
		classifyPanel.add(classificationMethodComboBox, gbc_classificationMethodComboBox);

	}
	
	private void fillOutlierPanel(JPanel outlierPanel) {
		GridBagLayout gbl_outlierPanel = new GridBagLayout();
		gbl_outlierPanel.columnWidths = new int[]{0, 0, 0};
		gbl_outlierPanel.rowHeights = new int[]{0, 0, 0};
		gbl_outlierPanel.columnWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
		gbl_outlierPanel.rowWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
		outlierPanel.setLayout(gbl_outlierPanel);
		
		lblEnterKNeighbors = new JLabel("Enter K neighbors:");
		lblEnterKNeighbors.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_lblEnterKNeighbors = new GridBagConstraints();
		gbc_lblEnterKNeighbors.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_lblEnterKNeighbors.fill = GridBagConstraints.NONE;
		gbc_lblEnterKNeighbors.insets = new Insets(10, 5, 0, 0);
		gbc_lblEnterKNeighbors.gridx = 0;
		gbc_lblEnterKNeighbors.gridy = 0;
		outlierPanel.add(lblEnterKNeighbors, gbc_lblEnterKNeighbors);
		
		enterKNeighborsSlider = new JSlider(5,45,25);
		enterKNeighborsSlider.setFont(new Font("Tahoma", Font.PLAIN, 11));
		enterKNeighborsSlider.setMajorTickSpacing(5);
		enterKNeighborsSlider.setMinorTickSpacing(1);
		enterKNeighborsSlider.setPaintLabels(true);
		enterKNeighborsSlider.setPaintTicks(true);
		enterKNeighborsSlider.setPreferredSize(new Dimension(400,40));
		//enterKNeighborsSlider.set
//		enterKNeighborsTextField.setText("5");
//		enterKNeighborsTextField.setColumns(4);
		enterKNeighborsSlider.setVisible(false);
		GridBagConstraints gbc_getEnterKNeighborsSlider = new GridBagConstraints();
		gbc_getEnterKNeighborsSlider.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_getEnterKNeighborsSlider.fill = GridBagConstraints.NONE;
		gbc_getEnterKNeighborsSlider.insets = new Insets(5, 5, 0, 0);
		gbc_getEnterKNeighborsSlider.gridx = 0;
		gbc_getEnterKNeighborsSlider.gridy = 1;
		outlierPanel.add(enterKNeighborsSlider, gbc_getEnterKNeighborsSlider);
	}
	
	private void fillMatrixRegPanel(JPanel matrixRegPanel) {
		GridBagLayout gbl_matrixRegPanel = new GridBagLayout();
		gbl_matrixRegPanel.columnWidths = new int[]{0, 0, 0};
		gbl_matrixRegPanel.rowHeights = new int[]{0, 0, 0};
		gbl_matrixRegPanel.columnWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
		gbl_matrixRegPanel.rowWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
		matrixRegPanel.setLayout(gbl_matrixRegPanel);
		
		lblSelectDepVar = new JLabel("Select Dependent Var:");
		lblSelectDepVar.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_lblSelectDepVar = new GridBagConstraints();
		gbc_lblSelectDepVar.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_lblSelectDepVar.fill = GridBagConstraints.NONE;
		gbc_lblSelectDepVar.insets = new Insets(10, 5, 0, 0);
		gbc_lblSelectDepVar.gridx = 0;
		gbc_lblSelectDepVar.gridy = 0;
		matrixRegPanel.add(lblSelectDepVar, gbc_lblSelectDepVar);
		
		matrixDepVarComboBox = new JComboBox<String>();
		matrixDepVarComboBox.setName("matrixDepVarComboBox");
		matrixDepVarComboBox.setFont(new Font("Tahoma", Font.PLAIN, 11));
		matrixDepVarComboBox.setBackground(Color.GRAY);
		matrixDepVarComboBox.setPreferredSize(new Dimension(250, 25));
		if(!numericalPropNames.isEmpty()) {
			matrixDepVarComboBox.setModel(new DefaultComboBoxModel<String>(numericalPropNames.toArray(new String[0])));
		}
		GridBagConstraints gbc_matrixDepVarComboBox = new GridBagConstraints();
		gbc_matrixDepVarComboBox.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_matrixDepVarComboBox.fill = GridBagConstraints.NONE;
		gbc_matrixDepVarComboBox.insets = new Insets(5, 5, 0, 0);
		gbc_matrixDepVarComboBox.gridx = 1;
		gbc_matrixDepVarComboBox.gridy = 0;
		matrixRegPanel.add(matrixDepVarComboBox, gbc_matrixDepVarComboBox);
		ClassificationSelectionListener classSelectList = new ClassificationSelectionListener();
		classSelectList.setView(this);
		matrixDepVarComboBox.addActionListener(classSelectList);
	}
	
	private void fillSOMPanel(JPanel somPanel) {
		GridBagLayout gbl_somPanel = new GridBagLayout();
		gbl_somPanel.columnWidths = new int[]{0, 0, 0};
		gbl_somPanel.rowHeights = new int[]{0, 0, 0};
		gbl_somPanel.columnWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
		gbl_somPanel.rowWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
		somPanel.setLayout(gbl_somPanel);
		
		double x = Math.sqrt((double) dataFrame.getNumCols() / (6*5));
		som_height = (int) Math.round(2*x);
		som_length = (int) Math.round(3*x);
		
		JLabel lblSizeGrid = new JLabel("Grid size is: " + som_height + " x " + som_length);
		lblSizeGrid.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_lblSizeGrid = new GridBagConstraints();
		gbc_lblSizeGrid.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_lblSizeGrid.fill = GridBagConstraints.NONE;
		gbc_lblSizeGrid.insets = new Insets(10, 5, 0, 0);
		gbc_lblSizeGrid.gridx = 0;
		gbc_lblSizeGrid.gridy = 0;
		gbc_lblSizeGrid.gridwidth = 2;
		somPanel.add(lblSizeGrid, gbc_lblSizeGrid);
		
		lblEnterR0 = new JLabel("Enter Initial Radius (r0): ");
		lblEnterR0.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_lblEnterR0 = new GridBagConstraints();
		gbc_lblEnterR0.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_lblEnterR0.fill = GridBagConstraints.NONE;
		gbc_lblEnterR0.insets = new Insets(10, 5, 0, 0);
		gbc_lblEnterR0.gridx = 0;
		gbc_lblEnterR0.gridy = 1;
		somPanel.add(lblEnterR0, gbc_lblEnterR0);
		
		int defaultMaxIt = 15;
		double defaultR0 = som_length / 6;
		double defaultTau = defaultMaxIt/defaultR0;
		
		enterR0TextField = new JTextField();
		enterR0TextField.setColumns(4);
		enterR0TextField.setText(defaultR0+"");
		GridBagConstraints gbc_enterR0TextField = new GridBagConstraints();
		gbc_enterR0TextField.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_enterR0TextField.fill = GridBagConstraints.NONE;
		gbc_enterR0TextField.insets = new Insets(10, 5, 0, 0);
		gbc_enterR0TextField.gridx = 1;
		gbc_enterR0TextField.gridy = 1;
		somPanel.add(enterR0TextField, gbc_enterR0TextField);
		
		lblEnterL0 = new JLabel("Enter Learning Rate (l0): ");
		lblEnterL0.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_lblEnterL0 = new GridBagConstraints();
		gbc_lblEnterL0.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_lblEnterL0.fill = GridBagConstraints.NONE;
		gbc_lblEnterL0.insets = new Insets(10, 5, 0, 0);
		gbc_lblEnterL0.gridx = 0;
		gbc_lblEnterL0.gridy = 2;
		somPanel.add(lblEnterL0, gbc_lblEnterL0);
		
		enterL0TextField = new JTextField();
		enterL0TextField.setColumns(4);
		enterL0TextField.setText("0.07");
		GridBagConstraints gbc_enterL0TextField = new GridBagConstraints();
		gbc_enterL0TextField.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_enterL0TextField.fill = GridBagConstraints.NONE;
		gbc_enterL0TextField.insets = new Insets(10, 5, 0, 0);
		gbc_enterL0TextField.gridx = 1;
		gbc_enterL0TextField.gridy = 2;
		somPanel.add(enterL0TextField, gbc_enterL0TextField);
		
		lblEnterTau = new JLabel("Enter Tau: ");
		lblEnterTau.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_lblEnterTau = new GridBagConstraints();
		gbc_lblEnterTau.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_lblEnterTau.fill = GridBagConstraints.NONE;
		gbc_lblEnterTau.insets = new Insets(10, 5, 0, 0);
		gbc_lblEnterTau.gridx = 0;
		gbc_lblEnterTau.gridy = 3;
		somPanel.add(lblEnterTau, gbc_lblEnterTau);
		
		enterTauTextField = new JTextField();
		enterTauTextField.setColumns(4);
		enterTauTextField.setText(defaultTau+"");
		GridBagConstraints gbc_enterTauTextField = new GridBagConstraints();
		gbc_enterTauTextField.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_enterTauTextField.fill = GridBagConstraints.NONE;
		gbc_enterTauTextField.insets = new Insets(10, 5, 0, 0);
		gbc_enterTauTextField.gridx = 1;
		gbc_enterTauTextField.gridy = 3;
		somPanel.add(enterTauTextField, gbc_enterTauTextField);
		
		lblEnterMaxIt = new JLabel("Enter Maximum Iterations: ");
		lblEnterMaxIt.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_lblEnterMaxIt = new GridBagConstraints();
		gbc_lblEnterMaxIt.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_lblEnterMaxIt.fill = GridBagConstraints.NONE;
		gbc_lblEnterMaxIt.insets = new Insets(10, 5, 0, 0);
		gbc_lblEnterMaxIt.gridx = 0;
		gbc_lblEnterMaxIt.gridy = 4;
		somPanel.add(lblEnterMaxIt, gbc_lblEnterMaxIt);
		
		enterMaxItTextField = new JTextField();
		enterMaxItTextField.setColumns(4);
		enterMaxItTextField.setText(defaultMaxIt+"");
		GridBagConstraints gbc_enterMaxItTextField = new GridBagConstraints();
		gbc_enterMaxItTextField.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_enterMaxItTextField.fill = GridBagConstraints.NONE;
		gbc_enterMaxItTextField.insets = new Insets(10, 5, 0, 0);
		gbc_enterMaxItTextField.gridx = 1;
		gbc_enterMaxItTextField.gridy = 4;
		somPanel.add(enterMaxItTextField, gbc_enterMaxItTextField);
	}
	
	private void fillFrequentSetsPanel(JPanel frequentSetsPanel) {
		GridBagLayout gbl_frequentSetsPanel = new GridBagLayout();
		gbl_frequentSetsPanel.columnWidths = new int[]{0, 0, 0};
		gbl_frequentSetsPanel.rowHeights = new int[]{0, 0, 0};
		gbl_frequentSetsPanel.columnWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
		gbl_frequentSetsPanel.rowWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
		frequentSetsPanel.setLayout(gbl_frequentSetsPanel);
		
		lblEnterNumRules = new JLabel("Enter Number of Rules:");
		lblEnterNumRules.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_lblEnterNumRules = new GridBagConstraints();
		gbc_lblEnterNumRules.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_lblEnterNumRules.fill = GridBagConstraints.NONE;
		gbc_lblEnterNumRules.insets = new Insets(10, 5, 0, 0);
		gbc_lblEnterNumRules.gridx = 0;
		gbc_lblEnterNumRules.gridy = 0;
		frequentSetsPanel.add(lblEnterNumRules, gbc_lblEnterNumRules);
		
		enterNumRulesTextField = new JTextField();
		enterNumRulesTextField.setText("10");
		enterNumRulesTextField.setColumns(4);
		GridBagConstraints gbc_numRulesTextField = new GridBagConstraints();
		gbc_numRulesTextField.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_numRulesTextField.fill = GridBagConstraints.NONE;
		gbc_numRulesTextField.insets = new Insets(10, 5, 0, 0);
		gbc_numRulesTextField.gridx = 1;
		gbc_numRulesTextField.gridy = 0;
		frequentSetsPanel.add(enterNumRulesTextField, gbc_numRulesTextField);
		
		lblEnterMinSupport = new JLabel("Enter Minimum Support:");
		lblEnterMinSupport.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_lblEnterMinSupport = new GridBagConstraints();
		gbc_lblEnterMinSupport.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_lblEnterMinSupport.fill = GridBagConstraints.NONE;
		gbc_lblEnterMinSupport.insets = new Insets(10, 5, 0, 0);
		gbc_lblEnterMinSupport.gridx = 0;
		gbc_lblEnterMinSupport.gridy = 1;
		frequentSetsPanel.add(lblEnterMinSupport, gbc_lblEnterMinSupport);
		
		enterMinSupportTextField = new JTextField();
		enterMinSupportTextField.setText("0.1");
		enterMinSupportTextField.setColumns(4);
		GridBagConstraints gbc_enterMinSupportTextField = new GridBagConstraints();
		gbc_enterMinSupportTextField.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_enterMinSupportTextField.fill = GridBagConstraints.NONE;
		gbc_enterMinSupportTextField.insets = new Insets(10, 5, 0, 0);
		gbc_enterMinSupportTextField.gridx = 1;
		gbc_enterMinSupportTextField.gridy = 1;
		frequentSetsPanel.add(enterMinSupportTextField, gbc_enterMinSupportTextField);
		
		lblEnterMaxSupport = new JLabel("Enter Maximum Support:");
		lblEnterMaxSupport.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_lblEnterMaxSupport = new GridBagConstraints();
		gbc_lblEnterMaxSupport.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_lblEnterMaxSupport.fill = GridBagConstraints.NONE;
		gbc_lblEnterMaxSupport.insets = new Insets(10, 5, 0, 0);
		gbc_lblEnterMaxSupport.gridx = 0;
		gbc_lblEnterMaxSupport.gridy = 2;
		frequentSetsPanel.add(lblEnterMaxSupport, gbc_lblEnterMaxSupport);
		
		enterMaxSupportTextField = new JTextField();
		enterMaxSupportTextField.setText("1.0");
		enterMaxSupportTextField.setColumns(4);
		GridBagConstraints gbc_enterMaxSupportTextField = new GridBagConstraints();
		gbc_enterMaxSupportTextField.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_enterMaxSupportTextField.fill = GridBagConstraints.NONE;
		gbc_enterMaxSupportTextField.insets = new Insets(10, 5, 0, 0);
		gbc_enterMaxSupportTextField.gridx = 1;
		gbc_enterMaxSupportTextField.gridy = 2;
		frequentSetsPanel.add(enterMaxSupportTextField, gbc_enterMaxSupportTextField);
		
		lblEnterConfInterval = new JLabel("Enter Confidence Value:");
		lblEnterConfInterval.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_lblEnterConfInterval = new GridBagConstraints();
		gbc_lblEnterConfInterval.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_lblEnterConfInterval.fill = GridBagConstraints.NONE;
		gbc_lblEnterConfInterval.insets = new Insets(10, 5, 0, 0);
		gbc_lblEnterConfInterval.gridx = 0;
		gbc_lblEnterConfInterval.gridy = 3;
		frequentSetsPanel.add(lblEnterConfInterval, gbc_lblEnterConfInterval);
		
		enterConfIntervalTextField = new JTextField();
		enterConfIntervalTextField.setText("0.9");
		enterConfIntervalTextField.setColumns(4);
		GridBagConstraints gbc_enterConfIntervalTextField = new GridBagConstraints();
		gbc_enterConfIntervalTextField.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_enterConfIntervalTextField.fill = GridBagConstraints.NONE;
		gbc_enterConfIntervalTextField.insets = new Insets(10, 5, 0, 0);
		gbc_enterConfIntervalTextField.gridx = 1;
		gbc_enterConfIntervalTextField.gridy = 3;
		frequentSetsPanel.add(enterConfIntervalTextField, gbc_enterConfIntervalTextField);
	}

	public void fillDrillDownPanel(JPanel drillDownPanel) {
		GridBagLayout gbl_drillDownPanel = new GridBagLayout();
		gbl_drillDownPanel.columnWidths = new int[]{0, 0, 0};
		gbl_drillDownPanel.rowHeights = new int[]{0, 0, 0};
		gbl_drillDownPanel.columnWeights = new double[]{0.0, 0.0, 1.0};
		gbl_drillDownPanel.rowWeights = new double[]{0.0, 0.0, 1.0};
		drillDownPanel.setLayout(gbl_drillDownPanel);
		
		lblDrillDownSelectTab = new JLabel("Select clustering run:");
		lblDrillDownSelectTab.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_lblDrillDownSelectTab = new GridBagConstraints();
		gbc_lblDrillDownSelectTab.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_lblDrillDownSelectTab.fill = GridBagConstraints.NONE;
		gbc_lblDrillDownSelectTab.insets = new Insets(10, 5, 0, 0);
		gbc_lblDrillDownSelectTab.gridx = 0;
		gbc_lblDrillDownSelectTab.gridy = 0;
		drillDownPanel.add(lblDrillDownSelectTab, gbc_lblDrillDownSelectTab);
		lblDrillDownSelectTab.setVisible(false);
		
		drillDownTabSelectorComboBox = new JComboBox<String>();
		drillDownTabSelectorComboBox.setFont(new Font("Tahoma", Font.PLAIN, 11));
		drillDownTabSelectorComboBox.setBackground(Color.GRAY);
		drillDownTabSelectorComboBox.setPreferredSize(new Dimension(250, 25));
		drillDownTabSelectorComboBox.setModel(new DefaultComboBoxModel<String>(new String[] {}));
		GridBagConstraints gbc_drillDownTabSelectorComboBox = new GridBagConstraints();
		gbc_drillDownTabSelectorComboBox.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_drillDownTabSelectorComboBox.fill = GridBagConstraints.NONE;
		gbc_drillDownTabSelectorComboBox.insets = new Insets(5, 5, 0, 0);
		gbc_drillDownTabSelectorComboBox.gridx = 1;
		gbc_drillDownTabSelectorComboBox.gridy = 0;
		drillDownPanel.add(drillDownTabSelectorComboBox, gbc_drillDownTabSelectorComboBox);
		drillDownTabSelectorComboBox.setVisible(false);
		
		clusterCheckBoxPanel = new JPanel();
		GridBagConstraints gbc_clusterCheckBoxPanel = new GridBagConstraints();
		gbc_clusterCheckBoxPanel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_clusterCheckBoxPanel.fill = GridBagConstraints.NONE;
		gbc_clusterCheckBoxPanel.gridwidth = 2;
		gbc_clusterCheckBoxPanel.gridx = 0;
		gbc_clusterCheckBoxPanel.gridy = 1;
		drillDownPanel.add(clusterCheckBoxPanel, gbc_clusterCheckBoxPanel);
		
		GridBagLayout gbl_clusterCheckBoxPanel = new GridBagLayout();
		gbl_clusterCheckBoxPanel.columnWidths = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_clusterCheckBoxPanel.rowHeights = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_clusterCheckBoxPanel.columnWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
		gbl_clusterCheckBoxPanel.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
		clusterCheckBoxPanel.setLayout(gbl_clusterCheckBoxPanel);
		
		JLabel lblClusterSelect = new JLabel("Select clusters to include:");
		lblClusterSelect.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_lblClusterSelect = new GridBagConstraints();
		gbc_lblClusterSelect.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_lblClusterSelect.fill = GridBagConstraints.NONE;
		//gbc_lblClusterSelect.gridwidth = 6;
		gbc_lblClusterSelect.insets = new Insets(10, 5, 0, 0);
		gbc_lblClusterSelect.gridx = 0;
		gbc_lblClusterSelect.gridy = 0;
		clusterCheckBoxPanel.add(lblClusterSelect, gbc_lblClusterSelect);
		
		//select all, deselect all
		String checkboxSelectLabel = "Select All";
		checkboxSelectAllClusters = new JCheckBox(checkboxSelectLabel);
		checkboxSelectAllClusters.setName(checkboxSelectLabel+"checkBox");
		checkboxSelectAllClusters.setSelected(true);
		GridBagConstraints gbc_checkboxSelect = new GridBagConstraints();
		gbc_checkboxSelect.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_checkboxSelect.fill = GridBagConstraints.NONE;
		gbc_checkboxSelect.gridwidth = 6;
		gbc_checkboxSelect.insets = new Insets(10, 10, 0, 0);
		gbc_checkboxSelect.gridx = 1;
		gbc_checkboxSelect.gridy = 0;
		clusterCheckBoxPanel.add(checkboxSelectAllClusters, gbc_checkboxSelect);

		selectAllClustersList = new SelectCheckboxesListener();
		selectAllClustersList.setCheckboxes(clusterCheckboxes);
		checkboxSelectAllClusters.addActionListener(selectAllClustersList);
		
		runDrillDown = new CustomButton("Drill Down");
		runDrillDown.setFont(new Font("Tahoma", Font.BOLD, 11));
		runDrillDown.setPreferredSize(new Dimension(150, 25));
		GridBagConstraints gbc_runDrillDown = new GridBagConstraints();
		gbc_runDrillDown.insets = new Insets(10, 5, 0, 40);
		gbc_runDrillDown.anchor = GridBagConstraints.FIRST_LINE_END;
		gbc_runDrillDown.fill = GridBagConstraints.NONE;
		gbc_runDrillDown.gridx = 1;
		gbc_runDrillDown.gridy = 2;
		drillDownPanel.add(runDrillDown, gbc_runDrillDown);
		Style.registerTargetClassName(runDrillDown,  ".createBtn");
		
	}
	
	public void showSelfOrganizingMap(Boolean show) {
		somPanel.setVisible(show);
		lblEnterR0.setVisible(show);
		enterR0TextField.setVisible(show);
		lblEnterL0.setVisible(show);
		enterL0TextField.setVisible(show);
		lblEnterTau.setVisible(show);
		enterTauTextField.setVisible(show);
		lblEnterMaxIt.setVisible(show);
		enterMaxItTextField.setVisible(show);
	}
	
	public void showAssociationLearning(Boolean show) {
		associationLearningPanel.setVisible(show);
		lblEnterNumRules.setVisible(show);
		lblEnterMinSupport.setVisible(show);
		lblEnterMaxSupport.setVisible(show);
		lblEnterConfInterval.setVisible(show);
		enterNumRulesTextField.setVisible(show);
		enterMinSupportTextField.setVisible(show);
		enterMaxSupportTextField.setVisible(show);
		enterConfIntervalTextField.setVisible(show);
	}
	
	public void showCluster(Boolean show) {
		clusterPanel.setVisible(show);
		lblSelectNumClusters.setVisible(show);
		selectNumClustersComboBox.setVisible(show);
		selectNumClustersTextField.setVisible(show);
		selectNumClustersComboBox.setSelectedItem(automaticallySelectNumClustersText);
	}
	public void showClassify(Boolean show) {
		classifyPanel.setVisible(show);
		lblSelectClass.setVisible(show);
		lblSelectClassMethod.setVisible(show);
		classComboBox.setVisible(show);
		classificationMethodComboBox.setVisible(show);
		enableAllCheckboxes();
		if(show) {
			String selection = classComboBox.getSelectedItem() + "";
			disableCheckBox(selection,true);
		}
	}
	public void showOutlier(Boolean show) {
		outlierPanel.setVisible(show);
		lblEnterKNeighbors.setVisible(show);
		enterKNeighborsSlider.setVisible(show);
	}
	public void showMatrixRegression(Boolean show) {
		matrixRegPanel.setVisible(show);
		lblSelectDepVar.setVisible(show);
		matrixDepVarComboBox.setVisible(show);
		enableAllCheckboxes();
		if(show) {
			disableCheckBoxes(categoricalPropNames.toArray(new String[0]),false);
			String selection = matrixDepVarComboBox.getSelectedItem() + "";
			disableCheckBox(selection,true);
		}
	}
	public void showNumericalCorrelation(Boolean show) {
		enableAllCheckboxes();
		if(show) {
			disableCheckBoxes(categoricalPropNames.toArray(new String[0]),false);
		}
	}
	public void enableDrillDown() {
		showDrillDownBtn.setEnabled(true);
	}
	public void showDrillDownPanel(Boolean show) {
		drillDownPanel.setVisible(show);
		lblDrillDownSelectTab.setVisible(show);
		drillDownTabSelectorComboBox.setVisible(show);
		checkboxSelectAllClusters.setVisible(show);
		//button to run drill down
		if(show){
			drillDownTabSelectorComboBox.setSelectedIndex(0);
			String tabName = (String) drillDownTabSelectorComboBox.getSelectedItem();
			ClusteringVizPlaySheet playSheet = (ClusteringVizPlaySheet) playSheetHash.get(tabName);
			int clusters = playSheet.getNumClusters();			
			updateClusterCheckboxes(clusters);
			resetClusterCheckboxesListener();
		} else {
			updateClusterCheckboxes(0); //remove all the checkboxes
		}
		
	}
	
	public void updateClusterCheckboxes(int numClusters) {
		//remove old clusters
		for(JCheckBox checkbox : clusterCheckboxes) {
			clusterCheckBoxPanel.remove(checkbox);
		}
		clusterCheckboxes = new ArrayList<JCheckBox>();
		int numColumns = 6;
		for(int i=0;i<numClusters;i+=numColumns) {
			for(int j=i;j<i+numColumns;j++) {
				if(j<numClusters) {
				String checkboxLabel = "" + j;
				JCheckBox checkbox = new JCheckBox(checkboxLabel);
				checkbox.setName("Cluster "+checkboxLabel+"checkBox");
				checkbox.setSelected(true);
				GridBagConstraints gbc_checkbox = new GridBagConstraints();
				gbc_checkbox.anchor = GridBagConstraints.NORTHWEST;
				gbc_checkbox.fill = GridBagConstraints.NONE;
				gbc_checkbox.insets = new Insets(5, 10, 0, 0);
				gbc_checkbox.gridx = j % numColumns + 1;
				gbc_checkbox.gridy = j / numColumns + 2;
				clusterCheckBoxPanel.add(checkbox, gbc_checkbox);
				clusterCheckboxes.add(checkbox);
				}
			}
		}
	}
	
	public void resetClusterCheckboxesListener() {
		selectAllClustersList.setCheckboxes(clusterCheckboxes);
		checkboxSelectAllClusters.setSelected(true);
	}
	
	public void setSelectedColumns(String[] selectedCols) {
		for(JCheckBox checkbox : ivCheckboxes) {
			checkbox.setSelected(false);
		}
		for(String checkboxToSelect : selectedCols) {
			for(JCheckBox checkbox : ivCheckboxes) {
				String name = checkbox.getName();
				if(name.equals(checkboxToSelect + checkboxName))
					checkbox.setSelected(true);
			}
		}
	}
	
	public void enableAllCheckboxes() {
		for(JCheckBox checkbox : ivCheckboxes) {
			checkbox.setEnabled(true);
		}
	}
	
	public void disableCheckBox(String checkboxToDisable,Boolean selected){//was true before
		for(JCheckBox checkbox : ivCheckboxes) {
			String name = checkbox.getName();
			if(name.equals(checkboxToDisable + checkboxName)) {
				checkbox.setSelected(selected);
				checkbox.setEnabled(false);
			}
		}
	}
	
	public void disableCheckBoxes(String[] checkboxesToDisable,Boolean selected){
		if(checkboxesToDisable==null) {
			LOGGER.info("No checkboxes to disable");
			return;
		}
		for(JCheckBox checkbox : ivCheckboxes) {
			String name = checkbox.getName();
			for(int i=0;i<checkboxesToDisable.length;i++) {
				if(name.equals(checkboxesToDisable[i] + checkboxName)) {
					checkbox.setSelected(selected);
					checkbox.setEnabled(false);
				}
			}
		}
	}

	public void showSelectNumClustersTextField(Boolean show) {
		selectNumClustersTextField.setVisible(show);
	}
	@Override
	public void setQuery(String query) {
		String[] querySplit = query.split("\\+\\+\\+");
		this.query = querySplit[0];
	}
	public JComboBox<String> getAlgorithmComboBox() {
		return algorithmComboBox;
	}
	public JComboBox<String> getClassificationMethodComboBox() {
		return classificationMethodComboBox;
	}
	public JComboBox<String> getClassComboBox() {
		return classComboBox;
	}
	public ArrayList<JCheckBox> getColumnCheckboxes() {
		return ivCheckboxes;
	}
	public ArrayList<JCheckBox> getClusterCheckboxes() {
		return clusterCheckboxes;
	}
	public JComboBox<String> getDrillDownTabSelectorComboBox() {
		return drillDownTabSelectorComboBox;
	}
	public JComboBox<String> getSelectNumClustersComboBox() {
		return selectNumClustersComboBox;
	}
	public String getAutomaticallySelectNumClustersText() {
		return automaticallySelectNumClustersText;
	}
	public String getManuallySelectNumClustersText() {
		return manuallySelectNumClustersText;
	}
	public JTextField getSelectNumClustersTextField() {
		return selectNumClustersTextField;
	}
	public JSlider getEnterKNeighborsSlider() {
		return enterKNeighborsSlider;
	}
	public JComboBox<String> getMatrixDepVarComboBox() {
		return matrixDepVarComboBox;
	}
	public JToggleButton getShowDrillDownBtn() {
		return showDrillDownBtn;
	}
	public JTabbedPane getJTab() {
		return jTab;
	}
	public JProgressBar getJBar() {
		return jBar;
	}
	public Hashtable<String, IPlaySheet> getPlaySheetHash(){
		return playSheetHash;
	}
	public JTextField getEnterNumRulesTextField() {
		return enterNumRulesTextField;
	}
	public JTextField getEnterMinSupportTextField() {
		return enterMinSupportTextField;
	}
	public JTextField getEnterConfIntervalTextField() {
		return enterConfIntervalTextField;
	}
	public JTextField getEnterMaxSupportTextField() {
		return enterMaxSupportTextField;
	}
	public JTextField getEnterR0TextField() {
		return enterR0TextField;
	}
	public JTextField getEnterL0TextField() {
		return enterL0TextField;
	}
	public JTextField getEnterTauTextField() {
		return enterTauTextField;
	}
	public boolean[] getIsNumeric() {
		return isNumeric;
	}
	
}
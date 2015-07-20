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

import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.IAnalyticRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.unsupervised.clustering.ClusteringRoutine;
import prerna.algorithm.learning.unsupervised.clustering.MultiClusteringRoutine;
import prerna.algorithm.learning.util.IClusterDistanceMode;
import prerna.math.BarChart;
import prerna.math.StatisticsUtilityMethods;
import prerna.om.SEMOSSParam;
import prerna.ui.components.GridScrollPane;
import prerna.ui.components.NewScrollBarUI;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.main.listener.impl.ClusteringDrillDownListener;
import prerna.ui.main.listener.impl.ClusteringRefreshParamListener;
import prerna.ui.swing.custom.CustomButton;
import prerna.util.ArrayUtilityMethods;
import prerna.util.CSSApplication;
import prerna.util.Constants;
import prerna.util.DIHelper;
import aurelienribon.ui.css.Style;

@SuppressWarnings("serial")
public class ClusteringVizPlaySheet extends BrowserPlaySheet {

	private static final Logger LOGGER = LogManager.getLogger(ClusteringVizPlaySheet.class.getName());

	private IAnalyticRoutine alg;
	private String[] columnHeaders;
	private boolean[] isNumeric;
	
	private int instanceIndex;
	private int numClusters;
	private int inputNumClusters;
	private int minNumclusters = 2;
	private int maxNumClusters = 50;
	
	private String clusterIDCol;
	private int clusterIDIndex;
	
	private ArrayList<JCheckBox> paramCheckboxes;
	private ArrayList<JCheckBox> clusterCheckboxes;
	private ArrayList<JCheckBox> paramsToCheck;
	private JPanel clusterSelectorPanel;
	
	private String paramSelectorTabName = "Param Selector";
	private String rawDataTabName = "Raw Data";

	private Hashtable<String, IPlaySheet> playSheetHash;
	private JComboBox<String> drillDownTabSelectorComboBox;

	private boolean addAsTab = false;// determines whether to add this playsheet as a tab to the jTab or to create a new playsheet

	private Map<String, IClusterDistanceMode.DistanceMeasure> distanceMeasure;
	private List<String> skipAttributes;
	
	public ClusteringVizPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(800, 600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/cluster.html";
	}
	
	@Override
	public void createData() {
		if(dataFrame == null || dataFrame.isEmpty())
			super.createData();
	}

	@Override
	public void runAnalytics() {
		Map<String, Object> selectedOptions = null;
		if (numClusters >= 2) {
			alg = new ClusteringRoutine();
			List<SEMOSSParam> options = alg.getOptions();
			selectedOptions = new HashMap<String, Object>();
			selectedOptions.put(options.get(0).getName(), numClusters); 
			selectedOptions.put(options.get(1).getName(), instanceIndex); // default of 0 is acceptable
			selectedOptions.put(options.get(2).getName(), distanceMeasure); 
			selectedOptions.put(options.get(3).getName(), skipAttributes); 
			alg.setSelectedOptions(selectedOptions);
			dataFrame.performAction(alg);
			this.numClusters = ((ClusteringRoutine) alg).getNumClusters();
		} else {
			alg = new MultiClusteringRoutine();
			List<SEMOSSParam> options = alg.getOptions();
			selectedOptions = new HashMap<String, Object>();
			selectedOptions.put(options.get(0).getName(), minNumclusters); 
			selectedOptions.put(options.get(1).getName(), maxNumClusters);
			selectedOptions.put(options.get(2).getName(), instanceIndex); // default of 0 is acceptable
			selectedOptions.put(options.get(3).getName(), distanceMeasure); 
			selectedOptions.put(options.get(4).getName(), skipAttributes);

			alg.setSelectedOptions(selectedOptions);
			dataFrame.performAction(alg);
			this.numClusters = ((MultiClusteringRoutine) alg).getNumClusters();
		}
		
		columnHeaders = dataFrame.getColumnHeaders();
		isNumeric = dataFrame.isNumeric();
		clusterIDCol = alg.getChangedColumns().get(0);
		clusterIDIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(columnHeaders, clusterIDCol);
	}

	@Override
	public void processQueryData() {
		//TODO: this is a bad format, will just use getData in future
		List<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();
		Iterator<Object[]> it1 = dataFrame.iterator(false);
		//TODO: remove skipAttributes from columnHeaders
		while(it1.hasNext()) {
			Object[] row = it1.next();
			Map<String, Object> hashRow = new Hashtable<String, Object>();
			for(int i = 0; i < row.length; i++) {
				if(i == instanceIndex) {
					hashRow.put("NodeName", row[i]);
					continue;
				}
				if(i == clusterIDIndex) {
					hashRow.put("ClusterID", row[i]);
					continue;
				}
				hashRow.put(columnHeaders[i], row[i]);
			}
			dataList.add(hashRow);
		}

		Hashtable<String, Hashtable<String, Object>>[] barData = new Hashtable[this.numClusters];
		Hashtable<String, Hashtable<String, Object>> clusterDataHash = new Hashtable<String, Hashtable<String, Object>>();
		Iterator<List<Object[]>> it = dataFrame.uniqueIterator(clusterIDCol, false);
		while(it.hasNext()) {
			List<Object[]> clusterData = it.next();
			int numInstancesInCluster = clusterData.size();
			int clusterNumber = (int) clusterData.get(0)[clusterIDIndex];
			
			for(int i = 0; i < columnHeaders.length; i++) {
				String propName = columnHeaders[i];
				if(i == instanceIndex || i == clusterIDIndex) {
					continue;
				}
				// get column from cluster data corresponding to ith attribute
				List<Object> values = new ArrayList<Object>();
				for(int j = 0; j < numInstancesInCluster; j++) {
					values.add(clusterData.get(j)[i]);
				}
				Object[] noNullValues = ArrayUtilityMethods.removeAllNulls(values.toArray());
				if(isNumeric[i]) {
					// dealing with numerical prop - determine range, calculate IQR, determine bin-size, group
					Double[] numValues = ArrayUtilityMethods.convertObjArrToDoubleWrapperArr(noNullValues);
					numValues = ArrayUtilityMethods.sortDoubleWrapperArr(numValues);
					Hashtable<String, Object>[] propBins = null;
					BarChart chart = new BarChart(numValues);
					if (chart.isUseCategoricalForNumericInput()) {
						chart.calculateCategoricalBins("?", true, true);
						chart.generateJSONHashtableCategorical();
						propBins = chart.getRetHashForJSON();
					} else {
						chart.generateJSONHashtableNumerical();
						propBins = chart.getRetHashForJSON();
					}
					Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
					String[] zScore = StatisticsUtilityMethods.getZScoreRangeAsStringIgnoringNull(numValues, true);
					// cause JS is dumb
					Object[] propBinsArr = new Object[] { propBins };
					innerHash.put("dataSeries", propBinsArr);
					innerHash.put("names", new String[] { propName, "Distribution" });
					innerHash.put("zScore", zScore);
					clusterDataHash.put(propName, innerHash);
				} else {
					String[] stringValues = ArrayUtilityMethods.convertObjArrToStringArr(noNullValues);
					BarChart chart = new BarChart(stringValues);
					chart.calculateCategoricalBins("?", true, true);
					chart.generateJSONHashtableCategorical();
					Hashtable<String, Object>[] propBins = chart.getRetHashForJSON();
					Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
					// cause JS is dumb
					Object[] propBinsArr = new Object[] { propBins };
					innerHash.put("dataSeries", propBinsArr);
					innerHash.put("names", new String[] { propName, "Frequency" });
					// need to create outerHash since bar chart takes in weird format - since it is set up to conver to stacked bar chart
					clusterDataHash.put(propName, innerHash);
				}
			}
			//TODO: is it worth it to run all these calculations? will people even understand the dataset similarity?
			// add in similarity for cluster
//			DatasetSimilarity simAlg = new DatasetSimilarity();
//			List<SEMOSSParam> options = simAlg.getOptions();
//			HashMap<String, Object> selectedOptions = new HashMap<String, Object>();
//			selectedOptions.put(options.get(0).getName(), instanceIndex); // default of 0 is acceptable
//			simAlg.setSelectedOptions(selectedOptions);
//			ITableDataFrame simAlgResults = simAlg.runAlgorithm(dataFrame);
//			Object[] simAlgResultsObj = simAlgResults.getColumn(simAlg.getChangedColumns().get(0));
//			double[] simValues = ArrayUtilityMethods.convertObjArrToDoubleArr(simAlgResultsObj);
//			Hashtable<String, Object>[] bins = null;
//			BarChart chart = new BarChart(simValues, "");
//			if (chart.isUseCategoricalForNumericInput()) {
//				chart.calculateCategoricalBins("?", true, true);
//				chart.generateJSONHashtableCategorical();
//				bins = chart.getRetHashForJSON();
//			} else {
//				chart.generateJSONHashtableNumerical();
//				bins = chart.getRetHashForJSON();
//			}
//			String[] zScore = StatisticsUtilityMethods.getZScoreRangeAsString(simValues, false);
//
//			Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
//			Object[] binArr = new Object[] { bins };
//			innerHash.put("dataSeries", binArr);
//			innerHash.put("names", new String[] { columnHeaders[instanceIndex].concat(" Similarity Distribution to Dataset Center"), "Distribution" });
//			innerHash.put("zScore", zScore);
//			clusterDataHash.put("Similarity Value to Dataset Center", innerHash);
			
			barData[clusterNumber] = clusterDataHash;
		}

		Hashtable<String, Object> allHash = new Hashtable<String, Object>();
		//TODO: this is a bad format, will just use getData in future
		allHash.put("dataSeries", dataList);
		allHash.put("barData", barData);
		this.dataHash = allHash;
	}

	/**
	 * Sets the string version of the SPARQL query on the playsheet. Pulls out the number of clusters and stores them in the numClusters
	 * @param query
	 */
	@Override
	public void setQuery(String query) {
		LOGGER.info("New Query " + query);
		String[] querySplit = query.split("\\+\\+\\+");
		if (querySplit.length == 1) {
			this.query = query;
		} else if (querySplit.length == 2) {
			this.query = querySplit[0];
			this.numClusters = Integer.parseInt(querySplit[1]);
			this.inputNumClusters = numClusters;
		}
	}

	public void setNumClusters(int numClusters) {
		this.inputNumClusters = numClusters;
		this.numClusters = numClusters;
	}

	public int getNumClusters() {
		return numClusters;
	}

	public void setDistanceMeasure(
			Map<String, IClusterDistanceMode.DistanceMeasure> distanceMeasure) {
		this.distanceMeasure = distanceMeasure;
	}

	public void setSkipAttributes(List<String> skipAttributes) {
		this.skipAttributes = skipAttributes;
		dataFrame.setColumnsToSkip(skipAttributes);
	}
	
	public String getClusterIDCol() {
		return clusterIDCol;
	}

	public void setClusterIDCol(String clusterIDCol) {
		this.clusterIDCol = clusterIDCol;
	}

	public int getClusterIDIndex() {
		return clusterIDIndex;
	}

	public void setClusterIDIndex(int clusterIDIndex) {
		this.clusterIDIndex = clusterIDIndex;
	}
	
	public void setInstanceIndex(int instanceIndex) {
		this.instanceIndex = instanceIndex;
	}
	
	/**
	 * Method addPanel. Creates a panel and adds the table to the panel.
	 */
	@Override
	public void addPanel() {
		// if this is to be a separate playsheet, create the tab in a new window
		// otherwise, if this is to be just a new tab in an existing playsheet,
		if (!addAsTab) {
			super.addPanel();
			if (jTab.indexOfTab(paramSelectorTabName) < 0) {// if param selecctor tab does not exist, add it
				addSelectorTab();
			}
			if (jTab.indexOfTab(rawDataTabName) > 0) {// there is already a grid tab, remove it
				jTab.remove(jTab.indexOfTab(rawDataTabName));
			}
			addGridTab();
		} else {
			String lastTabName = jTab.getTitleAt(jTab.getTabCount() - 1);
			LOGGER.info("Parsing integer out of last tab name");
			int count = 1;
			if (jTab.getTabCount() > 1)
				count = Integer.parseInt(lastTabName.substring(0, lastTabName.indexOf("."))) + 1;
			addPanelAsTab(count + ". Clustering");
			rawDataTabName = count + ". Clustering Raw Data";
			addGridTab();

			if (playSheetHash != null) {
				playSheetHash.put(rawDataTabName, this);
			}

			// if there is a drop down, update it to include tab name
			if (drillDownTabSelectorComboBox != null) {
				DefaultComboBoxModel<String> model = (DefaultComboBoxModel<String>) drillDownTabSelectorComboBox.getModel();
				model.addElement(rawDataTabName);
				drillDownTabSelectorComboBox.repaint();
			}

		}
		new CSSApplication(getContentPane());
	}

	public void addSelectorTab() {
		DecimalFormat formatter = new DecimalFormat("0.000E0");
		JPanel panel = new JPanel();

		GridBagLayout gbl_panel = new GridBagLayout();
		gbl_panel.columnWidths = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
		gbl_panel.rowHeights = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
		gbl_panel.columnWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 };
		gbl_panel.rowWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 };
		panel.setLayout(gbl_panel);

		JPanel paramSelectorPanel = new JPanel();
		GridBagConstraints gbc_paramSelectorPanel = new GridBagConstraints();
		gbc_paramSelectorPanel.anchor = GridBagConstraints.WEST;
		gbc_paramSelectorPanel.insets = new Insets(10, 5, 0, 0);
		gbc_paramSelectorPanel.gridx = 0;
		gbc_paramSelectorPanel.gridy = 0;
		panel.add(paramSelectorPanel, gbc_paramSelectorPanel);

		GridBagLayout gbl_paramSelectorPanel = new GridBagLayout();
		gbl_paramSelectorPanel.columnWidths = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
		gbl_paramSelectorPanel.rowHeights = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
		gbl_paramSelectorPanel.columnWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 };
		gbl_paramSelectorPanel.rowWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 };
		paramSelectorPanel.setLayout(gbl_paramSelectorPanel);

		JLabel lblParamSelect = new JLabel("Select Parameters:");
		lblParamSelect.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_lblParamSelect = new GridBagConstraints();
		gbc_lblParamSelect.anchor = GridBagConstraints.WEST;
		gbc_lblParamSelect.insets = new Insets(10, 5, 0, 0);
		gbc_lblParamSelect.gridx = 0;
		gbc_lblParamSelect.gridy = 0;
		paramSelectorPanel.add(lblParamSelect, gbc_lblParamSelect);
		paramCheckboxes = new ArrayList<JCheckBox>();

		JLabel entropyDensityLabel = new JLabel("Entropy Density For Parameter:");
		entropyDensityLabel.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_entropyDensityLabel = new GridBagConstraints();
		gbc_entropyDensityLabel.anchor = GridBagConstraints.NORTHWEST;
		gbc_entropyDensityLabel.fill = GridBagConstraints.NONE;
		gbc_entropyDensityLabel.insets = new Insets(10, 5, 0, 0);
		gbc_entropyDensityLabel.gridx = 1;
		gbc_entropyDensityLabel.gridy = 0;
		paramSelectorPanel.add(entropyDensityLabel, gbc_entropyDensityLabel);
		for (int i = 1; i < columnHeaders.length; i++) {
			if(i == instanceIndex || i == clusterIDIndex) {
				continue;
			}
			Double entropyDensityValue = dataFrame.getEntropyDensity(columnHeaders[i]);

			JLabel entropyDensityVal = new JLabel();
			entropyDensityVal.setText(formatter.format(entropyDensityValue));
			GridBagConstraints gbc_entropyDensityVal = new GridBagConstraints();
			gbc_entropyDensityVal.anchor = GridBagConstraints.NORTHWEST;
			gbc_entropyDensityVal.fill = GridBagConstraints.NONE;
			gbc_entropyDensityVal.insets = new Insets(5, 10, 0, 0);
			gbc_entropyDensityVal.gridx = 0;
			gbc_entropyDensityVal.gridy = i;
			paramSelectorPanel.add(entropyDensityVal, gbc_entropyDensityVal);

			String checkboxLabel = "";
			checkboxLabel = columnHeaders[i];
			JCheckBox checkbox = new JCheckBox(checkboxLabel);
			checkbox.setName(checkboxLabel + "checkBox");
			checkbox.setSelected(true);
			if (paramsToCheck != null) {
				boolean selected = paramsToCheck.get(i - 1).isSelected();
				checkbox.setSelected(selected);
			}
			GridBagConstraints gbc_checkbox = new GridBagConstraints();
			gbc_checkbox.anchor = GridBagConstraints.NORTHWEST;
			gbc_checkbox.fill = GridBagConstraints.NONE;
			gbc_checkbox.insets = new Insets(5, 10, 0, 0);
			gbc_checkbox.gridx = 1;
			gbc_checkbox.gridy = i;
			paramSelectorPanel.add(checkbox, gbc_checkbox);
			paramCheckboxes.add(checkbox);
		}

		JButton btnRefreshParam = new CustomButton("Refresh Parameters");
		btnRefreshParam.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnRefreshParam = new GridBagConstraints();
		gbc_btnRefreshParam.insets = new Insets(10, 5, 0, 0);
		gbc_btnRefreshParam.anchor = GridBagConstraints.NORTH;
		gbc_btnRefreshParam.fill = GridBagConstraints.VERTICAL;
		gbc_btnRefreshParam.gridx = 0;
		gbc_btnRefreshParam.gridy = 2;
		panel.add(btnRefreshParam, gbc_btnRefreshParam);
		Style.registerTargetClassName(btnRefreshParam, ".createBtn");

		ClusteringRefreshParamListener refListener = new ClusteringRefreshParamListener();
		refListener.setPlaySheet(this);
		refListener.setCheckBoxes(paramCheckboxes);
		refListener.setColumnHeaders(columnHeaders);
		btnRefreshParam.addActionListener(refListener);

		clusterSelectorPanel = new JPanel();
		GridBagConstraints gbc_clusterSelectorPanel = new GridBagConstraints();
		gbc_clusterSelectorPanel.anchor = GridBagConstraints.WEST;
		gbc_clusterSelectorPanel.insets = new Insets(10, 5, 0, 0);
		gbc_clusterSelectorPanel.gridx = 1;
		gbc_clusterSelectorPanel.gridy = 0;
		panel.add(clusterSelectorPanel, gbc_clusterSelectorPanel);

		GridBagLayout gbl_clusterSelectorPanel = new GridBagLayout();
		gbl_clusterSelectorPanel.columnWidths = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
		gbl_clusterSelectorPanel.rowHeights = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
		gbl_clusterSelectorPanel.columnWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 };
		gbl_clusterSelectorPanel.rowWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 };
		clusterSelectorPanel.setLayout(gbl_clusterSelectorPanel);

		JLabel lblClusterSelect = new JLabel("Select Clusters to drill down:");
		lblClusterSelect.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_lblClusterSelect = new GridBagConstraints();
		gbc_lblClusterSelect.anchor = GridBagConstraints.NORTHWEST;
		gbc_lblClusterSelect.fill = GridBagConstraints.NONE;
		gbc_lblClusterSelect.insets = new Insets(10, 5, 0, 0);
		gbc_lblClusterSelect.gridx = 0;
		gbc_lblClusterSelect.gridy = 0;
		clusterSelectorPanel.add(lblClusterSelect, gbc_lblClusterSelect);
		clusterCheckboxes = new ArrayList<JCheckBox>();

		// need to iterate through the cluster list
		updateClusterCheckboxes();

		JButton btnDrillDownCluster = new CustomButton("Drill Down on Selected Clusters");
		btnDrillDownCluster.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnDrillDownCluster = new GridBagConstraints();
		gbc_btnDrillDownCluster.insets = new Insets(10, 5, 0, 0);
		gbc_btnDrillDownCluster.anchor = GridBagConstraints.NORTH;
		gbc_btnDrillDownCluster.fill = GridBagConstraints.VERTICAL;
		gbc_btnDrillDownCluster.gridx = 1;
		gbc_btnDrillDownCluster.gridy = 2;
		panel.add(btnDrillDownCluster, gbc_btnDrillDownCluster);
		Style.registerTargetClassName(btnDrillDownCluster, ".createBtn");

		ClusteringDrillDownListener drillDownListener = new ClusteringDrillDownListener();
		drillDownListener.setCheckBoxes(paramCheckboxes);
		drillDownListener.setDataFrame(dataFrame);
		drillDownListener.setPlaySheet(this);
		drillDownListener.setClusterIDCol(clusterIDCol);
		drillDownListener.setClusterIDIndex(clusterIDIndex);
		drillDownListener.setNumClusters(inputNumClusters);

		btnDrillDownCluster.addActionListener(drillDownListener);

		JScrollPane scroll = new JScrollPane(panel);
		jTab.insertTab(paramSelectorTabName, null, scroll, null, 0);
		jTab.setSelectedIndex(0);
	}

	public void addGridTab() {
		GridScrollPane gsp = null;
		if(dataFrame != null) {
			gsp = new GridScrollPane(dataFrame.getColumnHeaders(), dataFrame.getData());
		}
		gsp.addHorizontalScroll();

		jTab.addTab(rawDataTabName, gsp);
	}
	
	private void updateClusterCheckboxes() {
		// remove old clusters
		for (JCheckBox checkbox : clusterCheckboxes) {
			clusterSelectorPanel.remove(checkbox);
		}
		clusterCheckboxes = new ArrayList<JCheckBox>();
		for (int i = 0; i < numClusters; i++) {
			String checkboxLabel = "" + i;
			JCheckBox checkbox = new JCheckBox(checkboxLabel);
			checkbox.setName("Cluster " + checkboxLabel + "checkBox");
			checkbox.setSelected(true);
			GridBagConstraints gbc_checkbox = new GridBagConstraints();
			gbc_checkbox.anchor = GridBagConstraints.NORTHWEST;
			gbc_checkbox.fill = GridBagConstraints.NONE;
			gbc_checkbox.insets = new Insets(5, 10, 0, 0);
			gbc_checkbox.gridx = 0;
			gbc_checkbox.gridy = i + 1;
			clusterSelectorPanel.add(checkbox, gbc_checkbox);
			clusterCheckboxes.add(checkbox);
		}
	}

	public ArrayList<JCheckBox> getClusterCheckboxes() {
		return clusterCheckboxes;
	}

	public void setAddAsTab(Boolean addAsTab) {
		this.addAsTab = addAsTab;
	}

	public void setPlaySheetHash(Hashtable<String, IPlaySheet> playSheetHash) {
		this.playSheetHash = playSheetHash;
	}

	public void setDrillDownTabSelectorComboBox(JComboBox<String> drillDownTabSelectorComboBox) {
		this.drillDownTabSelectorComboBox = drillDownTabSelectorComboBox;
	}

	public void addScrollPanel(JPanel panel, JComponent obj) {
		JScrollPane scrollPane = new JScrollPane(obj);
		scrollPane.getVerticalScrollBar().setUI(new NewScrollBarUI());
		scrollPane.setAutoscrolls(true);

		GridBagConstraints gbc_scrollPane = new GridBagConstraints();
		gbc_scrollPane.fill = GridBagConstraints.BOTH;
		gbc_scrollPane.gridx = 0;
		gbc_scrollPane.gridy = 0;
		panel.add(scrollPane, gbc_scrollPane);
	}
	
	public void setSelectedParams(ArrayList<JCheckBox> paramCheckboxes) {
		this.paramsToCheck = paramCheckboxes;
	}
	
	public void drillDownData(ITableDataFrame dataFrame, List<String> skipColumns, int inputNumClusters) {
		this.dataFrame = dataFrame;
		this.skipAttributes = skipColumns;
		this.inputNumClusters = inputNumClusters;
		this.numClusters = inputNumClusters;
		this.dataFrame.setColumnsToSkip(skipColumns);
//		reRunAlgorithm();
	}

	public void skipAttributes(List<String> skipAttributes) {
		this.skipAttributes = skipAttributes;
		this.dataFrame.setColumnsToSkip(skipAttributes);
		reRunAlgorithm();
	}
	
	private void reRunAlgorithm() {
		runAnalytics();
		processQueryData();
		createView();
		updateClusterCheckboxes();
	}
}
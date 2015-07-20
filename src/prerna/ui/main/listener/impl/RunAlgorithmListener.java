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
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JProgressBar;
import javax.swing.JSlider;
import javax.swing.JTabbedPane;
import javax.swing.JTextField;
import javax.swing.JToggleButton;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.weka.WekaClassification;
import prerna.engine.api.IEngine;
import prerna.om.SEMOSSParam;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.BasicProcessingPlaySheet;
import prerna.ui.components.playsheets.ClassifyClusterPlaySheet;
import prerna.ui.components.playsheets.ClusteringVizPlaySheet;
import prerna.ui.components.playsheets.CorrelationPlaySheet;
import prerna.ui.components.playsheets.LocalOutlierVizPlaySheet;
import prerna.ui.components.playsheets.MatrixRegressionPlaySheet;
import prerna.ui.components.playsheets.MatrixRegressionVizPlaySheet;
import prerna.ui.components.playsheets.NumericalCorrelationVizPlaySheet;
import prerna.ui.components.playsheets.SelfOrganizingMap3DBarChartPlaySheet;
import prerna.ui.components.playsheets.WekaAprioriVizPlaySheet;
import prerna.ui.components.playsheets.WekaClassificationPlaySheet;
import prerna.ui.helpers.ClusteringModuleUpdateRunner;
import prerna.ui.helpers.PlaysheetCreateRunner;
import prerna.util.Utility;

/**
 * Runs the algorithm selected on the Cluster/Classify playsheet and adds additional tabs. Tied to the button to the ClassifyClusterPlaySheet.
 */
public class RunAlgorithmListener extends AbstractListener {
	private static final Logger LOGGER = LogManager.getLogger(RunAlgorithmListener.class.getName());
	
	private ClassifyClusterPlaySheet playSheet;
	private JTabbedPane jTab;
	private JProgressBar jBar;
	private Hashtable<String, IPlaySheet> playSheetHash;
	private JComboBox<String> algorithmComboBox;
	
	//cluster
	private JComboBox<String> selectNumClustersComboBox;
	private String manuallySelectNumClustersText;
	private JTextField selectNumClustersTextField;

	//classify
	private JComboBox<String> classificationMethodComboBox;
	private JComboBox<String> classComboBox;
	
	//outlier
	private JSlider enterKNeighborsSlider;
	
	//matrix regression
	private JComboBox<String> matrixDepVarComboBox;
	
	private JToggleButton showDrillDownBtn;
	private JComboBox<String> drillDownTabSelectorComboBox;
	private ArrayList<JCheckBox> columnCheckboxes;
	private IEngine engine;
	private String title;

	private Double[] entropyArr;

	private ITableDataFrame dataFrame;
	private String[] attributeNames;
	
	/**
	 * Method actionPerformed.
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		//get the columns we're filtering out
		//create a new tab
		//figure out if we are clustering or classifying
		//if cluster.... figure out number of clusters to use or if automatically choosing
		//.............. pass tab, and call the clustering on it
		//if classifying... figure out the class and the method for classifying, run the classifying
		//need to put chart/grid on correct tabs, updating if necessary
		//if outlier.... figure out the kneighbors and run the outlier method
		//TODO add logger statements
		
		//filter the names array and list of data based on the independent variables selected
		//must make sure that we include the first col, even though there is no checkbox
		
		//Revert back to the original data frame structure
		List<String> newColumnHeaders = Arrays.asList(dataFrame.getColumnHeaders());
		List<String> originalColumnHeaders = Arrays.asList(playSheet.columnHeaders);
		Set<String> setDifference = new LinkedHashSet<String>(newColumnHeaders);
		setDifference.removeAll(originalColumnHeaders);
		String[] removeSet = setDifference.toArray(new String[setDifference.size()]);
		for(int i = removeSet.length - 1; i >=0; i--) {
			dataFrame.removeColumn(removeSet[i]);
		}
		

		
		dataFrame.setColumnsToSkip(null);
		//attributeNames = dataFrame.getColumnHeaders();
		
		List<String> skipColumns = new ArrayList<String>();
		for(int i = 0; i < columnCheckboxes.size(); i++) {
			if(!columnCheckboxes.get(i).isSelected()) {
				skipColumns.add(attributeNames[i+1]);
			}
		}
		if(skipColumns.size() == attributeNames.length) {
			Utility.showError("No variables were selected. Please select at least one and retry.");
			return;
		}
		
		dataFrame.setColumnsToSkip(skipColumns);
		//attributeNames = dataFrame.getColumnHeaders();
		
		String algorithm = algorithmComboBox.getSelectedItem() + "";
		if(algorithm.equals("Similarity")) {
			ClusteringModuleUpdateRunner runner = new ClusteringModuleUpdateRunner(playSheet);
			runner.setValues(dataFrame, skipColumns);
			Thread playThread = new Thread(runner);
			playThread.start();
			return;
		}
		
		
		BasicProcessingPlaySheet newPlaySheet;
		if(algorithm.equals("Cluster") ) {
			int numClusters = 0;
			String selectNumClustersText = (String) selectNumClustersComboBox.getSelectedItem();
			//if we are manually setting number of clusters, pull the number to use from the text field
			if(selectNumClustersText.equals(manuallySelectNumClustersText)) {
				//make sure that what you pull from text field is an integer, if not return with an error
				String numClustersText = selectNumClustersTextField.getText();
				try {
					numClusters = Integer.parseInt(numClustersText);
				} catch(NumberFormatException exception) {
					Utility.showError("Number of clusters must be an integer greater than 1. Please fix and rerun.");
					return;
				}
				if(numClusters<2) {//if error {
					Utility.showError("Number of clusters must be an integer greater than 1. Please fix and rerun.");
					return;
				}
			}

			newPlaySheet = new ClusteringVizPlaySheet();
			if(numClusters>=2) {
				((ClusteringVizPlaySheet)newPlaySheet).setNumClusters(numClusters);
			}
			((ClusteringVizPlaySheet)newPlaySheet).setAddAsTab(true);
			((ClusteringVizPlaySheet)newPlaySheet).setDataFrame(dataFrame);
			((ClusteringVizPlaySheet)newPlaySheet).setNumClusters(numClusters);
			((ClusteringVizPlaySheet)newPlaySheet).setSkipAttributes(skipColumns);
			((ClusteringVizPlaySheet)newPlaySheet).setDrillDownTabSelectorComboBox(drillDownTabSelectorComboBox);
			((ClusteringVizPlaySheet)newPlaySheet).setPlaySheetHash(playSheetHash);
			((ClusteringVizPlaySheet)newPlaySheet).setJTab(jTab);
			((ClusteringVizPlaySheet)newPlaySheet).setJBar(jBar);
			showDrillDownBtn.setVisible(true);			
	
		} else if(algorithm.equals("Classify")) {
			//method of classification to use
			String classMethod = classificationMethodComboBox.getSelectedItem() + "";
			
			//determine the column index and name to classify on
			String classifier = classComboBox.getSelectedItem() + "";
			int classifierIndex = attributeNames.length - 1;
			while(classifierIndex > -1 && !attributeNames[classifierIndex].equals(classifier)) {
				classifierIndex--;
			}
			
			if(classifierIndex<0){
				LOGGER.error("Cannot match classifier selected in drop down to list of classifiers");
				return;
			}
			
			newPlaySheet = new WekaClassificationPlaySheet();
			newPlaySheet.setDataFrame(dataFrame);
			((WekaClassificationPlaySheet)newPlaySheet).setModelName(classMethod);
			((WekaClassificationPlaySheet)newPlaySheet).setSkipAttributes(skipColumns);
			((WekaClassificationPlaySheet)newPlaySheet).setClassColumn(classifierIndex);
			((WekaClassificationPlaySheet)newPlaySheet).setJTab(jTab);
			((WekaClassificationPlaySheet)newPlaySheet).setJBar(jBar);
		
		} else if(algorithm.equals("Local Outlier Factor")) {
			int kneighbors = enterKNeighborsSlider.getValue();
			
			newPlaySheet = new LocalOutlierVizPlaySheet();
			newPlaySheet.setDataFrame(dataFrame);
			((LocalOutlierVizPlaySheet)newPlaySheet).setK(kneighbors);
			((LocalOutlierVizPlaySheet)newPlaySheet).setSkipAttributes(skipColumns);
			((LocalOutlierVizPlaySheet)newPlaySheet).setJTab(jTab);
			((LocalOutlierVizPlaySheet)newPlaySheet).setJBar(jBar);			
		
		} else if(algorithm.equals("Predictability")) {
			
			double[] accuracyArr = new double[columnCheckboxes.size()];
			double[] precisionArr = new double[columnCheckboxes.size()];
			String[] at = this.dataFrame.getColumnHeaders();
			for(int i=1; i < at.length; i++) {
				if(!skipColumns.contains(at[i])) {
					if(entropyArr[i] != 0) {
						WekaClassification weka = new WekaClassification();
						List<SEMOSSParam> options = weka.getOptions();
						Map<String, Object> selectedOptions = new HashMap<String, Object>();
						selectedOptions.put(options.get(0).getName(), "J48");
						selectedOptions.put(options.get(1).getName(), i);
						selectedOptions.put(options.get(2).getName(), skipColumns);
						weka.setSelectedOptions(selectedOptions);
						try {
							weka.runAlgorithm(dataFrame);
							// both stored as percents
							accuracyArr[i-1] = weka.getAccuracy(); 
							precisionArr[i-1] = weka.getPrecision(); 
						} catch (Exception ex) {
							ex.printStackTrace();
							LOGGER.error("Could not generate accuracy and precision values from WEKA classification");
						}
					} else {
						accuracyArr[i-1] = 100;
						precisionArr[i-1] = 100;
					}
				}
			}
			playSheet.fillAccuracyAndPrecision(accuracyArr, precisionArr);
			return;
			
		} else if(algorithm.equals("Association Learning")) { 
			String numRuleString = playSheet.getEnterNumRulesTextField().getText();
			String confIntervalString = playSheet.getEnterConfIntervalTextField().getText();
			String minSupportString = playSheet.getEnterMinSupportTextField().getText();
			String maxSupportString = playSheet.getEnterMaxSupportTextField().getText();
			
			String errorMessage = "";
			
			int numRule = -1;
			try {
				numRule = Integer.parseInt(numRuleString);
			} catch(NumberFormatException ex) {
				errorMessage += "Number of Rules must be a integer value.  Using default value of 10. \n";
				numRule = 10;
			}
			
			double confPer = -1;
			try {
				confPer = Double.parseDouble(confIntervalString);
				if(confPer < 0 || confPer > 1) {
					errorMessage += "Conf Interval must be a value between 0 and 1.  Using default value of 0.9. \n";
					confPer = 0.9;
				}
			} catch(NumberFormatException ex) {
				errorMessage += "Conf Interval must be a value between 0 and 1.  Using default value of 0.9. \n";
				confPer = 0.9;
			}
			
			double minSupport = -1;
			try {
				minSupport = Double.parseDouble(minSupportString);
				if(minSupport < 0 || minSupport > 1) {
					errorMessage += "Min Support must be a value between 0 and 1.  Using default value of 0.1. \n";
					minSupport = 0.1;
				}
			} catch(NumberFormatException ex) {
				errorMessage += "Min Support must be a value between 0 and 1.  Using default value of 0.1. \n";
				minSupport = 0.1;
			}
			
			double maxSupport = -1.0;
			try {
				maxSupport = Double.parseDouble(maxSupportString);
				if(maxSupport < 0 || maxSupport > 1) {
					errorMessage += "Max Support must be a value between 0 and 1.  Using default value of 1.0. \n";
					maxSupport = 1.0;
				}
			} catch(NumberFormatException ex) {
				errorMessage += "Max Support must be a value between 0 and 1.  Using default value of 1.0. \n";
				maxSupport = 1.0;
			}
			
			if(!errorMessage.isEmpty()) {
				Utility.showError(errorMessage);
			}
			newPlaySheet = new WekaAprioriVizPlaySheet();
			newPlaySheet.setDataFrame(dataFrame);
			((WekaAprioriVizPlaySheet)newPlaySheet).setJTab(jTab);
			((WekaAprioriVizPlaySheet)newPlaySheet).setNumRules(numRule);
			((WekaAprioriVizPlaySheet)newPlaySheet).setConfPer(confPer);
			((WekaAprioriVizPlaySheet)newPlaySheet).setMinSupport(minSupport);
			((WekaAprioriVizPlaySheet)newPlaySheet).setMaxSupport(maxSupport);
			((WekaAprioriVizPlaySheet)newPlaySheet).setSkipAttributes(skipColumns);

		} else if(algorithm.equals("Linear Regression")) {
			//column to use as dependent variable
			String depVar = matrixDepVarComboBox.getSelectedItem() + "";
			String[] at = dataFrame.getColumnHeaders();
			int depVarIndex = at.length - 1;
			while(depVarIndex > -1 && !at[depVarIndex].equals(depVar)) {
				depVarIndex--;
			}
			
			if(depVarIndex<0){
				LOGGER.error("Cannot match dependent variable selected in drop down to list of columns");
				return;
			}
			
			//create grid view
			MatrixRegressionPlaySheet gridPlaySheet = new MatrixRegressionPlaySheet();
			
			newPlaySheet = new MatrixRegressionVizPlaySheet();
			newPlaySheet.setDataFrame(dataFrame);
			((MatrixRegressionVizPlaySheet)newPlaySheet).setSkipAttributes(skipColumns);
			((MatrixRegressionVizPlaySheet)newPlaySheet).setbColumnIndex(depVarIndex);
			((MatrixRegressionVizPlaySheet)newPlaySheet).setJTab(jTab);
			((MatrixRegressionVizPlaySheet)newPlaySheet).setJBar(jBar);
			
		}  else if(algorithm.equals("Numerical Correlation")) {			
			newPlaySheet = new NumericalCorrelationVizPlaySheet();
			newPlaySheet.setDataFrame(dataFrame);

			((NumericalCorrelationVizPlaySheet)newPlaySheet).setSkipAttributes(skipColumns);
			((NumericalCorrelationVizPlaySheet)newPlaySheet).setJTab(jTab);
			((NumericalCorrelationVizPlaySheet)newPlaySheet).setJBar(jBar);
			
		} else if(algorithm.equals("Correlation")) {
			
			newPlaySheet = new CorrelationPlaySheet();
			newPlaySheet.setDataFrame(dataFrame);

			((CorrelationPlaySheet)newPlaySheet).setSkipAttributes(skipColumns);
			((CorrelationPlaySheet)newPlaySheet).setJTab(jTab);
			((CorrelationPlaySheet)newPlaySheet).setJBar(jBar);
			
		} else if(algorithm.equals("Self Organizing Map")) {
			newPlaySheet = new SelfOrganizingMap3DBarChartPlaySheet();
			newPlaySheet.setDataFrame(dataFrame);
			
			((SelfOrganizingMap3DBarChartPlaySheet)newPlaySheet).setSkipAttributes(skipColumns);
			((SelfOrganizingMap3DBarChartPlaySheet)newPlaySheet).setJTab(jTab);
			((SelfOrganizingMap3DBarChartPlaySheet)newPlaySheet).setJBar(jBar);
			
			String l0Text = playSheet.getEnterL0TextField().getText();
			String r0Text = playSheet.getEnterR0TextField().getText();
			String tauText = playSheet.getEnterTauTextField().getText();
			
			if(l0Text != null && !l0Text.isEmpty()) {
				try {
					double l0 = Double.parseDouble(l0Text);
					((SelfOrganizingMap3DBarChartPlaySheet)newPlaySheet).setLearningRate(l0);
				} catch(NumberFormatException ex) {
					Utility.showError("Entered value for l0, " + l0Text + ", is not a valid numerical input.\nWill use default value.");
				}
			}
			
			if(r0Text != null && !r0Text.isEmpty()) {
				try {
					double r0 = Double.parseDouble(r0Text);
					((SelfOrganizingMap3DBarChartPlaySheet)newPlaySheet).setInitalRadius(r0);
				} catch(NumberFormatException ex) {
					Utility.showError("Entered value for r0, " + r0Text + ", is not a valid numerical input.\nWill use default value.");
				}
			}
			
			if(tauText != null && !tauText.isEmpty()) {
				try {
					double tau = Double.parseDouble(tauText);
					((SelfOrganizingMap3DBarChartPlaySheet)newPlaySheet).setTau(tau);
				} catch(NumberFormatException ex) {
					Utility.showError("Entered value for tau, " + tauText + ", is not a valid numerical input.\nWill use default value.");
				}
			}
		} else {
			LOGGER.error("Cannot find algorithm");
			return;
		}

		newPlaySheet.setRDFEngine(engine);
		newPlaySheet.setTitle(title);
		
		PlaysheetCreateRunner runner = new PlaysheetCreateRunner(newPlaySheet);
		Thread playThread = new Thread(runner);
		playThread.start();
	}

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
		this.playSheet = (ClassifyClusterPlaySheet)view;
		this.columnCheckboxes = playSheet.getColumnCheckboxes();
		this.algorithmComboBox = playSheet.getAlgorithmComboBox();
		
		//cluster
		this.selectNumClustersComboBox = playSheet.getSelectNumClustersComboBox();
		this.manuallySelectNumClustersText = playSheet.getManuallySelectNumClustersText();
		this.selectNumClustersTextField = playSheet.getSelectNumClustersTextField();
		//classification
		this.classificationMethodComboBox = playSheet.getClassificationMethodComboBox();
		this.classComboBox = playSheet.getClassComboBox();
		//outlier
		this.enterKNeighborsSlider = playSheet.getEnterKNeighborsSlider();
		//matrix regression
		this.matrixDepVarComboBox = playSheet.getMatrixDepVarComboBox();
		//correlation
		this.dataFrame = playSheet.getDataFrame();
		this.attributeNames = dataFrame.getColumnHeaders();
		this.jTab = playSheet.getJTab();
		this.jBar = playSheet.getJBar();
		this.engine = playSheet.engine;
		this.drillDownTabSelectorComboBox = playSheet.getDrillDownTabSelectorComboBox();
		this.showDrillDownBtn = playSheet.getShowDrillDownBtn();
		this.playSheetHash = playSheet.getPlaySheetHash();
		this.title = playSheet.getTitle();
	}

	public void setEntropyArr(Double[] entropyArr) {
		this.entropyArr = entropyArr;
	}

}

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
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.Hashtable;

import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JProgressBar;
import javax.swing.JSlider;
import javax.swing.JTabbedPane;
import javax.swing.JTextField;
import javax.swing.JToggleButton;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IDatabaseEngine;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.MachineLearningModulePlaySheet;

/**
 * Runs the algorithm selected on the Cluster/Classify playsheet and adds additional tabs. Tied to the button to the ClassifyClusterPlaySheet.
 */
public class RunAlgorithmListener extends AbstractListener {
	private static final Logger LOGGER = LogManager.getLogger(RunAlgorithmListener.class.getName());

	private MachineLearningModulePlaySheet playSheet;
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
	private JComboBox<String> classifyClassComboBox;
	
	//hoeffding tree
	private JComboBox<String> HOFclassifyClassComboBox;
	private JSlider enterHOFConfidenceSlider;
	private JSlider enterHOFGraceRows;
	private JSlider enterTieThresholdSlider;
	
	//outlier
	private JSlider enterKNeighborsSlider;

	//matrix regression
	private JComboBox<String> matrixDepVarComboBox;

	private JToggleButton showDrillDownBtn;
	private JComboBox<String> drillDownTabSelectorComboBox;
	private ArrayList<JCheckBox> columnCheckboxes;
	private IDatabaseEngine engine;
	private String title;

	private Double[] entropyArr;

	private ITableDataFrame dataFrame;
	private String[] attributeNames;

	//perceptron
	private JComboBox<String> perceptronClassComboBox;
	private JComboBox<String> perceptronTypeComboBox;
	private JTextField selectDegreeTextField;
	private JTextField selectConstantTextField;
	private JComboBox<String> perceptronKernel;

	//static int num = 0;
	/**
	 * Method actionPerformed.
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
//		//get the columns we're filtering out
//		//create a new tab
//		//figure out if we are clustering or classifying
//		//if cluster.... figure out number of clusters to use or if automatically choosing
//		//.............. pass tab, and call the clustering on it
//		//if classifying... figure out the class and the method for classifying, run the classifying
//		//need to put chart/grid on correct tabs, updating if necessary
//		//if outlier.... figure out the kneighbors and run the outlier method
//		//TODO add logger statements
//
//		//filter the names array and list of data based on the independent variables selected
//		//must make sure that we include the first col, even though there is no checkbox
//
//		//Revert back to the original data frame structure
////		Iterator<Object[]> it = dataFrame.iterator(false);
////		for(int i = 0; i < 5; i++) {
////			System.out.println(Arrays.toString(it.next()));
////		}
//		//System.out.println("Before: "+dataFrame.getNumRows());
//		//num++;
//		//BTreeDataFrame.write2Excel4Testing((BTreeDataFrame)dataFrame, "C:\\Users\\rluthar\\Desktop\\TestData\\btree_Before_All"+num+".xlsx");
//		//BTreeDataFrame.write2Excel4Testing(((BTreeDataFrame)dataFrame).getData2(), "C:\\Users\\rluthar\\Desktop\\TestData\\btree_Before_Part"+num+".xlsx");
//		//num++;
//		//System.out.println();
//		List<String> newColumnHeaders = Arrays.asList(dataFrame.getColumnHeaders());
//		List<String> originalColumnHeaders = Arrays.asList(playSheet.columnHeaders);
//		Set<String> setDifference = new LinkedHashSet<String>(newColumnHeaders);
//		setDifference.removeAll(originalColumnHeaders);
//		String[] removeSet = setDifference.toArray(new String[setDifference.size()]);
//		for(int i = removeSet.length - 1; i >=0; i--) {
//			dataFrame.removeColumn(removeSet[i]);
//		}
//		
//		//List<String> skipColumns = Arrays.asList(dataFrame.getColumnHeaders());
//		dataFrame.setColumnsToSkip(null);
//		Set<String> skipSet = new LinkedHashSet<String>(Arrays.asList(dataFrame.getColumnHeaders()));
//		
//		List<String> checkedColumns = new ArrayList<String>();
//		for(int i = 0; i < columnCheckboxes.size(); i++) {
//			if(!columnCheckboxes.get(i).isSelected()) {
//				checkedColumns.add(attributeNames[i+1]);
//			}
//		}
//		skipSet.removeAll(checkedColumns);
//		List<String> skipColumns = new ArrayList<String>(skipSet.size());
//		for(String s : skipSet) {
//			skipColumns.add(s);
//		}
//		
//		if(checkedColumns.size() == attributeNames.length-1) {
////		if(skipColumns.size() == attributeNames.length) {
//			Utility.showError("No variables were selected. Please select at least one and retry.");
//			return;
//		}
//
//		//dataFrame.setColumnsToSkip(skipColumns);
//		dataFrame.setColumnsToSkip(checkedColumns);
//		skipColumns = checkedColumns;
//
//		String algorithm = algorithmComboBox.getSelectedItem() + "";
//		if(algorithm.equals("Similarity")) {
//			ClusteringModuleUpdateRunner runner = new ClusteringModuleUpdateRunner(playSheet);
//			runner.setValues(dataFrame, skipColumns);
//			Thread playThread = new Thread(runner);
//			playThread.start();
//			return;
//		}
//
//		TablePlaySheet newPlaySheet;
//		if(algorithm.equals("Cluster") ) {
//			int numClusters = 0;
//			String selectNumClustersText = (String) selectNumClustersComboBox.getSelectedItem();
//			//if we are manually setting number of clusters, pull the number to use from the text field
//			if(selectNumClustersText.equals(manuallySelectNumClustersText)) {
//				//make sure that what you pull from text field is an integer, if not return with an error
//				String numClustersText = selectNumClustersTextField.getText();
//				try {
//					numClusters = Integer.parseInt(numClustersText);
//				} catch(NumberFormatException exception) {
//					Utility.showError("Number of clusters must be an integer greater than 1. Please fix and rerun.");
//					return;
//				}
//				if(numClusters<2) {//if error {
//					Utility.showError("Number of clusters must be an integer greater than 1. Please fix and rerun.");
//					return;
//				}
//			}
//
//			newPlaySheet = new ClusteringVizPlaySheet();
//			if(numClusters>=2) {
//				((ClusteringVizPlaySheet)newPlaySheet).setNumClusters(numClusters);
//			}
//			((ClusteringVizPlaySheet)newPlaySheet).setAddAsTab(true);
//			((ClusteringVizPlaySheet)newPlaySheet).setDataMaker(dataFrame);
//			((ClusteringVizPlaySheet)newPlaySheet).setNumClusters(numClusters);
//			((ClusteringVizPlaySheet)newPlaySheet).setSkipAttributes(skipColumns);
//			((ClusteringVizPlaySheet)newPlaySheet).setDrillDownTabSelectorComboBox(drillDownTabSelectorComboBox);
//			((ClusteringVizPlaySheet)newPlaySheet).setPlaySheetHash(playSheetHash);
//			((ClusteringVizPlaySheet)newPlaySheet).setJTab(jTab);
//			((ClusteringVizPlaySheet)newPlaySheet).setJBar(jBar);
//
//			showDrillDownBtn.setVisible(true);			
//
//		} else if(algorithm.equals("Classify")) {
//			dataFrame.setColumnsToSkip(skipColumns);
//
//			//method of classification to use
//			String classMethod = classificationMethodComboBox.getSelectedItem() + "";
//
//			//determine the column index and name to classify on
//			String classifier = classifyClassComboBox.getSelectedItem() + "";
//
//			newPlaySheet = new WekaClassificationPlaySheet();
//			newPlaySheet.setDataMaker(dataFrame);
//			((WekaClassificationPlaySheet)newPlaySheet).setModelName(classMethod);
//			((WekaClassificationPlaySheet)newPlaySheet).setSkipAttributes(skipColumns);
//			((WekaClassificationPlaySheet)newPlaySheet).setClassColumn(classifier);
//			((WekaClassificationPlaySheet)newPlaySheet).setJTab(jTab);
//			((WekaClassificationPlaySheet)newPlaySheet).setJBar(jBar);
//
//		} else if(algorithm.equals("Hoeffding Tree")) {
//			//skipColumns.add(attributeNames[0]);
//			dataFrame.setColumnsToSkip(skipColumns);
//
//			//determine the column index and name to classify on
//			String classifier = HOFclassifyClassComboBox.getSelectedItem() + "";
//			// get number of rows and convert to a percentage (sorry this is messy)
//			int gracePeriod = (int)(((double)enterHOFGraceRows.getValue() / dataFrame.getNumRows())*100);
//			// get confidence 
//			int ConfidenceValue = enterHOFConfidenceSlider.getValue();
//			
//			int tieThreshold = enterTieThresholdSlider.getValue();
//
//			newPlaySheet = new MOAClassificationPlaySheet();
//			newPlaySheet.setDataMaker(dataFrame);
//			((MOAClassificationPlaySheet)newPlaySheet).setSkipAttributes(skipColumns);
//			((MOAClassificationPlaySheet)newPlaySheet).setClassColumn(classifier);
//			((MOAClassificationPlaySheet)newPlaySheet).setGracePeriod(gracePeriod);
//			((MOAClassificationPlaySheet)newPlaySheet).setConfValue(ConfidenceValue);
//			((MOAClassificationPlaySheet)newPlaySheet).setTieThreshold(tieThreshold);
//			((MOAClassificationPlaySheet)newPlaySheet).setJTab(jTab);
//			((MOAClassificationPlaySheet)newPlaySheet).setJBar(jBar);
//
//		} else if(algorithm.equals("Local Outlier Factor")) {
//			int kneighbors = enterKNeighborsSlider.getValue();
//
//			newPlaySheet = new OutlierVizPlaySheet();
//			newPlaySheet.setDataMaker(dataFrame);
//			((OutlierVizPlaySheet)newPlaySheet).setAlgorithmSelected(OutlierVizPlaySheet.LOF);
//			((OutlierVizPlaySheet)newPlaySheet).setK(kneighbors);
//			((OutlierVizPlaySheet)newPlaySheet).setSkipAttributes(skipColumns);
//			((OutlierVizPlaySheet)newPlaySheet).setJTab(jTab);
//			((OutlierVizPlaySheet)newPlaySheet).setJBar(jBar);			
//
//		} else if(algorithm.equals("Fast Outlier Detection")) {
//			newPlaySheet = new OutlierVizPlaySheet();
//			newPlaySheet.setDataMaker(dataFrame);
//			
//			String subsetSizeText = playSheet.getEnterSubsetSizeTextField().getText();
//			String numRunsText = playSheet.getNumRunsTextField().getText();
//
//			int numRuns = 10;
//			int numSubsetSize = 20;
//			
//			if(numRunsText != null && !numRunsText.isEmpty()) {
//				try {
//					numRuns = Integer.parseInt(numRunsText);
//				} catch(NumberFormatException ex) {
//					Utility.showError("Entered value for Number of Runs, " + numRunsText + ", is not a valid numerical input.\nWill use default value.");
//				}
//			}
//			
//			if(subsetSizeText != null && !subsetSizeText.isEmpty()) {
//				try {
//					numSubsetSize = Integer.parseInt(subsetSizeText);
//				} catch(NumberFormatException ex) {
//					Utility.showError("Entered value for Subset Size, " + subsetSizeText + ", is not a valid numerical input.\nWill use default value.");
//				}
//			}
//			
//			((OutlierVizPlaySheet)newPlaySheet).setAlgorithmSelected(OutlierVizPlaySheet.FOD);
//			((OutlierVizPlaySheet)newPlaySheet).setNumSubsetSize(numSubsetSize); //TODO: create field for input
//			((OutlierVizPlaySheet)newPlaySheet).setNumRuns(numRuns); //TODO: create field for input
//			((OutlierVizPlaySheet)newPlaySheet).setSkipAttributes(skipColumns);
//			((OutlierVizPlaySheet)newPlaySheet).setJTab(jTab);
//			((OutlierVizPlaySheet)newPlaySheet).setJBar(jBar);	
//
//		} else if(algorithm.equals("Predictability")) {
//
//			double[] accuracyArr = new double[columnCheckboxes.size()];
//			double[] precisionArr = new double[columnCheckboxes.size()];
//			String[] at = this.dataFrame.getColumnHeaders();
//			for(int i=1; i < at.length; i++) {
//				if(!skipColumns.contains(at[i])) {
//					if(entropyArr[i] != 0) {
//						WekaClassification weka = new WekaClassification();
//						List<SEMOSSParam> options = weka.getOptions();
//						Map<String, Object> selectedOptions = new HashMap<String, Object>();
//						selectedOptions.put(options.get(0).getName(), "J48");
//						selectedOptions.put(options.get(1).getName(), attributeNames[i]);
//						selectedOptions.put(options.get(2).getName(), skipColumns);
//						weka.setSelectedOptions(selectedOptions);
//						try {
//							weka.runAlgorithm(dataFrame);
//							// both stored as percents
//							accuracyArr[i-1] = weka.getAccuracy(); 
//							precisionArr[i-1] = weka.getPrecision(); 
//						} catch (Exception ex) {
//							classLogger.error(Constants.STACKTRACE, ex);
//							LOGGER.error("Could not generate accuracy and precision values from WEKA classification");
//						}
//					} else {
//						accuracyArr[i-1] = 100;
//						precisionArr[i-1] = 100;
//					}
//				}
//			}
//			playSheet.fillAccuracyAndPrecision(accuracyArr, precisionArr);
//			return;
//
//		} else if(algorithm.equals("Association Learning")) { 
//			String numRuleString = playSheet.getEnterNumRulesTextField().getText();
//			String confIntervalString = playSheet.getEnterConfIntervalTextField().getText();
//			String minSupportString = playSheet.getEnterMinSupportTextField().getText();
//			String maxSupportString = playSheet.getEnterMaxSupportTextField().getText();
//
//			String errorMessage = "";
//
//			int numRule = -1;
//			try {
//				numRule = Integer.parseInt(numRuleString);
//			} catch(NumberFormatException ex) {
//				errorMessage += "Number of Rules must be a integer value.  Using default value of 10. \n";
//				numRule = 10;
//			}
//
//			double confPer = -1;
//			try {
//				confPer = Double.parseDouble(confIntervalString);
//				if(confPer < 0 || confPer > 1) {
//					errorMessage += "Conf Interval must be a value between 0 and 1.  Using default value of 0.9. \n";
//					confPer = 0.9;
//				}
//			} catch(NumberFormatException ex) {
//				errorMessage += "Conf Interval must be a value between 0 and 1.  Using default value of 0.9. \n";
//				confPer = 0.9;
//			}
//
//			double minSupport = -1;
//			try {
//				minSupport = Double.parseDouble(minSupportString);
//				if(minSupport < 0 || minSupport > 1) {
//					errorMessage += "Min Support must be a value between 0 and 1.  Using default value of 0.1. \n";
//					minSupport = 0.1;
//				}
//			} catch(NumberFormatException ex) {
//				errorMessage += "Min Support must be a value between 0 and 1.  Using default value of 0.1. \n";
//				minSupport = 0.1;
//			}
//
//			double maxSupport = -1.0;
//			try {
//				maxSupport = Double.parseDouble(maxSupportString);
//				if(maxSupport < 0 || maxSupport > 1) {
//					errorMessage += "Max Support must be a value between 0 and 1.  Using default value of 1.0. \n";
//					maxSupport = 1.0;
//				}
//			} catch(NumberFormatException ex) {
//				errorMessage += "Max Support must be a value between 0 and 1.  Using default value of 1.0. \n";
//				maxSupport = 1.0;
//			}
//
//			if(!errorMessage.isEmpty()) {
//				Utility.showError(errorMessage);
//			}
//			newPlaySheet = new WekaAprioriVizPlaySheet();
//			newPlaySheet.setDataMaker(dataFrame);
//			((WekaAprioriVizPlaySheet)newPlaySheet).setJTab(jTab);
//			((WekaAprioriVizPlaySheet)newPlaySheet).setNumRules(numRule);
//			((WekaAprioriVizPlaySheet)newPlaySheet).setConfPer(confPer);
//			((WekaAprioriVizPlaySheet)newPlaySheet).setMinSupport(minSupport);
//			((WekaAprioriVizPlaySheet)newPlaySheet).setMaxSupport(maxSupport);
//			((WekaAprioriVizPlaySheet)newPlaySheet).setSkipAttributes(skipColumns);
//
//		} else if(algorithm.equals("Linear Regression")) {
//			//column to use as dependent variable
//			String depVar = matrixDepVarComboBox.getSelectedItem() + "";
//			String[] at = dataFrame.getColumnHeaders();
//			int depVarIndex = at.length - 1;
//			while(depVarIndex > -1 && !at[depVarIndex].equals(depVar)) {
//				depVarIndex--;
//			}
//
//			if(depVarIndex<0){
//				LOGGER.error("Cannot match dependent variable selected in drop down to list of columns");
//				return;
//			}
//
//			newPlaySheet = new MatrixRegressionVizPlaySheet();
//			newPlaySheet.setDataMaker(dataFrame);
//			((MatrixRegressionVizPlaySheet)newPlaySheet).setSkipAttributes(skipColumns);
//			((MatrixRegressionVizPlaySheet)newPlaySheet).setbColumnIndex(depVarIndex);
//			((MatrixRegressionVizPlaySheet)newPlaySheet).setJTab(jTab);
//			((MatrixRegressionVizPlaySheet)newPlaySheet).setJBar(jBar);
//
//		}  else if(algorithm.equals("Numerical Correlation")) {			
//			newPlaySheet = new NumericalCorrelationVizPlaySheet();
//			newPlaySheet.setDataMaker(dataFrame);
//
//			((NumericalCorrelationVizPlaySheet)newPlaySheet).setSkipAttributes(skipColumns);
//			((NumericalCorrelationVizPlaySheet)newPlaySheet).setJTab(jTab);
//			((NumericalCorrelationVizPlaySheet)newPlaySheet).setJBar(jBar);
//
//		} 
//		// NEVER FINISHED GENERIC CORRELATION
////		else if(algorithm.equals("Correlation")) {
////
////			newPlaySheet = new CorrelationPlaySheet();
////			newPlaySheet.setDataMaker(dataFrame);
////
////			((CorrelationPlaySheet)newPlaySheet).setSkipAttributes(skipColumns);
////			((CorrelationPlaySheet)newPlaySheet).setJTab(jTab);
////			((CorrelationPlaySheet)newPlaySheet).setJBar(jBar);
////
////		} 
//		else if(algorithm.equals("Self Organizing Map")) {
//			newPlaySheet = new SelfOrganizingMap3DBarChartPlaySheet();
//			newPlaySheet.setDataMaker(dataFrame);
//
//			((SelfOrganizingMap3DBarChartPlaySheet)newPlaySheet).setSkipAttributes(skipColumns);
//			((SelfOrganizingMap3DBarChartPlaySheet)newPlaySheet).setJTab(jTab);
//			((SelfOrganizingMap3DBarChartPlaySheet)newPlaySheet).setJBar(jBar);
//
//			String l0Text = playSheet.getEnterL0TextField().getText();
//			String r0Text = playSheet.getEnterR0TextField().getText();
//			String tauText = playSheet.getEnterTauTextField().getText();
//
//			if(l0Text != null && !l0Text.isEmpty()) {
//				try {
//					double l0 = Double.parseDouble(l0Text);
//					((SelfOrganizingMap3DBarChartPlaySheet)newPlaySheet).setLearningRate(l0);
//				} catch(NumberFormatException ex) {
//					Utility.showError("Entered value for l0, " + l0Text + ", is not a valid numerical input.\nWill use default value.");
//				}
//			}
//
//			if(r0Text != null && !r0Text.isEmpty()) {
//				try {
//					double r0 = Double.parseDouble(r0Text);
//					((SelfOrganizingMap3DBarChartPlaySheet)newPlaySheet).setInitalRadius(r0);
//				} catch(NumberFormatException ex) {
//					Utility.showError("Entered value for r0, " + r0Text + ", is not a valid numerical input.\nWill use default value.");
//				}
//			}
//
//			if(tauText != null && !tauText.isEmpty()) {
//				try {
//					double tau = Double.parseDouble(tauText);
//					((SelfOrganizingMap3DBarChartPlaySheet)newPlaySheet).setTau(tau);
//				} catch(NumberFormatException ex) {
//					Utility.showError("Entered value for tau, " + tauText + ", is not a valid numerical input.\nWill use default value.");
//				}
//			}
//		} else if(algorithm.equals("Perceptron")) {
//			//dataFrame.setColumnsToSkip(skipColumns);
//
//			//determine the column index and name to classify on
//			String classifier = perceptronClassComboBox.getSelectedItem() + "";
//			String type = perceptronTypeComboBox.getSelectedItem()+"";
//			int degree = 1;
//			double kappa = 1.0;
//			double constant = 1.0;
//			
//			String degreeText = selectDegreeTextField.getText();
//			String constantText = selectConstantTextField.getText();
//			String kernelType = perceptronKernel.getSelectedItem().toString();
//			
//			if(kernelType.equalsIgnoreCase("Polynomial")) {
//				try {
//					degree = Integer.parseInt(degreeText);//Double.parseDouble(degreeText);
//				} catch(NumberFormatException exception) {
//					Utility.showError("Degree must be an integer! Try again");
//					return;
//				}
//			} else {
//				try {
//					kappa = Double.parseDouble(degreeText);
//				} catch(NumberFormatException exception) {
//					Utility.showError("Kappa must be a number! Try again");
//				}
//			}
//			
//			try {
//				constant = Double.parseDouble(constantText);
//			} catch(NumberFormatException exception) {
//				Utility.showError("Constant must be a number! Try again");
//			}
//			
//			newPlaySheet = new PerceptronPlaySheet();
//			newPlaySheet.setDataMaker(dataFrame);
//			((PerceptronPlaySheet)newPlaySheet).setSkipAttributes(skipColumns);
//			((PerceptronPlaySheet)newPlaySheet).setClassColumn(classifier);
//			((PerceptronPlaySheet)newPlaySheet).setDegree(degree);
//			((PerceptronPlaySheet)newPlaySheet).setKernel(type);
//			((PerceptronPlaySheet)newPlaySheet).setKappa(kappa);
//			((PerceptronPlaySheet)newPlaySheet).setConstant(constant);
//			//((PerceptronPlaySheet)newPlaySheet).setClassColumn(classifier);
//			((PerceptronPlaySheet)newPlaySheet).setJTab(jTab);
//			//((PerceptronPlaySheet)newPlaySheet).setJBar(jBar);
//
//		} else {
//			LOGGER.error("Cannot find algorithm");
//			return;
//		}
//
//		newPlaySheet.setRDFEngine(engine);
//		newPlaySheet.setTitle(title);
//
//		PlaysheetCreateRunner runner = new PlaysheetCreateRunner(newPlaySheet);
//		Thread playThread = new Thread(runner);
//		playThread.start();
	}

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
		this.playSheet = (MachineLearningModulePlaySheet) view;
		this.columnCheckboxes = playSheet.getColumnCheckboxes();
		this.algorithmComboBox = playSheet.getAlgorithmComboBox();

		//cluster
		this.selectNumClustersComboBox = playSheet.getSelectNumClustersComboBox();
		this.manuallySelectNumClustersText = playSheet.getManuallySelectNumClustersText();
		this.selectNumClustersTextField = playSheet.getSelectNumClustersTextField();
		//classification
		this.classificationMethodComboBox = playSheet.getClassificationMethodComboBox();
		this.classifyClassComboBox = playSheet.getClassifyClassComboBox();
		//hoeffding tree
		this.HOFclassifyClassComboBox = playSheet.getHOFClassifyClassComboBox();
		this.enterHOFGraceRows = playSheet.getenterHOFGraceRows();
		this.enterHOFConfidenceSlider = playSheet.getenterHOFConfidenceSlider();
		this.enterTieThresholdSlider = playSheet.getenterTieThresholdSlider();
		//perceptron
		this.perceptronClassComboBox = playSheet.getPerceptronClassComboBox();
		this.perceptronTypeComboBox = playSheet.getPerceptronTypeComboBox();
		this.selectDegreeTextField = playSheet.getPerceptronDegree();
		this.selectConstantTextField = playSheet.getPerceptronConstant();
		this.perceptronKernel = playSheet.getPerceptronTypeComboBox();
		//outlier
		this.enterKNeighborsSlider = playSheet.getEnterKNeighborsSlider();
		//matrix regression
		this.matrixDepVarComboBox = playSheet.getMatrixDepVarComboBox();
		//correlation
		this.dataFrame = playSheet.getDataMaker();
		this.attributeNames = dataFrame.getColumnHeaders();
		this.jTab = playSheet.getJTab();
		this.jBar = playSheet.getJBar();
		this.engine = playSheet.engine;
		this.drillDownTabSelectorComboBox = playSheet.getDrillDownTabSelectorComboBox();
		this.showDrillDownBtn = playSheet.getShowDrillDownBtn();
		this.playSheetHash = playSheet.getPlaySheetHash();
		this.title = playSheet.getTitle();
	}

	public void setEntropyArr(Double[] entropyArr2) {
		this.entropyArr = entropyArr2;
	}

}

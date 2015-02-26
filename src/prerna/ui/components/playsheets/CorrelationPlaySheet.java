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

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.beans.PropertyVetoException;
import java.util.ArrayList;

import javax.swing.JComponent;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.learning.similarity.ClusterRemoveDuplicates;
import prerna.algorithm.learning.similarity.ClusteringDataProcessor;
import prerna.math.BarChart;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.ui.components.NewScrollBarUI;
import prerna.ui.main.listener.impl.GridPlaySheetListener;
import prerna.ui.main.listener.impl.JTableExcelExportListener;
import prerna.util.ArrayListUtilityMethods;
import prerna.util.ArrayUtilityMethods;

public class CorrelationPlaySheet extends GridPlaySheet{

	private static final Logger LOGGER = LogManager.getLogger(CorrelationPlaySheet.class.getName());
	protected JTabbedPane jTab;

	private Integer[] categoryPropIndices;

	ArrayList<String[]> valuesList = new ArrayList<String[]>();
	ArrayList<String[]> uniqueValuesList = new ArrayList<String[]>();
	private ArrayList<int[]> uniqueValueCountsList = new ArrayList<int[]>();
	
	@Override
	public void createData() {
		if(list==null) {
			super.createData();
			//make sure that we have no duplicates in the liast and names for the analysis
			LOGGER.info("Removing any duplicated instances...");
			ClusterRemoveDuplicates crd = new ClusterRemoveDuplicates(list, names);
			this.list = crd.getRetMasterTable();
			this.names = crd.getRetVarNames();
			
			LOGGER.info("Formatting dataset to run algorithm...");
			ClusteringDataProcessor cdp = new ClusteringDataProcessor(list, names);
			categoryPropIndices = cdp.getCategoryPropIndices();
		}

	}
	
	@Override
	public void runAnalytics() {
		
		int i;
		int j;
		int nameslength = names.length;
		int numVariables = names.length - 1;
		int numCategorical = categoryPropIndices.length;
		
		Boolean[] isCategoricalArr = new Boolean[nameslength];
		for(i=1;i<nameslength;i++) {
			Boolean isCategorical = false;
			for(j=0; j<numCategorical; j++)
				if(categoryPropIndices[j]==i) {
					isCategorical = true;
				}
			isCategoricalArr[i] = isCategorical;
		}
		
		//go through all variables and get the outputs for each
		
		for(i=1;i<nameslength;i++) {
			
			String[] values;
			String[] uniqueValues;
			int[] uniqueValueCount;
			
			//calculate the number of times each value corresponds to one of the instances
			if(isCategoricalArr[i]) {
				
				Object[] valuesObj = ArrayListUtilityMethods.getColumnFromList(list,i);
				String[] valuesArr = ArrayUtilityMethods.convertObjArrToStringArr(valuesObj);
				BarChart chart = new BarChart(valuesArr);
				chart.calculateCategoricalBins("?", true, true);
				values = chart.getStringValues();
				uniqueValues = chart.getStringUniqueValues();
				uniqueValueCount = chart.getStringUniqueCounts();
				
			}else {
				
				Object[] valuesObj = ArrayListUtilityMethods.getColumnFromList(list,i);
				double[] valuesArr = ArrayUtilityMethods.convertObjArrToDoubleArr(valuesObj);
				BarChart chart = new BarChart(valuesArr);
				if(chart.isUseCategoricalForNumericInput()) {
					chart.calculateCategoricalBins("?", true, true);
					values = chart.getStringValues();
					uniqueValues = chart.getStringUniqueValues();
					uniqueValueCount = chart.getStringUniqueCounts();
				} else {
					values = chart.getAssignmentForEachObject();
					uniqueValues = chart.getNumericalBinOrder();
					uniqueValueCount = chart.getNumericBinCounterArr();
				}		
			}

			valuesList.add(values);
			uniqueValuesList.add(uniqueValues);
			uniqueValueCountsList.add(uniqueValueCount);
		
		}
		
		//calculate the standard deviation for each column
		double[] standardDev = new double[numVariables];
		for(i=0; i<numVariables; i++) {
			standardDev[i] = Math.sqrt(calculateVariance(i));
		}

		//calculate the covariance for each pair of columns
		double[][] covariance = new double[numVariables][numVariables];
		for(i=0; i<numVariables; i++) {
			for(j=0; j<numVariables; j++) {
				if(i==j)
					covariance[i][j] = Math.pow(standardDev[i],2);
				else
					covariance[i][j] = calculateCovariance(i,j);
			}
		}
		
		//calculate the correlation for each pair of columns
		double[][] correlation = new double[numVariables][numVariables];
		for(i=0; i<numVariables; i++) {
			for(j=0; j<numVariables; j++) {
				correlation[i][j] = covariance[i][j] / (standardDev[i] * standardDev[j]);
			}
		}
		
		names[0] = "-";
		
		ArrayList<Object []> output = new ArrayList<Object []>();
		//create the list output
		for(i=0; i<numVariables; i++) {
			
			Object[] row = new Object[numVariables+1];
			row[0] = names[i+1];
			
			for(j=0; j<numVariables; j++) {
				row[j+1] = correlation[i][j];
			}
			output.add(row);
		}
		list = output;
	}
	
	/**
	 * Calculate the variance in column x
	 * @param x
	 * @return
	 */
	public double calculateVariance(int x) {
		
		int i;
		double numInstances = list.size()*1.0;
		int[] uniqueValueCounts = uniqueValueCountsList.get(x);
		int numUniqueValueCounts = uniqueValueCounts.length;
		
		double variance = 0.0;
		for(i=0; i<numUniqueValueCounts; i++) {
			variance += Math.pow(1 - uniqueValueCounts[i] / numInstances,2) * (uniqueValueCounts[i] / numInstances);
		}

		return variance;
	}
	
	/**
	 * Calculate the covariance between two columns, x and y
	 * @param x
	 * @param y
	 * @return
	 */
	public double calculateCovariance(int x, int y) {
		
		int i;
		int j;
		double numInstances = list.size();
		int[] xUniqueValueCounts = uniqueValueCountsList.get(x);
		int[] yUniqueValueCounts = uniqueValueCountsList.get(y);
		int numXUniqueValueCounts = xUniqueValueCounts.length;
		int numYUniqueValueCounts = yUniqueValueCounts.length;
		
		double[][] occurenceArr = calculateOccurence(x,y);
		
		double covariance = 0.0;
		for(i=0; i<numXUniqueValueCounts; i++) {
			for(j=0; j<numYUniqueValueCounts; j++) {
				covariance += (1 - xUniqueValueCounts[i] / numInstances) * (1 - yUniqueValueCounts[j] / numInstances) * (occurenceArr[i][j] / numInstances);
			}
		}

		return covariance;
	}
	
	private double[][] calculateOccurence(int x, int y) {
		
		int numInstances = list.size();
		String[] xValues = valuesList.get(x);
		String[] yValues = valuesList.get(y);
		if(numInstances!=xValues.length || numInstances !=yValues.length) {
			LOGGER.error("Variables do not have the same number of instances");
			return null;
		}
		
		String[] xUniqueValues = uniqueValuesList.get(x);
		String[] yUniqueValues = uniqueValuesList.get(y);

		int i;
		int j;
		int p;
		int numXUniqueValues = xUniqueValues.length;
		int numYUniqueValues = yUniqueValues.length;

		double[][] probArr = new double[numXUniqueValues][numYUniqueValues];
		for(i=0; i<numXUniqueValues; i++) {
			for(j=0; j<numYUniqueValues; j++) {
				probArr[i][j] = 0;
			}
		}
		
		for(p=0; p<numInstances; p++) {
			String xName = xValues[p];
			String yName = yValues[p];
			
			INNER : for(i=0; i<numXUniqueValues; i++) {
				if(xName.equals(xUniqueValues[i])) {
					
					for(j=0; j<numYUniqueValues; j++) {
						if(yName.equals(yUniqueValues[j])) {

							probArr[i][j] ++;
							break INNER;
							
						}
					}
				}
			}
		}

		return probArr;
	}
	
	@Override
	public void addPanel()
	{
		if(jTab==null) {
			super.addPanel();
		} else {
			String lastTabName = jTab.getTitleAt(jTab.getTabCount()-1);
			LOGGER.info("Parsing integer out of last tab name");
			int count = 1;
			if(jTab.getTabCount()>1)
				count = Integer.parseInt(lastTabName.substring(0,lastTabName.indexOf(".")))+1;
			addPanelAsTab(count+". Correlation");
		}
	}
	

	public void addPanelAsTab(String tabName) {
	//	setWindow();
		try {
			table = new JTable();
			
			//Add Excel export popup menu and menuitem
			JPopupMenu popupMenu = new JPopupMenu();
			JMenuItem menuItemAdd = new JMenuItem("Export to Excel");
			String questionTitle = this.getTitle();
			menuItemAdd.addActionListener(new JTableExcelExportListener(table, questionTitle));
			popupMenu.add(menuItemAdd);
			table.setComponentPopupMenu(popupMenu);
			
			GridPlaySheetListener gridPSListener = new GridPlaySheetListener();
			LOGGER.debug("Created the table");
			this.addInternalFrameListener(gridPSListener);
			LOGGER.debug("Added the internal frame listener ");
			//table.setAutoCreateRowSorter(true);
			
			JPanel panel = new JPanel();
			panel.add(table);
			GridBagLayout gbl_mainPanel = new GridBagLayout();
			gbl_mainPanel.columnWidths = new int[]{0, 0};
			gbl_mainPanel.rowHeights = new int[]{0, 0};
			gbl_mainPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
			gbl_mainPanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
			panel.setLayout(gbl_mainPanel);
			
			addScrollPanel(panel, table);
			
			jTab.addTab(tabName, panel);
			
			this.pack();
			this.setVisible(true);
			this.setSelected(false);
			this.setSelected(true);
			LOGGER.debug("Added new Correlation Sheet");
			
		} catch (PropertyVetoException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public Object getVariable(String varName, ISelectStatement sjss){
		return sjss.getVar(varName);
	}
	public void setJTab(JTabbedPane jTab) {
		this.jTab = jTab;
	}
	public void setJBar(JProgressBar jBar) {
		this.jBar = jBar;
	}
	public void setCategoryPropIndices(Integer[] categoryPropIndices) {
		this.categoryPropIndices = categoryPropIndices;
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
}

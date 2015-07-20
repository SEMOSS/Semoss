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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.learning.supervized.MatrixRegressionAlgorithm;
import prerna.om.SEMOSSParam;
import prerna.ui.components.GridScrollPane;
import prerna.ui.components.NewScrollBarUI;

public class MatrixRegressionPlaySheet extends GridPlaySheet{

	private static final Logger LOGGER = LogManager.getLogger(MatrixRegressionPlaySheet.class.getName());

	private String[] columnHeaders;

	private double[] b;
	private double[][] A;
	
	private int bIndex = -1;
	private double[] coeffArray;
	private double[] coeffErrorsArray;
	private double[] estimateArray;
	private double[] residualArray;
	private double standardError;

	private int numIndepVariables;
	private int variableStartIndex;
	private int outputNumCols;

	private boolean includesInstance = true;
	private List<String> skipAttributes;

	protected JTabbedPane jTab;

	@Override
	public void createData() {
		if(dataFrame == null || dataFrame.isEmpty())
			super.createData();
	}

	@Override
	public void runAnalytics() {
		this.columnHeaders = dataFrame.getColumnHeaders();
		if(includesInstance) {
			numIndepVariables = columnHeaders.length - 2;//-1 for instance, -1 for dep var
			variableStartIndex = 1;
		} else {
			numIndepVariables = columnHeaders.length - 1;// -1 for dep var
			variableStartIndex = 0;
		}
		outputNumCols = 1 + numIndepVariables + 4;//instance, numIndVariables, and results

		//the bIndex should have been provided. if not, will use the last column
		if(bIndex == -1) {
			bIndex = columnHeaders.length - 1;
		}

		//create the b and A arrays which are used in matrix regression to determine coefficients
		b = MatrixRegressionHelper.createB(dataFrame, skipAttributes, bIndex);
		A = MatrixRegressionHelper.createA(dataFrame, skipAttributes, variableStartIndex, bIndex);

		//run regression
		MatrixRegressionAlgorithm alg = new MatrixRegressionAlgorithm();
		List<SEMOSSParam> options = alg.getOptions();
		Map<String, Object> selectedOptions = new HashMap<String, Object>();
		selectedOptions.put(options.get(0).getName(), A);
		selectedOptions.put(options.get(1).getName(), b);
		alg.setSelectedOptions(selectedOptions);
		dataFrame.performAction(alg);

		coeffArray = alg.getCoeffArray();
		coeffErrorsArray = alg.getCoeffErrorsArray();
		estimateArray = alg.getEstimateArray();
		residualArray = alg.getResidualArray();
		standardError = alg.getStandardError();
	}

	@Override
	public List<Object[]> getTabularData() {
		//add in estimated and residuals for each row determined using coefficients
		List<Object[]> outputList = new ArrayList<Object []>();
		
		Iterator<Object[]> it = dataFrame.iterator(false, skipAttributes);
		int i = 0;
		int j = 0;
		while(it.hasNext()) {
			Object[] row = it.next();
			double actualVal = b[i];
			double estimatedVal = estimateArray[i];
			double residualVal = residualArray[i];
			double withinErrorRange = Math.abs(residualVal / standardError);

			Object[] newRow = new Object[outputNumCols];
			if(includesInstance) {
				newRow[0] = row[0];
			} else {
				newRow[0] = "";
			}
			for(j = 0; j < numIndepVariables; j++) {
				newRow[j+1] = A[i][j];
			}
			newRow[j+1] = actualVal;
			newRow[j+2] = estimatedVal;
			newRow[j+3] = residualVal;
			newRow[j+4] = withinErrorRange;
			outputList.add(newRow);
			i++;
		}

		//create a row with coefficient values
		Object[] coeffRow = new Object[outputNumCols];
		coeffRow[0] = "COEFFICIENTS";
		for(i=1;i<coeffArray.length;i++) {
			coeffRow[i] = coeffArray[i];
		}
		coeffRow[i] = "b is " + coeffArray[0];//first spot in the coefficient array is the constant value
		coeffRow[i+1] = "-";
		coeffRow[i+2] = "-";
		coeffRow[i+3] = "standard error of estimates is "+standardError;	
		outputList.add(0,coeffRow);

		//create a row with coefficient errors
		Object[] coeffErrorsRow = new Object[outputNumCols];
		coeffErrorsRow[0] = "COEFFICIENTS ERRORS";
		for(i=1;i<coeffErrorsArray.length;i++) {
			coeffErrorsRow[i] = coeffErrorsArray[i];
		}
		coeffErrorsRow[i] = "b error is " + coeffErrorsArray[0];//first spot in the coefficient array is the constant value
		coeffErrorsRow[i+1] = "-";
		coeffErrorsRow[i+2] = "-";
		coeffErrorsRow[i+3] = "-";	
		outputList.add(1,coeffErrorsRow);

		return outputList;
	}

	@Override
	public String[] getNames() {
		//update headers so that there are columns for estimated and residuals
		columnHeaders = MatrixRegressionHelper.moveNameToEnd(columnHeaders, bIndex);

		String[] namesWithEstimateAndResiduals = new String[outputNumCols];
		if(includesInstance)
			namesWithEstimateAndResiduals[0] = columnHeaders[0];
		else
			namesWithEstimateAndResiduals[0] = "";

		int newIndex = 1;
		for(int i=0; i < numIndepVariables; i++) {
			namesWithEstimateAndResiduals[newIndex] = columnHeaders[i + variableStartIndex];
			newIndex++;
		}

		namesWithEstimateAndResiduals[newIndex]  = "Actual- " + columnHeaders[numIndepVariables + 1];
		namesWithEstimateAndResiduals[newIndex+1] = "Estimated- " + columnHeaders[numIndepVariables + 1];
		namesWithEstimateAndResiduals[newIndex+2] = "Residual- " + columnHeaders[numIndepVariables + 1];
		namesWithEstimateAndResiduals[newIndex+3] = "Within Error Range?";
		return namesWithEstimateAndResiduals;
	}

	public void setbColumnIndex(int bColumnIndex) {
		this.bIndex = bColumnIndex;
	}

	public double[] getCoeffArray() {
		return coeffArray;
	}

	public double getStandardError() {
		return standardError;
	}

	public void setIncludesInstance(boolean includesInstance) {
		this.includesInstance = includesInstance;
	}
	
	public void setSkipAttributes(List<String> skipAttributes) {
		this.skipAttributes = skipAttributes;
		this.dataFrame.setColumnsToSkip(skipAttributes);
	}

	/////////////////////////////SWING DEPENDENT CODE/////////////////////////////
	@Override
	public void addPanel() {
		if (jTab == null) {
			super.addPanel();
		} else {
			String lastTabName = jTab.getTitleAt(jTab.getTabCount() - 1);
			LOGGER.info("Parsing integer out of last tab name");
			int count = 1;
			if (jTab.getTabCount() > 1)
				count = Integer.parseInt(lastTabName.substring(0, lastTabName.indexOf("."))) + 1;
			addPanelAsTab(count + ". Matrix Regression Raw Data");
		}
	}

	public void addPanelAsTab(String tabName) {
		table = new JTable();
		GridScrollPane gsp = null;
		gsp = new GridScrollPane(dataFrame.getColumnHeaders(), dataFrame.getData());
		gsp.addHorizontalScroll();
		jTab.addTab(tabName, gsp);
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

	public void setJTab(JTabbedPane jTab) {
		this.jTab = jTab;
	}

	public void setJBar(JProgressBar jBar) {
		this.jBar = jBar;
	}
}

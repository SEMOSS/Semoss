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
import java.awt.GridBagConstraints;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.learning.supervized.MatrixRegressionAlgorithm;
import prerna.om.SEMOSSParam;
import prerna.ui.components.GridScrollPane;
import prerna.ui.components.NewScrollBarUI;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class MatrixRegressionVizPlaySheet extends BrowserPlaySheet{

	private static final Logger LOGGER = LogManager.getLogger(MatrixRegressionVizPlaySheet.class.getName());	

	private String[] columnHeaders;

	private int bIndex = -1;
	private double[][] Ab;

	private double standardError;
	private double[] coeffArray;
	private double[][] correlationArray;

	private boolean includesInstance = true;
	private List<String> skipAttributes;

	/**
	 * Constructor for MatrixRegressionVizPlaySheet.
	 */
	public MatrixRegressionVizPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/scatter-plot-matrix.html";
	}

	@Override
	public void createData() {
		if(dataFrame == null || dataFrame.isEmpty())
			super.createData();
	}

	@Override
	public void runAnalytics() {
		dataFrame.setColumnsToSkip(skipAttributes);
		this.columnHeaders = dataFrame.getColumnHeaders();

		//the bIndex should have been provided. if not, will use the last column
		if(bIndex==-1)
			bIndex = columnHeaders.length - 1;

		//create the b and A arrays which are used in matrix regression to determine coefficients
		double[] b = MatrixRegressionHelper.createB(dataFrame, bIndex);
		double[][] A;
		if(includesInstance) {
			A = MatrixRegressionHelper.createA(dataFrame, 1, bIndex);
		} else {
			A = MatrixRegressionHelper.createA(dataFrame, 0, bIndex);
		}

		//run regression
		MatrixRegressionAlgorithm alg = new MatrixRegressionAlgorithm();
		List<SEMOSSParam> options = alg.getOptions();
		Map<String, Object> selectedOptions = new HashMap<String, Object>();
		selectedOptions.put(options.get(0).getName(), A);
		selectedOptions.put(options.get(1).getName(), b);
		alg.setSelectedOptions(selectedOptions);
		dataFrame.performAction(alg);

		this.coeffArray = alg.getCoeffArray();
		this.standardError = alg.getStandardError();

		//create Ab array
		this.Ab = MatrixRegressionHelper.appendB(A, b);
		PearsonsCorrelation correlation = new PearsonsCorrelation(Ab);
		this.correlationArray = correlation.getCorrelationMatrix().getData();		
	}

	@Override
	public void processQueryData() {	
		int numVariables;
		String id = "";
		if(includesInstance) {
			numVariables = columnHeaders.length - 1;
			id = columnHeaders[0];
		}else {
			numVariables = columnHeaders.length;
		}

		int i = 0;
		Object[] stdErrors = new Object[numVariables];
		for(i = 0; i<numVariables ; i++) {
			stdErrors[i] = standardError;
		}

		Object[] coefficients = new Object[numVariables + 1];
		for(i = 0; i< numVariables - 1; i++) {
			coefficients[i + 1] = coeffArray[i+1];
		}
		coefficients[numVariables] = coeffArray[0];

		Object[] correlations = new Object[numVariables + 1];
		for(i = 0; i<numVariables-1; i++) {
			correlations[i+1] = correlationArray[i][numVariables - 1];
		}

		Hashtable<String, Object> allHash = new Hashtable<String, Object>();
		allHash.put("one-row",true);
		allHash.put("id",id);
		allHash.put("names", columnHeaders);
		allHash.put("dataSeries", dataFrame.getData());
		allHash.put("shifts", stdErrors);
		allHash.put("coefficients", coefficients);
		allHash.put("correlations", correlations);
		this.dataHash = allHash;
	}

	public void setbColumnIndex(int bColumnIndex) {
		this.bIndex = bColumnIndex;
	}

	public int getbColumnIndex() {
		return bIndex;
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
			addPanelAsTab(count + ". Matrix Regression Viz Data");
			addGridTab(count + ". Matrix Regression Raw Data");
		}
	}

	public void addGridTab(String tabName) {
		table = new JTable();
		GridScrollPane gsp = null;
		gsp = new GridScrollPane(getNames(), getTabularData());
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

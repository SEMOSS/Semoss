///*******************************************************************************
// * Copyright 2015 Defense Health Agency (DHA)
// *
// * If your use of this software does not include any GPLv2 components:
// * 	Licensed under the Apache License, Version 2.0 (the "License");
// * 	you may not use this file except in compliance with the License.
// * 	You may obtain a copy of the License at
// *
// * 	  http://www.apache.org/licenses/LICENSE-2.0
// *
// * 	Unless required by applicable law or agreed to in writing, software
// * 	distributed under the License is distributed on an "AS IS" BASIS,
// * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * 	See the License for the specific language governing permissions and
// * 	limitations under the License.
// * ----------------------------------------------------------------------------
// * If your use of this software includes any GPLv2 components:
// * 	This program is free software; you can redistribute it and/or
// * 	modify it under the terms of the GNU General Public License
// * 	as published by the Free Software Foundation; either version 2
// * 	of the License, or (at your option) any later version.
// *
// * 	This program is distributed in the hope that it will be useful,
// * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
// * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * 	GNU General Public License for more details.
// *******************************************************************************/
//package prerna.ui.components.playsheets;
//
//import java.awt.Dimension;
//import java.awt.GridBagConstraints;
//import java.util.HashMap;
//import java.util.Hashtable;
//import java.util.List;
//import java.util.Map;
//
//import javax.swing.JComponent;
//import javax.swing.JPanel;
//import javax.swing.JProgressBar;
//import javax.swing.JScrollPane;
//import javax.swing.JTabbedPane;
//import javax.swing.JTable;
//
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
//
//import prerna.algorithm.learning.supervized.NumericalCorrelationAlgorithm;
//import prerna.om.SEMOSSParam;
//import prerna.ui.components.GridScrollPane;
//import prerna.ui.components.NewScrollBarUI;
//import prerna.util.Constants;
//import prerna.util.DIHelper;
//
//public class NumericalCorrelationVizPlaySheet extends BrowserPlaySheet{
//
//	private static final Logger LOGGER = LogManager.getLogger(NumericalCorrelationVizPlaySheet.class.getName());		
//	
//	private String[] columnHeaders;
//	private double[][] correlationArray;
//	
//	private boolean includesInstance = true;
//	private List<String> skipAttributes;
//
//
//	/**
//	 * Constructor for MatrixRegressionVizPlaySheet.
//	 */
//	public NumericalCorrelationVizPlaySheet() {
//		super();
//		this.setPreferredSize(new Dimension(800,600));
//		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
//		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/scatter-plot-matrix.html";
//	}
//
//	@Override
//	public void createData() {
//		if(dataFrame == null || dataFrame.isEmpty()) {
//			super.createData();
//		}
//		this.columnHeaders = getNames();
//	}
//
//	@Override
//	public void runAnalytics() {
//		NumericalCorrelationAlgorithm alg = new NumericalCorrelationAlgorithm();
//		List<SEMOSSParam> options = alg.getOptions();
//		Map<String, Object> selectedOptions = new HashMap<String, Object>();
//		selectedOptions.put(options.get(0).getName(), includesInstance);
//		selectedOptions.put(options.get(1).getName(), skipAttributes);
//		alg.setSelectedOptions(selectedOptions);
//		dataFrame.performAnalyticAction(alg);
//		correlationArray = alg.getCorrelationArray();
//	}
//
//	@Override
//	public void processQueryData() {
//		int numVariables;
//		String id;
//		if(includesInstance) {
//			numVariables = columnHeaders.length - 1;
//			id = columnHeaders[0];
//		}else {
//			numVariables = columnHeaders.length;
//			id = "";
//		}
//		
//		// reversing values since it is being painted by JS in reverse order
//		double[][] correlations = new double[numVariables][numVariables];
//		int i = 0;
//		int j = 0;
//		for(i = 0; i<numVariables; i++) {
//			for(j = 0; j<numVariables; j++) {
//				correlations[numVariables-i-1][numVariables-j-1] = correlationArray[i][j];
//			}
//		}
//
//		Hashtable<String, Object> allHash = new Hashtable<String, Object>();
//		allHash.put("one-row",false);
//		allHash.put("id",id);
//		allHash.put("names", getNames());
//		allHash.put("dataSeries", getList());
//		allHash.put("correlations", correlations);
//
//		this.dataHash = allHash;
//	}
//
//	public void setIncludesInstance(boolean includesInstance) {
//		this.includesInstance = includesInstance;
//	}
//	
//	public void setSkipAttributes(List<String> skipAttributes) {
//		this.skipAttributes = skipAttributes;
//	}
//
//	/////////////////////////////SWING DEPENDENT CODE/////////////////////////////
//	@Override
//	public void addPanel() {
//		if (jTab == null) {
//			super.addPanel();
//		} else {
//			String lastTabName = jTab.getTitleAt(jTab.getTabCount() - 1);
//			LOGGER.info("Parsing integer out of last tab name");
//			int count = 1;
//			if (jTab.getTabCount() > 1)
//				count = Integer.parseInt(lastTabName.substring(0, lastTabName.indexOf("."))) + 1;
//			addPanelAsTab(count + ". Correlation Viz Data");
//			addGridTab(count + ". Correlation Raw Data");
//		}
//	}
//
//	public void addGridTab(String tabName) {
//		table = new JTable();
//		GridScrollPane gsp = null;
//		gsp = new GridScrollPane(getNames(), getList());
//		gsp.addHorizontalScroll();
//		jTab.addTab(tabName, gsp);
//	}
//
//	public void addScrollPanel(JPanel panel, JComponent obj) {
//		JScrollPane scrollPane = new JScrollPane(obj);
//		scrollPane.getVerticalScrollBar().setUI(new NewScrollBarUI());
//		scrollPane.setAutoscrolls(true);
//
//		GridBagConstraints gbc_scrollPane = new GridBagConstraints();
//		gbc_scrollPane.fill = GridBagConstraints.BOTH;
//		gbc_scrollPane.gridx = 0;
//		gbc_scrollPane.gridy = 0;
//		panel.add(scrollPane, gbc_scrollPane);
//	}
//
//	public void setJTab(JTabbedPane jTab) {
//		this.jTab = jTab;
//	}
//
//	public void setJBar(JProgressBar jBar) {
//		this.jBar = jBar;
//	}
//}

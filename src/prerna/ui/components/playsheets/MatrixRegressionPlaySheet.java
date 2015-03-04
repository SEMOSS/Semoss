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
import java.util.Hashtable;

import javax.swing.JComponent;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;

import org.apache.commons.math3.stat.correlation.Covariance;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.teamdev.jxbrowser.chromium.JSValue;

import prerna.algorithm.learning.supervized.MatrixRegressionAlgorithm;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.ui.components.NewScrollBarUI;
import prerna.ui.main.listener.impl.GridPlaySheetListener;
import prerna.ui.main.listener.impl.JTableExcelExportListener;

public class MatrixRegressionPlaySheet extends GridPlaySheet{

	private static final Logger LOGGER = LogManager.getLogger(MatrixRegressionPlaySheet.class.getName());
	protected JTabbedPane jTab;
	private int bIndex = -1;
	private double[] coeffArray;
	private double standardError;
	
	@Override
	public void createData() {
		if(list==null)
			super.createData();
	}
	
	@Override
	public void runAnalytics() {
		
		int i;
		int j;
		int listNumRows = list.size();
		int listNumCols = names.length;
		int aNumCols = listNumCols - 2;//subtract out the title and the column where b initially was
		int outputNumCols = listNumCols+3; //add in the estimate and residuals and error range
		
		//the bIndex should have been provided. if not, will use the last column
		if(bIndex==-1)
			bIndex = listNumCols - 1;
		
		//create the b and A arrays which are used in matrix regression to determine coefficients
		double[] b = MatrixRegressionHelper.createB(list, bIndex);
		double[][] A = MatrixRegressionHelper.createA(list, bIndex);
		
		//run regression
		MatrixRegressionAlgorithm alg = new MatrixRegressionAlgorithm(A, b);
		alg.execute();
		coeffArray = alg.getCoeffArray();
		double[] coeffErrorsArray = alg.getCoeffErrorsArray();
		double[] estimateArray = alg.getEstimateArray();
		double[] residualArray = alg.getResidualArray();
		standardError = alg.getStandardError();

		//add in estimated and residuals for each row determined using coefficients
		ArrayList<Object []> outputList = new ArrayList<Object []>();
		for(i=0;i<listNumRows;i++) {
			double actualVal = b[i];
			double estimatedVal = estimateArray[i];
			double residualVal = residualArray[i];
			double withinErrorRange = Math.abs(residualVal / standardError);
			
			Object[] newRow = new Object[outputNumCols];
			newRow[0] = list.get(i)[0];
			for(j=0;j<aNumCols;j++) {
				newRow[j+1] = A[i][j];
			}
			newRow[j+1] = actualVal;
			newRow[j+2] = estimatedVal;
			newRow[j+3] = residualVal;
			newRow[j+4] = withinErrorRange;

			outputList.add(newRow);
		}
		
		//create a row with coefficient values
		Object[] coeffRow = new Object[outputNumCols];
		coeffRow[0] = "COEFFICIENTS";
		for(i=1;i<coeffArray.length;i++)
			coeffRow[i] = coeffArray[i];
		coeffRow[i] = "b is " + coeffArray[0];//first spot in the coefficient array is the constant value
		coeffRow[i+1] = "-";
		coeffRow[i+2] = "-";
		coeffRow[i+3] = "standard error of estimates is "+standardError;	
		outputList.add(0,coeffRow);
		
		//create a row with coefficient errors
		Object[] coeffErrorsRow = new Object[outputNumCols];
		coeffErrorsRow[0] = "COEFFICIENTS ERRORS";
		for(i=1;i<coeffErrorsArray.length;i++)
			coeffErrorsRow[i] = coeffErrorsArray[i];
		coeffErrorsRow[i] = "b error is " + coeffErrorsArray[0];//first spot in the coefficient array is the constant value
		coeffErrorsRow[i+1] = "-";
		coeffErrorsRow[i+2] = "-";
		coeffErrorsRow[i+3] = "-";	
		outputList.add(1,coeffErrorsRow);
		
		list = outputList;
		
		//update headers so that there are columns for estimated and residuals
		String[] namesWithEstimateAndResiduals = new String[outputNumCols];
		int newIndex = 0;
		for(i=0;i<listNumCols;i++) {
			if(i!=bIndex) {
				namesWithEstimateAndResiduals[newIndex]  = names[i];
				newIndex++;
			}
		}
		namesWithEstimateAndResiduals[newIndex]  = "Actual- " + names[bIndex];
		namesWithEstimateAndResiduals[newIndex+1] = "Estimated- " + names[bIndex];
		namesWithEstimateAndResiduals[newIndex+2] = "Residual- " + names[bIndex];
		namesWithEstimateAndResiduals[newIndex+3] = "Within Error Range?";

		names = namesWithEstimateAndResiduals;
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
			addPanelAsTab(count+". Linear Regression Raw Data");
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
			LOGGER.debug("Added new Regression Sheet");
			
		} catch (PropertyVetoException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public Object getVariable(String varName, ISelectStatement sjss){
		return sjss.getVar(varName);
	}
//	public void setMasterList(ArrayList<Object[]> masterList) {
//		this.masterList = masterList;
//	}
//	public void setMasterNames(String[] masterNames) {
//		this.masterNames = masterNames;
//	}
	public void setbColumnIndex(int bColumnIndex) {
		this.bIndex = bColumnIndex;
	}
	public void setJTab(JTabbedPane jTab) {
		this.jTab = jTab;
	}
	public void setJBar(JProgressBar jBar) {
		this.jBar = jBar;
	}
	public double[] getCoeffArray() {
		return coeffArray;
	}
	public double getStandardError() {
		return standardError;
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

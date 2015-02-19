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

import prerna.algorithm.cluster.MatrixRegressionAlgorithm;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.ui.components.NewScrollBarUI;
import prerna.ui.main.listener.impl.GridPlaySheetListener;
import prerna.ui.main.listener.impl.JTableExcelExportListener;

public class MatrixRegressionPlaySheet extends GridPlaySheet{
//	private ArrayList<Object[]> masterList;
//	private String[] masterNames;

	private static final Logger LOGGER = LogManager.getLogger(MatrixRegressionPlaySheet.class.getName());
	protected JTabbedPane jTab;
	private double[] b;
	private int bColumnIndex = -1;
	
	@Override
	public void createData() {
		if(list==null)
			super.createData();
	}
	
	@Override
	public void runAnalytics() {
		
		//assume first column is a string/identifier
		//if there is a b set, use it
		//otherwise if there is an index given for b, use it to create b
		//otherwise, use the last column to create b
		
		int numRows = list.size();
		int numCols = list.get(0).length;
		int i;
		int j;
		//create b if it doesnt exist
		if(b==null) {
			b = new double[numRows];
			
			//if a specified column for b given, use it. otherwise it is the last column
			if(bColumnIndex == -1)
				bColumnIndex = numCols-1;	

			for(i=0;i<numRows;i++)
				b[i] = (Double)list.get(i)[bColumnIndex];
			
		}
		
		//create A from the list
		int newNumCols;
		if(bColumnIndex==-1)
			newNumCols = numCols - 1;//subtract out string/identifier only since b created separately
		else
			newNumCols = numCols - 2;//subtract out string/identifier and b
			
		double[][] A = new double[numRows][newNumCols];
		for(i=0;i<numRows;i++) {
			Object[] oldRow = list.get(i);
			int newIndex = 1;
			for(j=1;j<numCols;j++) {
				if(j!=bColumnIndex) {
					A[i][newIndex-1] = (double)oldRow[j];
					newIndex++;
				}
			}
		}
		
		MatrixRegressionAlgorithm alg = new MatrixRegressionAlgorithm(A, b);
		alg.execute();
		
		double[] coeffArray = alg.getCoeffArray();
		Object[] coeffRow = new Object[coeffArray.length+2];
		int oldIndex = 0;
		for(i=0;i<coeffRow.length;i++) {
			if(i==0) {
				coeffRow[i] = "COEFFICIENTS";
			}else if(i==bColumnIndex) {
				coeffRow[i] = "-";
			}else if(i!=bColumnIndex) {
				coeffRow[i] = coeffArray[oldIndex];
				oldIndex++;
			}
		}
		list.add(0,coeffRow);

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
			addPanelAsTab(count+". Matrix Regression");
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
		this.bColumnIndex = bColumnIndex;
	}
	public void setbArray(double[] b) {
		this.b = b;
	}
	public void setJTab(JTabbedPane jTab) {
		this.jTab = jTab;
	}
	public void setJBar(JProgressBar jBar) {
		this.jBar = jBar;
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

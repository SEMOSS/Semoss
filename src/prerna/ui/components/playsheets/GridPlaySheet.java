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
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.beans.PropertyVetoException;
import java.util.List;

import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.GridFilterData;
import prerna.ui.components.GridRAWTableModel;
import prerna.ui.components.GridTableModel;
import prerna.ui.components.NewHoriScrollBarUI;
import prerna.ui.components.NewScrollBarUI;
import prerna.ui.main.listener.impl.GridPlaySheetListener;
import prerna.ui.main.listener.impl.JTableExcelExportListener;

/**
 * The GridPlaySheet class creates the panel and table for a grid view of data from a SPARQL query.
 */
@SuppressWarnings("serial")
public class GridPlaySheet extends BasicProcessingPlaySheet{
	
	Boolean horizontalScrollBar = false;
	
	/**
	 * Method addPanel.  Creates a panel and adds the table to the panel.
	 */
	private static final Logger logger = LogManager.getLogger(GridPlaySheet.class.getName());
	
	@Override
	public void addPanel()
	{
		setWindow();
		try {
			table = new JTable();
			
			//Add Excel export popup menu and menuitem
			JPopupMenu popupMenu = new JPopupMenu();
			JMenuItem menuItemAdd = new JMenuItem("Export to Excel");
			String questionTitle = this.getTitle();
			menuItemAdd.addActionListener(new JTableExcelExportListener(table, questionTitle));
			popupMenu.add(menuItemAdd);
			table.setComponentPopupMenu(popupMenu);
			
			JPanel mainPanel = new JPanel();
			GridPlaySheetListener gridPSListener = new GridPlaySheetListener();
			logger.debug("Created the table");
			this.addInternalFrameListener(gridPSListener);
			logger.debug("Added the internal frame listener ");
			//table.setAutoCreateRowSorter(true);
			this.setContentPane(mainPanel);
			GridBagLayout gbl_mainPanel = new GridBagLayout();
			gbl_mainPanel.columnWidths = new int[]{0, 0};
			gbl_mainPanel.rowHeights = new int[]{0, 0};
			gbl_mainPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
			gbl_mainPanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
			mainPanel.setLayout(gbl_mainPanel);

			addScrollPanel(mainPanel);
			
			updateProgressBar("0%...Preprocessing", 0);
			resetProgressBar();
			JPanel barPanel = new JPanel();
			
			GridBagConstraints gbc_barPanel = new GridBagConstraints();
			gbc_barPanel.fill = GridBagConstraints.BOTH;
			gbc_barPanel.gridx = 0;
			gbc_barPanel.gridy = 1;
			mainPanel.add(barPanel, gbc_barPanel);
			barPanel.setLayout(new BorderLayout(0, 0));
			barPanel.add(jBar, BorderLayout.CENTER);

			pane.add(this);
			
			this.pack();
			this.setVisible(true);
			this.setSelected(true);
			this.show();
			
			logger.debug("Added the main pane");
		} catch (PropertyVetoException e) {
			e.printStackTrace();
		}
	}
	
	public void addScrollPanel(JPanel mainPanel) {
		JScrollPane scrollPane = new JScrollPane(table);
		scrollPane.getVerticalScrollBar().setUI(new NewScrollBarUI());
		if(horizontalScrollBar) {
			scrollPane.getHorizontalScrollBar().setUI(new NewHoriScrollBarUI());
			table.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
		}
		scrollPane.setAutoscrolls(true);
		
		GridBagConstraints gbc_scrollPane = new GridBagConstraints();
		gbc_scrollPane.fill = GridBagConstraints.BOTH;
		gbc_scrollPane.gridx = 0;
		gbc_scrollPane.gridy = 0;
		mainPanel.add(scrollPane, gbc_scrollPane);
	}
	
	@Override
	public GridRAWTableModel setGridModel(GridFilterData gfd) {
		GridTableModel model = new GridTableModel(gfd);
		return model;
	}
	
	@Override
	public List<Object[]> getTabularData() {
		return dataFrame.getData();
	}
	
	public void setHorizontalScrollBar(Boolean horizontalScrollBar) {
		this.horizontalScrollBar = horizontalScrollBar;
	}
	
}

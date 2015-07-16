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
package prerna.ui.components;

import java.awt.Dimension;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.SwingConstants;
import javax.swing.table.DefaultTableCellRenderer;

/**
 * This class is used for the scroll bar functionality for the grid display.
 * Sets the UI for the scrollbar.
 */
public class GridScrollPane extends JScrollPane{
	
	private JTable table;
	/**
	 * Constructor for GridScrollPane.
	 * @param colNames 	List of column names.
	 * @param list 		List of data.
	 */
	public GridScrollPane(String[] colNames, List<Object []> list)
	{
		GridFilterData gfd = new GridFilterData();
		gfd.setColumnNames(colNames);
		gfd.setDataList(list);
		table = new JTable();
//		table.setAutoCreateRowSorter(true);

		GridTableModel model = new GridTableModel(gfd);
		table.setModel(model);
		table.setRowSorter(new GridTableRowSorter(model));
		this.setViewportView(table);
		this.setAutoscrolls(true);
		this.getVerticalScrollBar().setUI(new NewScrollBarUI());
		
	}
		
	public void rightAlignColumn(int col) {
		DefaultTableCellRenderer rightRenderer = new DefaultTableCellRenderer();
		rightRenderer.setHorizontalAlignment(SwingConstants.RIGHT);
		table.getColumnModel().getColumn(col).setCellRenderer(rightRenderer);
	}
	
	/**
	 * Creates a table.
	 * @param colNames 	List of column names.
	 * @param list 		List of data.
	 */
	public void createTable(String[] colNames, ArrayList <Object []> list)
	{
		
	}
	
	public void addHorizontalScroll()
	{
		table.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
		this.getHorizontalScrollBar().setUI(new NewHoriScrollBarUI());
		this.setPreferredSize(new Dimension(200, this.getPreferredSize().height));
	}

}

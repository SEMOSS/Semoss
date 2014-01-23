/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.ui.components;

import java.util.ArrayList;

import javax.swing.JScrollPane;
import javax.swing.JTable;

/**
 * This class is used for the scroll bar functionality for the grid display.
 * Sets the UI for the scrollbar.
 */
public class GridScrollPane extends JScrollPane{
	
	
	/**
	 * Constructor for GridScrollPane.
	 * @param colNames 	List of column names.
	 * @param list 		List of data.
	 */
	public GridScrollPane(String[] colNames, ArrayList <Object []> list)
	{
		GridFilterData gfd = new GridFilterData();
		gfd.setColumnNames(colNames);
		gfd.setDataList(list);
		JTable table = new JTable();
		table.setAutoCreateRowSorter(true);
		GridTableModel model = new GridTableModel(gfd);
		table.setModel(model);
		this.setViewportView(table);
		this.setAutoscrolls(true);
		this.getVerticalScrollBar().setUI(new NewScrollBarUI());
	}
	
	/**
	 * Creates a table.
	 * @param colNames 	List of column names.
	 * @param list 		List of data.
	 */
	public void createTable(String[] colNames, ArrayList <Object []> list)
	{
		
	}

}

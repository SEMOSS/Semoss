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

import javax.swing.table.AbstractTableModel;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


/**
 * This class is used to adjust the edge values in a table.
 */
public class EdgeAdjusterTableModel extends AbstractTableModel {
	
	VertexFilterData data = null;
	static final Logger logger = LogManager.getLogger(EdgeAdjusterTableModel.class.getName());

	/**
	 * Constructor for EdgeAdjusterTableModel.
	 * @param data VertexFilterData
	 */
	public EdgeAdjusterTableModel(VertexFilterData data)
	{
		//super(data.getRows(), data.columnNames);
		this.data = data;
	}
	/**
	 * Gets the column count.
	
	 * @return int 	Column count. */
	@Override
	public int getColumnCount() {
		return 2;
	}
	
	/**
	 * Sets the vertex filter data.
	 * @param data 	Data to be set.
	 */
	public void setVertexFilterData(VertexFilterData data)
	{
		this.data = data;
	}

	/**
	 * Gets the column name of a particular column index.
	 * @param index 	Column index.
	
	 * @return String 	Column name. */
	public String getColumnName(int index)
	{
		return this.data.edgeTypeNames[index];
	}

	/**
	 * Counts the number of rows.
	
	 * @return int	Row count. */
	@Override
	public int getRowCount() {
		return data.edgeTypes.length;
	}

	/**
	 * Returns the value for the cell at a specified row and column index.
	 * @param arg0 	Row index.
	 * @param arg1 	Column index.
	
	 * @return Object 	Edge adjust value. */
	@Override
	public Object getValueAt(int arg0, int arg1) {
		// get the value first
		return data.getEdgeAdjustValueAt(arg0, arg1);
	}
	
	/**
	 * Get column class.
	 * @param column 	Column.
	
	 * @return Class	Column class. */
	public Class getColumnClass(int column)
	{
		if(column == 0)
			return String.class;
		else
			return Double.class;
	}

	/**
	 * Checks whether a cell can be edited.
	 * @param row 			Row number.
	 * @param column 		Column number.
	
	 * @return boolean		True if the cell  */
	public boolean isCellEditable(int row, int column)
	{
		if(column == 0)
			return false;
		else
			return true;
	}
		
	/**
	 * Sets the edge value at a particular row and column location.
	 * @param value 	Edge value.
	 * @param row 		Row number.
	 * @param column 	Column number.
	 */
	public void setValueAt(Object value, int row, int column)
	{
		data.setEdgeAdjustValueAt(value, row, column);
		fireTableDataChanged();
	}
}

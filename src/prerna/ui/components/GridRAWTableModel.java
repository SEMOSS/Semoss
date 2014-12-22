/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.ui.components;

import javax.swing.table.AbstractTableModel;

import com.bigdata.rdf.model.BigdataURIImpl;

import prerna.util.Utility;


/**
 * This class is used to create a table model for raw grids.
 */
public class GridRAWTableModel extends AbstractTableModel {
	
	GridFilterData data = null;

	/**
	 * Constructor for GridTableModel.
	 * @param data GridFilterData
	 */
	public GridRAWTableModel(GridFilterData data)
	{
		//super(data.getRows(), data.columnNames);
		this.data = data;
	}
	/**
	 * Returns the column count from the grid filter data.
	
	 * @return int 	Column count. */
	@Override
	public int getColumnCount() {
		return this.data.columnNames.length;
	}
	
	/**
	 * Sets the grid filter data.
	 * @param data GridFilterData
	 */
	public void setGridFilterData(GridFilterData data)
	{
		this.data = data;
	}
	
	/**
	 * Returns the column name from a particular index.
	 * @param index 	Column index.
	
	 * @return String 	Column name. */
	public String getColumnName(int index)
	{
		return this.data.columnNames[index];
	}

	/**
	 * Gets the row count from the grid filter data.
	
	 * @return int 	Row count. */
	@Override
	public int getRowCount() {
		return data.getNumRows();
	}

	/**
	 * Gets the value from a specific row and column index.
	 * @param arg0 	Row index.
	 * @param arg1 	Column index.
	
	 * @return Object Value. */
	@Override
	public Object getValueAt(int arg0, int arg1) {
		// get the value first
		return data.getValueAt(arg0, arg1);
	}
	
	/**
	 * Sets the value at a particular x and y coordinate.
	 * @param x 	X-coordinate.
	 * @param y 	Y-coordinate.
	 */
	public void setValueAt(int x, int y)
	{
		// sets the value
	}
	
	/**
	 * Gets the column class at a particular index.
	 * @param column 	Column index.
	
	 * @return Class 	Class. */
	@Override
	public Class getColumnClass(int column)
	{
		//Need to return everything as object so the built-in swing table renderers do not break with cast exceptions
		//Up to the row sorter to handle all objects and sort accordingly
		return Object.class;
//		Class returnValue = null;
//		if ((column >= 0) && (column < getColumnCount())) {
//
//			boolean exit = false;
//			int rowCount = 0;
//			while (!exit && rowCount<getRowCount())
//			{
//				if (getValueAt(rowCount, column)!=null)
//				{
//					exit = true;
//					returnValue = getValueAt(rowCount, column).getClass();
//				}
//				rowCount++;
//			}
//			if (!exit)
//			{
//				returnValue = String.class;
//			}
//			
//		} 
//		else {
//			returnValue = Object.class;
//		}
//		return returnValue;
	}
	/**
	 * Checks whether the cell is editable at a particular row and column index.
	 * @param row 		Row index.
	 * @param column 	Column index.
	
	 * @return boolean 	True if cell is editable. */
	public boolean isCellEditable(int row, int column)
	{
		return true;
	}
		
	/**
	 * Sets the cell value at a particular row and column location.
	 * @param value 	Cell value.
	 * @param row 		Row index.
	 * @param column 	Column index.
	 */
	public void setValueAt(Object value, int row, int column)
	{
		data.setValueAt(value, row, column);
		fireTableDataChanged();
	}
	
}

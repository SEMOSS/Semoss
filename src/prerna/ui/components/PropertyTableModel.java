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

import javax.swing.JTextField;
import javax.swing.table.AbstractTableModel;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.util.Constants;
import prerna.util.DIHelper;


/**
 * This class is used to create a table model for property specific data.
 */
public class PropertyTableModel extends AbstractTableModel {
	
	PropertySpecData data = null;
	static final Logger logger = LogManager.getLogger(PropertyTableModel.class.getName());
	String uriVal = Constants.PROP_URI;
	String uriVal2 = Constants.PREDICATE_URI;

	/**
	 * Constructor for PropertyTableModel.
	 * @param data PropertySpecData
	 */
	public PropertyTableModel(PropertySpecData data)
	{
		this.data = data;
	}
	
	/**
	 * Returns the column count.
	
	 * @return int 	Column count. */
	@Override
	public int getColumnCount() {
		return data.columnNames.length;
	}
	
	/**
	 * Sets the control data.
	 * @param data PropertySpecData
	 */
	public void setControlData(PropertySpecData data)
	{
		this.data = data;
	}

	/**
	 * Gets the column name at a particular index.
	 * @param index 	Column index.
	
	 * @return String 	Column name. */
	public String getColumnName(int index)
	{
		return this.data.columnNames[index];
	}

	/**
	 * Returns the row count.
	
	 * @return int 	Row count. */
	@Override
	public int getRowCount() {
		return data.getNumRows();
	}

	/**
	 * Gets the cell value at a particular row and column index.
	 * @param arg0 		Row index.
	 * @param arg1 		Column index.
	
	 * @return Object 	Cell value. */
	@Override
	public Object getValueAt(int arg0, int arg1) {
		// get the value first
		return data.getValueAt(arg0, arg1);
	}
	
	/**
	 * Gets the column class at a particular index.
	 * @param column 	Column index.
	
	 * @return Class 	Column class. */
	public Class getColumnClass(int column)
	{
		//logger.debug("Getting clolumn " + column);
		Object val = data.dataList[0][column];
		return val.getClass();
	}

	/**
	 * Checks whether the cell at a particular row and column index is editable.
	 * @param row 		Row index.
	 * @param column 	Column index.
	
	 * @return boolean 	True if cell is editable. */
	public boolean isCellEditable(int row, int column)
	{
		if(column >= 2)
			return true;
		else
			return false;
	}
		
	/**
	 * Sets the label value at a particular row and column index.
	 * @param value 	Label value.
	 * @param row 		Row index.
	 * @param column 	Column index.
	 */
	public void setValueAt(Object value, int row, int column)
	{
		logger.debug("Calling the edge filter set value at");
		//data.setValueAt2(uriVal2, value, row, column);
		data.setValueAt(uriVal, value, row, column);
				
		fireTableDataChanged();
		
		// need to figure out a better way to do this
		JTextField field = (JTextField)DIHelper.getInstance().getLocalProp(Constants.DATA_PROP_STRING);
		field.setText(DIHelper.getInstance().getProperty(Constants.PROP_URI));
		JTextField field2 = (JTextField)DIHelper.getInstance().getLocalProp(Constants.OBJECT_PROP_STRING);
		field.setText(DIHelper.getInstance().getProperty(Constants.PREDICATE_URI));

	}	
}

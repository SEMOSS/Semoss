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

import javax.swing.JTextField;
import javax.swing.table.AbstractTableModel;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.util.Constants;
import prerna.util.DIHelper;


/**
 * This class is used to create a table model for property specific data.
 */
public class OPropertyTableModel extends AbstractTableModel {
	
	PropertySpecData data = null;
	static final Logger logger = LogManager.getLogger(OPropertyTableModel.class.getName());
	String uriVal = Constants.PREDICATE_URI;

	/**
	 * Constructor for OPropertyTableModel.
	 * @param data PropertySpecData
	 */
	public OPropertyTableModel(PropertySpecData data)
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
		return this.data.columnNames2[index];
	}

	/**
	 * Returns the row count.
	
	 * @return int 	Row count. */
	@Override
	public int getRowCount() {
		//return data.getPredicateRows();
		return 0;
	}

	/**
	 * Gets the cell value at a particular row and column index.
	 * @param arg0 		Row index.
	 * @param arg1 		Column index.
	
	 * @return Object 	Cell value. */
	@Override
	public Object getValueAt(int arg0, int arg1) {
		// get the value first
		//return data.getValueAt2(arg0, arg1);
		return null;
	}
	
	/**
	 * Gets the column class at a particular index.
	 * @param column 	Column index.
	
	 * @return Class 	Column class. */
	public Class getColumnClass(int column)
	{
		//logger.debug("Getting clolumn " + column);
		Object val = data.dataList2[0][column];
		return val.getClass();
	}

	/**
	 * Checks whether the cell at a particular row and column index is editable.
	 * @param row 		Row index.
	 * @param column 	Column index.
	
	 * @return boolean 	True if cell is editable. */
	public boolean isCellEditable(int row, int column)
	{
		if(column == 1 || column == 2)
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
		//data.setValueAt2(uriVal, value, row, column);
		fireTableDataChanged();
		
		// need to figure out a better way to do this
		JTextField field = (JTextField)DIHelper.getInstance().getLocalProp(Constants.OBJECT_PROP_STRING);
		field.setText(DIHelper.getInstance().getProperty(Constants.PREDICATE_URI));

	}
	
}

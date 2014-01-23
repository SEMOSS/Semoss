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

import javax.swing.table.AbstractTableModel;

import org.apache.log4j.Logger;


/**
 * This class is used to create a table model for shapes and colors of vertices.
 */
public class ShapeColorTableModel extends AbstractTableModel {
	
	VertexColorShapeData data = null;
	Logger logger = Logger.getLogger(getClass());

	/**
	 * Constructor for ShapeColorTableModel.
	 * @param data VertexColorShapeData
	 */
	public ShapeColorTableModel(VertexColorShapeData data)
	{
		this.data = data;
	}
	
	/**
	 * Returns the column count.
	
	 * @return int 	Column count. */
	@Override
	public int getColumnCount() {
		return data.scColumnNames.length;
	}
	
	/**
	 * Gets the column name at a particular index.
	 * @param index 	Column index.
	
	 * @return String 	Column name. */
	public String getColumnName(int index)
	{
		return data.scColumnNames[index] + "";
	}

	/**
	 * Returns the row count.
	
	 * @return int 	Row count. */
	@Override
	public int getRowCount() {
		return data.count;
	}

	/**
	 * Gets the cell value at a particular row and column index.
	 * @param arg0 		Row index.
	 * @param arg1 		Column index.
	
	 * @return Object 	Cell value. */
	@Override
	public Object getValueAt(int row, int column) {
		
		return data.getValueAt(row, column);
	}
	
	/**
	 * Gets the column class at a particular index.
	 * @param column 	Column index.
	
	 * @return Class 	Column class. */
	public Class getColumnClass(int column)
	{
		return data.shapeColorRows[0][column].getClass();
	}

	/**
	 * Checks whether the cell at a particular row and column index is editable.
	 * @param row 		Row index.
	 * @param column 	Column index.
	
	 * @return boolean 	True if cell is editable. */
	public boolean isCellEditable(int row, int column)
	{
		if(column == 2 || column == 3)
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
		logger.debug("Calling the shape color value set");
		data.setValueAt(value, row, column);
	}
	
}

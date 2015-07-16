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

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


/**
 * This class is the primary ones used for vertex filtering.
 */
public class GridFilterData {
	
	// need to have vertex and type information
	// everytime a vertex is added here
	// need to figure out a type so that it can show
	// the types are not needed after or may be it is
	// we need a structure which keeps types with vector
	// the vector will have all of the vertex specific to the type
	// additionally, there needs to be another structure so that when I select or deselect something
	// it marks it on the matrix
	// need to come back and solve this one
	Hashtable <String, Vector> typeHash = new Hashtable<String, Vector>();
	public String [] columnNames = null;
	
	public List<Object []> dataList = null;
	
	static final Logger logger = LogManager.getLogger(GridFilterData.class.getName());
	
	/**
	 * Gets the value at a particular row and column index.
	 * @param row 		Row index.
	 * @param column 	Column index.
	
	 * @return Object 	Cell value. */
	public Object getValueAt(int row, int column)
	{
		Object [] val = dataList.get(row);
		Object retVal = val[column];
		if(column == 0)
			logger.debug(row + "<>" + column + "<>" + retVal);
		return retVal;
	}
	
	/**
	 * Sets the data list.
	 * @param tabularData 	List of data.
	 */
	public void setDataList(List<Object[]> tabularData)
	{
		this.dataList = tabularData;
	}
	
	/**
	 * Sets the column names.
	 * @param columnNames 	Column names.
	 */
	public void setColumnNames(String [] columnNames)
	{
		this.columnNames = columnNames;
	}
	
	/**
	 * Gets the number of rows from the data.
	
	 * @return int 	Number of rows. */
	public int getNumRows()
	{
		// use this call to convert the thing to array
		return dataList.size();
	}
	
	/**
	 * Method setValueAt.
	 //TODO: method never called
	 * @param value Object
	 * @param row int
	 * @param column int
	 */
	public void setValueAt(Object value, int row, int column)
	{
		// ignore this should never be called
	}
}

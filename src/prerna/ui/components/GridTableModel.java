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

import prerna.util.Utility;

import com.bigdata.rdf.model.BigdataURIImpl;


/**
 * This class is used to create a table model for grids.
 */
public class GridTableModel extends GridRAWTableModel {
	
	public GridTableModel(GridFilterData data) {
		super(data);
		// TODO Auto-generated constructor stub
	}

	/**
	 * Gets the value from a specific row and column index.
	 * @param arg0 	Row index.
	 * @param arg1 	Column index.
	
	 * @return Object Value. */
	@Override
	public Object getValueAt(int arg0, int arg1) {
		// get the value first
		Object val = data.getValueAt(arg0, arg1);
		if (val instanceof BigdataURIImpl) {
			val = Utility.getInstanceName(val.toString());
		}
		return val;
	}
}

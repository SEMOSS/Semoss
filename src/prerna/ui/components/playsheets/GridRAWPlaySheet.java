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
package prerna.ui.components.playsheets;

import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.ui.components.GridFilterData;
import prerna.ui.components.GridRAWTableModel;
import prerna.ui.components.GridTableModel;

/**
 */
public class GridRAWPlaySheet extends GridPlaySheet {
	
	/**
	 * Method getVariable. Gets the variable names from the query results.
	 * @param varName String - the variable name.
	 * @param sjss SesameJenaSelectStatement - the associated sesame jena select statement.
	
	 * @return Object - results with given URI.*/
	@Override
	public Object getVariable(String varName, SesameJenaSelectStatement sjss){
		return sjss.getRawVar(varName).toString();
	}
	
	@Override
	public GridRAWTableModel setGridModel(GridFilterData gfd) {
		GridRAWTableModel model = new GridRAWTableModel(gfd);
		return model;
	}
}

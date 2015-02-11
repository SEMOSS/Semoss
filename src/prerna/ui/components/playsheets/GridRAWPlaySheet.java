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
package prerna.ui.components.playsheets;

import prerna.rdf.engine.api.ISelectStatement;
import prerna.ui.components.GridFilterData;
import prerna.ui.components.GridRAWTableModel;

/**
 */
public class GridRAWPlaySheet extends GridPlaySheet {
	
	/**
	 * Method getVariable. Gets the variable names from the query results.
	 * @param varName String - the variable name.
	 * @param sjss SesameJenaSelectStatement - the associated sesame jena select statement.
	
	 * @return Object - results with given URI.*/
	@Override
	public Object getVariable(String varName, ISelectStatement sjss){
		return sjss.getRawVar(varName).toString();
	}
	
	@Override
	public GridRAWTableModel setGridModel(GridFilterData gfd) {
		GridRAWTableModel model = new GridRAWTableModel(gfd);
		return model;
	}
}

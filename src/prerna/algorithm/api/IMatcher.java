/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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
package prerna.algorithm.api;

import java.util.List;
import java.util.Map;

import prerna.engine.api.IEngine;
import prerna.om.SEMOSSParam;

public interface IMatcher {
	
	//TODO: delete this class and modify abstract table data frame interface
	
	enum MATCHER_ACTION {BIND, REGEX, NONE}
	
	/**
	 * This method determines how the query that ultimately feeds the join using the matcher gets modified
	 * For example, my exact string matcher will always use BIND because the matcher needs an exact match anyway
	 * A wordnet matcher, however, will use NONE because the query cannot subselect what will match--the matcher will have to do all the work
	 * @return	How to add the list to the query to prefilter future results
	 */
	MATCHER_ACTION getQueryModType();
	
	List<Object> getQueryModList(ITableDataFrame dm, String columnNameInDM, IEngine engine, String columnNameInNewEngine);
	
	/**
	 * Get the name of the algorithm
	 * @return
	 */
	String getName();
	
	/**
	 * Get the description of the algorithm output
	 * @return
	 */
	String	getResultDescription();
	
	/**
	 * Set the options for the analytic routines
	 * @param options			A mappings of the option type and their values
	 */
	void setSelectedOptions(Map<String, Object> selected);
	
	/**
	 * Get the list of parameter options used for the analytic routine
	 * @return					The list of parameters required for the analytic routine
	 */
	List<SEMOSSParam> getOptions();
	
	/**
	 * Perform an algorithm on a data-frame. The routine does not necessarily have to 
	 * alter/modify the existing data-frame
	 * @param data				An array of data-frame containing the input data for the analytical routine
	 * @return					The resulting data-frame as a result of the analytical routine
	 */
//	List<TreeNode[]> runAlgorithm(ITableDataFrame... data);
	
	/**
	 * Get the default visualization type for the algorithm output
	 * @return
	 */
	String getDefaultViz();
	
	/**
	 * Get the list of the columns that have been altered as a result of the algorithm
	 * This will return null when no columns have been changed
	 * @return
	 */
	List<String> getChangedColumns();
	
	/**
	 * Get the meta-data for the results of the algorithm 
	 * @return
	 */
	Map<String, Object> getResultMetadata();
	
}

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

import prerna.algorithm.impl.SearchMasterDB;

/**
 * The SearchMasterDBPlaySheet class is used to test the Search feature for the MasterDB.
 */
@SuppressWarnings("serial")
public class SearchMasterDBPlaySheet extends GridPlaySheet{
	
	/**
	 * Method createData.  Creates the data needed to be printout in the grid.
	 */
	@Override
	public void createData() {
		SearchMasterDB searchAlgo = new SearchMasterDB();
		if(query.equals("true"))
			searchAlgo.setCountBoolean(true);
		else
			searchAlgo.setCountBoolean(false);
		list = searchAlgo.searchDB();
		names = searchAlgo.headers;
	}
}

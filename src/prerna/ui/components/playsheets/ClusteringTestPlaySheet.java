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

import prerna.algorithm.impl.ClusteringAlgorithm;
import prerna.error.BadInputException;
import prerna.util.Utility;

/**
 * The GridPlaySheet class creates the panel and table for a grid view of data from a SPARQL query.
 */
@SuppressWarnings("serial")
public class ClusteringTestPlaySheet extends GridPlaySheet{
	

	@Override
	public void createData() {
		super.createData();
		try{
			ClusteringAlgorithm clusterAlg = new ClusteringAlgorithm(list,names);
			clusterAlg.setNumClusters(10);
			clusterAlg.execute();
		}catch(BadInputException e) {
			e.printStackTrace();
			Utility.showError(e.getMessage());
		}
		
	}

}

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

import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.cluster.ClusteringAlgorithm;

/**
 * The GridPlaySheet class creates the panel and table for a grid view of data from a SPARQL query.
 */
@SuppressWarnings("serial")
public class ClusteringPlaySheet extends GridPlaySheet{

	private static final Logger logger = LogManager.getLogger(ClusteringPlaySheet.class.getName());
	private int numClusters;

	@Override
	public void createData() {
		super.createData();
		ClusteringAlgorithm clusterAlg = new ClusteringAlgorithm(list,names);
		clusterAlg.setNumClusters(numClusters);
		clusterAlg.execute();
		int[] clusterAssigned = clusterAlg.getClustersAssigned();
		Hashtable<String, Integer> instanceIndexHash = clusterAlg.getInstanceIndexHash();
		ArrayList<Object[]> newList = new ArrayList<Object[]>();
		for(Object[] dataRow : list) {
			Object[] newDataRow = new Object[dataRow.length + 1];
			String instance = "";
			for(int i = 0; i < dataRow.length; i++) {
				if(i == 0) {
					instance = dataRow[i].toString();
				}
				newDataRow[i] = dataRow[i];
			}
			int clusterNumber = clusterAssigned[instanceIndexHash.get(instance)];
			newDataRow[dataRow.length] = clusterNumber;
			newList.add(newDataRow);
		}
		list = newList;
		String[] newNames = new String[names.length + 1];
		for(int i = 0; i < names.length; i++) {
			newNames[i] = names[i];
		}
		newNames[names.length] = "ClusterID";
		names = newNames;

		list.addAll(0,clusterAlg.getSummaryClusterRows());
	}


	/**
	 * Sets the string version of the SPARQL query on the playsheet.
	 * Pulls out the number of clusters and stores them in the numClusters
	 * @param query String
	 */
	@Override
	public void setQuery(String query) {
		if(query.startsWith("SELECT")||query.startsWith("CONSTRUCT"))
			this.query=query;
		else{
			logger.info("New Query " + query);
			int semi=query.indexOf(";");
			numClusters = Integer.parseInt(query.substring(0,semi));
			this.query = query.substring(semi+1);
		}
	}
}

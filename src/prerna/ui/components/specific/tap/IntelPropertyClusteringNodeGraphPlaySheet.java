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
package prerna.ui.components.specific.tap;

import java.util.Enumeration;
import java.util.Hashtable;

import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * This is the graph playsheet for intel property clustering on nodes.
 */
public class IntelPropertyClusteringNodeGraphPlaySheet extends GraphPlaySheet {
	/**
	 * Constructor for IntelPropertyClusteringNodeGraphPlaySheet.
	 */
	public IntelPropertyClusteringNodeGraphPlaySheet() {
		super();
	}

	/**
	 * Creates the forest. Sets the property to cluster as well as the range of values for it.
	 */
	@Override
	public void createForest() {
		super.createForest();
		Hashtable<String, SEMOSSVertex> myVertStore = this.getGraphData().getVertStore();
		Hashtable<String, SEMOSSEdge> myEdgeStore = this.getGraphData().getEdgeStore();
		Enumeration keyList = myVertStore.keys();
		String questionId = this.questionNum;
		String propToCluster = DIHelper.getInstance().getProperty(questionId + "_" + "CLUSTER_PROP");
		String propRangeVal = DIHelper.getInstance().getProperty(questionId + "_" + "CLUSTER_RANGES");
		String[] propRanges = new String[0];
		if(propRangeVal != null) {
			propRanges = propRangeVal.split("-");
		}

		if(myEdgeStore.keys().hasMoreElements())
		{
			while(keyList.hasMoreElements()) {
				String currKey = (String) keyList.nextElement();
				SEMOSSVertex vert1 = myVertStore.get(currKey);
				SEMOSSVertex vert2 = null;
				SEMOSSEdge edge = null;
				String propRangeURI ="http://health.mil/ontologies/dbcm/Concept/" + propToCluster + "_Range/";
				String predicate = "http://health.mil/ontologies/dbcm/Relation/Contains/";

				if(vert1.getProperty(Constants.VERTEX_TYPE).toString().equals("City") || vert1.getProperty(Constants.VERTEX_TYPE).toString().equals("State"))
				{
					if(vert1.getProperty(propToCluster) != null)
					{
						String propValue = vert1.getProperty(propToCluster).toString();
						if(propValue == null || propValue.isEmpty()) {
							continue;
						}
						
						for(int i = 0; i < propRanges.length; i++) {
							if(Double.parseDouble(propValue) > Double.parseDouble(propRanges[i])) {
								if(i == propRanges.length-1) {
									propRangeURI += "Range: >" + Double.parseDouble(propRanges[i]);
								}
								continue;
							} else {
								if(i == 0) {
									propRangeURI += "Range: 0-" + Double.parseDouble(propRanges[i]);
									break;
								}
								propRangeURI += "Range: " + Double.parseDouble(propRanges[i-1]) + "-" + Double.parseDouble(propRanges[i]);
								break;
							}
						}
					}
					else {
						propRangeURI+="TBD";
					}

					vert2=myVertStore.get(propRangeURI);
					if(vert2==null)
					{
						vert2 = new SEMOSSVertex(propRangeURI);
						filterData.addVertex(vert2);
					}

					myVertStore.put(propRangeURI, vert2);
					predicate += vert1.getProperty(Constants.VERTEX_NAME) + ":" + vert2.getProperty(Constants.VERTEX_NAME);
					edge = new SEMOSSEdge(vert1, vert2, predicate);
					myEdgeStore.put(predicate, edge);
					this.forest.addEdge(edge,vert1,vert2);
//					genControlData(vert2);
//					genControlData(edge);
				}
			}
		}

//		genBaseConcepts();
//		genBaseGraph();
//		genAllData();
	}

	/**
	 * Sets the string version of the SPARQL query on the playsheet. 
	 * @param query String
	 */
	@Override
	public void setQuery(String query) {
		this.query = query;
	}
}

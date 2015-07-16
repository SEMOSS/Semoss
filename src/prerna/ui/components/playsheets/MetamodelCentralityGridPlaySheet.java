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
package prerna.ui.components.playsheets;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.impl.CentralityCalculator;
import prerna.algorithm.impl.PageRankCalculator;
import prerna.algorithm.impl.SubclassingMapGenerator;
import prerna.ds.BTreeDataFrame;
import prerna.engine.impl.AbstractEngine;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.util.Constants;
import edu.uci.ics.jung.graph.DelegateForest;

@SuppressWarnings("serial")
public class MetamodelCentralityGridPlaySheet extends GridPlaySheet {

	private static final Logger logger = LogManager.getLogger(MetamodelCentralityGridPlaySheet.class.getName());
		
	@Override
	public void createData() {
		GraphPlaySheet graphPS = CentralityCalculator.createMetamodel(((AbstractEngine)engine).getBaseDataEngine().getRc(), query);

		Hashtable<String, SEMOSSVertex> vertStore  = graphPS.getGraphData().getVertStore();
		Hashtable<String, SEMOSSEdge> edgeStore = graphPS.getGraphData().getEdgeStore();

		SubclassingMapGenerator subclassGen = new SubclassingMapGenerator();
		subclassGen.processSubclassing(engine);
		subclassGen.updateVertAndEdgeStoreForSubclassing(vertStore, edgeStore);

		vertStore = subclassGen.getVertStore();
		edgeStore = subclassGen.getEdgeStore();
		
		String[] names = new String[]{"Type","Undirected Closeness Centrality","Undirected Betweeness Centrality","Undirected Eccentricity Centrality","Undirected Page Rank"};
		dataFrame = new BTreeDataFrame(names);
		
		Hashtable<SEMOSSVertex, Double> unDirCloseness = CentralityCalculator.calculateCloseness(vertStore, false);
		Hashtable<SEMOSSVertex, Double> unDirBetweenness = CentralityCalculator.calculateBetweenness(vertStore, false);
		Hashtable<SEMOSSVertex, Double> unDirEccentricity = CentralityCalculator.calculateEccentricity(vertStore,false);
		
		DelegateForest<SEMOSSVertex,SEMOSSEdge> forest = subclassGen.updateForest(graphPS.forest);
		forest = CentralityCalculator.makeForestUndirected(edgeStore, forest);
		PageRankCalculator pCalc = new PageRankCalculator();
		Hashtable<SEMOSSVertex, Double> ranks = pCalc.calculatePageRank(forest);
		
		Hashtable<SEMOSSVertex, Double> ranksTimesNodes = new Hashtable<SEMOSSVertex, Double>();
		for(SEMOSSVertex vert : ranks.keySet()) {
			ranksTimesNodes.put(vert, ranks.get(vert)*ranks.keySet().size());
		}
		
		for(String node : vertStore.keySet()) {
			SEMOSSVertex vert = vertStore.get(node);
			String type = (String)vert.propHash.get(Constants.VERTEX_NAME);
			Map<String, Object> row = new HashMap<String, Object>();
			row.put(names[0], type);
			row.put(names[1], unDirCloseness.get(vert));
			row.put(names[2], unDirBetweenness.get(vert));
			row.put(names[3], unDirEccentricity.get(vert));
			row.put(names[4], ranksTimesNodes.get(vert));
			dataFrame.addRow(row, row);
		}
	}
}

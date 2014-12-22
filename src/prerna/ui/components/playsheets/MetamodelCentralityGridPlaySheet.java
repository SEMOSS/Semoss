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

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.impl.CentralityCalculator;
import prerna.algorithm.impl.PageRankCalculator;
import prerna.algorithm.impl.SubclassingMapGenerator;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.util.Constants;
import edu.uci.ics.jung.graph.DelegateForest;

@SuppressWarnings("serial")
public class MetamodelCentralityGridPlaySheet extends GridPlaySheet {

	private static final Logger logger = LogManager.getLogger(MetamodelCentralityGridPlaySheet.class.getName());
		
	@Override
	public void createData() {
		GraphPlaySheet graphPS = CentralityCalculator.createMetamodel(((AbstractEngine)engine).getBaseDataEngine().getRC(), query);

		Hashtable<String, SEMOSSVertex> vertStore  = graphPS.getGraphData().getVertStore();
		Hashtable<String, SEMOSSEdge> edgeStore = graphPS.getGraphData().getEdgeStore();

		SubclassingMapGenerator subclassGen = new SubclassingMapGenerator();
		subclassGen.processSubclassing(engine);
		subclassGen.updateVertAndEdgeStoreForSubclassing(vertStore, edgeStore);

		vertStore = subclassGen.getVertStore();
		edgeStore = subclassGen.getEdgeStore();
		
		names = new String[]{"Type","Undirected Closeness Centrality","Undirected Betweeness Centrality","Undirected Eccentricity Centrality","Undirected Page Rank"};

		list = new ArrayList<Object[]>();
		
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
			Object[] row = new Object[5];
			row[0] = type;
			row[1] = unDirCloseness.get(vert);
			row[2] = unDirBetweenness.get(vert);
			row[3] = unDirEccentricity.get(vert);
			row[4] = ranksTimesNodes.get(vert);
			list.add(row);
		}
	}
}

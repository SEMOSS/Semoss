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
package prerna.algorithm.impl;

import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;

import prerna.om.SEMOSSVertex;
import edu.uci.ics.jung.algorithms.scoring.PageRank;
import edu.uci.ics.jung.graph.DelegateForest;

public class PageRankCalculator {
	
	private double alpha = 0.15;
	private double tolerance = 0.001;
	private int maxIterations = 100;
	
	public Hashtable<SEMOSSVertex, Double> calculatePageRank(DelegateForest forest) {

		PageRank<SEMOSSVertex, Integer> ranker = new PageRank<SEMOSSVertex, Integer>(forest, alpha);
		ranker.setTolerance(tolerance) ;
		ranker.setMaxIterations(maxIterations);
		ranker.evaluate();
		
		Hashtable<SEMOSSVertex, Double> ranks = new Hashtable<SEMOSSVertex, Double>();
		
		Collection<String> col =  forest.getVertices();
		Iterator it = col.iterator();
		while(it.hasNext()) {
			SEMOSSVertex v= (SEMOSSVertex) it.next();
			double r = ranker.getVertexScore(v);
			ranks.put(v, r);
		}
		return ranks;
	}
}

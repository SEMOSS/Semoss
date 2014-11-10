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

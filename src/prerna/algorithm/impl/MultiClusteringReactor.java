package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.util.Cluster;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.util.ArrayUtilityMethods;

public class MultiClusteringReactor extends MathReactor {

	private static final Logger LOGGER = LogManager.getLogger(ClusteringReactor.class.getName());

	public static final String INSTANCE_INDEX = "instanceIndex";
	public static final String MIN_NUM_CLUSTERS = "minNumClusters";
	public static final String MAX_NUM_CLUSTERS = "maxNumClusters";
	public static final String DISTANCE_MEASURE	= "distanceMeasure";

	// parameters for the algorithm
	private int instanceIndex;
	private int minNumClusters;
	private int maxNumClusters;

	// values from data
	private String[] attributeNames;
	private List<String> attributeNamesList;
	private boolean[] isNumeric;

	private String clusterColName;

	@Override
	public Iterator process() {
		modExpression();

		///////////////// start of initializing some stuff... needs to be put away somewhere else
		if(myStore.containsKey(PKQLEnum.MATH_PARAM)) {
			Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MATH_PARAM);
			if(options.containsKey(INSTANCE_INDEX.toUpperCase())) {
				this.instanceIndex = Integer.parseInt(options.get(INSTANCE_INDEX.toUpperCase()) + "");
			} else {
				// i think its a fair assumption that the first column written can be assumed to be the instance
				this.instanceIndex = 0;
			}
			
			if(options.containsKey(MIN_NUM_CLUSTERS.toUpperCase())) {
				this.minNumClusters = Integer.parseInt(options.get(MIN_NUM_CLUSTERS.toUpperCase()) + "");
			} else {
				// TODO: need to throw an error saying min number of clusters is required
				this.minNumClusters = 2;
			}
			
			if(options.containsKey(MAX_NUM_CLUSTERS.toUpperCase())) {
				this.maxNumClusters = Integer.parseInt(options.get(MAX_NUM_CLUSTERS.toUpperCase()) + "");
			} else {
				// TODO: need to throw an error saying min number of clusters is required
				this.maxNumClusters = 2;
			}
		} else {
			//TODO: need to throw an error saying parameters are required
			return null;
		}
		
		this.attributeNamesList = (List<String>)myStore.get(PKQLEnum.COL_DEF);
		this.attributeNames = attributeNamesList.toArray(new String[]{});

		ITableDataFrame dataFrame = (ITableDataFrame)myStore.get("G");
		this.isNumeric = new boolean[this.attributeNames.length];
		for(int i = 0; i < this.attributeNames.length; i++) {
			this.isNumeric[i] = dataFrame.isNumeric(this.attributeNames[i]);
		}
		
		int numInstances = dataFrame.getUniqueInstanceCount(attributeNames[instanceIndex]);
		if(numInstances == 1) {
			throw new IllegalArgumentException("Instance column only contains one unqiue value.");
		}
		if(maxNumClusters > numInstances) {
			maxNumClusters = numInstances;
		}

		Map<Object, Integer> results = null;
		if(minNumClusters != maxNumClusters) {
			results = runGoldenSelectionForNumberOfClusters(dataFrame, minNumClusters, maxNumClusters);
		} else { // usually occurs when there is too few data points
			results = runClusteringRoutine(dataFrame, minNumClusters, new ArrayList<Cluster>(), new HashMap<Integer, Double>());
		}

		String attributeName = attributeNames[instanceIndex];
		// to avoid adding columns with same name
		int counter = 0;
		this.clusterColName = attributeName + "_CLUSTER_" + counter;
		while(ArrayUtilityMethods.arrayContainsValue(attributeNames, clusterColName)) {
			counter++;
			this.clusterColName = attributeName + "_CLUSTER_" + counter;
		}

		Map<String, String> dataType = new HashMap<String, String>();
		dataType.put(clusterColName, "DOUBLE");
		dataFrame.connectTypes(attributeName, clusterColName, dataType);
		for(Object instance : results.keySet()) {
			int val = results.get(instance);

			Map<String, Object> clean = new HashMap<String, Object>();
			clean.put(attributeName, instance);
			clean.put(clusterColName, val);

			dataFrame.addRelationship(clean);
		}

		return null;
	}

	private Map<Object, Integer> runGoldenSelectionForNumberOfClusters(ITableDataFrame data, int start, int end) {		
		Map<Object, Integer> bestResults = null;
		int a = start;
		int b = end;
		double phi = (double) (1 + Math.sqrt(5)) / 2;
		int x1 = (int) Math.round((phi - 1)*a + (2-phi)*b);
		int x2 = (int) Math.round((2-phi)*a + (phi - 1)*b);

		double bestVal = -1;
		Map<Integer, Double> previousResults = new Hashtable<Integer, Double>();

		List<Cluster> startClusterList = new ArrayList<Cluster>();
		double startVal = 0;
		Map<Object, Integer> startClusterResult = null;
		String errorMessage1 = null;
		try {
			startClusterResult = runClusteringRoutine(data, x1, startClusterList, previousResults);
			startVal = computeClusteringScore(data, startClusterResult, startClusterList, previousResults, x1);
			previousResults.put(x1, startVal);
		} catch (IllegalArgumentException ex) {
			// do nothing
			errorMessage1 = ex.getMessage();
		}

		List<Cluster> endClusterList = new ArrayList<Cluster>();
		double endVal = 0;
		Map<Object, Integer> endClusterResult = null;
		String errorMessage2 = null;
		try {
			endClusterResult = runClusteringRoutine(data, x2, endClusterList, previousResults);
			endVal = computeClusteringScore(data, endClusterResult, endClusterList, previousResults, x2);
			previousResults.put(x2, endVal);
		} catch (IllegalArgumentException ex) {
			// do nothing
			errorMessage2 = ex.getMessage();
		}

		if(startClusterResult == null && endClusterResult == null) {
			throw new IllegalArgumentException(errorMessage1 + ".\n" + errorMessage2);
		}

		if(startVal >= endVal) {
			if(startVal > bestVal) {
				bestVal = startVal;
				bestResults = startClusterResult;
			}
		} else {
			if(endVal > bestVal) {
				bestVal = endVal;
				bestResults = endClusterResult;
			}
		}

		while(Math.abs(b - a) > 1) {
			if(startVal < endVal) {
				a = x1;
				x1 = x2;
				x2 = (int) Math.round((2-phi)*a + (phi-1)*b);
			} else {
				b = x2;
				x2 = x1;
				x1 = (int) Math.round((phi-1)*a + (2-phi)*b);
			}

			try {
				startClusterList.clear();
				startClusterResult = runClusteringRoutine(data, x1, startClusterList, previousResults);
				startVal = computeClusteringScore(data, startClusterResult, startClusterList, previousResults, x1);
				previousResults.put(x1, startVal);
			} catch (IllegalArgumentException ex) {
				// do nothing
			}

			try {
				endClusterList.clear();
				endClusterResult = runClusteringRoutine(data, x2, endClusterList, previousResults);
				endVal = computeClusteringScore(data, endClusterResult, endClusterList, previousResults, x2);
				previousResults.put(x2, endVal);
			} catch (IllegalArgumentException ex) {
				//do nothing
			}

			if(startVal > endVal) {
				if(startVal > bestVal) {
					bestVal = startVal;
					if(startClusterResult != null) { // if null, its already stored
						bestResults = startClusterResult;
					}
				}
			} else {
				if(endVal > bestVal) {
					bestVal = endVal;
					if(endClusterResult != null) { // if null, its already stored
						bestResults = endClusterResult;
					}
				}
			}
		}

		return bestResults;
	}

	private double computeClusteringScore(ITableDataFrame data, Map<Object, Integer> results, List<Cluster> clusters, Map<Integer, Double> previousResults, int numClusters) {
		if(previousResults.containsKey(numClusters)) {
			return previousResults.get(numClusters);
		}

		double innerClusterSimilairty = 0;
		Iterator<List<Object[]>> it = this.getUniqueScaledData(attributeNames[instanceIndex], attributeNamesList, data);
		while(it.hasNext()) {
			List<Object[]> instance = it.next();
			Object instanceName = instance.get(0)[instanceIndex];
			//			List<Object[]> instanceResult = getRowsForInstance(groupByMap, instanceName, headers);
			int clusterIndex = results.get(instanceName);
			double simVal = clusters.get(clusterIndex).getSimilarityForInstance(instance, this.attributeNames, isNumeric, this.instanceIndex);
			innerClusterSimilairty += simVal / clusters.get(clusterIndex).getNumInstances();
		}

		// calculate cluster-cluster similarity
		double clusterToClusterSimilarity = 0;
		for(int i = 0; i < numClusters-1; i++) {
			Cluster c1 = clusters.get(i);
			for(int j = i+1; j < numClusters; j++) {
				Cluster c2 = clusters.get(j);
				double simVal = c1.getClusterSimilarity(c2, this.attributeNames[this.instanceIndex]);
				clusterToClusterSimilarity += (1-simVal);
			}
		}

		return innerClusterSimilairty/numClusters + clusterToClusterSimilarity / ( (double) (numClusters * (numClusters-1) /2));
	}

	private Map<Object, Integer> runClusteringRoutine(ITableDataFrame dataframe, int numClusters, List<Cluster> clusters, Map<Integer, Double> previousResults) {
		if(previousResults.containsKey(numClusters)) {
			return null;
		}

		LOGGER.info("Running clustering for " + numClusters + " number of clusters");
		
		java.util.Map<String, Object> params = new java.util.Hashtable<String, Object>();
		params.put(prerna.algorithm.impl.ClusteringReactor.INSTANCE_INDEX.toUpperCase(), instanceIndex);
		params.put(prerna.algorithm.impl.ClusteringReactor.NUM_CLUSTERS.toUpperCase(), numClusters);
		
		prerna.algorithm.impl.ClusteringReactor alg = new prerna.algorithm.impl.ClusteringReactor();
		alg.setAddToFrame(false);
		alg.put("G", dataframe);
		alg.put(PKQLEnum.MATH_PARAM, params);
		alg.put(PKQLEnum.COL_DEF, this.attributeNamesList);
		alg.process();
		
		// keep track of the clusters to determine best clustering number
		clusters.addAll(alg.getClusters());
		return alg.getResults();
	}
}

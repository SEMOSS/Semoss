package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.util.Cluster;
import prerna.algorithm.learning.util.IClusterDistanceMode;
import prerna.algorithm.learning.util.IClusterDistanceMode.DistanceMeasure;
import prerna.math.SimilarityWeighting;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

public class ClusteringReactor extends MathReactor {

	private Map<Object, Integer> results = new HashMap<Object, Integer>();

	// keeping track of cluster information
	private List<Cluster> clusters = new ArrayList<Cluster>();
	private List<Integer> numInstancesInCluster = new ArrayList<Integer>();

	// values from data
	private String[] attributeNames;
	private List<String> attributeNamesList;

	private boolean[] isNumeric;
	
	// defined in SEMOSS options
	private int numClusters;
	private int instanceIndex;
	private Map<String, IClusterDistanceMode.DistanceMeasure> distanceMeasure;

	private Map<String, Double> numericalWeights = new HashMap<String, Double>();
	private Map<String, Double> categoricalWeights = new HashMap<String, Double>();
	
	private String clusterColName;
	private boolean appendOntoDataMaker = true;
	
	@Override
	public Iterator process() {
		modExpression();
		
		///////////////// start of initializing some stuff... needs to be put away somewhere else
		if(myStore.containsKey(PKQLEnum.MATH_PARAM)) {
			Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MATH_PARAM);
			if(options.containsKey("instanceIndex".toUpperCase())) {
				this.instanceIndex = Integer.parseInt(options.get("instanceIndex".toUpperCase()) + "");
			} else {
				this.instanceIndex = 0;
			}
			
			if(options.containsKey("numClusters".toUpperCase())) {
				this.numClusters = Integer.parseInt(options.get("numClusters".toUpperCase()) + "");
			} else {
				this.numClusters = 5;
			}
		}
		this.attributeNamesList = (List<String>)myStore.get(PKQLEnum.COL_DEF);
		this.attributeNames = attributeNamesList.toArray(new String[]{});

		ITableDataFrame dataFrame = (ITableDataFrame)myStore.get("G");
		this.isNumeric = new boolean[this.attributeNames.length];
		for(int i = 0; i < this.attributeNames.length; i++) {
			String type = dataFrame.getDataType(this.attributeNames[i]);
			if(type.equalsIgnoreCase("STRING")) {
				this.isNumeric[i] = false;
			} else {
				this.isNumeric[i] = true;
			}
		}
		
		// set the type of distance measure to be used for each numerical property - default is using mean
		if(this.distanceMeasure == null) {
			distanceMeasure = new HashMap<String, IClusterDistanceMode.DistanceMeasure>();
			for(int i = 0; i < attributeNames.length; i++) {
				if(isNumeric[i]) {
					distanceMeasure.put(attributeNames[i], DistanceMeasure.MEAN);
				}
			}
		} else {
			for(int i = 0; i < attributeNames.length; i++) {
				if(!distanceMeasure.containsKey(attributeNames[i])) {
					distanceMeasure.put(attributeNames[i], DistanceMeasure.MEAN);
				}
			}
		}
		///////////////// end of initializing some stuff... needs to be put away somewhere else

		
		///////////////// basic checks
		int numInstances = dataFrame.getUniqueInstanceCount(attributeNames[instanceIndex]);
		if(numInstances == 1) {
			throw new IllegalArgumentException("Instance column only contains one unqiue value.");
		}
		if(numClusters > numInstances) {
			throw new IllegalArgumentException("There are " + numClusters + " number of clusters while only " + numInstances + " unique instances.\nNumber of instances must be larger than number of clusters.");
		}
		///////////////// end basic checks

		
		// fills in numericalWeights and categoricalWeights maps
		SimilarityWeighting.calculateWeights(dataFrame, instanceIndex, attributeNames, isNumeric, numericalWeights, categoricalWeights);
		
		initializeClusters(dataFrame, attributeNamesList);
		int maxIt = 100_000;
		boolean go = true;
		int currIt = 0;
		while(go) {
			go = false;
			Iterator<List<Object[]>> it = this.getUniqueScaledData(attributeNames[instanceIndex], attributeNamesList, dataFrame);
			while(it.hasNext()) {
				List<Object[]> instance = it.next();
				Object instanceName = instance.get(0)[instanceIndex];
				int bestCluster = findBestClusterForInstance(instance, attributeNames, isNumeric, instanceIndex, clusters);
				boolean instanceChangeCluster = isInstanceChangedCluster(results, instanceName, bestCluster);
				if(instanceChangeCluster) {
					go = true;
					Integer currCluster = results.get(instanceName);
//					System.err.println("Moving " + instanceName + " from cluster " + currCluster + " to " + bestCluster);
					results.put(instanceName, bestCluster);
					updateInstanceIndex(instance, attributeNames, isNumeric, clusters.get(bestCluster));
					if(currCluster != null) {
						removeInstanceIndex(instance, attributeNames, isNumeric, clusters.get(currCluster));
					}
				}
			}
			currIt++;
			// break if taking too many iterations
			if(currIt > maxIt) {
				go = false;
			}
		}

		String attributeName = attributeNames[instanceIndex];
		// to avoid adding columns with same name
		int counter = 0;
		this.clusterColName = attributeName + "_CLUSTER_" + counter;
		while(ArrayUtilityMethods.arrayContainsValue(dataFrame.getColumnHeaders(), clusterColName)) {
			counter++;
			this.clusterColName = attributeName + "_CLUSTER_" + counter;
		}
		
		if(appendOntoDataMaker) {
			Map<String, String> dataType = new HashMap<>();
			dataType.put(clusterColName, "double");
			dataFrame.connectTypes(attributeName, clusterColName, dataType);
			for(Object instance : results.keySet()) {
				int val = results.get(instance);

				Map<String, Object> raw = new HashMap<String, Object>();
				raw.put(attributeName, instance);
				raw.put(clusterColName, val);
				
				Map<String, Object> clean = new HashMap<String, Object>();
				if(instance.toString().startsWith("http://semoss.org/ontologies/Concept/")) {
					instance = Utility.getInstanceName(instance.toString());
				}
				clean.put(attributeName, instance);
				clean.put(clusterColName, val);
				
				dataFrame.addRelationship(clean, raw);
			}
			
			String[] newHeaders = new String[]{clusterColName};
			String[] newHeaderType = new String[]{"INT"};
			dataFrame.addMetaDataTypes(newHeaders, newHeaderType);
		}
		
		return null;
	}
	
	/**
	 * Will generate the clusters by picking the most different instances
	 */
	private void initializeClusters(ITableDataFrame dataFrame, List<String> attributeNamesList) {
		Iterator<List<Object[]>> it = this.getUniqueScaledData(attributeNames[instanceIndex], attributeNamesList, dataFrame);
		List<Object[]> firstInstance = it.next();

		Cluster firstCluster = new Cluster(categoricalWeights, numericalWeights);
		firstCluster.setDistanceMode(this.distanceMeasure);
		firstCluster.addToCluster(firstInstance, attributeNames, isNumeric);
		clusters.add(firstCluster);
		results.put(firstInstance.get(0)[instanceIndex].toString(), 0);
		// update cluster instance count
		numInstancesInCluster.add(1);

		// create a cluster to serve as a combination of all the starting seeds
		Cluster combinedInstances = new Cluster(categoricalWeights, numericalWeights);
		combinedInstances.setDistanceMode(this.distanceMeasure);
		combinedInstances.addToCluster(firstInstance, attributeNames, isNumeric);

		for(int i = 1; i < numClusters; i++) {
			double simVal = 2;
			List<Object[]> bestInstance = null;
			while(it.hasNext()) {
				List<Object[]> instance = it.next();
				double val = combinedInstances.getSimilarityForInstance(instance, attributeNames, isNumeric, instanceIndex);
				if(val < simVal && !results.containsKey(instance.get(0)[instanceIndex])) {
					bestInstance = instance;
				}
				if(val == 0) {
					break;
				}
			}
			// update combined cluster
			combinedInstances.addToCluster(bestInstance, attributeNames, isNumeric);

			// create new cluster and add as a seed
			Cluster newCluster = new Cluster(categoricalWeights, numericalWeights);
			newCluster.setDistanceMode(this.distanceMeasure);
			newCluster.addToCluster(bestInstance, attributeNames, isNumeric);
			clusters.add(newCluster);
			results.put(bestInstance.get(0)[instanceIndex].toString(), i);
			// update cluster instance count
			numInstancesInCluster.add(1);

			// generate new iterator
			it = this.getUniqueScaledData(attributeNames[instanceIndex], attributeNamesList, dataFrame);
		}
	}

	public int findBestClusterForInstance(List<Object[]> instance, String[] attributeNames, boolean[] isNumeric, int instanceIndex, List<Cluster> clusters) {
		int bestIndex = -1;
		double simVal = -1;
		int i = 0;
		for(; i < numClusters; i++) {
			double newSimVal = clusters.get(i).getSimilarityForInstance(instance, attributeNames, isNumeric, instanceIndex);
			if(newSimVal > simVal) {
				bestIndex = i;
				simVal = newSimVal;
			}
			if(simVal == 1) {
				break;
			}
		}
		return bestIndex;
	}

	public boolean isInstanceChangedCluster(Map<Object, Integer> results, Object instanceName, int bestCluster) {
		if(results.containsKey(instanceName)) {
			if(results.get(instanceName) == bestCluster) {
				return false;
			} else {
				return true;
			}
		}
		return true;
	}

	public void updateInstanceIndex(List<Object[]> instance, String[] attributeNames, boolean[] isNumeric, Cluster clusterToAdd) {
		clusterToAdd.addToCluster(instance, attributeNames, isNumeric);
	}

	public void removeInstanceIndex(List<Object[]> instance, String[] attributeNames, boolean[] isNumeric, Cluster clusterToRemove) {
		clusterToRemove.removeFromCluster(instance, attributeNames, isNumeric);
	}
}

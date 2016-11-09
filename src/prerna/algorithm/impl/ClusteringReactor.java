package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.util.Cluster;
import prerna.algorithm.learning.util.IClusterDistanceMode;
import prerna.algorithm.learning.util.IClusterDistanceMode.DistanceMeasure;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.util.ArrayUtilityMethods;

public class ClusteringReactor extends MathReactor {

	private static final Logger LOGGER = LogManager.getLogger(ClusteringReactor.class.getName());
	
	public static final String NUM_CLUSTERS = "numClusters";
	public static final String INSTANCE_INDEX = "instanceIndex";
	
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
	
	// ughhhh... since we call this class within the 
	// multi clustering reactor
	// need to add this so each iteration of that routine
	// does add to the frame
	private boolean addToFrame = true;
	
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
			
			if(options.containsKey(NUM_CLUSTERS.toUpperCase())) {
				this.numClusters = Integer.parseInt(options.get(NUM_CLUSTERS.toUpperCase()) + "");
			} else {
				// TODO: need to throw an error saying number of clusters is required
				this.numClusters = 5;
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

		
		//TODO: process of getting these weights is outdated
		// fills in numericalWeights and categoricalWeights maps
		
		
		// TESTING
		for(int i = 0; i < this.isNumeric.length; i++) {
			if(isNumeric[i]) {
				numericalWeights.put(attributeNames[i], 1.0);
			} else {
				categoricalWeights.put(attributeNames[i], 1.0);
			}
		}
//		SimilarityWeighting.calculateWeights(dataFrame, instanceIndex, attributeNames, isNumeric, numericalWeights, categoricalWeights);
		
		LOGGER.info("Start creation of initial cluster centers...");		
		initializeClusters(dataFrame, attributeNamesList);
		LOGGER.info("Done creation of initial cluster centers...");		

		int maxIt = 100_000;
		boolean go = true;
		int currIt = 0;
		
		LOGGER.info("Start iterating through dataset until convergence...");		
		while(go) {
			LOGGER.info("Start iteration number " + (currIt+1) + "...");		
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
				LOGGER.info("Convergence Error ::: clustering routine did not converge after " + maxIt + " iterations");		
				go = false;
			}
		}

		LOGGER.info("Done iterating ...");		

		// ughhhh... since we call this class within the 
		// multi clustering reactor
		// need to add this so each iteration of that routine
		// does add to the frame
		if(addToFrame) {
			String[] allColNames = dataFrame.getColumnHeaders();
			String attributeName = attributeNames[instanceIndex];
			// to avoid adding columns with same name
			int counter = 0;
			this.clusterColName = attributeName + "_Cluster";
			while(ArrayUtilityMethods.arrayContainsValue(allColNames, clusterColName)) {
				counter++;
				this.clusterColName = attributeName + "_Cluster_" + counter;
			}
			
			// TODO: need to return an iterator and not automatically append the data to the frame
			Map<String, String> dataType = new HashMap<>();
			dataType.put(clusterColName, "double");
			dataFrame.connectTypes(attributeName, clusterColName, dataType);
			for(Object instance : results.keySet()) {
				int val = results.get(instance);

				Map<String, Object> clean = new HashMap<String, Object>();
				clean.put(attributeName, instance);
				clean.put(clusterColName, val);
				
				dataFrame.addRelationship(clean);
			}
		}
		
		myStore.put("STATUS",STATUS.SUCCESS);

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
	
	public Map<Object, Integer> getResults() {
		return this.results;
	}

	public List<Cluster> getClusters() {
		return this.clusters;
	}
	
	public void setAddToFrame(boolean addToFrame) {
		this.addToFrame = addToFrame;
	}
}

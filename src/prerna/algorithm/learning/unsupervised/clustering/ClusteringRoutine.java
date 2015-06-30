package prerna.algorithm.learning.unsupervised.clustering;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.util.Cluster;
import prerna.algorithm.learning.util.IClusterDistanceMode.DistanceMeasure;

public class ClusteringRoutine extends AbstractClusteringRoutine {

	private Map<String, Integer> results = new HashMap<String, Integer>();
	private Map<String, Double> ranges = new HashMap<String, Double>();

	public ClusteringRoutine() {
		super();
	}

	@Override
	public ITableDataFrame runAlgorithm(ITableDataFrame... data) {
		// values defined in options
		this.numClusters = (int) options.get(0).getSelected();
		this.instanceIndex = (int) options.get(1).getSelected();
		this.distanceMeasure = (Map<String, DistanceMeasure>) options.get(2).getSelected();
		this.dataFrame = data[0];
		this.isNumeric = dataFrame.isNumeric();
		this.attributeNames = dataFrame.getColumnHeaders();

		// set the type of distance measure to be used for each numerical property - default is using mean
		if(this.distanceMeasure == null) {
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

		int numRows = dataFrame.getNumRows();
		if(numClusters > numRows) {
			throw new IllegalArgumentException("Cannot have more clusters than instances");
		}

		calculateWeights();
		calculateRanges();
		initializeClusters();
		int maxIt = 100_000;
		boolean go = true;
		int currIt = 0;
		while(go) {
			go = false;
			Iterator<List<Object[]>> it = dataFrame.uniqueIterator(attributeNames[instanceIndex]);
			while(it.hasNext()) {
				List<Object[]> instance = it.next();
				String instanceName = instance.get(0)[instanceIndex].toString();
				int bestCluster = findBestClusterForInstance(instance, attributeNames, isNumeric, instanceIndex, clusters);
				boolean instanceChangeCluster = isInstanceChangedCluster(results, instanceName, bestCluster);
				if(instanceChangeCluster) {
					go = true;
					results.put(instanceName, bestCluster);
				}
			}
			// test convergence
			if(go) {
				// update cluster centers
				for(int i = 0; i < numClusters; i++) {
					// clear values in clusters
					clusters.get(i).reset();
				}
				it = dataFrame.uniqueIterator(attributeNames[instanceIndex]);
				while(it.hasNext()) {
					List<Object[]> instance = it.next();
					int clusterIndex = results.get(instance.get(0)[instanceIndex]);
					updateInstanceIndex(instance, attributeNames, isNumeric, clusters.get(clusterIndex));
				}
			} else {
				success = true;
			}

			currIt++;
			// break if taking too many iterations
			if(currIt > maxIt) {
				go = false;
				success = false;
			}
		}

		return null;
	}

	private void calculateRanges() {
		for(int i = 0; i < attributeNames.length; i++) {
			if(isNumeric[i]) {
				double min = dataFrame.getMin(attributeNames[i]);
				double max = dataFrame.getMin(attributeNames[i]);
				ranges.put(attributeNames[i], max-min);
			}
		}
	}

	/**
	 * Will generate the clusters by picking the most different instances
	 */
	private void initializeClusters() {
		Iterator<Object[]> it = dataFrame.iterator();
		Object[] firstInstance = it.next();

		Cluster firstCluster = new Cluster(categoricalWeights, numericalWeights, ranges);
		firstCluster.addToCluster(firstInstance, attributeNames, isNumeric);
		firstCluster.setDistanceMode(this.distanceMeasure);
		clusters.add(firstCluster);
		results.put(firstInstance[instanceIndex].toString(), 1);
		// update cluster instance count
		numInstancesInCluster.add(1);

		// create a cluster to serve as a combination of all the starting seeds
		Cluster combinedInstances = new Cluster(categoricalWeights, numericalWeights, ranges);
		combinedInstances.addToCluster(firstInstance, attributeNames, isNumeric);

		for(int i = 1; i < numClusters; i++) {
			double simVal = 2;
			Object[] bestInstance = null;
			while(it.hasNext()) {
				Object[] instance = it.next();
				double val = combinedInstances.getSimilarityForInstance(instance, attributeNames, isNumeric, instanceIndex);
				if(val < simVal && !results.containsKey(instance[instanceIndex])) {
					bestInstance = instance;
				}
				if(val == 0) {
					break;
				}
			}
			// update combined cluster
			combinedInstances.addToCluster(bestInstance, attributeNames, isNumeric);

			// create new cluster and add as a seed
			Cluster newCluster = new Cluster(categoricalWeights, numericalWeights, ranges);
			newCluster.addToCluster(bestInstance, attributeNames, isNumeric);
			newCluster.setDistanceMode(this.distanceMeasure);
			clusters.add(newCluster);
			results.put(bestInstance[instanceIndex].toString(), i);
			// update cluster instance count
			numInstancesInCluster.add(1);

			// generate new iterator
			it = dataFrame.iterator();
		}
	}

	@Override
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
		}
		return bestIndex;
	}

	@Override
	public boolean isInstanceChangedCluster(Map<String, Integer> results, String instanceName, int bestCluster) {
		if(results.containsKey(instanceName)) {
			if(results.get(instanceName) == bestCluster) {
				return false;
			} else {
				return true;
			}
		}
		return true;
	}

	@Override
	public void updateInstanceIndex(List<Object[]> instance, String[] attributeNames, boolean[] isNumeric, Cluster clusterToAdd) {
		clusterToAdd.addToCluster(instance, attributeNames, isNumeric);
	}

	//TODO: say it will combine non-unique values together s.t. they get the assigned to the same cluster
	// i.e. clustering on unique values of column
	@Override
	public String getResultDescription() {
		// TODO Auto-generated method stub
		return null;
	}

}

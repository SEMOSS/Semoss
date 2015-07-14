package prerna.algorithm.learning.unsupervised.clustering;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.util.Cluster;
import prerna.algorithm.learning.util.IClusterDistanceMode;
import prerna.algorithm.learning.util.IClusterDistanceMode.DistanceMeasure;
import prerna.ds.BTreeDataFrame;

public class ClusteringRoutine extends AbstractClusteringRoutine {

	private Map<Object, Integer> results = new HashMap<Object, Integer>();

	public ClusteringRoutine() {
		super();
	}

	@Override
	public ITableDataFrame runAlgorithm(ITableDataFrame... data) {
		// values defined in options
		//TODO: below is simply for ease in testing
		this.numClusters = 12;
		this.instanceIndex = 0;
		this.clusterColumnID = "clusterID";
//		this.numClusters = (Integer) options.get(0).getSelected();
//		this.instanceIndex = (Integer) options.get(1).getSelected();
		
		this.distanceMeasure = (Map<String, DistanceMeasure>) options.get(2).getSelected();
		this.dataFrame = data[0];
		this.isNumeric = dataFrame.isNumeric();
		this.attributeNames = dataFrame.getColumnHeaders();
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

		int numRows = dataFrame.getNumRows();
		if(numClusters > numRows) {
			throw new IllegalArgumentException("Cannot have more clusters than instances");
		}

		calculateWeights();
		initializeClusters();
		int maxIt = 100_000;
		boolean go = true;
		int currIt = 0;
		while(go) {
			go = false;
			Iterator<List<Object[]>> it = dataFrame.scaledUniqueIterator(attributeNames[instanceIndex], false);
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
				success = false;
			}
		}

		ITableDataFrame returnTable = new BTreeDataFrame(new String[]{attributeNames[instanceIndex], clusterColumnID});
		for(Object instance : results.keySet()) {
			Map<String, Object> row = new HashMap<String, Object>();
			row.put(attributeNames[instanceIndex], instance);
			row.put(clusterColumnID, results.get(instance));
			returnTable.addRow(row, row);
		}
		
		return returnTable;
	}

	/**
	 * Will generate the clusters by picking the most different instances
	 */
	private void initializeClusters() {
		Iterator<List<Object[]>> it = dataFrame.scaledUniqueIterator(attributeNames[instanceIndex], false);
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
			it = dataFrame.scaledUniqueIterator(attributeNames[instanceIndex], false);
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
			if(simVal == 1) {
				break;
			}
		}
		return bestIndex;
	}

	@Override
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

	@Override
	public void updateInstanceIndex(List<Object[]> instance, String[] attributeNames, boolean[] isNumeric, Cluster clusterToAdd) {
		clusterToAdd.addToCluster(instance, attributeNames, isNumeric);
	}

	@Override
	public void removeInstanceIndex(List<Object[]> instance, String[] attributeNames, boolean[] isNumeric, Cluster clusterToRemove) {
		clusterToRemove.removeFromCluster(instance, attributeNames, isNumeric);
	}
	
	//TODO: say it will combine non-unique values together s.t. they get the assigned to the same cluster
	// i.e. clustering on unique values of column
	@Override
	public String getResultDescription() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Object> getResultMetadata() {
		// TODO Auto-generated method stub
		return null;
	}
}

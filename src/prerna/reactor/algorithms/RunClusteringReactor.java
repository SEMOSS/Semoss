package prerna.reactor.algorithms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.util.Cluster;
import prerna.algorithm.learning.util.IClusterDistanceMode;
import prerna.algorithm.learning.util.IClusterDistanceMode.DistanceMeasure;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ArrayUtilityMethods;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class RunClusteringReactor extends AbstractFrameReactor {

	private static final String CLASS_NAME = RunClusteringReactor.class.getName();

	/**
	 * RunClustering(instance = column, numClusters = #, columns = attributeNamesList);
	 */

	private String[] attributeNames;
	private List<String> attributeNamesList;
	private List<Cluster> clusters = new ArrayList<>();
	private List<Integer> numInstancesInCluster = new ArrayList<>();
	private boolean[] isNumeric;
	private int numClusters;
	private String instanceColumn;
	private Map<String, IClusterDistanceMode.DistanceMeasure> distanceMeasure;
	private int instanceIndex;
	private boolean addToFrame = true;
	private AlgorithmSingleColStore<Integer> results = new AlgorithmSingleColStore<>();
	
	public RunClusteringReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.INSTANCE_KEY.getKey(), ReactorKeysEnum.CLUSTER_KEY.getKey(), ReactorKeysEnum.ATTRIBUTES.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = this.getLogger(CLASS_NAME);
		ITableDataFrame dataFrame = getFrame();
		dataFrame.setLogger(logger);
		
		//get inputs
		this.instanceColumn = getInstanceColumn();
		this.instanceIndex = 0;
		this.attributeNamesList = getColumns();
		this.attributeNames = this.attributeNamesList.toArray(new String[0]);
		this.numClusters = getNumClusters();
		if(this.numClusters == -1) {
			this.numClusters = 5; //set default in case it wasn't retrieved from the command
		}

		this.isNumeric = new boolean[this.attributeNames.length];
		for(int i = 0; i < this.attributeNames.length; i++) {
			this.isNumeric[i] = dataFrame.isNumeric(this.attributeNames[i]);
		}

		if(this.distanceMeasure == null) {
			distanceMeasure = new HashMap<>();
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

		///////////////// basic checks
		int numInstances = dataFrame.getUniqueInstanceCount(instanceColumn);
		if(numInstances == 1) {
			throw new IllegalArgumentException("Instance column only contains one unqiue value.");
		}
		if(numClusters > numInstances) {
			throw new IllegalArgumentException("There are " + numClusters + " number of clusters while only " + numInstances + " unique instances.\nNumber of instances must be larger than number of clusters.");
		}
		///////////////// end basic checks

		logger.info("Start creation of initial cluster centers...");
		initializeClusters(dataFrame, attributeNamesList, logger);
		logger.info("Done creation of initial cluster centers...");		

		int maxIt = 1000;
		boolean go = true;
		int currIt = 0;
		logger.info("Start iterating through dataset until convergence...");		
		while (go) {
			logger.info("Start iteration number " + (currIt+1) + "...");		
			go = false;
			Configurator.setLevel(logger.getName(), Level.OFF);
			int counter = 0;
			Iterator<List<Object[]>> it = dataFrame.scaledUniqueIterator(instanceColumn, attributeNamesList);
			while (it.hasNext()) {
				List<Object[]> instance = it.next();
				Object instanceName = instance.get(0)[instanceIndex];
				int bestCluster = findBestClusterForInstance(instance, attributeNames, isNumeric, instanceIndex, clusters);
				boolean instanceChangeCluster = isInstanceChangedCluster(results, instanceName, bestCluster);
				if (instanceChangeCluster) {
					go = true;
					Integer currCluster = results.get(instanceName);
					results.put(instanceName, bestCluster);
					updateInstanceIndex(instance, attributeNames, isNumeric, clusters.get(bestCluster));
					if (currCluster != null) {
						removeInstanceIndex(instance, attributeNames, isNumeric, clusters.get(currCluster));
					}
				}
				
				// logging
				if(counter % 100 == 0) {
					Configurator.setLevel(logger.getName(), Level.INFO);
					logger.info("Finished execution for loop number = " + (currIt+1) + ", unique instance number = " + counter);
					Configurator.setLevel(logger.getName(), Level.OFF);
				}
				counter++;
			}
			currIt++;
			// break if taking too many iterations
			if (currIt > maxIt) {
				Configurator.setLevel(logger.getName(), Level.INFO);
				logger.info("Convergence Error ::: clustering routine did not converge after " + maxIt + " iterations");		
				go = false;
			}
		}
		Configurator.setLevel(logger.getName(), Level.INFO);
		logger.info("Done iterating ...");		
		
		// ughhhh... since we call this class within the 
		// multi clustering reactor
		// need to add this so each iteration of that routine
		// does add to the frame
		if(addToFrame) {
			// to avoid adding columns with same name
			int counter = 0;
			String[] allColNames = dataFrame.getColumnHeaders();
			String newColName = instanceColumn + "_Cluster";
			while(ArrayUtilityMethods.arrayContainsValue(allColNames, newColName)) {
				counter++;
				newColName = instanceColumn + "_Cluster_" + counter;
			}
			// merge data back onto the frame
			AlgorithmMergeHelper.mergeSimpleAlgResult(dataFrame, this.instanceColumn, newColName, "NUMBER", this.results);
		}

		// track GA data
//		UserTrackerFactory.getInstance().trackAnalyticsPixel(this.insight, "Clustering");
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				dataFrame, 
				"Clustering", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		return new NounMetadata(dataFrame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
	}

	public boolean isInstanceChangedCluster(AlgorithmSingleColStore<Integer> results, Object instanceName, int bestCluster) {
		if (results.containsKey(instanceName)) {
			if ((int) results.get(instanceName) == bestCluster) {
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

	// helper methods for clustering
	private void initializeClusters(ITableDataFrame dataFrame, List<String> attributeNamesList, Logger logger) {
		Configurator.setLevel(logger.getName(), Level.OFF);
		Iterator<List<Object[]>> it = dataFrame.scaledUniqueIterator(instanceColumn, attributeNamesList);
		List<Object[]> firstInstance = it.next();
		Cluster firstCluster = new Cluster(attributeNames, isNumeric);
		firstCluster.setDistanceMode(this.distanceMeasure);

		// use columns
		firstCluster.addToCluster(firstInstance, attributeNames, isNumeric);
		clusters.add(firstCluster);
	
		if (firstInstance.get(0)[instanceIndex] == null) {
			results.put(null, 0);
		} else {
			results.put(firstInstance.get(0)[instanceIndex].toString(), 0);
		}
		// update cluster instance count
		numInstancesInCluster.add(1);

		// create a cluster to serve as a combination of all the starting seeds
		Cluster combinedInstances =  new Cluster(attributeNames, isNumeric);
		combinedInstances.setDistanceMode(this.distanceMeasure);
		combinedInstances.addToCluster(firstInstance, attributeNames, isNumeric);

		for (int i = 1; i < numClusters; i++) {
			double simVal = 2;
			List<Object[]> bestInstance = null;
			int counter = 0;
			while (it.hasNext()) {
				List<Object[]> instance = it.next();
				// ignore instances already used
				if(results.containsKey(instance.get(0)[instanceIndex])) {
					continue;
				}
				double val = combinedInstances.getSimilarityForInstance(instance, attributeNames, isNumeric, instanceIndex);
				if (val < simVal) {
					bestInstance = instance;
				}
				if (val == 0) {
					break;
				}
				
				// logging
				if(counter % 100 == 0) {
					Configurator.setLevel(logger.getName(), Level.INFO);
					logger.info("Trying to determine intial point for cluster # " + i + ". Looped through " + counter + " instances trying to determine inital point");
					Configurator.setLevel(logger.getName(), Level.OFF);
				}
				
				counter++;
			}
			Configurator.setLevel(logger.getName(), Level.INFO);
			if (bestInstance == null) {
				throw new NullPointerException("bestInstance should not be null here.");
			}
			logger.info("Found new initial instance for cluster # " + i + " with instance = " + bestInstance.get(0)[instanceIndex]);
			Configurator.setLevel(logger.getName(), Level.OFF);
			
			// update combined cluster
			combinedInstances.addToCluster(bestInstance, attributeNames, isNumeric);

			// create new cluster and add as a seed
			Cluster newCluster = new Cluster(attributeNames, isNumeric);
			newCluster.setDistanceMode(this.distanceMeasure);
			newCluster.addToCluster(bestInstance, attributeNames, isNumeric);
			clusters.add(newCluster);
			results.put(bestInstance.get(0)[instanceIndex].toString(), i);
			// update cluster instance count
			numInstancesInCluster.add(1);

			// generate new iterator
			it = dataFrame.scaledUniqueIterator(instanceColumn, attributeNamesList);
		}
		Configurator.setLevel(logger.getName(), Level.INFO);
	}

	public int findBestClusterForInstance(List<Object[]> instance, String[] attributeNames, boolean[] isNumeric, int instanceIndex, List<Cluster> clusters) {
		int bestIndex = -1;
		double simVal = -1;
		int i = 0;
		for (; i < numClusters; i++) {
			double newSimVal = clusters.get(i).getSimilarityForInstance(instance, attributeNames, isNumeric, instanceIndex);
			if (newSimVal > simVal) {
				bestIndex = i;
				simVal = newSimVal;
			}
			if (simVal == 1) {
				break;
			}
		}
		return bestIndex;
	}

	public AlgorithmSingleColStore<Integer> getResults() {
		return this.results;
	}

	public List<Cluster> getClusters() {
		return this.clusters;
	}

	public void setAddToFrame(boolean addToFrame) {
		this.addToFrame = addToFrame;
	}
	
	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////
	//////////////////////Input Methods///////////////////////////
	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////
	
	private String getInstanceColumn() {
		GenRowStruct instanceGrs = this.store.getNoun(keysToGet[0]);
		String instanceCol = "";
		NounMetadata instanceColNoun;
		if (instanceGrs != null) {
			instanceColNoun = instanceGrs.getNoun(0);
			instanceCol = (String) instanceColNoun.getValue();
		} else {
			instanceColNoun = this.curRow.getNoun(0);
			instanceCol = (String) instanceColNoun.getValue();
		}

		return instanceCol;

	}
	
	private int getNumClusters() {
		GenRowStruct numClustersGrs = this.store.getNoun(keysToGet[1]);
		int numClusters = -1;
		NounMetadata numClustersNoun;
		if(numClustersGrs != null) {
			numClustersNoun = numClustersGrs.getNoun(0);
			numClusters = (int)numClustersNoun.getValue();
		} else {
			// else, we assume it is the first index in the current row --> RunClustering(instanceIndex, numClusters, selectors);
			numClustersNoun = this.curRow.getNoun(1);
			numClusters = (int)numClustersNoun.getValue();
		}
		return numClusters;

	}

	private List<String> getColumns() {
		// see if defined as indiviudal key
		List<String> retList = new ArrayList<>();
		retList.add(this.instanceColumn);
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[2]);
		if (columnGrs != null) {
			for (NounMetadata noun : columnGrs.vector) {
				String attribute = noun.getValue().toString();
				if (!(attribute.equals(this.instanceColumn))) {
					retList.add(attribute);
				}
			}
		} else {
			// else, we assume it is the second index in the current row
			// grab lengths 2-> end columns
			int rowLength = this.curRow.size();
			for (int i = 2; i < rowLength; i++) {
				NounMetadata colNoun = this.curRow.getNoun(i);
				String attribute = colNoun.getValue().toString();
				if (!(attribute.equals(this.instanceColumn))) {
					retList.add(attribute);
				}
			}
		}

		return retList;
	}
}

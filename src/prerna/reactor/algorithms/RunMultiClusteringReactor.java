package prerna.reactor.algorithms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.util.Cluster;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ArrayUtilityMethods;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class RunMultiClusteringReactor extends AbstractFrameReactor {
	
	private static final String CLASS_NAME = RunMultiClusteringReactor.class.getName();
	
	private static final String MIN_NUM_CLUSTERS = "minNumClusters";
	private static final String MAX_NUM_CLUSTERS = "maxNumClusters";

	// parameters for the algorithm
	private int instanceIndex;
	private int minNumClusters;
	private int maxNumClusters;
	private String instanceColumn;
	
	// values from data
	private String[] attributeNames;
	private List<String> attributeNamesList;
	private boolean[] isNumeric;

	/**
	 * RunMultiClustering(instance = column, minNumClusters = min#, maxNumCluster = max#, col1, col2....);
	 */
	
	public RunMultiClusteringReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.INSTANCE_KEY.getKey(), MIN_NUM_CLUSTERS, MAX_NUM_CLUSTERS, ReactorKeysEnum.ATTRIBUTES.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = this.getLogger(CLASS_NAME);
		ITableDataFrame dataFrame = getFrame();
		dataFrame.setLogger(logger);
		
		///////////////// start of initializing some stuff
		this.instanceIndex = 0;
		this.instanceColumn = getInstanceColumn();
		this.minNumClusters = getMinNumClusters();
		this.maxNumClusters = getMaxNumClusters();
		this.attributeNamesList = getColumns();
		this.attributeNames = attributeNamesList.toArray(new String[] {});

		this.isNumeric = new boolean[this.attributeNames.length];
		for (int i = 0; i < this.attributeNames.length; i++) {
			this.isNumeric[i] = dataFrame.isNumeric(this.attributeNames[i]);
		}

		int numInstances = dataFrame.getUniqueInstanceCount(instanceColumn);
		if (numInstances == 1) {
			throw new IllegalArgumentException("Instance column only contains one unqiue value.");
		}
		if (maxNumClusters > numInstances) {
			maxNumClusters = numInstances;
		}

		String[] allColNames = dataFrame.getColumnHeaders();
		String attributeName = instanceColumn;
		// to avoid adding columns with same name
		int counter = 0;
		String newColName = attributeName + "_CLUSTER";
		while (ArrayUtilityMethods.arrayContainsValue(allColNames, newColName)) {
			counter++;
			newColName = attributeName + "_CLUSTER_" + counter;
		}

		AlgorithmSingleColStore<Integer> results = null;
		if (minNumClusters != maxNumClusters) {
			results = runGoldenSelectionForNumberOfClusters(dataFrame, minNumClusters, maxNumClusters, logger);
		} else { 
			// usually occurs when there is too few data points
			results = runClusteringRoutine(dataFrame, minNumClusters, new ArrayList<Cluster>(), new HashMap<Integer, Double>(), logger);
		}
		// merge data back onto the frame
		AlgorithmMergeHelper.mergeSimpleAlgResult(dataFrame, instanceColumn, newColName, "NUMBER", results);

		// track GA data
//		UserTrackerFactory.getInstance().trackAnalyticsPixel(this.insight, "ClusterOptimization");

		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				dataFrame, 
				"ClusterOptimization", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		return new NounMetadata(dataFrame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
	}

	private AlgorithmSingleColStore<Integer> runGoldenSelectionForNumberOfClusters(ITableDataFrame data, int start, int end, Logger logger) {
		logger.info("Start execution of golden selection logic to determine best cluster...");
		AlgorithmSingleColStore<Integer> bestResults = null;
		int a = start;
		int b = end;
		double phi = (double) (1 + Math.sqrt(5)) / 2;
		int x1 = (int) Math.round((phi - 1)*a + (2-phi)*b);
		int x2 = (int) Math.round((2-phi)*a + (phi - 1)*b);

		double bestVal = -1;
		Map<Integer, Double> previousResults = new Hashtable<Integer, Double>();

		List<Cluster> startClusterList = new ArrayList<Cluster>();
		double startVal = 0;
		AlgorithmSingleColStore<Integer> startClusterResult = null;
		String errorMessage1 = null;
		try {
			startClusterResult = runClusteringRoutine(data, x1, startClusterList, previousResults, logger);
			startVal = computeClusteringScore(data, startClusterResult, startClusterList, previousResults, x1, logger);
			previousResults.put(x1, startVal);
		} catch (IllegalArgumentException ex) {
			// do nothing
			errorMessage1 = ex.getMessage();
		}

		List<Cluster> endClusterList = new ArrayList<Cluster>();
		double endVal = 0;
		AlgorithmSingleColStore<Integer> endClusterResult = null;
		String errorMessage2 = null;
		try {
			endClusterResult = runClusteringRoutine(data, x2, endClusterList, previousResults, logger);
			endVal = computeClusteringScore(data, endClusterResult, endClusterList, previousResults, x2, logger);
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
				startClusterResult = runClusteringRoutine(data, x1, startClusterList, previousResults, logger);
				startVal = computeClusteringScore(data, startClusterResult, startClusterList, previousResults, x1, logger);
				previousResults.put(x1, startVal);
			} catch (IllegalArgumentException ex) {
				// do nothing
			}

			try {
				endClusterList.clear();
				endClusterResult = runClusteringRoutine(data, x2, endClusterList, previousResults, logger);
				endVal = computeClusteringScore(data, endClusterResult, endClusterList, previousResults, x2, logger);
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

	private double computeClusteringScore(
			ITableDataFrame data, 
			AlgorithmSingleColStore<Integer> startClusterResult, 
			List<Cluster> clusters, 
			Map<Integer, Double> previousResults, 
			int numClusters,
			Logger logger) {
		if(previousResults.containsKey(numClusters)) {
			return previousResults.get(numClusters);
		}

		double innerClusterSimilairty = 0;
		Configurator.setLevel(logger.getName(), Level.OFF);
		Iterator<List<Object[]>> it = data.scaledUniqueIterator(instanceColumn, attributeNamesList);
		while(it.hasNext()) {
			List<Object[]> instance = it.next();
			Object instanceName = instance.get(0)[instanceIndex];
			int clusterIndex = startClusterResult.get(instanceName);
			double simVal = clusters.get(clusterIndex).getSimilarityForInstance(instance, this.attributeNames, isNumeric, this.instanceIndex);
			innerClusterSimilairty += simVal / clusters.get(clusterIndex).getNumInstances();
		}
		Configurator.setLevel(logger.getName(), Level.INFO);
		logger.info("For cluster # " + numClusters + ", inner cluster score = " + innerClusterSimilairty);
		
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
		logger.info("For cluster # " + numClusters + ", cluster-to-cluster similatiry score = " + clusterToClusterSimilarity);

		return innerClusterSimilairty/numClusters + clusterToClusterSimilarity / ( (double) (numClusters * (numClusters-1) /2));
	}

	private AlgorithmSingleColStore<Integer> runClusteringRoutine(
			ITableDataFrame dataframe, 
			int numClusters, 
			List<Cluster> clusters, 
			Map<Integer, Double> previousResults, 
			Logger logger) {
		if(previousResults.containsKey(numClusters)) {
			return null;
		}

		logger.info("Running clustering for " + numClusters + " number of clusters");
		//Need to execute this command
		//RunClustering(instance = column, numClusters = numCluters, columns = attributeNamesList);
		prerna.reactor.algorithms.RunClusteringReactor alg = new prerna.reactor.algorithms.RunClusteringReactor();
		
		// directly manipulate the noun store for the alg
		NounStore nounStore = alg.getNounStore();
		//set instance column
		GenRowStruct instanceColGRS = new GenRowStruct();
		instanceColGRS.addColumn(instanceColumn);
		nounStore.addNoun(ReactorKeysEnum.INSTANCE_KEY.getKey(), instanceColGRS);
		//set number of clusters
		GenRowStruct clusterNumGRS = new GenRowStruct();
		clusterNumGRS.addInteger(numClusters);
		nounStore.addNoun(ReactorKeysEnum.CLUSTER_KEY.getKey(), clusterNumGRS);
		//set attribute columns need to remove instance column
		GenRowStruct columnsGRS = new GenRowStruct();
		for(int i = 1; i < attributeNamesList.size(); i++) {
			columnsGRS.addColumn(attributeNamesList.get(i));	
		}
		nounStore.addNoun(ReactorKeysEnum.ATTRIBUTES.getKey(), columnsGRS);
		
		//set frame 
		alg.setInsight(this.insight);
		alg.setPixelPlanner(this.planner);
		alg.setAddToFrame(false);
		alg.execute();
		
		// keep track of the clusters to determine best clustering number
		clusters.addAll(alg.getClusters());
		// reset the logger on the frame
		// since the reactor will modify the frame reactor
		dataframe.setLogger(logger);
		return alg.getResults();
	}
	
	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////
	//////////////////////Input Methods///////////////////////////
	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////
	
	private String getInstanceColumn() {
		GenRowStruct instanceIndexGrs = this.store.getNoun(keysToGet[0]);
		String instanceIndex = "";
		NounMetadata instanceIndexNoun;
		if (instanceIndexGrs != null) {
			instanceIndexNoun = instanceIndexGrs.getNoun(0);
			instanceIndex = (String) instanceIndexNoun.getValue();
		} else {
			instanceIndexNoun = this.curRow.getNoun(0);
			instanceIndex = (String) instanceIndexNoun.getValue();
		}
		return instanceIndex;
	}

	private int getMinNumClusters() {
		// TODO: need to throw an error saying min number of clusters is required
		GenRowStruct minGrs = this.store.getNoun(MIN_NUM_CLUSTERS);
		int min = -1;
		NounMetadata minNoun;
		if (minGrs != null) {
			minNoun = minGrs.getNoun(0);
			min = ((Number) minNoun.getValue()).intValue();
		} else {
			minNoun = this.curRow.getNoun(1);
			min = ((Number) minNoun.getValue()).intValue();
		}
		return min;
	}

	private int getMaxNumClusters() {
		// TODO: need to throw an error saying max number of clusters is required
		GenRowStruct maxGrs = this.store.getNoun(MAX_NUM_CLUSTERS);
		int max = -1;
		NounMetadata maxNoun;
		if (maxGrs != null) {
			maxNoun = maxGrs.getNoun(0);
			max = ((Number) maxNoun.getValue()).intValue();
		} else {
			maxNoun = this.curRow.getNoun(2);
			max = ((Number) maxNoun.getValue()).intValue();
		}
		return max;
	}

	private List<String> getColumns() {
		// see if defined as indiviudal key
		List<String> retList = new ArrayList<String>();
		retList.add(this.instanceColumn);
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[3]);
		if (columnGrs != null) {
			for (NounMetadata noun : columnGrs.vector) {
				String attribute = noun.getValue().toString();
				if (!(attribute.equals(this.instanceColumn))) {
					retList.add(attribute);
				}
			}
		} else {
			int rowLength = this.curRow.size();
			for (int i = 3; i < rowLength; i++) {
				NounMetadata colNoun = this.curRow.getNoun(i);
				String attribute = colNoun.getValue().toString();
				if (!(attribute.equals(this.instanceColumn))) {
					retList.add(attribute);
				}
			}
		}
		return retList;
	}
	
///////////////////////// KEYS /////////////////////////////////////
	
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(MIN_NUM_CLUSTERS)) {
			return "The minimum number of clusters";
		} else if (key.equals(MAX_NUM_CLUSTERS)) {
			return "The maximum number of clusters";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}

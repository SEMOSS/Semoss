package prerna.reactor.algorithms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.special.Erf;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.unsupervised.outliers.KDTree;
import prerna.algorithm.learning.util.DuplicationReconciliation;
import prerna.algorithm.learning.util.DuplicationReconciliation.ReconciliationMode;
import prerna.math.StatisticsUtilityMethods;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ArrayUtilityMethods;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class RunLOFReactor extends AbstractFrameReactor {
	
	private static final String CLASS_NAME = RunLOFReactor.class.getName();

	private static final String K_NEIGHBORS = "kNeighbors";

	private String[] attributeNames;
	private List<String> attributeNamesList;

	private int instanceIndex;
	private String instanceColumn;
	private int numInstances; 
	private	int dimensions;

	private Map<String, DuplicationReconciliation> dups;

	private Object[] index;
	private int k;                    // How many neighbors to examine?
	public KDTree Tree;               // Points in dataset are put into KDTree to help find nearest neighbors
	private double[][] reachDistance; // This stores the reachDistance between 2 points. Not symmetrical! 
	private double[] kDistance;       // This stores the k-Distance for each point in dataset
	private double[] LRD;             // This stores the LRD for each point in dataset
	private double[] LOF;             // This stores the LOF for each point in dataset
	private double[] LOP;             // This stores the LOP for each point in dataset
	private double[][] dataFormatted; // This stores the formatted data from dataTable
	
	/*
	 * RunLOF(instance = col, subsetSize = 25, columns = col1, col2, ...);
	 */
	
	public RunLOFReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.INSTANCE_KEY.getKey(), K_NEIGHBORS, ReactorKeysEnum.ATTRIBUTES.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		// TODO: need to throw an error saying parameters are required
		this.instanceIndex = 0;
		this.k = getKNeighborhoodSize();
		this.instanceColumn = getInstanceColumn();
		this.attributeNamesList = getColumns();
		this.attributeNames = attributeNamesList.toArray(new String[] {});

		if (dups == null) {
			dups = new HashMap<String, DuplicationReconciliation>();
			for (int i = 0; i < attributeNames.length; i++) {
				dups.put(attributeNames[i], new DuplicationReconciliation(ReconciliationMode.MEAN));
			}
		}

		// get number of rows and cols
		Logger logger = this.getLogger(CLASS_NAME);
		ITableDataFrame dataFrame = getFrame();
		dataFrame.setLogger(logger);

		// get number of rows and cols
		this.numInstances = dataFrame.getUniqueInstanceCount(instanceColumn);

		if (k > numInstances) {
			throw new IllegalArgumentException("Number of unqiue instances: " + this.numInstances + ", is less than the selected K value: " + k + ".");
		}

		boolean[] isNumeric = new boolean[this.attributeNames.length];
		for (int i = 0; i < this.attributeNames.length; i++) {
			isNumeric[i] = dataFrame.isNumeric(this.attributeNames[i]);
			if (i != instanceIndex && !isNumeric[i]) {
				throw new IllegalArgumentException(
						"All columns must be numbers! \n" + "Column " + attributeNames[i] + " is not all numbers!");
			}
		}

		this.dimensions = this.attributeNames.length - 1;

		// Initialize arrays
		kDistance = new double[numInstances];
		LRD = new double[numInstances];
		LOF = new double[numInstances];
		LOP = new double[numInstances];
		reachDistance = new double[numInstances][numInstances];
		index = new Object[numInstances];
		dataFormatted = new double[numInstances][dimensions];

		this.Tree = new KDTree(dimensions);
		logger.info("Starting to process instances..");
		Configurator.setLevel(logger.getName(), Level.OFF);
		// This code flattens out instances, incase there are repeat appearances of an identifier
		Iterator<List<Object[]>> it = dataFrame.scaledUniqueIterator(instanceColumn, attributeNamesList);

		int numInstance = 0;
		while (it.hasNext()) {
			List<Object[]> instance = it.next();
			for (int i = 0; i < instance.size(); i++) {
				Object[] instanceRow = instance.get(i);
				for (int j = 0; j < attributeNames.length; j++) {
					if (j == instanceIndex) {
						continue;
					}
					dups.get(attributeNames[j]).addValue(instanceRow[j]);
				}
			}

			double[] recRow = new double[attributeNames.length - 1];
			int counter = 0;
			for (int i = 0; i < attributeNames.length; i++) {
				if (i == instanceIndex) {
					continue;
				}
				recRow[counter] = dups.get(attributeNames[i]).getReconciliatedValue();
				counter++;
				dups.get(attributeNames[i]).clearValue();
			}

			index[numInstance] = instance.get(0)[instanceIndex];
			dataFormatted[numInstance] = recRow;
			// add to tree!
			Tree.add(recRow, numInstance);
			
			// logging
			if(numInstance % 100 == 0) {
				Configurator.setLevel(logger.getName(), Level.INFO);
				logger.info("Finished execution for unique instance number = " + numInstance);
				Configurator.setLevel(logger.getName(), Level.OFF);
			}
			numInstance++;
		}
		Configurator.setLevel(logger.getName(), Level.INFO);
		logger.info("Done iterating ...");
		
		String[] allColNames = dataFrame.getColumnHeaders();
		String attributeName = instanceColumn;
		
		// run scoring algorithm
		logger.info("Start calculating score ...");
		AlgorithmSingleColStore<Double> results = score(k, logger);
		// add the data to the frame
		// to avoid adding columns with same name
		int counter = 0;
		String newColName = attributeName + "_LOF";
		while (ArrayUtilityMethods.arrayContainsValue(allColNames, newColName)) {
			counter++;
			newColName = attributeName + "_LOF_" + counter;
		}
		// merge data back onto the frame
		AlgorithmMergeHelper.mergeSimpleAlgResult(dataFrame, this.instanceColumn, newColName, "NUMBER", results);
		
		// track GA data
//		UserTrackerFactory.getInstance().trackAnalyticsPixel(this.insight, "LOF");
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				dataFrame, 
				"LOF", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		return new NounMetadata(dataFrame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
	}

	/**
	 * Calculate a list of LOP scores for each point in dataset
	 * @param k                     how many neighbors to examine
	 * @return						hashtable of Title and LOP scores
	 */
	public AlgorithmSingleColStore<Double> score(int k, Logger logger) {
		// 1. Get the K-distance for each point in our array 
		//    (using KDTree's nearest k-neighbors method)
		logger.info("Start generating K-Distance for each instance...");
		for (int i=0; i<numInstances; i++) {
			// returns IDs for each of k nearest neighbors
			Object[] neighborIDs = Tree.getNearestNeighbors(dataFormatted[i], k).returnData();
			kDistance[i] = dist(dataFormatted[i], dataFormatted[(int)neighborIDs[neighborIDs.length - 1]]);
			if(kDistance[i] == 0) {
				kDistance[i] = .0000001;
			}
			
			if(numInstances % 100 == 0) {
				logger.info("Finished K-Distance for instance number = " + i);
			}
		}
		logger.info("Done generating K-Distance for each instance...");

		// 2. Fill out ReachDistance[][]. 
		// ReachDistance from a->b is defined as the maximum of:
		//     A. Point a's kdistance OR
		//     B. The actual distance from a->b.
		logger.info("Start generating Reach-Distance for each instance...");
		for (int i=0; i<numInstances; i++) {
			double kdistance = kDistance[i];
			for (int j=0; j<this.numInstances; j++) {
				double distance = dist(dataFormatted[i], dataFormatted[j]);
				if (kdistance > distance) {
					this.reachDistance[i][j] = kdistance;
				} else {
					this.reachDistance[i][j] = distance; 
				}
			}
			
			if(numInstances % 100 == 0) {
				logger.info("Finished Reach-Distance for instance number = " + i);
			}
		}
		logger.info("Done generating Reach-Distance for each instance...");

		// 3. Fill out LRD array. 
		//    For a point A, LRD equals k divided by the sum of kdistances of point A's nearest k neighbors.
		logger.info("Start generating LRD Array for each instance...");
		for (int i=0; i<this.numInstances; i++) {
			// grab k nearest neighbors
			Object[] neighborIDs = Tree.getNearestNeighbors(dataFormatted[i], k).returnData();
			// sum up reachDistance for each of them
			double sum_reachDistance = 0;
			for (int j=0; j<neighborIDs.length; j++) {
				sum_reachDistance += reachDistance[i][(int)neighborIDs[j]];
			}
			LRD[i] = k/sum_reachDistance;
			
			if(numInstances % 100 == 0) {
				logger.info("Finished LRD calculation for instance number = " + i);
			}
		}
		logger.info("Done generating LRD for each instance...");

		// 4. Fill out LOF array.
		//    For a point A, LOF equals the average LRD value of A's k nearest neighbors,
		//    divided by A's LRD value.
		logger.info("Start generating LOF for each instance...");
		for (int i=0; i<this.numInstances; i++) {
			// grab k nearest neighbors
			Object[] neighborIDs = Tree.getNearestNeighbors(dataFormatted[i], k).returnData();
			// for each, sum up the ratio of LRD of the point to LRD of the neighbor
			double sum_ratioLRDs = 0;
			for (int j=0; j<neighborIDs.length; j++) {
				sum_ratioLRDs += LRD[(int)neighborIDs[j]]/LRD[i];
			}
			// LRD = k/sum_reachDistance
			LOF[i] = sum_ratioLRDs/k;
			
			if(numInstances % 100 == 0) {
				logger.info("Finished LOF calculation for instance number = " + i);
			}
		}
		logger.info("Done generating LOF for each instance...");

		// 5. Calculate LOP
		//    This transforms a LOF to a value between 0 and 1.
		//    See here for more information: http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.439.2035&rep=rep1&type=pdf
		logger.info("Start generating LOP for each instance...");
		double[] ploof = new double[numInstances];
		for (int i=0; i<this.numInstances; i++) {
			ploof[i] = LOF[i] - 1;
		}

		double stdev = StatisticsUtilityMethods.getSampleStandardDeviationIgnoringInfinity(ploof);
		double squareRoot2 = Math.sqrt(2);
		if (stdev == 0) {
			for (int i = 0; i < this.numInstances; i++) {
				if (Double.isInfinite(LOF[i])) { 
					LOP[i] = 1; 
				} else { 
					LOP[i] = 0; 
				}
				
				if(numInstances % 100 == 0) {
					logger.info("Finished LOP calculation for instance number = " + i);
				}
			}
		} else {
			for(int i = 0; i < numInstances; i++) {
				LOP[i] = Math.max(0, Erf.erf(ploof[i] / (stdev * squareRoot2)));
				
				if(numInstances % 100 == 0) {
					logger.info("Finished LOP calculation for instance number = " + i);
				}
			}
		}
		logger.info("Done generating LOP for each instance...");


		// 6. Insert all LOP values into a hashtable, with the unique ID as the
		// ID.
		AlgorithmSingleColStore<Double> results = new AlgorithmSingleColStore<Double>();
		for (int i = 0; i < this.numInstances; i++) {
			results.put(index[i], LOP[i]);
		}

		return results;
	}

	/**
	 * Calculate distance between two n-dimensional coordinates.
	 * 
	 * @param p1,
	 *            p2 two points in n dimensions.
	 * @return the scalar distance between them.
	 */
	private static final double dist(double[] p1, double[] p2) {
		double d = 0;
		double q = 0;
		for (int i = 0; i < p1.length; ++i) {
			d += (q = (p1[i] - p2[i])) * q;
		}
		return Math.sqrt(d);
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
	
	private int getKNeighborhoodSize() {
		GenRowStruct numClustersGrs = this.store.getNoun(K_NEIGHBORS);
		int kSize = -1;
		NounMetadata numClustersNoun;
		if(numClustersGrs != null) {
			numClustersNoun = numClustersGrs.getNoun(0);
			kSize = ((Number) numClustersNoun.getValue()).intValue();
		} else {
			numClustersNoun = this.curRow.getNoun(1);
			kSize = ((Number) numClustersNoun.getValue()).intValue();
		}
		return kSize;
	}

	private List<String> getColumns() {
		List<String> retList = new ArrayList<String>();
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
	
///////////////////////// KEYS /////////////////////////////////////
	
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(K_NEIGHBORS)) {
			return "The k neighborhood size";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}

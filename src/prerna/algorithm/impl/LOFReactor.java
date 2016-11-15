package prerna.algorithm.impl;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.special.Erf;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.unsupervised.outliers.KDTree;
import prerna.algorithm.learning.util.DuplicationReconciliation;
import prerna.algorithm.learning.util.DuplicationReconciliation.ReconciliationMode;
import prerna.math.StatisticsUtilityMethods;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.util.ArrayUtilityMethods;

public class LOFReactor extends MathReactor {

	public static final String K_NEIGHBORS = "k";
	public static final String INSTANCE_INDEX = "instanceIndex";
	public static final String DUPLICATION_RECONCILIATION	= "dupReconciliation";

	private String[] attributeNames;
	private List<String> attributeNamesList;

	private int instanceIndex;
	private int numInstances; 
	private	int dimensions;

	private String changedColumn;

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

	public LOFReactor() {
		setMathRoutine("LocalOutlierFactor");
	}
	
	public Iterator process() {
		modExpression();

		///////////////// start of initializing some stuff... needs to be put away somewhere else
		if(myStore.containsKey(PKQLEnum.MAP_OBJ)) {
			Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MAP_OBJ);
			if(options.containsKey(INSTANCE_INDEX.toUpperCase())) {
				this.instanceIndex = Integer.parseInt(options.get(INSTANCE_INDEX.toUpperCase()) + "");
			} else {
				// i think its a fair assumption that the first column written can be assumed to be the instance
				this.instanceIndex = 0;
			}

			if(options.containsKey(K_NEIGHBORS.toUpperCase())) {
				this.k = Integer.parseInt(options.get(K_NEIGHBORS.toUpperCase()) + "");
			} else {
				// TODO: throw an error
				this.k = 5;
			}

			//TODO: need to add something for the similarity metric
			// if it is mean/max/min

		} else {
			//TODO: need to throw an error saying parameters are required
			return null;
		}

		this.attributeNamesList = (List<String>)myStore.get(PKQLEnum.COL_DEF);
		this.attributeNames = attributeNamesList.toArray(new String[]{});

		if(dups == null) {
			dups = new HashMap<String, DuplicationReconciliation>();
			for(int i = 0; i < attributeNames.length; i++) {
				dups.put(attributeNames[i], new DuplicationReconciliation(ReconciliationMode.MEAN));
			}
		}

		// get number of rows and cols
		ITableDataFrame dataFrame = (ITableDataFrame)myStore.get("G");

		int numInstances = dataFrame.getUniqueInstanceCount(attributeNames[instanceIndex]); 
		// get number of rows and cols
		this.numInstances = dataFrame.getUniqueInstanceCount(attributeNames[instanceIndex]); 

		if(k > numInstances) {
			throw new IllegalArgumentException("Number of unqiue instances: " + numInstances + ", is less than the selected K value: " + k + ".");
		}

		boolean[] isNumeric = new boolean[this.attributeNames.length];
		for(int i = 0; i < this.attributeNames.length; i++) {
			isNumeric[i] = dataFrame.isNumeric(this.attributeNames[i]);
			if(i != instanceIndex && !isNumeric[i]) {
				throw new IllegalArgumentException("All columns must be numbers! \n"
						+ "Column "+ attributeNames[i] + " is not all numbers!");
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
		// This code flattens out instances, incase there are repeat appearances of an identifier
		Iterator<List<Object[]>> it = dataFrame.scaledUniqueIterator(attributeNames[instanceIndex], null);
		int numInstance = 0;
		while(it.hasNext()) {
			List<Object[]> instance = it.next();
			for(int i = 0; i < instance.size(); i++) {
				Object[] instanceRow = instance.get(i);
				for(int j = 0; j < attributeNames.length; j++) {
					if(j == instanceIndex) {
						continue;
					}
					dups.get(attributeNames[j]).addValue(instanceRow[j]);
				}
			}

			double[] recRow = new double[attributeNames.length - 1];
			int counter = 0;
			for(int i = 0; i < attributeNames.length; i++) {
				if(i == instanceIndex) {
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
			numInstance++;
		}

		// run scoring algorithm
		Hashtable<Object, Double> results = score(k);

		String[] allColNames = dataFrame.getColumnHeaders();
		String attributeName = attributeNames[instanceIndex];
		// to avoid adding columns with same name
		int counter = 0;
		this.changedColumn = attributeName + "_LOP";
		while(ArrayUtilityMethods.arrayContainsValue(allColNames, changedColumn)) {
			counter++;
			this.changedColumn = attributeName + "_LOP_" + counter;
		}

		Map<String, String> dataType = new HashMap<String, String>();
		dataType.put(changedColumn, "DOUBLE");
		dataFrame.connectTypes(attributeName, changedColumn, dataType);
		for(Object instance : results.keySet()) {
			Double val = results.get(instance);
			Map<String, Object> clean = new HashMap<String, Object>();
			clean.put(attributeName, instance);
			clean.put(changedColumn, val);

			dataFrame.addRelationship(clean);
		}
		
		myStore.put("STATUS",STATUS.SUCCESS);

		return null;
	}

	/**
	 * Calculate a list of LOP scores for each point in dataset
	 * @param k                     how many neighbors to examine
	 * @return						hashtable of Title and LOP scores
	 */
	public Hashtable<Object, Double> score(int k) {
		// 1. Get the K-distance for each point in our array 
		//    (using KDTree's nearest k-neighbors method)
		for (int i=0; i<numInstances; i++) {
			// returns IDs for each of k nearest neighbors
			Object[] neighborIDs = Tree.getNearestNeighbors(dataFormatted[i], k).returnData();
			kDistance[i] = dist(dataFormatted[i], dataFormatted[(int)neighborIDs[neighborIDs.length - 1]]);
			if(kDistance[i] == 0) {
				kDistance[i] = .0000001;
			}
		}

		// 2. Fill out ReachDistance[][]. 
		// ReachDistance from a->b is defined as the maximum of:
		//     A. Point a's kdistance OR
		//     B. The actual distance from a->b.
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
		}

		// 3. Fill out LRD array. 
		//    For a point A, LRD equals k divided by the sum of kdistances of point A's nearest k neighbors.
		//    

		for (int i=0; i<this.numInstances; i++) {
			// grab k nearest neighbors
			Object[] neighborIDs = Tree.getNearestNeighbors(dataFormatted[i], k).returnData();
			// sum up reachDistance for each of them
			double sum_reachDistance = 0;
			for (int j=0; j<neighborIDs.length; j++) {
				sum_reachDistance += reachDistance[i][(int)neighborIDs[j]];
			}
			LRD[i] = k/sum_reachDistance;
		}

		// 4. Fill out LOF array.
		//    For a point A, LOF equals the average LRD value of A's k nearest neighbors,
		//    divided by A's LRD value.
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
		}

		// 5. Calculate LOP
		//    This transforms a LOF to a value between 0 and 1.
		//    See here for more information: http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.439.2035&rep=rep1&type=pdf
		double[] ploof = new double[numInstances];
		for (int i=0; i<this.numInstances; i++)
			ploof[i] = LOF[i] - 1;

		double stdev = StatisticsUtilityMethods.getSampleStandardDeviationIgnoringInfinity(ploof);
		double squareRoot2 = Math.sqrt(2);
		if (stdev == 0) {
			for (int i = 0; i < this.numInstances; i++) {
				if (Double.isInfinite(LOF[i])) { 
					LOP[i] = 1; 
				} else { 
					LOP[i] = 0; 
				}
			}
		} else {
			for(int i = 0; i < numInstances; i++) {
				LOP[i] = Math.max(0, Erf.erf(ploof[i] / (stdev * squareRoot2)));
			}
		}

		// 6. Insert all LOP values into a hashtable, with the unique ID as the ID.
		Hashtable<Object, Double> results = new Hashtable<Object, Double>();
		for (int i=0; i<this.numInstances; i++) {
			results.put(index[i], LOP[i]);
		}

		return results;
	}

	/**
	 * Calculate distance between two n-dimensional coordinates.
	 * 
	 * @param p1, p2                two points in n dimensions.
	 * @return						the scalar distance between them.
	 */
	private static final double dist(double[] p1, double[] p2) {
		double d = 0;
		double q = 0;
		for (int i = 0; i < p1.length; ++i) {
			d += (q=(p1[i] - p2[i]))*q;
		}
		return Math.sqrt(d);
	}



}

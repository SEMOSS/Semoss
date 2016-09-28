package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.util.DuplicationReconciliation;
import prerna.algorithm.learning.util.DuplicationReconciliation.ReconciliationMode;
import prerna.algorithm.learning.util.InstanceSimilarity;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.util.ArrayUtilityMethods;

public class OutlierReactor extends MathReactor {

	public static final String INSTANCE_INDEX = "instanceIndex";
	public static final String NUM_SAMPLE_SIZE = "numSampleSize";
	public static final String NUMBER_OF_RUNS	= "numRuns";
	public static final String DUPLICATION_RECONCILIATION	= "dupReconciliation";

	private static Random random = new Random();

	private List<String> attributeNamesList;
	private String[] attributeNames;
	
	private String outlierColName;

	private int instanceIndex;
	private int numSubsetSize;                  				// How many neighbors to examine?
	private int numRuns;										// number of Runs to make
	private Map<String, DuplicationReconciliation> dups;

	private HashMap<Object, Double> results;    // This stores the FastDistance for each point in dataset
	
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
			
			if(options.containsKey(NUM_SAMPLE_SIZE.toUpperCase())) {
				this.numSubsetSize = Integer.parseInt(options.get(NUM_SAMPLE_SIZE.toUpperCase()) + "");
			} else {
				// TODO: need to throw an error saying number of clusters is required
			}
			
			if(options.containsKey(NUMBER_OF_RUNS.toUpperCase())) {
				this.numRuns = Integer.parseInt(options.get(NUMBER_OF_RUNS.toUpperCase()) + "");
			} else {
				// TODO: need to throw an error saying number of clusters is required
			}
			
			//TODO: need to add something for the similarity metric
			// if it is mean/max/min
			
		} else {
			//TODO: need to throw an error saying parameters are required
			return null;
		}
		
		this.attributeNamesList = (List<String>)myStore.get(PKQLEnum.COL_DEF);
		this.attributeNames = attributeNamesList.toArray(new String[]{});
		
		// get number of rows and cols
		ITableDataFrame dataFrame = (ITableDataFrame)myStore.get("G");

		int numInstances = dataFrame.getUniqueInstanceCount(attributeNames[instanceIndex]); 
		boolean[] isNumeric = new boolean[this.attributeNames.length];
		for(int i = 0; i < this.attributeNames.length; i++) {
			isNumeric[i] = dataFrame.isNumeric(this.attributeNames[i]);
		}
		
		if(numSubsetSize > numInstances) {
			throw new IllegalArgumentException("Subset size is larger than the number of instances.");
		}
		
		if(dups == null) {
			dups = new HashMap<String, DuplicationReconciliation>();
			for(int i = 0; i < attributeNames.length; i++) {
				if(isNumeric[i])
					dups.put(attributeNames[i], new DuplicationReconciliation(ReconciliationMode.MEAN));
			}
		}

		// make sure numSampleSize isn't too big.
		if (numSubsetSize > numInstances/3) {
			System.out.println("R (subset size) is too big!");
			numSubsetSize = numInstances/3;
		}

		int random_skip = numInstances/numSubsetSize; // won't be smaller than 3.

		results = new HashMap<Object, Double>();
		
		for (int k = 0; k < numRuns; k++) {
			// grab R random rows
			Iterator<List<Object[]>> it = this.getUniqueScaledData(attributeNames[instanceIndex], attributeNamesList, dataFrame);
			List<List<Object[]>> rSubset = new ArrayList<List<Object[]>>();
			for (int i = 0; i < numSubsetSize; i++) {
				// skip over a number between 0 and random_skip rows
				int skip = random.nextInt(random_skip);
				for (int j = 0; j < skip - 1; j++) {
					it.next();
				}
				rSubset.add(it.next());
			}
	
			// for row in dataTable, grab R random rows
			it = this.getUniqueScaledData(attributeNames[instanceIndex], attributeNamesList, dataFrame);
			while(it.hasNext()) {
				List<Object[]> instance = it.next();
				Object instanceName = instance.get(0)[instanceIndex];
				double maxSim = 0;
				for(int i= 0; i < numSubsetSize; i++) {
					List<Object[]> subsetInstance = rSubset.get(i);
					if(subsetInstance.get(0)[instanceIndex].equals(instance.get(0)[instanceIndex])) {
						continue;
					}
					double sim = InstanceSimilarity.getInstanceSimilarity(instance, subsetInstance, isNumeric, attributeNames, dups);
					if(maxSim < sim) {
						maxSim = sim;
					}
				}
				if (results.get(instanceName) == null) {
					results.put(instanceName, maxSim);
				}
				else {
					double oldVal = results.get(instanceName);
					results.put(instanceName, oldVal+maxSim);
				}
			}
		}

		String attributeName = attributeNames[instanceIndex];
		// to avoid adding columns with same name
		int counter = 0;
		this.outlierColName = attributeName + "_Outlier";
		while(ArrayUtilityMethods.arrayContainsValue(attributeNames, outlierColName)) {
			counter++;
			this.outlierColName = attributeName + "_Outlier_" + counter;
		}

		Map<String, String> dataType = new HashMap<String, String>();
		dataType.put(outlierColName, "DOUBLE");
		dataFrame.connectTypes(attributeName, outlierColName, dataType);
		for(Object instance : results.keySet()) {
			Double val = (numRuns-results.get(instance))/numRuns;

			Map<String, Object> clean = new HashMap<String, Object>();
			clean.put(attributeName, instance);
			clean.put(outlierColName, val);

			dataFrame.addRelationship(clean);
		}

		return null;
	}
	
}

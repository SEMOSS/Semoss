package prerna.reactor.algorithms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.util.DuplicationReconciliation;
import prerna.algorithm.learning.util.DuplicationReconciliation.ReconciliationMode;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.algorithm.learning.util.InstanceSimilarity;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ArrayUtilityMethods;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class RunOutlierReactor extends AbstractFrameReactor {
	
	private static final String CLASS_NAME = RunOutlierReactor.class.getName();

	private static final String NUMRUNS_KEY = "numRuns";
	private static final String SUBSETSIZE_KEY = "subsetSize";
	
	private static Random random = new Random();
	private Map<String, DuplicationReconciliation> dups;
	
	/**
	 * RunOutlier(instance = column, subsetSize = [numSubsetSize], numRuns = [numRuns], columns = attributeNamesList);
	 */ 
	
	public RunOutlierReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.INSTANCE_KEY.getKey(), SUBSETSIZE_KEY, NUMRUNS_KEY, ReactorKeysEnum.ATTRIBUTES.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = this.getLogger(CLASS_NAME);
		ITableDataFrame dataFrame = getFrame();
		dataFrame.setLogger(logger);
		
		AlgorithmSingleColStore<Double> results = new AlgorithmSingleColStore<Double>();
		// get inputs -> assume runOutlier(FRAME_COL, numSubsetSize, numRuns,
		// [FRAME_COL1, ... , FRAME_COLN])
		// or runOutlier(instance=[FRAME_COL], subsetSize = [numSubsetSize],
		// numRuns = [numRuns], columns = [FRAME_COL1, ... , FRAME_COLN]
		// initialize variables from pixel command
		int numSubsetSize = getSubsetSize();
		int numRuns = getNumRuns();
		String instanceColumn = getInstanceCol();
		List<String> attributeNamesList = getAttributeNames(instanceColumn);
		String[] attributeNames = attributeNamesList.toArray(new String[]{});
		int numAttributes = attributeNames.length;
		int instanceIndex = attributeNamesList.indexOf(instanceColumn);

		// running outliers algorithm
		int numInstances = dataFrame.getUniqueInstanceCount(instanceColumn);
		boolean[] isNumeric = new boolean[numAttributes];

		// check which attributes are numeric
		for (int i = 0; i < numAttributes; i++) {
			isNumeric[i] = dataFrame.isNumeric(attributeNamesList.get(i));
		}

		// make sure parameters are valid
		if (numSubsetSize > numInstances) {
			throw new IllegalArgumentException("Subset size is larger than the number of instances.");
		}

		if (dups == null) {
			dups = new HashMap<String, DuplicationReconciliation>();
			for (int i = 0; i < numAttributes; i++) {
				if (isNumeric[i])
					dups.put(attributeNamesList.get(i), new DuplicationReconciliation(ReconciliationMode.MEAN));
			}
		}

		// make sure numSampleSize isn't too big.
		if (numSubsetSize > numInstances / 3) {
			logger.info("R (subset size) is too big!");
			numSubsetSize = numInstances / 3;
		}

		int random_skip = numInstances / numSubsetSize;

		for (int k = 0; k < numRuns; k++) {
			Configurator.setLevel(logger.getName(), Level.INFO);
			logger.info("Starting execution for run # " + k);
			// grab R random rows
			logger.info("Determining random subset of initial instances");
			Configurator.setLevel(logger.getName(), Level.OFF);
			Iterator<List<Object[]>> it = dataFrame.scaledUniqueIterator(instanceColumn, attributeNamesList);
			List<List<Object[]>> rSubset = new ArrayList<List<Object[]>>();
			for (int i = 0; i < numSubsetSize; i++) {
				// skip over a number between 0 and random_skip rows
				int skip = random.nextInt(random_skip);
				for (int j = 0; j < skip - 1; j++) {
					it.next();
				}
				rSubset.add(it.next());
			}
			Configurator.setLevel(logger.getName(), Level.INFO);
			logger.info("Done determining initial instances");
			Configurator.setLevel(logger.getName(), Level.OFF);
			// for row in dataTable, grab R random rows
			int counter = 0;
			it = dataFrame.scaledUniqueIterator(instanceColumn, attributeNamesList);
			while (it.hasNext()) {
				List<Object[]> instance = it.next();
				Object instanceName = instance.get(0)[instanceIndex];
				double maxSim = 0;
				for (int i = 0; i < numSubsetSize; i++) {
					List<Object[]> subsetInstance = rSubset.get(i);
					Object subsetObj = subsetInstance.get(0)[instanceIndex];
					if (subsetObj != null && instanceName != null) {
						if (subsetInstance.get(0)[instanceIndex].equals(instance.get(0)[instanceIndex])) {
							continue;
						}
						double sim = InstanceSimilarity.getInstanceSimilarity(instance, subsetInstance, isNumeric, attributeNames, dups);
						if (maxSim < sim) {
							maxSim = sim;
						}
					}
				}
				// since similarity will be 1 when we have an exact match
				// and 0 when they are completely different
				// we want to assign the value to be 1 - the similarity
				double dissimarityValue = 1 - maxSim;
				if (results.get(instanceName) == null) {
					results.put(instanceName, dissimarityValue);
				} else {
					double oldVal = results.get(instanceName);
					// we dont want the number of runs to increase our value
					// so we will undo it
					oldVal *= k;
					double newVal = (oldVal + dissimarityValue) / (k+1);
					results.put(instanceName, newVal);
				}
				
				// logging
				if(counter % 100 == 0) {
					Configurator.setLevel(logger.getName(), Level.INFO);
					logger.info("Finished execution for run number = " + k + ", unique instance number = " + counter);
					Configurator.setLevel(logger.getName(), Level.OFF);
				}
				counter++;
			}
		}

		int counter = 0;
		String[] allColNames = dataFrame.getColumnHeaders();
		String newColName = instanceColumn + "_Outlier";
		while (ArrayUtilityMethods.arrayContainsValue(allColNames, newColName)) {
			counter++;
			newColName = instanceColumn + "_Outlier_" + counter;
		}
		// merge data back onto the frame
		AlgorithmMergeHelper.mergeSimpleAlgResult(dataFrame, instanceColumn, newColName, "NUMBER", results);

		// track GA data
//		UserTrackerFactory.getInstance().trackAnalyticsPixel(this.insight, "Outlier");
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				dataFrame, 
				"OutliersAlgorithm", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		return new NounMetadata(dataFrame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
	}

	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////
	//////////////////////Input Methods///////////////////////////
	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////

	private List<String> getAttributeNames(String instanceColumn) {
		List<String> retList = new ArrayList<String>();
		retList.add(instanceColumn); // always add instance column to attribute list at index 0
		
		// check if attributeList was entered with key or not
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[3]);
		if (columnGrs != null) {
			for (NounMetadata noun : columnGrs.vector) {
				String attribute = noun.getValue().toString();
				if (!(attribute.equals(instanceColumn))) {
					retList.add(attribute);
				}
			}
		} else {
			// grab lengths 3-> end columns
			int rowLength = this.curRow.size();
			for (int i = 3; i < rowLength; i++) {
				NounMetadata colNoun = this.curRow.getNoun(i);
				String attribute = colNoun.getValue().toString();
				if (!(attribute.equals(instanceColumn))) {
					retList.add(attribute);
				}
			}
		}

		return retList;
	}

	private String getInstanceCol() {
		// check if the instance column is entered with a key
		GenRowStruct instanceIndexGrs = this.store.getNoun(keysToGet[0]);
		String instanceCol = "";
		NounMetadata instanceIndexNoun;
		if (instanceIndexGrs != null) {
			instanceIndexNoun = instanceIndexGrs.getNoun(0);
			instanceCol = (String) instanceIndexNoun.getValue();
		} else {
			// else, we assume it is the zero index in the current row -->
			// runOutlier(FRAME_COL, numSubsetSize, numRuns, [FRAME_COL1, ... ,
			// FRAME_COLN]);
			instanceIndexNoun = this.curRow.getNoun(0);
			instanceCol = (String) instanceIndexNoun.getValue();
		}

		return instanceCol;
	}

	private int getNumRuns() {
		GenRowStruct numRunsGrs = this.store.getNoun(NUMRUNS_KEY);
		int numRuns = -1;
		NounMetadata numRunsNoun;
		if (numRunsGrs != null) {
			numRunsNoun = numRunsGrs.getNoun(0);
			numRuns = ((Number) numRunsNoun.getValue()).intValue();
		} else {
			// else, we assume it is the second index in the current row -->
			// runOutlier(FRAME_COL, numSubsetSize, numRuns, [FRAME_COL1, ... ,
			// FRAME_COLN]);
			numRunsNoun = this.curRow.getNoun(2);
			numRuns = ((Number) numRunsNoun.getValue()).intValue();
		}
		return numRuns;

	}

	private int getSubsetSize() {
		GenRowStruct subsetSizeGrs = this.store.getNoun(SUBSETSIZE_KEY);
		int subsetSize = -1;
		NounMetadata subsetSizeNoun;
		if (subsetSizeGrs != null) {
			subsetSizeNoun = subsetSizeGrs.getNoun(0);
			subsetSize = ((Number) subsetSizeNoun.getValue()).intValue();
		} else {
			// else, we assume it is the first index in the current row -->
			// runOutlier(FRAME_COL, numSubsetSize, numRuns, [FRAME_COL1, ... ,
			// FRAME_COLN]);
			subsetSizeNoun = this.curRow.getNoun(1);
			subsetSize = ((Number) subsetSizeNoun.getValue()).intValue();
		}
		return subsetSize;
	}
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(SUBSETSIZE_KEY)) {
			return "The subset size";
		} else if (key.equals(NUMRUNS_KEY)) {
			return "The number of runs";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}

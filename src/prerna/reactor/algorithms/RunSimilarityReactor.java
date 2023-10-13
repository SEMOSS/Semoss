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

public class RunSimilarityReactor extends AbstractFrameReactor {

	private static final String CLASS_NAME = RunSimilarityReactor.class.getName();

	private List<String> attributeNamesList;
	private String[] attributeNames;
	private int instanceIndex;
	private String instanceColumn;
	private Map<String, IClusterDistanceMode.DistanceMeasure> distanceMeasure;

	/**
	 * RunSimilarity(instance = column, columns = attributeNamesList);
	 */ 

	public RunSimilarityReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.INSTANCE_KEY.getKey(), ReactorKeysEnum.ATTRIBUTES.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = this.getLogger(CLASS_NAME);
		ITableDataFrame dataFrame = getFrame();
		dataFrame.setLogger(logger);
		
		AlgorithmSingleColStore<Double> results = new AlgorithmSingleColStore<Double>();

		//get inputs from pixel command 
		this.instanceColumn = getInstanceColumn();
		this.attributeNamesList = getAttributes(instanceColumn);
		this.attributeNames = this.attributeNamesList.toArray(new String[0]);
		this.instanceIndex = attributeNamesList.indexOf(this.instanceColumn);

		//store which attributes are numeric
		boolean[] isNumeric = new boolean[this.attributeNames.length];
		for (int i = 0; i < this.attributeNames.length; i++) {
			isNumeric[i] = dataFrame.isNumeric(this.attributeNames[i]);
		}

		// set the type of distance measure to be used for each numerical
		// property - default is using mean
		if (this.distanceMeasure == null) {
			distanceMeasure = new HashMap<String, IClusterDistanceMode.DistanceMeasure>();
			for (int i = 0; i < attributeNames.length; i++) {
				if (isNumeric[i]) {
					distanceMeasure.put(attributeNames[i], DistanceMeasure.MEAN);
				}
			}
		} else {
			for (int i = 0; i < attributeNames.length; i++) {
				if (!distanceMeasure.containsKey(attributeNames[i])) {
					distanceMeasure.put(attributeNames[i], DistanceMeasure.MEAN);
				}
			}
		}

		Cluster cluster = new Cluster(attributeNames, isNumeric);
		cluster.setDistanceMode(distanceMeasure);
		logger.info("Start generating cluster center for similarity of instances");
		Configurator.setLevel(logger.getName(), Level.OFF);
		generateClusterCenters(dataFrame, cluster, isNumeric);
		Configurator.setLevel(logger.getName(), Level.INFO);
		logger.info("Done generating cluster centers for similarity of instances");
		logger.info("Start generating similarity of instance to dataset center");
		getSimilarityValuesForInstances(dataFrame, cluster, results, isNumeric, logger);
		Configurator.setLevel(logger.getName(), Level.INFO);
		logger.info("Done generating similarity of instance to dataset center");

		String[] allColNames = dataFrame.getColumnHeaders();
		String attributeName = attributeNames[instanceIndex];

		// to avoid adding columns with same name
		int counter = 0;
		String newColName = attributeName + "_SIMILARITY";
		while (ArrayUtilityMethods.arrayContainsValue(allColNames, newColName)) {
			counter++;
			newColName = attributeName + "_SIMILARITY_" + counter;
		}
		// merge data back onto the frame
		AlgorithmMergeHelper.mergeSimpleAlgResult(dataFrame, this.instanceColumn, newColName, "NUMBER", results);
		
		// track GA data
//		UserTrackerFactory.getInstance().trackAnalyticsPixel(this.insight, "Similarity");
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				dataFrame, 
				"Similarity", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		//return successful frame change to FE
		return new NounMetadata(dataFrame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
	}

	private void generateClusterCenters(ITableDataFrame dataFrame, Cluster cluster, boolean[] isNumeric) {
		Iterator<List<Object[]>> it = dataFrame.scaledUniqueIterator(attributeNames[instanceIndex], attributeNamesList);
		while (it.hasNext()) {
			cluster.addToCluster(it.next(), attributeNames, isNumeric);
		}
	}

	public void getSimilarityValuesForInstances(
			ITableDataFrame dataFrame, 
			Cluster cluster, 
			AlgorithmSingleColStore<Double> results, 
			boolean[] isNumeric,
			Logger logger
			) {
		Configurator.setLevel(logger.getName(), Level.OFF);
		int counter = 0;
		Iterator<List<Object[]>> it = dataFrame.scaledUniqueIterator(attributeNames[instanceIndex], attributeNamesList);
		while (it.hasNext()) {
			List<Object[]> instance = it.next();
			String instanceName = (String) instance.get(0)[instanceIndex];
			double sim = cluster.getSimilarityForInstance(instance, attributeNames, isNumeric, instanceIndex);

			//add similarity output to results store
			results.put(instanceName, sim);
			
			if(counter % 100 == 0) {
				Configurator.setLevel(logger.getName(), Level.INFO);
				logger.info("Finished execution for unique instance number = " + counter);
				Configurator.setLevel(logger.getName(), Level.OFF);
			}
			counter++;
		}
		Configurator.setLevel(logger.getName(), Level.INFO);
	}

	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////
	//////////////////////Input Methods///////////////////////////
	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////

	private String getInstanceColumn() {
		//check if instance column was input with the key 
		GenRowStruct instanceIndexGrs = this.store.getNoun(keysToGet[0]);
		String instanceColumn = "";
		NounMetadata instanceColumnNoun;
		if (instanceIndexGrs != null) {
			instanceColumnNoun = instanceIndexGrs.getNoun(0);
			instanceColumn = (String) instanceColumnNoun.getValue();
		} else {
			//else assume the column is the zero index noun in the curRow
			instanceColumnNoun = this.curRow.getNoun(0);
			instanceColumn = (String) instanceColumnNoun.getValue();
		}

		return instanceColumn;

	}

	private List<String> getAttributes(String instanceColumn) {
		// see if defined as individual key
		List<String> retList = new ArrayList<String>();
		retList.add(instanceColumn);
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[1]);
		if (columnGrs != null) {
			for (NounMetadata noun : columnGrs.vector) {
				String attribute = noun.getValue().toString();
				if (!(attribute.equals(instanceColumn))) {
					retList.add(attribute);
				}
			}
		} else {
			int rowLength = this.curRow.size();
			for (int i = 1; i < rowLength; i++) {
				NounMetadata colNoun = this.curRow.getNoun(i);
				String attribute = colNoun.getValue().toString();
				if (!(attribute.equals(instanceColumn))) {
					retList.add(attribute);
				}
			}
		}

		return retList;
	}

}

package prerna.algorithm.impl;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Locale;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class ClusteringDataProcessor {

	static final Logger LOGGER = LogManager.getLogger(ClusteringDataProcessor.class.getName());

	// matrix to hold the instance numerical property values
	private Double[][] numericalMatrix;
	// matrix to hold the instance categorical property values
	private String[][] categoricalMatrix;

	// Hashtable containing the instance name as the key and the value being the row it's information is contained in the numerical and categorical Matrices
	private Hashtable<String, Integer> instanceHash = new Hashtable<String, Integer>();
	// list of all the categorical property names 
	private String[] categoryPropNames;
	// list of all the numerical property names
	private String[] numericalPropNames;
	// list the weights associated with each categorical property used to calculate the categorical similarity score
	private double[] weights; // has length the same as category prop names

	// static class used to calculate different distance measures for numerical similarity score
	private static DistanceCalculator disCalculator = new DistanceCalculator();

	// instance variables that must be defined for clustering to work
	// table containing all the information - assumes the first column contains the instance name
	private ArrayList<Object[]> masterTable;
	// array list containing the variable names for the entire query 
	private String[] varNames;

	public ClusteringDataProcessor(ArrayList<Object[]> masterTable, String[] varNames) {
		this.masterTable = masterTable;
		this.varNames = varNames;
		
		formatDuplicateResults();
		// these methods must be called for the similarity score to be computed 
		processMasterTable();
		calculateWeights();
	}
	
	public ArrayList<Object[]> getMasterTable() {
		return masterTable;
	}
	public void setMasterTable(ArrayList<Object[]> masterTable) {
		this.masterTable = masterTable;
	}
	public String[] getVarNames() {
		return varNames;
	}
	public void setVarNames(String[] varNames) {
		this.varNames = varNames;
	}
	
	//indexing used for visualization
	private int[] totalNumericalPropIndices;
	private Integer[] categoryPropIndices; 

	public int[] getTotalNumericalPropIndices() {
		return totalNumericalPropIndices;
	}
	public Integer[] getCategoryPropIndices() {
		if(categoryPropIndices != null) {
			return categoryPropIndices.clone();
		} else {
			return null;
		}
	}
	public Hashtable<String, Integer> getInstanceHash() {
		return (Hashtable<String, Integer>) instanceHash.clone();
	}
	public Double[][] getNumericalMatrix() {
		if(numericalMatrix != null) {
			return numericalMatrix.clone();
		} else {
			return null;
		}
	}
	public String[][] getCategoricalMatrix() {
		if(categoricalMatrix != null) {
			return categoricalMatrix.clone();
		} else {
			return null;
		}
	}
	public String[] getCategoryPropNames() {
		if(categoryPropNames != null) {
			return categoryPropNames.clone();
		} else {
			return null;
		}
	}
	public String[] getNumericalPropNames() {
		if(numericalPropNames != null) {
			return numericalPropNames.clone();
		} else {
			return null;
		}
	}

	/**
	 * Calculates the similarity score between an instance and a cluster
	 * @param dataIdx						The index corresponding to the instance row location in the numerical and categorical matrices
	 * @param clusterIdx					The index of the specific cluster we are looking to add the instance into
	 * @param allNumericalClusterInfo		Contains the numerical cluster information for all the different clusters
	 * @param categoryClusterInfo			The categorical cluster information for the cluster we are looking to add the instance into
	 * @return								The similarity score between the instance and the cluster
	 * @throws IllegalArgumentException		The number of rows for the two arrays being compared to calculate Euclidian distance are of different length
	 */
	public Double getSimilarityScore(int dataIdx, int clusterIdx, Double[][] allNumericalClusterInfo, ArrayList<Hashtable<String, Integer>> categoryClusterInfo) throws IllegalArgumentException {
		double numericalSimilarity = (double) 0;
		double categorySimilarity = (double) 0;
		
		if(numericalMatrix != null) {
			Double[] instanceNumericalInfo = numericalMatrix[dataIdx];
			numericalSimilarity = calcuateNumericalSimilarity(clusterIdx, instanceNumericalInfo, allNumericalClusterInfo);
		}
		if(categoricalMatrix != null) {
			String[] instaceCategoricalInfo = categoricalMatrix[dataIdx];
			categorySimilarity = calculateCategorySimilarity(instaceCategoricalInfo, categoryClusterInfo);
		}

		return numericalSimilarity + categorySimilarity;
	}

	/**
	 * Calculates the similarity score for the categorical entries
	 * @param instaceCategoricalInfo	The categorical information for the specific instance
	 * @param categoryClusterInfo		The categorical cluster information for the cluster we are looking to add the instance into
	 * @return 							The similarity score associated with the categorical properties
	 */
	private double calculateCategorySimilarity(String[] instaceCategoricalInfo, ArrayList<Hashtable<String, Integer>> categoryClusterInfo) 
	{
		double categorySimilarity = 0;
		// loop through all the categorical properties (each weight corresponds to one categorical property)
		for(int i = 0; i < weights.length; i++) {
			// sumProperties contains the total number of instances for the property
			double sumProperties = 0;
			Hashtable<String, Integer> propertyHash = categoryClusterInfo.get(i);
			Set<String> propKeySet = propertyHash.keySet();
			for(String propName : propKeySet) {
				sumProperties += propertyHash.get(propName);
			}
			// numOccuranceInCluster contains the number of instances in the cluster that contain the same prop value as the instance
			int numOccuranceInCluster = 0;
			if(propertyHash.get(instaceCategoricalInfo[i]) != null) {
				numOccuranceInCluster = propertyHash.get(instaceCategoricalInfo[i]);
			}
			categorySimilarity += weights[i] * numOccuranceInCluster / sumProperties;
		}
		// categorical similarity value is normalized based on the ratio of categorical variables to the total number of variables
		double coeff = 1.0 * categoryPropNames.length / varNames.length;

//		logger.info("Calculated similarity score for categories: " + coeff * categorySimilarity);
		return coeff * categorySimilarity;
	}

	/**
	 * Calculates the similarity score for the numerical entries
	 * @param clusterIdx					The index of the specific cluster we are looking to add the instance into
	 * @param instanceNumericalInfo			The numerical information for the specific index
	 * @param allNumericalClusterInfo		Contains the numerical cluster information for all the different clusters
	 * @return								The similarity score value associated with the numerical properties
	 * @throws IllegalArgumentException		The number of rows for the two arrays being compared to calculate Euclidian distance are of different length 
	 */
	private Double calcuateNumericalSimilarity(int clusterIdx, Double[] instanceNumericalInfo, Double[][] allNumericalClusterInfo) throws IllegalArgumentException 
	{
		double numericalSimilarity = 0;
		double distanceNormalization = 0;
		
		// categorical similarity value is normalized based on the ratio of categorical variables to the total number of variables
		double coeff = 1.0 * numericalPropNames.length / varNames.length;
		
		int numClusters = allNumericalClusterInfo.length;
		double[] distance = new double[numClusters];

		// generate array of distances between the instance and the cluster for all numerical properties
		for(int i = 0; i < allNumericalClusterInfo.length; i++) {
			Double[] numericalClusterInfo = allNumericalClusterInfo[i];
			// deal with null values
			// set the values to be the same for this property such that the distance becomes 0
			Double[] copyInstanceNumericalInfo = new Double[instanceNumericalInfo.length];
			for(int j = 0; j < numericalClusterInfo.length; j++) {
				if(numericalClusterInfo[j] == null) {
					copyInstanceNumericalInfo = instanceNumericalInfo;
					if(copyInstanceNumericalInfo[j] == null) {
						numericalClusterInfo[j] = new Double(0);
						copyInstanceNumericalInfo[j] = new Double(0);
					} else {
						numericalClusterInfo[j] = copyInstanceNumericalInfo[j];
					}
				} else if(copyInstanceNumericalInfo[j] == null) {
					copyInstanceNumericalInfo[j] = numericalClusterInfo[j];
				} else {
					copyInstanceNumericalInfo[j] = numericalClusterInfo[j];
				}
			}
			distance[i] = disCalculator.calculateEuclidianDistance(copyInstanceNumericalInfo, numericalClusterInfo);
			distanceNormalization += distance[i];
		}
		
		if(distanceNormalization == 0) {
			return coeff; // values are exactly the same
		}
		// normalize all the distances to avoid distortion
		for(int i = 0; i < distance.length; i++) {
			distance[i] /= distanceNormalization;
		}
		// distance of instance from cluster is a value between 0 and 1
		double distanceFromCluster = Math.exp(-0.5 * distance[clusterIdx]);
		double sumDistanceFromCluster = 0;
		for(int i = 0; i < distance.length; i++) {
			sumDistanceFromCluster += Math.exp(-0.5 * distance[i]);
		}
		// 
		numericalSimilarity = distanceFromCluster/sumDistanceFromCluster;

//		logger.info("Calculated similarity score for numerical properties: " + coeff * numericalSimilarity);
		return coeff * numericalSimilarity;
	}

	/**
	 * Generate weights for categorical similarity matrix
	 */
	private void calculateWeights() {
		ArrayList<Hashtable<String, Integer>> trackPropOccurance = getPropOccurance();
		double[] entropyArr = calculateEntropy(trackPropOccurance);
		int numWeights = entropyArr.length;
		weights = new double[numWeights];

		double totalEntropy = 0;
		for(int i = 0; i < numWeights; i++) {
			totalEntropy += entropyArr[i];
		}

		int counter = 0;
		for(int i = 0; i < numWeights; i++) {
			weights[counter] = (entropyArr[i] / totalEntropy);
			counter++;
		}

		// output category and weight to console
		for(int i = 0; i < weights.length; i++) {
			LOGGER.info("Category " + categoryPropNames[i] + " has weight " + weights[i]);
		}
	}

	/**
	 * Generate entropy array for each category to create weights
	 * @param trackPropOccurance	A list containing a hashtable that stores all the different instances of a given property
	 * @return						A list containing the entropy for each categorical property
	 */
	private double[] calculateEntropy(ArrayList<Hashtable<String, Integer>> trackPropOccurance) {
		int numWeights = trackPropOccurance.size();
		double[] entropyArr = new double[numWeights];
		int i;
		for(i = 0; i < numWeights; i++) {
			Hashtable<String, Integer> columnInformation = trackPropOccurance.get(i);
			ArrayList<Integer> columnPropInstanceCountArr = new ArrayList<Integer>();

			int totalCountOfPropInstances = 0;
			int unqiueCountOfPropInstances = 0;
			for(String columnProp : columnInformation.keySet()) {
				columnPropInstanceCountArr.add(columnInformation.get(columnProp));
				totalCountOfPropInstances += columnInformation.get(columnProp);
				unqiueCountOfPropInstances++;
			}

			double sumProb = 0;
			int columnPropInstanceSize = columnPropInstanceCountArr.size();
			for(int j = 0; j < columnPropInstanceSize; j++) {
				Integer propCount = columnPropInstanceCountArr.get(j);
				Double probability = (double) ( 1.0 * propCount / totalCountOfPropInstances);
				sumProb += probability * logBase2(probability);
			}
			
			Double entropy = sumProb / unqiueCountOfPropInstances;
			entropyArr[i] = entropy;
		}
		
		return entropyArr;
	}

	/**
	 * Generate occurrence of instance categorical properties to calculate entropy
	 * @return	A list containing a hashtable that stores all the different instances of a given property
	 */
	private ArrayList<Hashtable<String, Integer>> getPropOccurance() {
		ArrayList<Hashtable<String, Integer>> trackPropOccuranceArr = new ArrayList<Hashtable<String, Integer>>();

		if(categoricalMatrix != null)
		{
			for(int i = 0; i < categoricalMatrix[0].length; i++) {
				trackPropOccuranceArr.add(new Hashtable<String, Integer>());
			}
	
			for(String[] results : categoricalMatrix) {
				for(int i = 0; i < results.length; i++) {		
					Hashtable<String, Integer> columnInformationHash = trackPropOccuranceArr.get(i);
					if(columnInformationHash.isEmpty()) {
						columnInformationHash.put(results[i], 1);
						//logger.info("Category " + categoryPropNames.get(i) + "with instance " + results[i] + " occurred 1 time.");
					} else {
						if(columnInformationHash.get(results[i]) == null) {
							columnInformationHash.put(results[i], 1);
							//logger.info("Category " + categoryPropNames.get(i) + "with instance " + results[i] + " occurred 1 time.");
						} else {
							int currCount = columnInformationHash.get(results[i]);
							columnInformationHash.put(results[i], ++currCount);
							//logger.info("Category " + categoryPropNames.get(i) + "with instance " + results[i] + " has occured " + currCount + " times.");
						}
					}
				}
			}
		}

		return trackPropOccuranceArr;
	}

	/**
	 * Processes through the masterTable and determines which columns are numerical and which are categorical
	 */
	private void processMasterTable() {
		categoryPropNames = new String[varNames.length - 1];
		categoryPropIndices = new Integer[varNames.length - 1];
		numericalPropNames = new String[varNames.length - 1];
		Integer[] numericalPropIndices = new Integer[varNames.length - 1];
		Integer[] dateTypeIndices = new Integer[varNames.length - 1];
		Integer[] simpleDateTypeIndices = new Integer[varNames.length - 1];
		
		int categoryPropNamesCounter = 0;
		int numericalPropNamesCounter = 0;
		int dateTypeIndicesCounter = 0;
		int simpleDateTypeIndicesCounter = 0;
		
		//iterate through columns
		for(int j = 0; j < varNames.length; j++) {
			String type = "";
			if(j != 0) {
				//iterate through rows
				int numCategorical = 0;
				int numNumerical = 0;
				for(int i = 0; i < masterTable.size(); i++) {
					Object[] dataRow = masterTable.get(i);
					if(dataRow[j] != null && !dataRow[j].toString().equals("")) {
						String colEntryAsString = dataRow[j].toString();
						type = processType(colEntryAsString);
						if(type.equals("STRING")) {
							numCategorical++;
						} else {
							numNumerical++;
						}
					}
				}
				if(numCategorical > numNumerical) {
					categoryPropNames[categoryPropNamesCounter] = varNames[j];
					categoryPropIndices[categoryPropNamesCounter] = j;
					categoryPropNamesCounter++;
					LOGGER.info("Found " + varNames[j] + " to be a categorical data column");
				} else {
					numericalPropNames[numericalPropNamesCounter] = varNames[j];
					numericalPropIndices[numericalPropNamesCounter] = j;
					numericalPropNamesCounter++;
					LOGGER.info("Found " + varNames[j] + " to be a numerical data column");
					if(type.equals("DATE")){
						dateTypeIndices[dateTypeIndicesCounter] = j;
						dateTypeIndicesCounter++;
					} else if(type.equals("SIMPLEDATE")){
						simpleDateTypeIndices[simpleDateTypeIndicesCounter] = j;
						simpleDateTypeIndicesCounter++;
					}
				}
			} else {
				// get list of all instances in the order they are being stored
				for(int i = 0; i < masterTable.size(); i++) {
					Object[] dataRow = masterTable.get(i);
					String colEntry = dataRow[j].toString();
					instanceHash.put(colEntry, i);
				}
			}
		}
		categoryPropNames = (String[]) ArrayUtilityMethods.trimEmptyValues(categoryPropNames);
		categoryPropIndices = (Integer[]) ArrayUtilityMethods.trimEmptyValues(categoryPropIndices);
		numericalPropNames = (String[]) ArrayUtilityMethods.trimEmptyValues(numericalPropNames);
		numericalPropIndices = (Integer[]) ArrayUtilityMethods.trimEmptyValues(numericalPropIndices);
		dateTypeIndices = (Integer[]) ArrayUtilityMethods.trimEmptyValues(dateTypeIndices);
		simpleDateTypeIndices = (Integer[]) ArrayUtilityMethods.trimEmptyValues(simpleDateTypeIndices);
		
		constructMatrices(categoryPropIndices, numericalPropIndices, dateTypeIndices, simpleDateTypeIndices);
		
		int numericSize = 0;
		if(numericalPropIndices != null) {
			numericSize = numericalPropIndices.length;
		}
		int dateSize = 0;
		if(dateTypeIndices != null) {
			dateSize = dateTypeIndices.length;
		}
		int simpleDateSize = 0;
		if(simpleDateTypeIndices != null) {
			simpleDateSize = simpleDateTypeIndices.length;
		}
		totalNumericalPropIndices = new int[numericSize + dateSize + simpleDateSize];
		int i = 0;
		for(i = 0; i < numericSize; i++) {
			totalNumericalPropIndices[i] = numericalPropIndices[i];
		}
		for(i = 0; i < dateSize; i++) {
			totalNumericalPropIndices[i+numericSize] = dateTypeIndices[i];
		}
		for(i = 0; i < simpleDateSize; i++) {
			totalNumericalPropIndices[i+numericSize+dateSize] = dateTypeIndices[i];
		}
	}

	/**
	 * Build the categorical and numerical matrices based on the master table and which columns are categories or numbers/dates
	 * @param categoryPropIndices		The indices in the masterTable that contains the categorical properties
	 * @param numericalPropIndices		The indices in the masterTable that contains the numerical properties
	 * @param dateTypeIndices			The indices in the masterTable that contain date numerical properties
	 * @param simpleDateTypeIndices 
	 */
	private void constructMatrices(Integer[] categoryPropIndices, Integer[] numericalPropIndices, Integer[] dateTypeIndices, Integer[] simpleDateTypeIndices) {
		
		if(numericalPropIndices != null) {
			numericalMatrix = new Double[masterTable.size()][numericalPropIndices.length];
		}
		if(categoryPropIndices != null) {
			categoricalMatrix = new String[masterTable.size()][categoryPropIndices.length];
		}
		
		for(int row = 0; row < masterTable.size(); row++) {
			int counter = 0;

			Object[] dataRow = masterTable.get(row);
			if(numericalPropIndices != null)
			{
				for(Integer idx : numericalPropIndices) {
					if(dataRow[idx] != null && !dataRow[idx].toString().equals("")) {
						try {
							if(dateTypeIndices != null) {
								if(ArrayUtilityMethods.arrayContainsValue(dateTypeIndices, idx)) {
									SimpleDateFormat formatLongDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
									formatLongDate.setLenient(true);
									Date valDate = formatLongDate.parse(dataRow[idx].toString());
									Long dateAsLong = valDate.getTime();
									numericalMatrix[row][counter] = dateAsLong.doubleValue();
								} 
							} else if(simpleDateTypeIndices != null) {
								if(ArrayUtilityMethods.arrayContainsValue(simpleDateTypeIndices, idx)){
									SimpleDateFormat formatLongDate = new SimpleDateFormat("mm/dd/yyyy", Locale.US);
									formatLongDate.setLenient(true);
									Date valDate = formatLongDate.parse(dataRow[idx].toString());
									Long dateAsLong = valDate.getTime();
									numericalMatrix[row][counter] = dateAsLong.doubleValue();
								}
							} else {
								numericalMatrix[row][counter] = (Double) dataRow[idx];
							}
						} catch (ParseException e) {
							LOGGER.error("Column Variable " + numericalPropNames[idx] + " was found to be a numerical (date) property but had a value of " + dataRow[idx]);
						} catch (ClassCastException e) {
							LOGGER.error("Column Variable " + numericalPropNames[idx] + " was found to be a numerical property but had a value of " + dataRow[idx]);
						}
					}
					// default values are null in new Double[][]
	//				else {
	//					numericalMatrix[row][counter] = null;
	//				} 
					counter++;
				}
			}

			counter = 0;
			if(categoryPropIndices != null) 
			{
				for(Integer idx : categoryPropIndices) {
					categoricalMatrix[row][counter] = (String) dataRow[idx].toString();
					counter++;
				}
			}
		}
	}

	/**
	 * Determines the type of a given value
	 * @param s		The value to determine the type off
	 * @return		The type of the value
	 */
	private static String processType(String s) {
		
		boolean isDouble = true;
		try {
			Double.parseDouble(s);
		} catch(NumberFormatException e) {
			isDouble = false;
		}

		if(isDouble) {
			return ("DOUBLE");
		}

		// will analyze date types as numerical data
		Boolean isLongDate = true;
		SimpleDateFormat formatLongDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
		Date longdate = null;
		try {
			formatLongDate.setLenient(true);
			longdate  = formatLongDate.parse(s);
		} catch (ParseException e) {
			isLongDate = false;
		}
		if(isLongDate){
			return ("DATE");
		}

		Boolean isSimpleDate = true;
		SimpleDateFormat formatSimpleDate = new SimpleDateFormat("mm/dd/yyyy", Locale.US);
		Date simpleDate = null;
		try {
			formatSimpleDate.setLenient(true);
			simpleDate  = formatSimpleDate.parse(s);
		} catch (ParseException e) {
			isSimpleDate = false;
		}
		if(isSimpleDate){
			return ("SIMPLEDATE");
		}

		return ("STRING");
	}

	/**
	 * Generate the log base 2 of a given input
	 * @param x		The value to take the log base 2 off
	 * @return		The log base 2 of the value inputed
	 */
	private double logBase2(double x) {
		return Math.log(x) / Math.log(2);
	}
	
	//TODO: generic
	private void formatDuplicateResults() {
		int instanceCounter = 0;
		String previousInstance = "";

		int i;
		int numRows = masterTable.size();
		int numCols = masterTable.get(0).length;
		String[] uniquePropNames = new String[50];
		String[] instances = new String[50];
		int counter = 0;
		for(i = 0; i < numCols; i++ ) {
			String previousProp = "";
			int j;
			for(j = 0; j < numRows; j++) {
				Object[] row = masterTable.get(j);

				if(i == 0) {
					if(previousInstance.equals(row[i])){
						continue;
					} else {
						previousInstance = row[i].toString();
						try{
							instances[instanceCounter] = previousInstance;
						} catch(IndexOutOfBoundsException ex) {
							instances = (String[]) ArrayUtilityMethods.resizeArray(instances, 2);
							instances[instanceCounter] = previousInstance;
						}
						instanceCounter++;
					}
				} else {
					if(previousProp.equals(row[i])) {
						continue;
					} else {
						previousProp = row[i].toString();
						try{
							uniquePropNames[counter] = row[i].toString();
						} catch(IndexOutOfBoundsException ex) {
							uniquePropNames = (String[]) ArrayUtilityMethods.resizeArray(uniquePropNames, 2);
							uniquePropNames[counter] = row[i].toString();
						}
						counter++;
					}
				}
			}
		}
		
		instances = (String[]) ArrayUtilityMethods.removeAllNulls(instances);
		int numInstances = instances.length;
		if(numInstances == numRows) {
			return;
		}
		ArrayList<Object[]> retMasterTable = new ArrayList<Object[]>();
		
		uniquePropNames = (String[]) ArrayUtilityMethods.removeAllNulls(uniquePropNames);
		uniquePropNames = ArrayUtilityMethods.getUniqueArray(uniquePropNames);
		varNames = uniquePropNames;
		
		int newNumCols = uniquePropNames.length;
		for(i = 0; i < numInstances; i++) {
			Object[] newRow = new Object[newNumCols + 1];
			newRow[0] = instances[i];
			retMasterTable.add(newRow);
		}
		
		OUTER: for(i = 0; i < numRows; i++) {
			Object[] row = masterTable.get(i);
			int j;
			for(j = 0; j < numInstances; j++) {
				Object[] newRow = retMasterTable.get(j);
				if(row[0].equals(newRow[0])){
					int k;
					INNER: for(k = 0; k < newNumCols; k++) {
						int l;
						for(l = 1; l < row.length; l++) {
							if(uniquePropNames[k].toString().equals(row[l].toString())){
								newRow[k+1] = "Yes";
								continue INNER;
							} else {
								newRow[k+1] = "No";
							}
						}
					}
					continue OUTER;
				}
			}
		}
		
		masterTable = retMasterTable;
	}
	
	/**
	 * 
	 * @param clusterIdx1				The index for the cluster we are observing
	 * @param clusterIdx2				The index for the cluster we are comparing the observed cluster to
	 * @param clusterNumberMatrix		All the numerical properties
	 * @param clusterCategoryMatrix		All the categorical properties
	 * @return
	 */
	public double calculateClusterToClusterSimilarity(int clusterIdx1, int clusterIdx2, Double[][] clusterNumberMatrix, ArrayList<ArrayList<Hashtable<String,Integer>>> clusterCategoryMatrix){
		double numericSimilarity = 0;
		double categoricalSimilarity = 0;
		
		if(clusterNumberMatrix != null) {
			Double[] numericClusterInfo1 = clusterNumberMatrix[clusterIdx1];
			numericSimilarity = calcuateNumericalSimilarity(clusterIdx2, numericClusterInfo1, clusterNumberMatrix);
		}
		
		if(clusterCategoryMatrix != null) {
			ArrayList<Hashtable<String, Integer>> categoricalClusterInfo1 = clusterCategoryMatrix.get(clusterIdx1);
			ArrayList<Hashtable<String, Integer>> categoricalClusterInfo2 = clusterCategoryMatrix.get(clusterIdx2);
			
			categoricalSimilarity = calculateClusterCategoricalSimilarity(categoricalClusterInfo1, categoricalClusterInfo2);
		}
		
		return numericSimilarity + categoricalSimilarity;
	}
	
	private double calculateClusterCategoricalSimilarity(ArrayList<Hashtable<String, Integer>> categoricalClusterInfo1, ArrayList<Hashtable<String, Integer>> categoricalClusterInfo2) {
		double coeff = 1.0 * categoryPropNames.length / varNames.length;
		double similarityScore = 0;

		int i;
		int size = categoricalClusterInfo1.size();
		// loop through all properties
		for(i = 0; i < size; i++) {
			// for specific property
			Hashtable<String, Integer> clusterInfo1 = categoricalClusterInfo1.get(i);
			Hashtable<String, Integer> clusterInfo2 = categoricalClusterInfo2.get(i);
			
			int normalizationCount1 = 0;
			for(String propInstance : clusterInfo1.keySet()) {
				normalizationCount1 += clusterInfo1.get(propInstance);
			}
			int normalizationCount2 = 0;
			for(String propInstance : clusterInfo2.keySet()) {
				normalizationCount2 += clusterInfo2.get(propInstance);
			}
			
			if(normalizationCount2 == 0){
				System.out.println("ERROR - count0");
			}
			if(normalizationCount1 == 0){
				System.out.println("ERROR - count0");
			}
			
			int possibleValues = 0;
			double sumClusterDiff = 0;
			for(String propInstance : clusterInfo1.keySet()) {
				if(clusterInfo2.containsKey(propInstance)) {
					possibleValues++;
					// calculate difference between counts
					int count1 = clusterInfo1.get(propInstance);
					int count2 = clusterInfo2.get(propInstance);
					sumClusterDiff += Math.abs((double) count1/normalizationCount1 - (double) count2/normalizationCount2);
				} else {
					possibleValues++;
					//include values that 1st cluster has and 2nd cluster doesn't have
					int count1 = clusterInfo1.get(propInstance);
					sumClusterDiff += (double) count1/normalizationCount1;
				}
			}
			//now include values that 2nd cluster has that 1st cluster doesn't have
			for(String propInstance: clusterInfo2.keySet()) {
				if(!clusterInfo1.containsKey(propInstance)) {
					possibleValues++;
					int count2 = clusterInfo2.get(propInstance);
					sumClusterDiff += (double) count2/normalizationCount2;
				}
			}

			similarityScore += weights[i] * (1 - sumClusterDiff/possibleValues);
		}
		
		return coeff * similarityScore;
	}
}
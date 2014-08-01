package prerna.algorithm.impl;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Set;

import org.apache.log4j.Logger;

public class ClusteringDataProcessor {

	Logger logger = Logger.getLogger(getClass());
	
	// matrix to hold the instance numerical property values
	private Double[][] numericalMatrix;
	// matrix to hold the instance categorical property values
	private String[][] categoricalMatrix;
	
	// Hashtable containing the instance name as the key and the value being the row it's information is contained in the numerical and categorical Matrices
	private Hashtable<String, Integer> instanceHash = new Hashtable<String, Integer>();
	// list of all the categorical property names 
	private ArrayList<String> categoryPropNames = new ArrayList<String>();
	// list of all the numerical property names
	private ArrayList<String> numericalPropNames = new ArrayList<String>();
	// list the weights associated with each categorical property used to calculate the categorical similarity score
	private ArrayList<Double> weights = new ArrayList<Double>(); // has length the same as category prop names

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
		
		// these methods must be called for the similarity score to be computed 
		processMasterTable();
		calculateWeights();
	}
	
	public Hashtable<String, Integer> getInstanceHash() {
		return instanceHash;
	}
	
	public Double[][] getNumericalMatrix() {
		return numericalMatrix;
	}
	
	public String[][] getCategoricalMatrix() {
		return categoricalMatrix;
	}

	public ArrayList<String> getCategoryPropNames() {
		return categoryPropNames;
	}
	public ArrayList<String> getNumericalPropNames() {
		return numericalPropNames;
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
		Double[] instanceNumericalInfo = numericalMatrix[dataIdx];
		String[] instaceCategoricalInfo = categoricalMatrix[dataIdx];

		double numericalSimilarity = calcuateNumericalSimilarity(clusterIdx, instanceNumericalInfo, allNumericalClusterInfo);
		double categorySimilarity = calculateCategorySimilarity(instaceCategoricalInfo, categoryClusterInfo);
		
		return numericalSimilarity + categorySimilarity;
	}
	
	/**
	 * Calculates the similarity score for the categorical entries
	 * @param instaceCategoricalInfo	The categorical information for the specific instance
	 * @param categoryClusterInfo		The categorical cluster information for the cluster we are looking to add the instance into
	 * @return 							The similarity score associated with the categorical properties
	 */
	private double calculateCategorySimilarity(String[] instaceCategoricalInfo, ArrayList<Hashtable<String, Integer>> categoryClusterInfo) {
		
		double categorySimilarity = 0;
		
		// loop through all the categorical properties (each weight corresponds to one categorical property)
		for(int i = 0; i < weights.size(); i++) {
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
			categorySimilarity += weights.get(i) * numOccuranceInCluster / sumProperties;
		}
		// categorical similarity value is normalized based on the ratio of categorical variables to the total number of variables
		double coeff = 1.0 * categoryPropNames.size() / varNames.length;
		
		logger.info("Calculated similarity score for categories: " + coeff * categorySimilarity);
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
	private Double calcuateNumericalSimilarity(int clusterIdx, Double[] instanceNumericalInfo, Double[][] allNumericalClusterInfo) throws IllegalArgumentException {
		
		double numericalSimilarity = 0;
		double distanceNormalization = 0;

		int numClusters = allNumericalClusterInfo.length;
		double[] distance = new double[numClusters];
		
		// generate array of distances between the instance and the cluster for all numerical properties
		for(int i = 0; i < allNumericalClusterInfo.length; i++) {
			Double[] numericalClusterInfo = allNumericalClusterInfo[i];
			// deal with null values
			// set the values to be the same for this property such that the distance becomes 0
			for(int j = 0; j < numericalClusterInfo.length; j++) {
				if(numericalClusterInfo[j] == null) {
					if(instanceNumericalInfo[j] == null) {
						numericalClusterInfo[j] = new Double(0);
						instanceNumericalInfo[j] = new Double(0);
					} else {
						numericalClusterInfo[j] = instanceNumericalInfo[j];
					}
				} else if(instanceNumericalInfo[j] == null) {
					instanceNumericalInfo[j] = numericalClusterInfo[j];
				}
			}
			distance[i] = disCalculator.calculateEuclidianDistance(instanceNumericalInfo, numericalClusterInfo);
			distanceNormalization += distance[i];
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
		
		// categorical similarity value is normalized based on the ratio of categorical variables to the total number of variables
		double coeff = 1.0 * numericalPropNames.size() / varNames.length;
		
		logger.info("Calculated similarity score for numerical properties: " + coeff * numericalSimilarity);
		return coeff * numericalSimilarity;
	}

	/**
	 * Generate weights for categorical similarity matrix
	 */
	private void calculateWeights() {
		ArrayList<Hashtable<String, Integer>> trackPropOccurance = getPropOccurance();
		ArrayList<Double> entropyArr = calculateEntropy(trackPropOccurance);
		
		double totalEntropy = 0;
		for(double entropyVal : entropyArr) {
			totalEntropy += entropyVal;
		}
		
		for(double entropyVal : entropyArr) {
			weights.add(entropyVal / totalEntropy);
		}
		
		// output category and weight to console
		for(int i = 0; i < weights.size(); i++) {
			logger.info("Category " + categoryPropNames.get(i) + " has weight " + weights.get(i));
		}
	}

	/**
	 * Generate entropy array for each category to create weights
	 * @param trackPropOccurance	A list containing a hashtable that stores all the different instances of a given property
	 * @return						A list containing the entropy for each categorical property
	 */
	private ArrayList<Double> calculateEntropy(ArrayList<Hashtable<String, Integer>> trackPropOccurance) {
		ArrayList<Double> entropyArr = new ArrayList<Double>();
		
		for(Hashtable<String, Integer> columnInformation : trackPropOccurance) {
			ArrayList<Integer> columnPropInstanceCountArr = new ArrayList<Integer>();
			
			int totalCountOfPropInstances = 0;
			int unqiueCountOfPropInstances = 0;
			for(String columnProp : columnInformation.keySet()) {
				columnPropInstanceCountArr.add(columnInformation.get(columnProp));
				totalCountOfPropInstances += columnInformation.get(columnProp);
				unqiueCountOfPropInstances++;
			}
			
			double sumProb = 0;
			for(Integer propCount : columnPropInstanceCountArr) {
				Double probability = (double) ( 1.0 * propCount / totalCountOfPropInstances);
				sumProb += probability * logBase2(probability);
			}
			
			Double entropy = sumProb / unqiueCountOfPropInstances;
			entropyArr.add(entropy);
		}
		
		return entropyArr;
	}
	
	/**
	 * Generate occurrence of instance categorical properties to calculate entropy
	 * @return	A list containing a hashtable that stores all the different instances of a given property
	 */
	private ArrayList<Hashtable<String, Integer>> getPropOccurance() {
		ArrayList<Hashtable<String, Integer>> trackPropOccuranceArr = new ArrayList<Hashtable<String, Integer>>();

		for(int i = 0; i < categoricalMatrix[0].length; i++) {
			trackPropOccuranceArr.add(new Hashtable<String, Integer>());
		}
		
		for(String[] results : categoricalMatrix) {
			for(int i = 0; i < results.length; i++) {		
				Hashtable<String, Integer> columnInformationHash = trackPropOccuranceArr.get(i);
				if(columnInformationHash.isEmpty()) {
					columnInformationHash.put(results[i], 1);
					logger.info("Category " + categoryPropNames.get(i) + "with instance " + results[i] + " occurred 1 time.");
				} else {
					if(columnInformationHash.get(results[i]) == null) {
						columnInformationHash.put(results[i], 1);
						logger.info("Category " + categoryPropNames.get(i) + "with instance " + results[i] + " occurred 1 time.");
					} else {
						int currCount = columnInformationHash.get(results[i]);
						columnInformationHash.put(results[i], ++currCount);
						logger.info("Category " + categoryPropNames.get(i) + "with instance " + results[i] + " has occured " + currCount + " times.");
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
		ArrayList<Integer> categoryPropIndices = new ArrayList<Integer>();
		ArrayList<Integer> numericalPropIndices = new ArrayList<Integer>();
		
		//iterate through columns
		for(int j = 0; j < varNames.length; j++) {
			if(j != 0) {
				//iterate through rows
				boolean categorical = false;
				for(int i = 0; i < masterTable.size(); i++) {
					Object[] dataRow = masterTable.get(i);
					if(dataRow[j] != null) {
						String colEntryAsString = dataRow[j].toString();
						String type = processType(colEntryAsString);
						if(type.equals("STRING")) {
							categorical = true;
							categoryPropNames.add(varNames[j]);
							categoryPropIndices.add(j);
							logger.info("Found " + varNames[j] + " to be a categorical data column");
							break;
						}
					}
				}
				if(!categorical) {
					numericalPropNames.add(varNames[j]);
					numericalPropIndices.add(j);
					logger.info("Found " + varNames[j] + " to be a numerical data column");
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
		
		constructMatrices(categoryPropIndices, numericalPropIndices);
	}
	
	/**
	 * Build the categorical and numerical matrices based on the master table and which columns are categories or numbers/dates
	 * @param categoryPropIndices		The indices in the masterTable that contains the categorical properties
	 * @param numericalPropIndices		The indices in the masterTable that contains the numerical properties
	 */
	private void constructMatrices(ArrayList<Integer> categoryPropIndices, ArrayList<Integer> numericalPropIndices) {
		
		numericalMatrix = new Double[masterTable.size()][numericalPropIndices.size()];
		categoricalMatrix = new String[masterTable.size()][categoryPropIndices.size()];
		
		for(int row = 0; row < masterTable.size(); row++) {
			int counter = 0;
			
			Object[] dataRow = masterTable.get(row);
			for(Integer idx : categoryPropIndices) {
				if(dataRow[idx] != null) {
					categoricalMatrix[row][counter] = (String) dataRow[idx].toString();
				} else {
					categoricalMatrix[row][counter] = "";
				}
				counter++;
			}
			counter = 0;
			for(Integer idx : numericalPropIndices) {
				if(dataRow[idx] != null) {
					numericalMatrix[row][counter] = (Double) dataRow[idx];
				} else {
					numericalMatrix[row][counter] = null;
				}
				counter++;
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
		SimpleDateFormat formatLongDate = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
		Date longdate = null;
		try {
			formatLongDate.setLenient(true);
			longdate  = formatLongDate.parse(s);
		} catch (ParseException e) {
			isLongDate = false;
		}
		if(isLongDate){
			return ("DOUBLE");
		}

		Boolean isSimpleDate = true;
		SimpleDateFormat formatSimpleDate = new SimpleDateFormat("mm/dd/yyyy");
		Date simpleDate = null;
		try {
			formatSimpleDate.setLenient(true);
			simpleDate  = formatSimpleDate.parse(s);
		} catch (ParseException e) {
			isSimpleDate = false;
		}
		if(isSimpleDate){
			return ("DOUBLE");
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
}

package prerna.algorithm.impl;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Set;

import org.apache.log4j.Logger;

import prerna.error.BadInputException;

public class ClusteringDataProcessor {

	Logger logger = Logger.getLogger(getClass());
	
	private double[][] numericalMatrix;
	private String[][] categoricalMatrix;
	
	private Hashtable<String, Integer> instanceHash = new Hashtable<String, Integer>();
	private ArrayList<String> categoryPropNames = new ArrayList<String>();
	private ArrayList<String> numericalPropNames = new ArrayList<String>();
	private ArrayList<Double> weights = new ArrayList<Double>(); // has length the same as category prop names

	private static DistanceCalculator disCalculator = new DistanceCalculator();
	
	// instance variables that must be defined for clustering to work
	private ArrayList<Object[]> masterTable;
	private String[] varNames;
	
	public Hashtable<String, Integer> getInstanceHash() {
		return instanceHash;
	}
	
	public ClusteringDataProcessor(ArrayList<Object[]> masterTable, String[] varNames) {
		this.masterTable = masterTable;
		this.varNames = varNames;
		
		processMasterTable();
		calculateWeights();
	}
	
	public double[][] getNumericalMatrix() {
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

	// Calculates the similarity score
	public Double getSimilarityScore(int dataIdx, int clusterIdx, double[][] allNumericalClusterInfo, ArrayList<Hashtable<String, Integer>> categoryClusterInfo) throws BadInputException {
		double[] instanceNumericalInfo = numericalMatrix[dataIdx];
		String[] instaceCategoricalInfo = categoricalMatrix[dataIdx];

		double numericalSimilarity = calcuateNumericalSimilarity(clusterIdx, instanceNumericalInfo, allNumericalClusterInfo);
		double categorySimilarity = calculateCategorySimilarity(clusterIdx, instaceCategoricalInfo, categoryClusterInfo);

		return numericalSimilarity + categorySimilarity;
	}
	
	// Calculates the similarity score for the categorical entries
	private double calculateCategorySimilarity(int clusterIdx, String[] instaceCategoricalInfo, ArrayList<Hashtable<String, Integer>> categoryClusterInfo) {
		double categorySimilarity = 0;
		
		for(int i = 0; i < weights.size(); i++) {
			double sumProperties = 0;
			Hashtable<String, Integer> propertyHash = categoryClusterInfo.get(i);
			Set<String> propKeySet = propertyHash.keySet();
			for(String propName : propKeySet) {
				sumProperties += propertyHash.get(propName);
			}
			int numOccuranceInCluster = 0;
			if(propertyHash.get(instaceCategoricalInfo[i]) != null) {
				numOccuranceInCluster = propertyHash.get(instaceCategoricalInfo[i]);
			}
			categorySimilarity += weights.get(i) * numOccuranceInCluster / sumProperties;
		}
		
		double coeff = 1.0 * categoryPropNames.size() / varNames.length;
		
		logger.info("Calculated similarity score for categories: " + coeff * categorySimilarity);
		return coeff * categorySimilarity;
	}

	// Calculates the similarity score for the numerical entries
	private Double calcuateNumericalSimilarity(int clusterIdx, double[] instanceNumericalInfo, double[][] allNumericalClusterInfo) throws BadInputException {
		double numericalSimilarity = 0;
		int numClusters = allNumericalClusterInfo.length;
		double[] distance = new double[numClusters];
		
		double distanceNormalization = 0;
		for(int i = 0; i < allNumericalClusterInfo.length; i++) {
			double[] numericalClusterInfo = allNumericalClusterInfo[i];
			distance[i] = disCalculator.calculateEuclidianDistance(instanceNumericalInfo, numericalClusterInfo);
			distanceNormalization += distance[i];
		}
		for(int i = 0; i < distance.length; i++) {
			distance[i] /= distanceNormalization;
		}
		
		double distanceFromCluster = Math.exp(-0.5 * distance[clusterIdx]);
		double sumDistanceFromCluster = 0;
		for(int i = 0; i < distance.length; i++) {
			sumDistanceFromCluster += Math.exp(-0.5 * distance[i]);
		}
		
		numericalSimilarity = distanceFromCluster/sumDistanceFromCluster;
		double coeff = 1.0 * numericalPropNames.size() / varNames.length;
		
		logger.info("Calculated similarity score for numerical properties: " + coeff * numericalSimilarity);
		return coeff * numericalSimilarity;
	}

	// generate weights for categorical similarity matrix
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
	
	// generate entropy array for each category to create weights
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
	
	// generate occurrence of instance categorical properties to calculate entropy
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
	
	// process master data
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
					String colEntry = dataRow[j].toString();
					String type = processType(colEntry);
					if(type.equals("STRING")) {
						categorical = true;
						categoryPropNames.add(varNames[j]);
						categoryPropIndices.add(j);
						logger.info("Found " + varNames[j] + " to be a categorical data column");
						break;
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
	
	// build the categorical and numerical matrices based on the master table and which columns are categories vs. numbers
	private void constructMatrices(ArrayList<Integer> categoryPropIndices, ArrayList<Integer> numericalPropIndices) {
		
		numericalMatrix = new double[masterTable.size()][numericalPropIndices.size()];
		categoricalMatrix = new String[masterTable.size()][categoryPropIndices.size()];
		
		for(int row = 0; row < masterTable.size(); row++) {
			int counter = 0;
			
			Object[] dataRow = masterTable.get(row);
			for(Integer idx : categoryPropIndices) {
				categoricalMatrix[row][counter] = (String) dataRow[idx].toString();
				counter++;
			}
			counter = 0;
			for(Integer idx : numericalPropIndices) {
				numericalMatrix[row][counter] = (Double) dataRow[idx];
				counter++;
			}
		}
	}

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
	
	private double logBase2(double x) {
		return Math.log(x) / Math.log(2);
	}
	
}

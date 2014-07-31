package prerna.algorithm.impl;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;

import org.apache.log4j.Logger;

import prerna.error.BadInputException;

public class ClusteringDataProcessor {

	Logger logger = Logger.getLogger(getClass());
	
//	private ArrayList<ArrayList<Double>> numericalMatrix = new ArrayList<ArrayList<Double>>();
//	private ArrayList<ArrayList<String>> categoricalMatrix = new ArrayList<ArrayList<String>>();
	
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
	
	// Calculates the similarity score
	public Double getSimilarityScore(int dataIdx, int cluserIdx, double[][] allNumericalClusterInfo, ArrayList<Hashtable<String, Double>> categoryClusterInfo) throws BadInputException {
//		ArrayList<String> instanceCategoricalInfo = categoricalMatrix.get(dataIdx);
//		ArrayList<Double> instanceNumericalInfo = numericalMatrix.get(dataIdx);
		
		double[] instanceNumericalInfo = numericalMatrix[dataIdx];
		String[] instaceCategoricalInfo = categoricalMatrix[dataIdx];

		double numericalSimilarity = calcuateNumericalSimilarity(cluserIdx, instanceNumericalInfo, allNumericalClusterInfo);
		double categorySimilarity = calculateCategorySimilarity(cluserIdx, instaceCategoricalInfo, categoryClusterInfo);

		return numericalSimilarity + categorySimilarity;
	}
	
	// Calculates the similarity score for the categorical entries
	private double calculateCategorySimilarity(int cluserIdx, String[] instaceCategoricalInfo, ArrayList<Hashtable<String, Double>> categoryClusterInfo) {
		double categorySimilarity = 0;
		
		for(int i = 0; i < weights.size(); i++) {
			double sumProperties = 0;
			Hashtable<String, Double> propertyHash = categoryClusterInfo.get(i);
			for(String propName : propertyHash.keySet()) {
				sumProperties += propertyHash.get(propName);
			}
			categorySimilarity += weights.get(i) * propertyHash.get(categoryPropNames.get(i)) / sumProperties;
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
		
		double normalization = 0;
		for(int i = 0; i < allNumericalClusterInfo.length; i++) {
			double[] numericalClusterInfo = allNumericalClusterInfo[i];
			distance[i] = disCalculator.calculateEuclidianDistance(instanceNumericalInfo, numericalClusterInfo);
			normalization += Math.exp(-0.5 * distance[i]);
		}
		
		double distanceFromCluster = Math.exp(-0.5 * distance[clusterIdx]);
		numericalSimilarity = distanceFromCluster/normalization;
		double coeff = 1.0 * numericalPropNames.size() / varNames.length;
		
		logger.info("Calculated similarity score for categories: " + coeff * numericalSimilarity);
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
				Double probability = (double) (propCount / totalCountOfPropInstances);
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
		
		for(String[] results : categoricalMatrix) {
			for(int i = 0; i < results.length; i++) {		
				Hashtable<String, Integer> columnInformationHash = new Hashtable<String, Integer>();
				columnInformationHash = trackPropOccuranceArr.get(i);
				if(columnInformationHash == null) {
					trackPropOccuranceArr.add(new Hashtable<String, Integer>());
					columnInformationHash = trackPropOccuranceArr.get(i);
				}
				Integer occurance = columnInformationHash.get(categoryPropNames.get(i));
				if(occurance == null) {
					columnInformationHash.put(categoryPropNames.get(i), 1);
				} else {
					occurance++;
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
						break;
					}
				}
				if(!categorical) {
					numericalPropNames.add(varNames[j]);
					numericalPropIndices.add(j);
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
		int counter = 0;
		
		numericalMatrix = new double[masterTable.size()][numericalPropIndices.size()];
		categoricalMatrix = new String[masterTable.size()][categoryPropIndices.size()];
		
		for(int row = 0; row < masterTable.size(); row++) {
//			categoricalMatrix.add(new ArrayList<String>());
//			numericalMatrix.add(new ArrayList<Double>());
			
			Object[] dataRow = masterTable.get(row);
//			ArrayList<String> categoricalValues = new ArrayList<String>();
//			ArrayList<Double> numericalValues = new ArrayList<Double>();
			
			for(Integer idx : categoryPropIndices) {
				logger.info("Found " + varNames[idx] + " to be a categorical data column");
				categoricalMatrix[row][counter] = (String) dataRow[idx];
				counter++;
//				categoricalValues.add((String) dataRow[idx]);
			}
			counter = 0;
			for(Integer idx : categoryPropIndices) {
				logger.info("Found " + varNames[idx] + " to be a numerical data column");
				numericalMatrix[row][counter] = (Double) dataRow[idx];
				counter++;
//				numericalValues.add((Double) dataRow[idx]);
			}
			
//			categoricalMatrix.get(row).addAll(numericalValues);
//			numericalMatrix.get(row).addAll(numericalValues);
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

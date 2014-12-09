package prerna.algorithm.cluster;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Locale;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.impl.AlgorithmDataFormatting;
import prerna.math.BarChart;
import prerna.math.StatisticsUtilityMethods;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

public class ClusteringDataProcessor {

	private static final Logger LOGGER = LogManager.getLogger(ClusteringDataProcessor.class.getName());

	// table containing all the information - assumes the first column contains the instance name
	private ArrayList<Object[]> masterTable;
	// array list containing the variable names for the entire query 
	private String[] varNames;
	
	// matrix to hold the instance numerical property values
	private Double[][] numericalMatrix;
//	public Double[][] getNumericalMatrix() {
	//	if(numericalMatrix != null) {
	//		return numericalMatrix.clone();
	//	} else {
	//		return null;
	//	}
	//}
	
	// matrix to hold the numerical bin property values
	private String[][] numericalBinMatrix;
	// matrix to hold the numerical bin ordering
	private String[][] numericalBinOrderingMatrix;
	
	// matrix to hold the instance categorical property values
	private String[][] categoricalMatrix;
	
	
	public String[][] getNumericalBinMatrix() {
		if(numericalBinMatrix != null) {
			return numericalBinMatrix.clone();
		} else {
			return null;
		}
	}
	public String[][] getNumericalBinOrderingMatrix(){
		if(numericalBinOrderingMatrix != null) {
			return numericalBinOrderingMatrix.clone();
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

	// Hashtable containing the instance name as the key and the value being the row it's information is contained in the numerical and categorical Matrices
	private Hashtable<String, Integer> instanceHash = new Hashtable<String, Integer>();
	// list of all the categorical property names 
	private String[] categoryPropNames;
	// list of all the numerical property names
	private String[] numericalPropNames;
	
	@SuppressWarnings("unchecked")
	public Hashtable<String, Integer> getInstanceHash() {
		return (Hashtable<String, Integer>) instanceHash.clone();
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
	
	// list the weights associated with each categorical property used to calculate the categorical similarity score
	private double[] categoricalWeights; // has length the same as category prop names
	// list the weights associated with each numerical property used to calculate the numerical similarity score
	private double[] numericalWeights; // has length the same as numerical prop names
	
	public double[] getCategoricalWeights() {
		if(categoricalWeights != null) {
			return categoricalWeights.clone();
		} else {
			return null;
		}
	}
	public double[] getNumericalWeights() {
		if(numericalWeights != null) {
			return numericalWeights.clone();
		} else {
			return null;
		}
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

	int totalProps;
	int numericProps;
	int categoricalProps;

	public ClusteringDataProcessor(ArrayList<Object[]> masterTable, String[] varNames) {
		this.masterTable = masterTable;
		this.varNames = varNames;

		totalProps = varNames.length;
		// these methods must be called for the similarity score to be computed 
		processMasterTable();
		calculateCategoricalWeights();
		calculateNumericalWeights();
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
						if(!colEntryAsString.isEmpty()) {
							type = Utility.processType(colEntryAsString);
							if(type.equals("STRING")) {
								numCategorical++;
							} else {
								numNumerical++;
							}
						}
					}
				}
				if(numCategorical > numNumerical) {
					categoryPropNames[categoryPropNamesCounter] = varNames[j];
					categoryPropIndices[categoryPropNamesCounter] = j;
					categoryPropNamesCounter++;
//					LOGGER.info("Found " + varNames[j] + " to be a categorical data column");
				} else {
					numericalPropNames[numericalPropNamesCounter] = varNames[j];
					numericalPropIndices[numericalPropNamesCounter] = j;
					numericalPropNamesCounter++;
//					LOGGER.info("Found " + varNames[j] + " to be a numerical data column");
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

		if(totalNumericalPropIndices != null) {
		numericProps = totalNumericalPropIndices.length;
		}
		if(categoryPropIndices != null) {
			categoricalProps = categoryPropIndices.length;
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
									SimpleDateFormat formatLongDate = new SimpleDateFormat("MM/dd/yyyy", Locale.US);
									formatLongDate.setLenient(true);
									Date valDate = formatLongDate.parse(dataRow[idx].toString());
									Long dateAsLong = valDate.getTime();
									numericalMatrix[row][counter] = dateAsLong.doubleValue();
								}
							} else {
								numericalMatrix[row][counter] = (Double) dataRow[idx];
							}
						} catch (ParseException e) {
							LOGGER.error("Column Variable " + numericalPropNames[counter] + " was found to be a numerical (date) property but had a value of " + dataRow[idx]);
						} catch (ClassCastException e) {
							LOGGER.error("Column Variable " + numericalPropNames[counter] + " was found to be a numerical property but had a value of " + dataRow[idx]);
						}
					}
					// default values are null in new Double[][]
					counter++;
				}
			}

			counter = 0;
			if(categoryPropIndices != null) 
			{
				for(Integer idx : categoryPropIndices) {
					categoricalMatrix[row][counter] = dataRow[idx].toString();
					counter++;
				}
			}
		}
	}

	/**
	 * Generate occurrence of instance categorical properties to calculate entropy
	 * @return	A list containing a hashtable that stores all the different instances of a given property
	 */
	private ArrayList<Hashtable<String, Integer>> getCategoricalPropOccurance() {
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
	 * Generate weights for categorical similarity matrix
	 */
	private void calculateCategoricalWeights() {
		ArrayList<Hashtable<String, Integer>> trackPropOccurance = getCategoricalPropOccurance();
		double[] entropyArr = calculateEntropy(trackPropOccurance);
		int numWeights = entropyArr.length;
		categoricalWeights = new double[numWeights];

		if(numWeights == 1) {
			categoricalWeights[0] = 1;
			return;
		}
		double totalEntropy = 0;
		for(int i = 0; i < numWeights; i++) {
			totalEntropy += entropyArr[i];
		}

		int counter = 0;
		for(int i = 0; i < numWeights; i++) {
			categoricalWeights[counter] = (entropyArr[i] / totalEntropy);
			counter++;
		}

		// output category and weight to console
//		for(int i = 0; i < categoricalWeights.length; i++) {
//			LOGGER.info("Category " + categoryPropNames[i] + " has weight " + categoricalWeights[i]);
//		}
	}

	
	/**
	 * Generate weights for categorical similarity matrix
	 */
	private void calculateNumericalWeights() {
		ArrayList<Hashtable<String, Integer>> trackPropOccurance = getNumericalPropOccurance();
		double[] entropyArr = calculateEntropy(trackPropOccurance);
		int numWeights = entropyArr.length;
		numericalWeights = new double[numWeights];

		double totalEntropy = 0;
		for(int i = 0; i < numWeights; i++) {
			totalEntropy += entropyArr[i];
		}

		int counter = 0;
		for(int i = 0; i < numWeights; i++) {
			if(totalEntropy == 0) {
				numericalWeights[counter] = 0.0;
			} else {
				numericalWeights[counter] = (entropyArr[i] / totalEntropy);
			}
			counter++;
		}

		// output category and weight to console
//		for(int i = 0; i < numericalWeights.length; i++) {
//			LOGGER.info("NumericalProp " + numericalPropNames[i] + " has weight " + numericalWeights[i]);
//		}
	}
	
	/**
	 * Generate occurrence of instance numerical properties to calculate entropy
	 * @return	A list containing a hashtable that stores all the different instances of a given property
	 */
	private ArrayList<Hashtable<String, Integer>> getNumericalPropOccurance() {
		ArrayList<Hashtable<String, Integer>> trackPropOccuranceArr = new ArrayList<Hashtable<String, Integer>>();
		
		if(numericalMatrix != null)
		{
			int numRows = masterTable.size();
			int numCols = numericalPropNames.length;
			numericalBinMatrix = new String[numRows][numCols];
			
			AlgorithmDataFormatting formatter = new AlgorithmDataFormatting();
			Object[][] data = formatter.convertColumnValuesToRows(numericalMatrix);
			int size = data.length;
			numericalBinOrderingMatrix = new String[size][];
			for(int i = 0; i < size; i++) {
				Object[] propColumn = data[i];
				trackPropOccuranceArr.add(new Hashtable<String, Integer>());
				// if no instances contain a value for the property, skip it
				if(ArrayUtilityMethods.removeAllNulls(propColumn).length == 0) {
					continue;
				}
				Double[] dataRow = ArrayUtilityMethods.convertObjArrToDoubleWrapperArr(propColumn);
				BarChart chart = new BarChart(dataRow);
				numericalBinOrderingMatrix[i] = chart.getNumericalBinOrder();
				generateNumericalBinMatrix(i, chart.getAssignmentForEachObject());
				Hashtable<String, Integer> colInformationHash = trackPropOccuranceArr.get(i);
				Hashtable<String, Object>[] bins = chart.getRetHashForJSON();
				int j;
				for(j = 0; j < bins.length; j++) {
					String range = (String) bins[j].get("x");
					Integer count = (int) bins[j].get("y");
					colInformationHash.put(range, count);
				}
			}
		}

		return trackPropOccuranceArr;
	}
	
	private void generateNumericalBinMatrix(int col, String[] values) {
		int i;
		int size = values.length;
		for(i = 0; i < size; i++) {
			String val = values[i];
			if(val == null) {
				numericalBinMatrix[i][col] = "NaN";
			} else {
				numericalBinMatrix[i][col] = values[i];
			}
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
				if(columnInformation.get(columnProp) > 0) {
					columnPropInstanceCountArr.add(columnInformation.get(columnProp));
					totalCountOfPropInstances += columnInformation.get(columnProp);
					unqiueCountOfPropInstances++;
				}
			}
			
			//if all values missing for property, entropy is 0
			if(columnPropInstanceCountArr.size() <= 1) {
				continue;
			}
			
			double sumProb = 0;
			int columnPropInstanceSize = columnPropInstanceCountArr.size();
			for(int j = 0; j < columnPropInstanceSize; j++) {
				Integer propCount = columnPropInstanceCountArr.get(j);
				Double probability = (double) ( 1.0 * propCount / totalCountOfPropInstances);
				sumProb += probability * StatisticsUtilityMethods.logBase2(probability);
			}

			Double entropy = sumProb / unqiueCountOfPropInstances;
			entropyArr[i] = entropy;
		}

		return entropyArr;
	}
	
//	private void generateNumericalBinMatrix(ArrayList<Hashtable<String, Integer>> trackPropOccurance) {
//		if(numericalPropNames != null) {
//			int numRows = masterTable.size();
//			int numCols = numericalPropNames.length;
//			
//			numericalBinMatrix = new String[numRows][numCols];
//			int i;
//			int j;
//			double minBinVal = Double.MAX_VALUE;
//			double maxBinVal = Double.MIN_VALUE;
//			for(i = 0; i < numRows; i++) {
//				COL_LOOP: for(j = 0; j < numCols; j++) {
//					double val = numericalMatrix[i][j];
//					System.out.println(val); //TODO: delete
//					Hashtable<String, Integer> colInstances = trackPropOccurance.get(j);
//					// due to loss of significant digits, the val can be larger than the max
//					String minBin = "";
//					String maxBin = "";
//					for(String binRange : colInstances.keySet()) {
//						String[] split = binRange.split(" - ");
//						if(split.length == 2) {
//							double min = Double.parseDouble(split[0].trim());
//							double max = Double.parseDouble(split[1].trim());
//							if(maxBinVal < max) {
//								maxBinVal = max;
//								maxBin = binRange;
//							}
//							if(minBinVal > min) {
//								minBinVal = min;
//								minBin = binRange;
//							}
//							if(val > min && val <= max) {
//								numericalBinMatrix[i][j] = binRange;
//								continue COL_LOOP;
//							}
//						} else {
//							// not enough unique values to create bins - columns become unique values
//							if(val == Double.parseDouble(binRange)) {
//								numericalBinMatrix[i][j] = binRange;
//								continue COL_LOOP;
//							}
//						}
//					}
//					// this check is technically unnecessary since we only get here when it is null -> otherwise it goes to the continue
//					if(numericalBinMatrix[i][j] == null) {
//						if(Math.abs(minBinVal - val) > Math.abs(maxBinVal - val)) {
//							numericalBinMatrix[i][j] = maxBin;
//						} else {
//							numericalBinMatrix[i][j] = minBin;
//						}
//					}
//				}
//			}
//		}
//	}
}

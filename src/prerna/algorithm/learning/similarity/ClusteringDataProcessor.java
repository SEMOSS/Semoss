/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.algorithm.learning.similarity;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Locale;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.impl.AlgorithmDataFormatter;
import prerna.math.BarChart;
import prerna.math.CalculateEntropy;
import prerna.math.SimilarityWeighting;
import prerna.math.StatisticsUtilityMethods;
import prerna.util.ArrayUtilityMethods;

public class ClusteringDataProcessor {

	private static final Logger LOGGER = LogManager.getLogger(ClusteringDataProcessor.class.getName());

	// table containing all the information - assumes the first column contains the instance name
	private ArrayList<Object[]> masterTable;
	// array list containing the variable names for the entire query 
	private String[] varNames;

	// matrix to hold the instance numerical property values
	private Double[][] numericalMatrix;
	// matrix to hold the numerical bin property values
	private String[][] numericalBinMatrix;
	// matrix to hold the numerical bin ordering
	private String[][] numericalBinOrderingMatrix;

	// matrix to hold the instance categorical property values
	private String[][] categoricalMatrix;

	// list of all the categorical property names 
	private String[] categoryPropNames;
	// list of all the numerical property names
	private String[] numericalPropNames;

	// list the weights associated with each categorical property used to calculate the categorical similarity score
	private double[] categoricalWeights; // has length the same as category prop names
	// list the weights associated with each numerical property used to calculate the numerical similarity score
	private double[] numericalWeights; // has length the same as numerical prop names
	// the entropy values for the categorical properties
	private double[] categoricalEntropy; // has length the same as category prop names 
	// the entropy values for the numerical properties
	private double[] numericalEntropy; // has length the same as numerical prop names

	//indexing used for visualization
	private int[] totalNumericalPropIndices;
	private Integer[] categoryPropIndices; 

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
		
		AlgorithmDataFormatter.determineColumnTypes(varNames, masterTable, categoryPropNames, categoryPropIndices, numericalPropNames, numericalPropIndices, dateTypeIndices, simpleDateTypeIndices);

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
			totalNumericalPropIndices[i+numericSize+dateSize] = simpleDateTypeIndices[i];
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
								try {
									numericalMatrix[row][counter] = (Double) dataRow[idx];
								} catch(ClassCastException e) {
									try {
										numericalMatrix[row][counter] = Double.parseDouble(dataRow[idx].toString());
									} catch (NumberFormatException ex) {
										LOGGER.error("Column Variable " + numericalPropNames[counter] + " was found to be a numerical property but had a value of " + dataRow[idx]);
									}
								}
							}
						} catch (ParseException e) {
							LOGGER.error("Column Variable " + numericalPropNames[counter] + " was found to be a numerical (date) property but had a value of " + dataRow[idx]);
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
	 * Generate weights for categorical similarity matrix
	 */
	private void calculateCategoricalWeights() {
		if(categoricalMatrix != null)
		{
			int size = categoricalMatrix[0].length;
			if(size == 1) {
				categoricalWeights = new double[]{1};
				return;
			}
			categoricalEntropy = new double[size];

			int i = 0;
			for(; i < size; i++) {
				CalculateEntropy getEntropy = new CalculateEntropy();
				getEntropy.setDataArr(ArrayUtilityMethods.getColumnFromList(categoricalMatrix, i));
				getEntropy.addDataToCountHash();
				categoricalEntropy[i] = getEntropy.calculateEntropyDensity();
			}

			categoricalWeights = SimilarityWeighting.generateWeighting(categoricalEntropy);

			// output category and weight to console
			//			i = 0;
			//			for(; i < categoricalWeights.length; i++) {
			//				LOGGER.info("Category " + categoryPropNames[i] + " has weight " + categoricalWeights[i]);
			//			}
		}
	}

	/**
	 * Generate weights for categorical similarity matrix
	 */
	private void calculateNumericalWeights() {

		if(numericalMatrix != null) {
			calculateNumericalEntropyAndGenerateNumericalBinMatrix();
			int numWeights = numericalEntropy.length;
			numericalWeights = new double[numWeights];

			double totalEntropy = StatisticsUtilityMethods.getSum(numericalEntropy);

			int counter = 0;
			for(int i = 0; i < numWeights; i++) {
				if(totalEntropy == 0) {
					numericalWeights[counter] = 0.0;
				} else {
					numericalWeights[counter] = (numericalEntropy[i] / totalEntropy);
				}
				counter++;
			}
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
	private void calculateNumericalEntropyAndGenerateNumericalBinMatrix() {
		int numRows = masterTable.size();
		int numCols = numericalPropNames.length;
		numericalBinMatrix = new String[numRows][numCols];
		numericalEntropy = new double[numCols];
		AlgorithmDataFormatter formatter = new AlgorithmDataFormatter();
		Object[][] data = formatter.convertColumnValuesToRows(numericalMatrix);
		int size = data.length;
		numericalBinOrderingMatrix = new String[size][];
		for(int i = 0; i < size; i++) {
			Object[] propColumn = data[i];
			// if no instances contain a value for the property, skip it
			if(ArrayUtilityMethods.removeAllNulls(propColumn).length == 0) {
				generateNumericalBinMatrix(i, new String[numRows]);
				continue;
			}
			Double[] dataRow = ArrayUtilityMethods.convertObjArrToDoubleWrapperArr(propColumn);
			Hashtable<String, Object>[] bins = null;
			BarChart chart = new BarChart(dataRow);
			numericalBinOrderingMatrix[i] = chart.getNumericalBinOrder();
			if(chart.isUseCategoricalForNumericInput()) {
				chart.calculateCategoricalBins("NaN", true, true);
				generateNumericalBinMatrix(i, chart.getAssignmentForEachObject());
				chart.generateJSONHashtableCategorical();
				bins = chart.getRetHashForJSON();
			} else {
				generateNumericalBinMatrix(i, chart.getAssignmentForEachObject());
				chart.generateJSONHashtableNumerical();
				bins = chart.getRetHashForJSON();
			}
			double entropy = 0;
			int j;
			int uniqueValues = bins.length;
			for(j = 0; j < uniqueValues; j++) {
				int count = (int) bins[j].get("y");
				if(count != 0) {
					entropy += ( (double) count / numRows) * StatisticsUtilityMethods.logBase2((double) count / numRows);
				}
			}
			numericalEntropy[i] = entropy / uniqueValues;
		}
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

	public int[] getTotalNumericalPropIndices() {
		return totalNumericalPropIndices;
	}

	public Integer[] getCategoryPropIndices() {
		return categoryPropIndices;
	}

	public String[][] getNumericalBinMatrix() {
		return numericalBinMatrix;
	}
	public String[][] getNumericalBinOrderingMatrix(){
		return numericalBinOrderingMatrix;
	}
	public String[][] getCategoricalMatrix() {
		return categoricalMatrix;
	}

	public String[] getCategoryPropNames() {
		return categoryPropNames;
	}
	public String[] getNumericalPropNames() {
		return numericalPropNames;
	}

	public double[] getCategoricalWeights() {
		return categoricalWeights;
	}

	public double[] getNumericalWeights() {
		return numericalWeights;
	}
	
	public double[] getCategoricalEntropy() {
		return categoricalEntropy;
	}

	public void setCategoricalEntropy(double[] categoricalEntropy) {
		this.categoricalEntropy = categoricalEntropy;
	}

	public double[] getNumericalEntropy() {
		return numericalEntropy;
	}

	public void setNumericalEntropy(double[] numericalEntropy) {
		this.numericalEntropy = numericalEntropy;
	}
}

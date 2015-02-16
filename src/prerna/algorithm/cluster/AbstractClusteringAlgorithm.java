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
package prerna.algorithm.cluster;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.util.ArrayUtilityMethods;

public abstract class AbstractClusteringAlgorithm {

	private static final Logger LOGGER = LogManager.getLogger(ClusteringAlgorithm.class.getName());
	
	// instance variables that must be defined for clustering to work
	protected ArrayList<Object[]> masterTable;
	protected String[] varNames;

	protected ClusteringDataProcessor cdp;
	protected ClusteringNumericalMethods cnm;
	
	// the number of instances, the total number of clusters, the number of instances in each cluster, and the cluster each instance is assigned to
	protected int numInstances;
	protected int numClusters;
	protected int[] numInstancesInCluster;
	protected int[] clusterAssignment;
	protected int[] orderedOriginalClusterAssignment;
	
	//the category and number values for each instance
	protected String[][] instanceCategoryMatrix;
	protected String[][] instanceNumberBinMatrix;
	protected String[][] instanceNumberBinOrderingMatrix;
	
	//the category and number values for each cluster
	protected ArrayList<ArrayList<Hashtable<String, Integer>>> clusterCategoryMatrix;
	protected ArrayList<ArrayList<Hashtable<String, Integer>>> clusterNumberBinMatrix;
	
	protected double[] categoricalWeights;
	protected double[] numericalWeights;
	
	//rows for the summarizing cluster
	protected ArrayList<Object[]> clusterSummaryRows;

	//indexing used for visualization
	protected int[] numericalPropIndices;
	protected Integer[] categoryPropIndices; 
	protected String[] numericalPropNames;
	protected String[] categoryPropNames;
	
	// to store previous cluster information
	private boolean recreateStartingClusterValues = true;
		
	//success of algorithm 
	protected boolean success;

	public AbstractClusteringAlgorithm() {
		
	}
	
	public AbstractClusteringAlgorithm(ArrayList<Object[]> masterTable, String[] varNames) {
		this.masterTable = masterTable;
		this.varNames = varNames;
	}
	
	// method to be defined in specific clustering algorithms
	public abstract boolean execute();
	
	public void setDataVariables() {
		cdp = new ClusteringDataProcessor(masterTable,varNames);
		instanceCategoryMatrix = cdp.getCategoricalMatrix();
		instanceNumberBinMatrix = cdp.getNumericalBinMatrix();
		instanceNumberBinOrderingMatrix = cdp.getNumericalBinOrderingMatrix();
		numInstances = masterTable.size();
		numericalPropNames = cdp.getNumericalPropNames();
		categoryPropNames = cdp.getCategoryPropNames();
		//need indices for visualization
		categoryPropIndices = cdp.getCategoryPropIndices();
		numericalPropIndices = cdp.getTotalNumericalPropIndices();
		
		categoricalWeights = cdp.getCategoricalWeights();
		numericalWeights = cdp.getNumericalWeights();
	}
	
	protected void setAlgorithmVariables(){
		
		cnm = new ClusteringNumericalMethods(instanceNumberBinMatrix, instanceCategoryMatrix, instanceNumberBinOrderingMatrix);
		cnm.setCategoricalWeights(categoricalWeights);
		cnm.setNumericalWeights(numericalWeights);
		
		//create cluster assignment matrix for each instance
		if(numInstancesInCluster == null) {
			numInstancesInCluster = initalizeClusterMatrix(numClusters);
		}
		
		if(recreateStartingClusterValues) {
			randomlyAssignClusters(numInstances, numClusters); //randomly assign one instance to each cluster, creating clusterAssignment and orderedOriginalClusterAssignment
			//make the custer number matrix from initial assignments
			clusterNumberBinMatrix = ClusterUtilityMethods.createClustersCategoryProperties(instanceNumberBinMatrix, clusterAssignment, numClusters);
			//make the cluster category matrix from initial assignments
			clusterCategoryMatrix = ClusterUtilityMethods.createClustersCategoryProperties(instanceCategoryMatrix, clusterAssignment, numClusters);
		}
	}

	protected final int[] initalizeClusterMatrix(int numClusters) {
		//initializes the cluster assignment matrix for each instance
		int[] clustersNumInstances = new int[numClusters];
		int i;
		for(i = 0; i < numClusters; i++) {
			clustersNumInstances[i] = 1;
		}
		return clustersNumInstances;
	}

	protected final void randomlyAssignClusters(int numInstances, int numClusters) {
		clusterAssignment = new int[numInstances];
		orderedOriginalClusterAssignment = new int[numClusters];
		int i;
		for(i = 0; i < numInstances; i++) {
			clusterAssignment[i] = -1;
		}
		clusterAssignment[0] = 0;
		ArrayList<ArrayList<Hashtable<String, Integer>>> initialClusterNumberMatrix = ClusterUtilityMethods.createClustersCategoryProperties(instanceNumberBinMatrix, clusterAssignment, 1);
		ArrayList<ArrayList<Hashtable<String, Integer>>> initialClusterCategoryMatrix = ClusterUtilityMethods.createClustersCategoryProperties(instanceCategoryMatrix, clusterAssignment, 1);
	
		for(i = 1; i < numClusters; i++) {
			int j;
			int minIndex = -1; //initialize to impossible value for index
			double minSimilarity = (double) 2; //initialize to value larger than 1 since max value is 1
			for(j = 1; j < numInstances; j++) {
				double similarityClusterVal;
				if(initialClusterCategoryMatrix != null) {
					if(initialClusterNumberMatrix != null) {
						similarityClusterVal = cnm.getSimilarityScore(j, initialClusterNumberMatrix.get(0), initialClusterCategoryMatrix.get(0));
					} else {
						similarityClusterVal = cnm.getSimilarityScore(j, null, initialClusterCategoryMatrix.get(0));
					}
				} else {
					similarityClusterVal = cnm.getSimilarityScore(j, initialClusterNumberMatrix.get(0), null);
				}
				if(similarityClusterVal < minSimilarity && (clusterAssignment[j] == -1) ) {
					minIndex = j;
					minSimilarity =  similarityClusterVal;
					// if similarity is 0, then can exit the loop
					if(minSimilarity == 0) {
						break;
					}
				}
			}
			initialClusterNumberMatrix = ClusterUtilityMethods.updateClustersCategoryProperties(minIndex, -1, 0, instanceNumberBinMatrix, initialClusterNumberMatrix);
			initialClusterCategoryMatrix = ClusterUtilityMethods.updateClustersCategoryProperties(minIndex, -1, 0, instanceCategoryMatrix, initialClusterCategoryMatrix);
			
			clusterAssignment[minIndex] = i;
			orderedOriginalClusterAssignment[i] = minIndex;
		}
	}

	public double calculateFinalInstancesToClusterSimilarity() {
		if(success) {
			double sumSimiliarities = 0;
			int dataIdx;
			for(dataIdx = 0; dataIdx < numInstances; dataIdx++) {				
				int clusterIdx = clusterAssignment[dataIdx];
				if(clusterCategoryMatrix != null && !clusterCategoryMatrix.isEmpty()) {
					if(clusterNumberBinMatrix != null && !clusterNumberBinMatrix.isEmpty()) {
						sumSimiliarities += cnm.getSimilarityScore(dataIdx, clusterNumberBinMatrix.get(clusterIdx), clusterCategoryMatrix.get(clusterIdx));
					} else {
						sumSimiliarities += cnm.getSimilarityScore(dataIdx, null, clusterCategoryMatrix.get(clusterIdx));
					}
				} else {
					sumSimiliarities += cnm.getSimilarityScore(dataIdx, clusterNumberBinMatrix.get(clusterIdx), null);
				}
			}
			return sumSimiliarities;
		} else {
			return Double.NaN;
		}
	}
	
	public double calculateFinalTotalClusterToClusterSimilarity() {
		if(success) {
			double sumSimiliarities = 0;
			int i;
			for(i = 0; i < numClusters - 1; i++) {
				int j;
				for(j = i+1; j < numClusters; j++) {
					if(clusterCategoryMatrix != null && !clusterCategoryMatrix.isEmpty()) {
						if(clusterNumberBinMatrix != null && !clusterNumberBinMatrix.isEmpty()) {
							sumSimiliarities += cnm.calculateClusterToClusterSimilarity(i, j, clusterNumberBinMatrix, clusterCategoryMatrix);
						} else {
							sumSimiliarities += cnm.calculateClusterToClusterSimilarity(i, j, null, clusterCategoryMatrix);
						}
					} else {
						sumSimiliarities += cnm.calculateClusterToClusterSimilarity(i, j, clusterNumberBinMatrix, null);
					}
				}
			}
			return sumSimiliarities;
		} else {
			return Double.NaN;
		}
	}
	
	/**
	 * Print each cluster with categorical and numerical properties and a list of all instances
	 */
	protected void createClusterSummaryRowsForGrid() {
		clusterSummaryRows = new ArrayList<Object[]>();

		for(int clusterInd = 0;clusterInd<numInstancesInCluster.length;clusterInd++) {
			Object[] clusterRow = new Object[varNames.length+1];
			clusterRow[0] = "";

			for(int propInd=1;propInd<varNames.length;propInd++) {
				String prop = varNames[propInd];
				if(categoryPropNames != null) {
					int categoryInd = ArrayUtilityMethods.calculateIndexOfArray(categoryPropNames, prop);
					if(categoryInd >-1) {
						Hashtable<String, Integer> propValHash = clusterCategoryMatrix.get(clusterInd).get(categoryInd);
						String propWithHighFreq = printMostFrequentProperties(clusterInd, propValHash);
						if(!propWithHighFreq.equals("")) {
							int freq = propValHash.get(propWithHighFreq);
							double percent = (1.0*freq)/(1.0*numInstancesInCluster[clusterInd])*100;
							DecimalFormat nf = new DecimalFormat("###.##");
							clusterRow[propInd] = propWithHighFreq +": "+nf.format(percent)+"%";
						}
					}
				}
				
				if(numericalPropNames != null) {
					int numberInd = ArrayUtilityMethods.calculateIndexOfArray(numericalPropNames, prop);
					if(numberInd >-1) {
						Hashtable<String, Integer> propValHash = clusterNumberBinMatrix.get(clusterInd).get(numberInd);
						String propWithHighFreq = printMostFrequentProperties(clusterInd, propValHash);
						if(!propWithHighFreq.equals("")) {
							int freq = propValHash.get(propWithHighFreq);
							double percent = (1.0*freq)/(1.0*numInstancesInCluster[clusterInd])*100;
							DecimalFormat nf = new DecimalFormat("###.##");
							clusterRow[propInd] = propWithHighFreq +": "+nf.format(percent)+"%";
						}
					}
				}
			}
			clusterRow[varNames.length] = clusterInd;
			clusterSummaryRows.add(clusterRow);
		}		
	}
	
	private String printMostFrequentProperties(int clusterInd, Hashtable<String, Integer> propValHash) {
		String propWithHighFreq = "";
		int highestFreq = -1;
		for(String propVal : propValHash.keySet()) {
			int freq = propValHash.get(propVal);
			if(freq>highestFreq) {
				highestFreq = freq;
				propWithHighFreq = propVal;
			}
		}
		return propWithHighFreq;
	}
	
	public void setMasterTable(ArrayList<Object[]> masterTable) {
		this.masterTable = masterTable;
	}
	public void setVarNames(String[] varNames) {
		this.varNames = varNames;
	}
	public void setCategoricalWeights(double[] categoricalWeights) {
		this.categoricalWeights = categoricalWeights;
	}
	public void setNumericalWeights(double[] numericalWeights) {
		this.numericalWeights = numericalWeights;
	}
	public void setClusterCategoricalMatrix(ArrayList<ArrayList<Hashtable<String, Integer>>> clusterCategoryMatrix) {
		this.clusterCategoryMatrix = clusterCategoryMatrix;
	}
	public void setClusterNumberBinMatrix(ArrayList<ArrayList<Hashtable<String, Integer>>> clusterNumberBinMatrix) {
		this.clusterNumberBinMatrix = clusterNumberBinMatrix;
	}
	public void setInstanceCategoricalMatrix(String[][] instanceCategoricalMatrix) {
		this.instanceCategoryMatrix = instanceCategoricalMatrix;
	}
	public void setInstanceNumberBinMatrix(String[][] instanceNumberBinMatrix) {
		this.instanceNumberBinMatrix = instanceNumberBinMatrix;
	}
	public void setInstanceNumberBinOrderingMatrix(String[][] instanceNumberBinOrderingMatrix) {
		this.instanceNumberBinOrderingMatrix = instanceNumberBinOrderingMatrix;
	}
	public void setNumInstances(int numInstances) {
		this.numInstances = numInstances;
	}
	public boolean getSuccess() {
		return success;
	}
	public void setNumClusters(int numClusters) {
		this.numClusters = numClusters;
	}
	public int[] getNumInstancesInCluster() {
		return numInstancesInCluster;
	}
	public void setNumInstancesInCluster(int[] numInstancesInCluster) {
		this.numInstancesInCluster = numInstancesInCluster;
	}
	public ArrayList<Object[]> getSummaryClusterRows() {
		return clusterSummaryRows;
	}
	public int[] getClusterAssignment() {
		return clusterAssignment;
	}
	public void setClusterAssignment(int[] clusterAssignment) {
		this.clusterAssignment = clusterAssignment;
	}
	public void setNumericalPropIndices(int[] numericalPropIndices) {
		this.numericalPropIndices = numericalPropIndices;
	}
	public void setCategoricalPropIndices(Integer[] categoryPropIndices) {
		this.categoryPropIndices = categoryPropIndices;
	}
	public void setRecreateStartingClusterValues(boolean recreateStartingClusterValues) {
		this.recreateStartingClusterValues = recreateStartingClusterValues;
	}
	public int[] getNumericalPropIndices() {
		return numericalPropIndices;
	}
	public Integer[] getCategoryPropIndices() {
		return categoryPropIndices;
	}
	public void setNumericalPropNames(String[] numericalPropNames) {
		this.numericalPropNames = numericalPropNames;
	}
	public void setCategoryPropNames(String[] categoryPropNames) {
		this.categoryPropNames = categoryPropNames;
	}
	
}

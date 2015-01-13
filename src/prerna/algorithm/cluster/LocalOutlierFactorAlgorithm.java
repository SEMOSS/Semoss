/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.algorithm.cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.commons.math3.special.Erf;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.math.StatisticsUtilityMethods;

public class LocalOutlierFactorAlgorithm {

	private static final Logger LOGGER = LogManager.getLogger(LocalOutlierFactorAlgorithm.class.getName());

	private ArrayList<Object[]> masterTable;
	private String[] masterNames;
	private int numInstances;

	private String[][] numericalBinMatrix;
	private String[][] numericalBinOrderingMatrix;
	private String[][] categoricalMatrix;
	private double[] categoricalWeights;
	private double[] numericalWeights;

	private Queue<Integer> processingQueue;
	int numProcessors;

	InstanceNumericalMethods inm;

	private double[] lrd;
	private double[] lof;
	private double[] lop;

	private int k;

	private double[][] similarityMatrix;
	private double[] kSimilarityArr; 
	private int[][] kSimilarityIndicesMatrix;
	private double[][] reachSimMatrix;

	public void setK(int k) {
		this.k = k;
	}

	public ArrayList<Object[]> getMasterTable() {
		return masterTable;
	}

	public String[] getNames() {
		return masterNames;
	}

	public double[] getLRD() {
		return lrd;
	}

	public double[] getLOF() {
		return lof;
	}

	public double[] getLOP() {
		return lop;
	}

	public LocalOutlierFactorAlgorithm(ArrayList<Object[]> list, String[] names) {
		LOGGER.info("Removing any duplicated instances...");
		ClusterRemoveDuplicates crd = new ClusterRemoveDuplicates(list, names);
		this.masterTable = crd.getRetMasterTable();
		this.masterNames = crd.getRetVarNames();
		this.numInstances = masterTable.size();

		LOGGER.info("Formatting dataset to run algorithm...");
		ClusteringDataProcessor cdp = new ClusteringDataProcessor(masterTable, masterNames);
		numericalBinMatrix = cdp.getNumericalBinMatrix();
		numericalBinOrderingMatrix = cdp.getNumericalBinOrderingMatrix();
		categoricalMatrix = cdp.getCategoricalMatrix();
		categoricalWeights = cdp.getCategoricalWeights();
		numericalWeights = cdp.getNumericalWeights();

		inm = new InstanceNumericalMethods(numericalBinMatrix, categoricalMatrix, numericalBinOrderingMatrix);
		inm.setCategoricalWeights(categoricalWeights);
		inm.setNumericalWeights(numericalWeights);

		numProcessors = Runtime.getRuntime().availableProcessors();
	}

	public LocalOutlierFactorAlgorithm(ArrayList<Object[]> masterTable, String[] masterNames, int k) {
		this(masterTable, masterNames);
		this.k = k;
		LOGGER.info("Starting local outlier algorithm using " + k + "-size neighborhood...");
	}

	public void execute() {

		long startTime = System.currentTimeMillis();

		LOGGER.info("Generating similarity matrix between every instance...");
		calculateSimilarityMatrix();
		LOGGER.info("Generating " + k + "-neighborhood similarity matrix for every instance...");
		calculateKSimilarityMatrix();
		LOGGER.info("Generating reach similarity matrix...");
		calculateReachSimilarity();
		LOGGER.info("Generating local reach density...");
		calculateLRD();
		LOGGER.info("Generating local outlier factor...");
		calculateLOF();
		LOGGER.info("Generating local outlier probability...");
		calculateLOOP();

		long endTime = System.currentTimeMillis();
		System.out.println("Total Time = " + (endTime-startTime)/1000 );
	}

	private void calculateSimilarityMatrix() {
		similarityMatrix = new double[numInstances][numInstances];
		processingQueue = new PriorityQueue<Integer>();

		List<Thread> threads = new ArrayList<Thread>();
		int i;
		synchronized(processingQueue){
			for(i = 0; i < numInstances; i++) {
				InstanceNumericalMethods inm = new InstanceNumericalMethods(numericalBinMatrix, categoricalMatrix, numericalBinOrderingMatrix);
				inm.setCategoricalWeights(categoricalWeights);
				inm.setNumericalWeights(numericalWeights);
				inm.setNumInstances(numInstances);
				inm.setSimilarityMatrix(similarityMatrix);
				inm.setCalculateSimMatrix(true);
				inm.setIndex(i);
				inm.setStart(i);
				inm.setProcessingQueue(processingQueue);
				processingQueue.add(i);
				while(processingQueue.size() >= numProcessors * 5) {
					try {
						LOGGER.info("Waiting for queue...");
						processingQueue.wait();
					} catch (InterruptedException ex) {
						ex.printStackTrace();
					}
				}
				Thread t = new Thread(inm);
				threads.add(t);
				t.start();
			}
		}
		int size = threads.size();
		i = 0;
		for(; i < size; i++) {
			try {
				threads.get(i).join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		// print out similarity matrix for debugging
		//		System.out.println("SIMILARITY MATRIX");
		//		for(int i = 0; i < numInstances; i++) {
		//			for(int j = 0; j < numInstances; j++) {
		//				System.out.print(similarityMatrix[i][j] + ", ");
		//			}
		//			System.out.println();
		//		}
	}

	private void calculateKSimilarityMatrix() {
		kSimilarityArr = new double[numInstances];
		kSimilarityIndicesMatrix = new int[numInstances][];
		processingQueue = new PriorityQueue<Integer>();

		List<Thread> threads = new ArrayList<Thread>();
		int i;
		synchronized(processingQueue){
			for(i = 0; i < numInstances; i++) {
				InstanceNumericalMethods inm = new InstanceNumericalMethods(numericalBinMatrix, categoricalMatrix, numericalBinOrderingMatrix);
				inm.setCategoricalWeights(categoricalWeights);
				inm.setNumericalWeights(numericalWeights);
				inm.setNumInstances(numInstances);
				inm.setSimilarityMatrix(similarityMatrix);
				inm.setkSimilarityArr(kSimilarityArr);
				inm.setkSimilarityIndicesMatrix(kSimilarityIndicesMatrix);
				inm.setCalculateSimMatrix(false);
				inm.setCalculateKSimMatrix(true);
				inm.setIndex(i);
				inm.setK(k);
				inm.setProcessingQueue(processingQueue);
				processingQueue.add(i);
				while(processingQueue.size() >= numProcessors * 5) {
					try {
						LOGGER.info("Waiting for queue...");
						processingQueue.wait();
					} catch (InterruptedException ex) {
						ex.printStackTrace();
					}
				}
				Thread t = new Thread(inm);
				threads.add(t);
				t.start();
			}
		}
		int size = threads.size();
		i = size - (numProcessors * 10) - 1;
		for(; i < size; i++) {
			try {
				threads.get(i).join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		// print out k neighborhood matrix for debugging
		//		System.out.println("K-NEIGHBORHOOD SIM MATRIX");
		//		for(i = 0; i < numInstances; i++) {
		//			int[] simIndicies = kSimilarityIndicesMatrix[i];
		//			if(simIndicies != null) {
		//				for(int j = 0; j < simIndicies.length - 1; j++) {
		//					int col = simIndicies[j];
		//					System.out.print(similarityMatrix[i][col] + ", ");
		//				}
		//			}
		//			System.out.println();
		//		}

		// print out k neighborhood matrix for debugging
		//		System.out.println("K-NEIGHBORHOOD MATRIX");
		//		for(i = 0; i < numInstances; i++) {
		//			int[] simIndicies = kSimilarityIndicesMatrix[i];
		//			if(simIndicies != null) {
		//				for(int j = 0; j < simIndicies.length - 1; j++) {
		//					System.out.print(simIndicies[j] + ", ");
		//				}
		//			}
		//			System.out.println();
		//		}
	}

	private void calculateReachSimilarity() {
		reachSimMatrix = new double[numInstances][numInstances];
		processingQueue = new PriorityQueue<Integer>();

		List<Thread> threads = new ArrayList<Thread>();
		int i;
		synchronized(processingQueue){
			for(i = 0; i < numInstances; i++) {
				InstanceNumericalMethods inm = new InstanceNumericalMethods(numericalBinMatrix, categoricalMatrix, numericalBinOrderingMatrix);
				inm.setCategoricalWeights(categoricalWeights);
				inm.setNumericalWeights(numericalWeights);
				inm.setNumInstances(numInstances);
				inm.setSimilarityMatrix(similarityMatrix);
				inm.setkSimilarityArr(kSimilarityArr);
				inm.setReachSimMatrix(reachSimMatrix);
				inm.setCalculateSimMatrix(false);
				inm.setCalculateReachability(true);
				inm.setIndex(i);
				inm.setStart(i);
				inm.setProcessingQueue(processingQueue);
				processingQueue.add(i);
				while(processingQueue.size() >= numProcessors * 5) {
					try {
						LOGGER.info("Waiting for queue...");
						processingQueue.wait();
					} catch (InterruptedException ex) {
						ex.printStackTrace();
					}
				}
				Thread t = new Thread(inm);
				threads.add(t);
				t.start();
			}
		}
		int size = threads.size();
		i = 0;
		for(; i < size; i++) {
			try {
				threads.get(i).join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		// print out reach-similarity matrix for debugging
		//		System.out.println("REACH SIMILARITY MATRIX");
		//		for(i = 0; i < numInstances; i++) {
		//			for(j = 0;  j < numInstances; j++) {
		//				System.out.print(reachSimMatrix[i][j] + ", ");
		//			}
		//			System.out.println();
		//		}
	}

	private void calculateLRD() {
		lrd = new double[numInstances];
//		processingQueue = new PriorityQueue<Integer>();
//
//		List<Thread> threads = new ArrayList<Thread>();
//		int i;
//		synchronized(processingQueue){
//			for(i = 0; i < numInstances; i++) {
//				InstanceNumericalMethods inm = new InstanceNumericalMethods(numericalBinMatrix, categoricalMatrix, numericalBinOrderingMatrix);
//				inm.setCategoricalWeights(categoricalWeights);
//				inm.setNumericalWeights(numericalWeights);
//				inm.setNumInstances(numInstances);
//				inm.setkSimilarityIndicesMatrix(kSimilarityIndicesMatrix);
//				inm.setReachSimMatrix(reachSimMatrix);
//				inm.setLrd(lrd);
//				inm.setCalculateSimMatrix(false);
//				inm.setCalculateLRD(true);
//				inm.setIndex(i);
//				inm.setProcessingQueue(processingQueue);
//				processingQueue.add(i);
//				while(processingQueue.size() >= numProcessors * 5) {
//					try {
//						LOGGER.info("Waiting for queue...");
//						processingQueue.wait();
//					} catch (InterruptedException ex) {
//						ex.printStackTrace();
//					}
//				}
//				Thread t = new Thread(inm);
//				threads.add(t);
//				t.start();
//			}
//		}
//		int size = threads.size();
//		i = 0;
//		for(; i < size; i++) {
//			try {
//				threads.get(i).join();
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}
		
		int i;
		for(i = 0; i < numInstances; i++) {
			int[] kClosestNeighbors = kSimilarityIndicesMatrix[i];
			double sumReachSim = 0;
			if(kClosestNeighbors != null) {
				for(int j : kClosestNeighbors) {
					sumReachSim += reachSimMatrix[i][j];
				}
				lrd[i] = sumReachSim/kClosestNeighbors.length;
			} else {
				lrd[i] = 0;
			}
		}
	}

	private void calculateLOF() {
		lof = new double[numInstances];

		int i;
		for(i = 0; i < numInstances; i++) {
			double sumLRD = 0;
			double sumReachSim = 0;
			int[] kClosestNeighbors = kSimilarityIndicesMatrix[i];
			if(kClosestNeighbors != null) {
				for(int j : kClosestNeighbors) {
					sumLRD += lrd[j];
					sumReachSim += reachSimMatrix[i][j];
				}
				lof[i] = sumLRD / sumReachSim;
			} else {
				lof[i] = Double.POSITIVE_INFINITY;
			}
		}
	}

	private void calculateLOOP() {
		lop = new double[numInstances];

		double[] ploof = new double[numInstances];
		int i;
		for(i = 0; i < numInstances; i++) {
			ploof[i] = lof[i] - 1;
		}

		double stdev = StatisticsUtilityMethods.getSampleStandardDeviationIgnoringInfinity(ploof);
		double squareRoot2 = Math.sqrt(2);
		// no variation
		if(stdev == 0) {
			for(i = 0; i < numInstances; i++) {
				if(Double.isInfinite(lof[i])) {
					lop[i] = 1;
				} else {
					lop[i] = 0;
				}
			}
		} else {
			for(i = 0; i < numInstances; i++) {
				lop[i] = Math.max(0, Erf.erf(ploof[i]/(stdev * squareRoot2)));
			}
		}

	}
}

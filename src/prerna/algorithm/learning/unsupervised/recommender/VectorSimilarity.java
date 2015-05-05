package prerna.algorithm.learning.unsupervised.recommender;

import org.apache.commons.math3.linear.BlockRealMatrix;
import org.apache.commons.math3.linear.RealVector;

public class VectorSimilarity{

	public static double[] getUserSimilarityArray(BlockRealMatrix dataMatrix, int user) {

		long start = System.currentTimeMillis();

		int numRows = dataMatrix.getRowDimension();
		double[] similarityArray = new double[numRows];

		int i = 0;
		for(; i < numRows; i++){
			RealVector v1 = dataMatrix.getRowVector(user);
			double simVal = v1.cosine(dataMatrix.getRowVector(i));
			similarityArray[i] = simVal;
		}

		long end = System.currentTimeMillis();

		System.out.println("Time in sec = " + ((end-start)/1000) );

		return similarityArray;
	}	
}


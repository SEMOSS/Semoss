package prerna.algorithm.learning.unsupervised.recommender;

import java.util.List;
import java.util.Map;

import org.apache.commons.math3.linear.BlockRealMatrix;
import org.apache.commons.math3.linear.RealVector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.om.SEMOSSParam;

/*
 * algorithm to calculate the similarity of users with a user using cosine similarity
 */
public class VectorSimilarity implements SimilarityAnalytics{

	private Map options;
	private final String USER = "user";
	
	@Override
	public double[] runAlgorithm(BlockRealMatrix dataMatrix) {

		if(!options.containsKey(USER)) {
			throw new NullPointerException("No User Defined in Options To Run Algorithm");
		}
		//long start = System.currentTimeMillis();

		int numRows = dataMatrix.getRowDimension();
		double[] similarityArray = new double[numRows];

		RealVector v1 = (RealVector) options.get(USER);
		int i = 0;
		for(; i < numRows; i++){
			double simVal = v1.cosine(dataMatrix.getRowVector(i));
			similarityArray[i] = simVal;
		}

		//long end = System.currentTimeMillis();
		//System.out.println("Time in sec = " + ((end-start)/1000) );

		return similarityArray;
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getResultDescription() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setSelectedOptions(Map<String, Object> selected) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<SEMOSSParam> getOptions() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ITableDataFrame runAlgorithm(ITableDataFrame... data) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDefaultViz() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getChangedColumns() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Object> getResultMetadata() {
		// TODO Auto-generated method stub
		return null;
	}
}


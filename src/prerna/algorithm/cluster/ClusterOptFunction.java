package prerna.algorithm.cluster;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.math3.analysis.UnivariateFunction;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class ClusterOptFunction implements UnivariateFunction{

	private static final Logger LOGGER = LogManager.getLogger(ClusterOptFunction.class.getName());
	
	private ArrayList<Object[]> list;
	private String[] names;
//	PrintWriter writer = null;
	private HashMap<Integer, Double> values = new HashMap<Integer, Double>();
	
	@Override
	public double value(double arg0) {
//		//TODO: delete writing to file
//		if(writer == null) {
//			try {
//				writer = new PrintWriter("Clustering_Algorithm_Optimization.txt");
//				writer.println("Clusters\t\t\tAverage");
//			} catch (FileNotFoundException e) {
//				e.printStackTrace();
//			}
//		} 
		
		int numClusterFloor = (int) Math.floor(arg0);
		int numClusterCeil = (int) Math.ceil(arg0);
		
		double avg1;
		double avg2;
		
		if(values.containsKey(numClusterFloor)) {
			avg1 = values.get(numClusterFloor);
		} else {
			avg1 = value(numClusterFloor);
		}
		
		if(values.containsKey(numClusterCeil)) {
			avg2 = values.get(numClusterCeil);
		} else {
			avg2 = value(numClusterCeil);
		}
		// assume linear relationship for similarity density between individual points
		double diff = avg2 - avg1;
		double ratio = arg0 - (int) arg0;
		double retVal = avg1 + diff*ratio;
//		writer.println(arg0 + "\t\t\t" + retVal);

		return retVal;
	}
	
	public double value(int arg0) {
//		if(writer == null) {
//			try {
//				writer = new PrintWriter("Clustering_Algorithm_Optimization.txt");
//				writer.println("Clusters\t\t\tAverage");
//			} catch (FileNotFoundException e) {
//				e.printStackTrace();
//			}
//		} 
		AbstractClusteringAlgorithm clusterAlg = new ClusteringAlgorithm(list, names);
		clusterAlg.setNumClusters(arg0);
		clusterAlg.execute();
		double instanceToClusterSim = clusterAlg.calculateFinalInstancesToClusterSimilarity();
		double clusterToClusterSim = clusterAlg.calculateFinalTotalClusterToClusterSimilarity();
		double sum = instanceToClusterSim + clusterToClusterSim;
		double items = list.size() + (double) (arg0 * (arg0-1) /2);
		double average = sum/items;
		
		values.put(arg0, average);
//		writer.println(arg0 + "\t\t\t" + average);

		return average;
	}
	
//	public void closerWriter(){
//		writer.close();
//	}
	
	public ArrayList<Object[]> getList() {
		return list;
	}
	public void setList(ArrayList<Object[]> list) {
		this.list = list;
	}
	public String[] getNames() {
		return names;
	}
	public void setNames(String[] names) {
		this.names = names;
	}
	
}

package prerna.algorithm.learning.unsupervised.clustering;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.IAnalyticRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.util.Cluster;
import prerna.math.SimilarityWeighting;
import prerna.om.SEMOSSParam;

public abstract class AbstractClusteringRoutine implements IClustering, IAnalyticRoutine {

	private ITableDataFrame data;
	private String[] attributeNames;
	private boolean[] isNumeric;
	
	private int numClusters;
	private int numInstances;
	private int instanceIndex;

	
	private int[] numInstancesInCluster;
	private int[] clusterAssignmentForInstance;
	
	private List<Cluster> clusters;
	
	private Map<String, Double> numericalWeights;
	private Map<String, Double> categoricalWeights;
	
	// potentially move the calculating weights logic into the ITableDataFrame
	public void calculateWeights() {
		int i = 0;
		int size = attributeNames.length;
		
		List<Double> numericalEntropy = new ArrayList<Double>();
		List<String> numericalNames = new ArrayList<String>();
		
		List<Double> categoricalEntropy = new ArrayList<Double>();
		List<String> categoricalNames = new ArrayList<String>();
		
		for(; i < size; i++) {
			String attribute = attributeNames[i];
			if(isNumeric[i]) {
				numericalNames.add(attribute);
				numericalEntropy.add(data.getEntropyDensity(attribute));
			} else {
				categoricalNames.add(attribute);
				categoricalEntropy.add(data.getEntropyDensity(attribute));
			}
		}
		
		double[] numericalWeightsArr = SimilarityWeighting.generateWeighting((Double[]) numericalEntropy.toArray());
		i = 0;
		int numNumeric = numericalNames.size();
		for(; i < numNumeric; i++) {
			numericalWeights.put(numericalNames.get(i), numericalWeightsArr[i]);
		}
		
		double[] categoricalWeightsArr = SimilarityWeighting.generateWeighting((Double[]) categoricalEntropy.toArray());
		i = 0;
		int numCategorical = categoricalNames.size();
		for(; i < numCategorical; i++) {
			categoricalWeights.put(categoricalNames.get(i), categoricalWeightsArr[i]);
		}
	}
	
	/**
	 * Will generate the clusters by picking the most different instances
	 */
	public void initializeClusters() {
		Cluster allInitial = new Cluster(categoricalWeights, numericalWeights);
		Iterator<Object[]> it = data.iterator();
		allInitial.addToCluster(it.next(), attributeNames, isNumeric);
		clusters.add(allInitial);
		
		for(int i = 1; i < numClusters; i++) {
			double simVal = 2;
			Object[] bestInstance = null;
			while(it.hasNext()) {
				Object[] instance = it.next();
				double val = allInitial.getSimilarityForInstance(instance, attributeNames, isNumeric);
				if(val < simVal) {
					bestInstance = instance;
				}
				if(val == 0) {
					break;
				}
			}
			allInitial.addToCluster(bestInstance, attributeNames, isNumeric);
			
			Cluster newCluster = new Cluster(categoricalWeights, numericalWeights);
			newCluster.addToCluster(bestInstance, attributeNames, isNumeric);
			clusters.add(newCluster);
			it = data.iterator();
		}
	}
	
	@Override
	public String getName() {
		return "Clustering Algorithm";
	}

	@Override
	public void setSelectedOptions(Map<String, Object> selected) {
		this.numClusters = (int) selected.get("numClusters");
		this.instanceIndex = (int) selected.get("selectedInstance");
		
		
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

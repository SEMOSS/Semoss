package prerna.algorithm.learning.unsupervised.clustering;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.IAnalyticRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.util.Cluster;
import prerna.om.SEMOSSParam;

public class MultiClusteringRoutine implements IAnalyticRoutine {

	protected List<SEMOSSParam> options;
	protected final String MIN_NUM_CLUSTERS = "minNumClusters";
	protected final String MAX_NUM_CLUSTERS = "maxNumClusters";
	protected final String INSTANCE_INDEX_KEY = "instanceIdx";
	protected final String DISTANCE_MEASURE	= "distanceMeasure";
	protected String clusterColumnID = "";

	protected Map<Integer, Double> clusterScores = new HashMap<Integer, Double>();
	
	private String[] attributeNames;
	private String instanceType;
	private int instanceIndex;
	private boolean[] isNumeric;
	
	public MultiClusteringRoutine() {
		this.options = new ArrayList<SEMOSSParam>();

		SEMOSSParam p1 = new SEMOSSParam();
		p1.setName(this.MIN_NUM_CLUSTERS);
		options.add(0, p1);

		SEMOSSParam p2 = new SEMOSSParam();
		p2.setName(this.MAX_NUM_CLUSTERS);
		options.add(1, p2);
		
		SEMOSSParam p3 = new SEMOSSParam();
		p3.setName(this.INSTANCE_INDEX_KEY);
		options.add(2, p3);
		
		SEMOSSParam p4 = new SEMOSSParam();
		p4.setName(this.DISTANCE_MEASURE);
		options.add(3, p4);
	}
	
	@Override
	public ITableDataFrame runAlgorithm(ITableDataFrame... data) {
		// values defined in options
		//TODO: below is simply for ease in testing
		int start = 2;
		int end = 50;
		this.instanceIndex = 0;
		this.clusterColumnID = "clusterID";
//		int start = (int) options.get(0).getSelected();
//		int end = (int) options.get(1).getSelected();
//		this.instanceIndex = (Integer) options.get(2).getSelected();
		
		
		this.attributeNames = data[0].getColumnHeaders();
		this.instanceType = attributeNames[instanceIndex];
		this.isNumeric = data[0].isNumeric();
		ITableDataFrame results = runGoldenSelectionForNumberOfClusters(data[0], start, end);
		return results;
	}
	
	private ITableDataFrame runGoldenSelectionForNumberOfClusters(ITableDataFrame data, int start, int end) {		
		ITableDataFrame bestResults = null;
		int a = start;
		int b = end;
		double phi = (double) (1 + Math.sqrt(5)) / 2;
		int x1 = (int) Math.round((phi - 1)*a + (2-phi)*b);
		int x2 = (int) Math.round((2-phi)*a + (phi - 1)*b);
		
		double bestVal = -1;
		Map<Integer, Double> previousResults = new Hashtable<Integer, Double>();
		
		List<Cluster> startClusterList = new ArrayList<Cluster>();
		ITableDataFrame startClusterResult = runClusteringRoutine(data, x1, startClusterList);
		double startVal = computeClusteringScore(data, startClusterResult, startClusterList, previousResults);
		previousResults.put(x1, startVal);

		List<Cluster> endClusterList = new ArrayList<Cluster>();
		ITableDataFrame endClusterResult = runClusteringRoutine(data, x2, endClusterList);
		double endVal = computeClusteringScore(data, endClusterResult, endClusterList, previousResults);
		previousResults.put(x2, endVal);

		while(Math.abs(b - a) > 1) {
			if(startVal < endVal) {
				a = x1;
				x1 = x2;
				x2 = (int) Math.round((2-phi)*a + (phi-1)*b);
			} else {
				b = x2;
				x2 = x1;
				x1 = (int) Math.round((phi-1)*a + (2-phi)*b);
			}
			
			startClusterList.clear();
			startClusterResult = runClusteringRoutine(data, x1, startClusterList);
			startVal = computeClusteringScore(data, startClusterResult, startClusterList, previousResults);
			previousResults.put(x1, startVal);
			
			endClusterList.clear();
			endClusterResult = runClusteringRoutine(data, x2, endClusterList);
			endVal = computeClusteringScore(data, endClusterResult, endClusterList, previousResults);
			previousResults.put(x2, endVal);
			
			if(startVal > endVal) {
				if(startVal > bestVal) {
					bestVal = startVal;
					bestResults = startClusterResult;
				}
			} else {
				if(endVal > bestVal) {
					bestVal = endVal;
					bestResults = endClusterResult;
				}
			}
		}
		
		return bestResults;
	}
	
	private double computeClusteringScore(ITableDataFrame data, ITableDataFrame results, List<Cluster> clusters, Map<Integer, Double> previousResults) {
		int numClusters = clusters.size();
		if(previousResults.containsKey(numClusters)) {
			return previousResults.get(numClusters);
		}
		
		// calculate inner-cluster similarity
		double innerClusterSimilairty = 0;
		Iterator<List<Object[]>> it = data.scaledUniqueIterator(instanceType, false);
		while(it.hasNext()) {
			List<Object[]> instance = it.next();
			Object instanceName = instance.get(0)[instanceIndex];
			List<Object[]> instanceResult = results.getData(instanceType, instanceName);
			int clusterIndex = (int) instanceResult.get(0)[1];
			double simVal = clusters.get(clusterIndex).getSimilarityForInstance(instance, attributeNames, isNumeric, instanceIndex);
			innerClusterSimilairty += simVal / clusters.get(clusterIndex).getNumInstances();
		}

		// calculate cluster-cluster similarity
		double clusterToClusterSimilarity = 0;
		for(int i = 0; i < numClusters-1; i++) {
			Cluster c1 = clusters.get(i);
			for(int j = i+1; j < numClusters; j++) {
				Cluster c2 = clusters.get(j);
				double simVal = c1.getClusterSimilarity(c2, instanceType);
				clusterToClusterSimilarity += (1-simVal);
			}
		}
		
		return innerClusterSimilairty/numClusters + clusterToClusterSimilarity / ( (double) (numClusters * (numClusters-1) /2));
	}
	
	private ITableDataFrame runClusteringRoutine(ITableDataFrame data, int numClusters, List<Cluster> clusters) {
		//TODO: below is simply for ease in testing
		this.options.get(2).setSelected(0);
		
		IClustering clustering = new ClusteringRoutine();
		List<SEMOSSParam> params = clustering.getOptions();
		Map<String, Object> selectedOptions = new HashMap<String, Object>();
		selectedOptions.put(params.get(0).getName(), numClusters);
		selectedOptions.put(params.get(1).getName(), this.options.get(2).getSelected());
		selectedOptions.put(params.get(2).getName(), this.options.get(3).getSelected());
		clustering.setSelectedOptions(selectedOptions);
		
		ITableDataFrame results = clustering.runAlgorithm(data);
		clusters.addAll(clustering.getClusters());
		
		return results;
	}
	
	@Override
	public String getName() {
		return "Clustering Algorithm";
	}

	@Override
	public String getResultDescription() {
		return "Clustering algorithm without specifying the number of clusters.";
	}

	@Override
	public void setSelectedOptions(Map<String, Object> selected) {
		Set<String> keySet = selected.keySet();
		for(String key : keySet) {
			for(SEMOSSParam param : options) {
				if(param.getName().equals(key)){
					param.setSelected(selected.get(key));
					// set cluster id name based on the instance selected
					if(param.getName().equals(INSTANCE_INDEX_KEY)) {
						this.clusterColumnID = selected.get(key).toString().toUpperCase() + "_CLUSTER_ID";
					}
					break;
				}
			}
		}
	}

	@Override
	public List<SEMOSSParam> getOptions() {
		return this.options;
	}
	
	@Override
	public String getDefaultViz() {
		return "prerna.ui.components.playsheets.ClusteringVizPlaySheet";
	}

	@Override
	public List<String> getChangedColumns() {
		List<String> changedCols = new ArrayList<String>();
		changedCols.add(this.clusterColumnID);
		return changedCols;
	}

	@Override
	public Map<String, Object> getResultMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

}

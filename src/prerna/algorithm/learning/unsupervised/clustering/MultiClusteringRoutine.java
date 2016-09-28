package prerna.algorithm.learning.unsupervised.clustering;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.IAnalyticTransformationRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.util.Cluster;
import prerna.om.SEMOSSParam;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

public class MultiClusteringRoutine implements IAnalyticTransformationRoutine {

	protected List<SEMOSSParam> options;
	public static final String MIN_NUM_CLUSTERS = "minNumClusters";
	public static final String MAX_NUM_CLUSTERS = "maxNumClusters";
	public static final String INSTANCE_INDEX_KEY = "instanceIdx";
	public static final String DISTANCE_MEASURE	= "distanceMeasure";
	public static final String SKIP_ATTRIBUTES	= "skipAttributes";

	protected String clusterColName = "";

	protected Map<Integer, Double> clusterScores = new HashMap<Integer, Double>();
	
	private String[] attributeNames;
	private String instanceType;
	private int instanceIndex;
	private boolean[] isNumeric;
	
	private int optimalNumClusters;
	
	public MultiClusteringRoutine() {
		this.options = new ArrayList<SEMOSSParam>();

		SEMOSSParam p1 = new SEMOSSParam();
		p1.setName(MIN_NUM_CLUSTERS);
		options.add(0, p1);

		SEMOSSParam p2 = new SEMOSSParam();
		p2.setName(MAX_NUM_CLUSTERS);
		options.add(1, p2);
		
		SEMOSSParam p3 = new SEMOSSParam();
		p3.setName(INSTANCE_INDEX_KEY);
		options.add(2, p3);
		
		SEMOSSParam p4 = new SEMOSSParam();
		p4.setName(DISTANCE_MEASURE);
		options.add(3, p4);
		
		SEMOSSParam p5 = new SEMOSSParam();
		p5.setName(SKIP_ATTRIBUTES);
		options.add(4, p5);
	}
	
	@Override
	public ITableDataFrame runAlgorithm(ITableDataFrame... data) {
		ITableDataFrame dataFrame = data[0];
		// values defined in options
		int start = ((Number) options.get(0).getSelected()).intValue();
		int end = ((Number) options.get(1).getSelected()).intValue();
		this.instanceIndex = ((Number) options.get(2).getSelected()).intValue();
		
		this.attributeNames = data[0].getColumnHeaders();
		this.instanceType = attributeNames[instanceIndex];
		this.isNumeric = data[0].isNumeric();
		
		int numInstances = data[0].getUniqueInstanceCount(attributeNames[instanceIndex]);
		if(numInstances == 1) {
			throw new IllegalArgumentException("Instance column only contains one unqiue value.");
		}
		if(end > numInstances) {
			end = numInstances;
		}
		
		Map<Object, Integer> results = null;
		if(start != end) {
			results = runGoldenSelectionForNumberOfClusters(data[0], start, end);
		} else { // usually occurs when there is too few data points
			results = runClusteringRoutine(data[0], start, new ArrayList<Cluster>(), new HashMap<Integer, Double>());
			this.optimalNumClusters = start;
		}
		
		String attributeName = attributeNames[instanceIndex];
		// to avoid adding columns with same name
		int counter = 0;
		this.clusterColName = attributeName + "_CLUSTER_" + counter;
		while(ArrayUtilityMethods.arrayContainsValue(attributeNames, clusterColName)) {
			counter++;
			this.clusterColName = attributeName + "_CLUSTER_" + counter;
		}

		Map<String, String> dataType = new HashMap<String, String>();
		dataType.put(clusterColName, "DOUBLE");
		dataFrame.connectTypes(attributeName, clusterColName, dataType);
		for(Object instance : results.keySet()) {
			int val = results.get(instance);

			Map<String, Object> clean = new HashMap<String, Object>();
			if(instance.toString().startsWith("http://semoss.org/ontologies/Concept/")) {
				instance = Utility.getInstanceName(instance.toString());
			}
			clean.put(attributeName, instance);
			clean.put(clusterColName, val);

			dataFrame.addRelationship(clean);
		}

		return null;
	}
	
	private Map<Object, Integer> runGoldenSelectionForNumberOfClusters(ITableDataFrame data, int start, int end) {		
		Map<Object, Integer> bestResults = null;
		int a = start;
		int b = end;
		double phi = (double) (1 + Math.sqrt(5)) / 2;
		int x1 = (int) Math.round((phi - 1)*a + (2-phi)*b);
		int x2 = (int) Math.round((2-phi)*a + (phi - 1)*b);
		
		double bestVal = -1;
		Map<Integer, Double> previousResults = new Hashtable<Integer, Double>();
		
		List<Cluster> startClusterList = new ArrayList<Cluster>();
		double startVal = 0;
		Map<Object, Integer> startClusterResult = null;
		String errorMessage1 = null;
		try {
			startClusterResult = runClusteringRoutine(data, x1, startClusterList, previousResults);
			startVal = computeClusteringScore(data, startClusterResult, startClusterList, previousResults, x1);
			previousResults.put(x1, startVal);
		} catch (IllegalArgumentException ex) {
			// do nothing
			errorMessage1 = ex.getMessage();
		}
		
		List<Cluster> endClusterList = new ArrayList<Cluster>();
		double endVal = 0;
		Map<Object, Integer> endClusterResult = null;
		String errorMessage2 = null;
		try {
			endClusterResult = runClusteringRoutine(data, x2, endClusterList, previousResults);
			endVal = computeClusteringScore(data, endClusterResult, endClusterList, previousResults, x2);
			previousResults.put(x2, endVal);
		} catch (IllegalArgumentException ex) {
			// do nothing
			errorMessage2 = ex.getMessage();
		}
		
		if(startClusterResult == null && endClusterResult == null) {
			throw new IllegalArgumentException(errorMessage1 + ".\n" + errorMessage2);
		}
		
		if(startVal >= endVal) {
			if(startVal > bestVal) {
				bestVal = startVal;
				bestResults = startClusterResult;
				this.optimalNumClusters = x1;
			}
		} else {
			if(endVal > bestVal) {
				bestVal = endVal;
				bestResults = endClusterResult;
				this.optimalNumClusters = x2;
			}
		}
		
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
			
			try {
				startClusterList.clear();
				startClusterResult = runClusteringRoutine(data, x1, startClusterList, previousResults);
				startVal = computeClusteringScore(data, startClusterResult, startClusterList, previousResults, x1);
				previousResults.put(x1, startVal);
			} catch (IllegalArgumentException ex) {
				// do nothing
			}
			
			try {
				endClusterList.clear();
				endClusterResult = runClusteringRoutine(data, x2, endClusterList, previousResults);
				endVal = computeClusteringScore(data, endClusterResult, endClusterList, previousResults, x2);
				previousResults.put(x2, endVal);
			} catch (IllegalArgumentException ex) {
				//do nothing
			}
			
			if(startVal > endVal) {
				if(startVal > bestVal) {
					bestVal = startVal;
					if(startClusterResult != null) { // if null, its already stored
						bestResults = startClusterResult;
					}
					this.optimalNumClusters = x1;
				}
			} else {
				if(endVal > bestVal) {
					bestVal = endVal;
					if(endClusterResult != null) { // if null, its already stored
						bestResults = endClusterResult;
					}
					this.optimalNumClusters = x2;
				}
			}
		}
		
		return bestResults;
	}
	
	private double computeClusteringScore(ITableDataFrame data, Map<Object, Integer> results, List<Cluster> clusters, Map<Integer, Double> previousResults, int numClusters) {
		if(previousResults.containsKey(numClusters)) {
			return previousResults.get(numClusters);
		}
		
		// calculate inner-cluster similarity
		double innerClusterSimilairty = 0;
//		if(results instanceof BTreeDataFrame) {
//			Iterator<List<Object[]>> it = data.scaledUniqueIterator(instanceType, false);
//			while(it.hasNext()) {
//				List<Object[]> instance = it.next();
//				Object instanceName = instance.get(0)[instanceIndex];
//				List<Object[]> instanceResult = results.getData(instanceType, instanceName);
//				int clusterIndex = (int) instanceResult.get(0)[1];
//				double simVal = clusters.get(clusterIndex).getSimilarityForInstance(instance, attributeNames, isNumeric, instanceIndex);
//				innerClusterSimilairty += simVal / clusters.get(clusterIndex).getNumInstances();
//			}
//		} else if(data instanceof TinkerFrame){
//			String[] headers = results.getColumnHeaders();
//			GremlinBuilder builder = ((TinkerFrame) results).getGremlinBuilder(Arrays.asList(results.getColumnHeaders()));
//			builder.setGroupBySelector(instanceType);
//			GraphTraversal gt = builder.executeScript();
//			Map<Object, Object> groupByMap = null;
//			if(gt.hasNext()) {
//				groupByMap = (Map<Object, Object>) gt.next();
//			}
			
			Iterator<List<Object[]>> it = data.scaledUniqueIterator(instanceType, null);
			while(it.hasNext()) {
				List<Object[]> instance = it.next();
				Object instanceName = instance.get(0)[instanceIndex];
//				List<Object[]> instanceResult = getRowsForInstance(groupByMap, instanceName, headers);
				int clusterIndex = results.get(instanceName);
				double simVal = clusters.get(clusterIndex).getSimilarityForInstance(instance, attributeNames, isNumeric, instanceIndex);
				innerClusterSimilairty += simVal / clusters.get(clusterIndex).getNumInstances();
			}
//		} else {
//			throw new IllegalArgumentException("Unknown data frame");
//		}

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
	
//	private List<Object[]> getRowsForInstance(Map<Object, Object> groupByMap, Object instanceName, String[] headers) {
//		List<Map> instanceArrayMap = (List<Map>) groupByMap.get(instanceName);
//		
//		int size = instanceArrayMap.size();
//		List<Object[]> retVector = new Vector<Object[]>(size);
//		for(int i = 0; i < size; i++) {
//			Map rowMap = instanceArrayMap.get(i);
//			Object[] row = new Object[headers.length];
//			for(int j = 0; j < headers.length; j++) {
//				row[j] = ((Vertex) rowMap.get(headers[j])).value(Constants.NAME);
//			}
//			retVector.add(row);
//		}
//		
//		return retVector;
//	}
	
	private Map<Object, Integer> runClusteringRoutine(ITableDataFrame data, int numClusters, List<Cluster> clusters, Map<Integer, Double> previousResults) {
		if(previousResults.containsKey(numClusters)) {
			return null;
		}
		
		System.out.println("Running clustering for " + numClusters + " number of clusters");
		ClusteringRoutine clustering = new ClusteringRoutine();
		clustering.setAppendOntoDataMaker(false);
		List<SEMOSSParam> params = clustering.getOptions();
		Map<String, Object> selectedOptions = new HashMap<String, Object>();
		selectedOptions.put(params.get(0).getName(), numClusters);
		selectedOptions.put(params.get(1).getName(), this.options.get(2).getSelected());
		selectedOptions.put(params.get(2).getName(), this.options.get(3).getSelected());
		selectedOptions.put(params.get(3).getName(), this.options.get(4).getSelected());

		clustering.setSelectedOptions(selectedOptions);
		clustering.runAlgorithm(data);
		clusters.addAll(clustering.getClusters());
		return clustering.getResults();
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
		changedCols.add(this.clusterColName);
		return changedCols;
	}

	@Override
	public Map<String, Object> getResultMetadata() {
		return null;
	}

	public int getNumClusters() {
		return this.optimalNumClusters;
	}
}

package prerna.algorithm.learning.unsupervised.clustering;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.util.Cluster;
import prerna.algorithm.learning.util.IClusterDistanceMode;
import prerna.math.SimilarityWeighting;
import prerna.om.SEMOSSParam;

public abstract class AbstractClusteringRoutine implements IClustering {

	protected boolean success;
	
	protected List<SEMOSSParam> options;
	protected final String NUM_CLUSTERS_KEY = "numClusters";
	protected final String INSTANCE_INDEX_KEY = "instanceIdx";
	protected final String DISTANCE_MEASURE	= "distanceMeasure";
	
	protected String clusterColumnID = "";
	
	// keeping track of cluster information
	protected List<Cluster> clusters = new ArrayList<Cluster>();
	protected List<Integer> numInstancesInCluster = new ArrayList<Integer>();

	// values from data
	protected ITableDataFrame dataFrame;
	protected String[] attributeNames;
	protected boolean[] isNumeric;
	
	// defined in SEMOSS options
	protected int numClusters;
	protected int instanceIndex;
	protected Map<String, IClusterDistanceMode.DistanceMeasure> distanceMeasure;

	protected Map<String, Double> numericalWeights = new HashMap<String, Double>();
	protected Map<String, Double> categoricalWeights = new HashMap<String, Double>();
	
	// set distance mode
	public AbstractClusteringRoutine() {
		this.options = new ArrayList<SEMOSSParam>();

		SEMOSSParam p1 = new SEMOSSParam();
		p1.setName(this.NUM_CLUSTERS_KEY);
		options.add(0, p1);

		SEMOSSParam p2 = new SEMOSSParam();
		p2.setName(this.INSTANCE_INDEX_KEY);
		options.add(1, p2);
		
		SEMOSSParam p3 = new SEMOSSParam();
		p3.setName(this.DISTANCE_MEASURE);
		options.add(2, p3);
	}
	
	// potentially move the calculating weights logic into the ITableDataFrame
	public void calculateWeights() {
		int i = 0;
		int size = attributeNames.length;
		String instanceType = attributeNames[instanceIndex];
		
		List<Double> numericalEntropy = new ArrayList<Double>();
		List<String> numericalNames = new ArrayList<String>();
		
		List<Double> categoricalEntropy = new ArrayList<Double>();
		List<String> categoricalNames = new ArrayList<String>();
		
		for(; i < size; i++) {
			String attribute = attributeNames[i];
			if(attribute.equals(instanceType)) {
				continue;
			}
			if(isNumeric[i]) {
				numericalNames.add(attribute);
				numericalEntropy.add(dataFrame.getEntropyDensity(attribute));
			} else {
				categoricalNames.add(attribute);
				categoricalEntropy.add(dataFrame.getEntropyDensity(attribute));
			}
		}
		
		if(!numericalEntropy.isEmpty()){
			double[] numericalWeightsArr = SimilarityWeighting.generateWeighting(numericalEntropy.toArray(new Double[0]));
			i = 0;
			int numNumeric = numericalNames.size();
			for(; i < numNumeric; i++) {
				numericalWeights.put(numericalNames.get(i), numericalWeightsArr[i]);
			}
		}
		if(!categoricalEntropy.isEmpty()){
			double[] categoricalWeightsArr = SimilarityWeighting.generateWeighting(categoricalEntropy.toArray(new Double[0]));
			i = 0;
			int numCategorical = categoricalNames.size();
			for(; i < numCategorical; i++) {
				categoricalWeights.put(categoricalNames.get(i), categoricalWeightsArr[i]);
			}
		}
	}
	
	@Override
	public String getName() {
		return "Clustering Algorithm";
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
	
	public List<Cluster> getClusters() {
		return this.clusters;
	}
}

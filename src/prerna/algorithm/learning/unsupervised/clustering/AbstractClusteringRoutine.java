package prerna.algorithm.learning.unsupervised.clustering;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.util.Cluster;
import prerna.algorithm.learning.util.IClusterDistanceMode;
import prerna.om.SEMOSSParam;

public abstract class AbstractClusteringRoutine implements IClustering {

	protected boolean success;
	
	protected List<SEMOSSParam> options;
	protected static final String NUM_CLUSTERS_KEY = "numClusters";
	protected static final String INSTANCE_INDEX_KEY = "instanceIdx";
	protected static final String DISTANCE_MEASURE = "distanceMeasure";
	protected static final String SKIP_ATTRIBUTES = "skipAttributes";

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
	protected List<String> skipAttributes;

	protected Map<String, Double> numericalWeights = new HashMap<String, Double>();
	protected Map<String, Double> categoricalWeights = new HashMap<String, Double>();
	
	protected String clusterColName;
	
	// set distance mode
	public AbstractClusteringRoutine() {
		this.options = new ArrayList<SEMOSSParam>();

		SEMOSSParam p1 = new SEMOSSParam();
		p1.setName(NUM_CLUSTERS_KEY);
		options.add(0, p1);

		SEMOSSParam p2 = new SEMOSSParam();
		p2.setName(INSTANCE_INDEX_KEY);
		options.add(1, p2);
		
		SEMOSSParam p3 = new SEMOSSParam();
		p3.setName(DISTANCE_MEASURE);
		options.add(2, p3);
		
		SEMOSSParam p4 = new SEMOSSParam();
		p4.setName(SKIP_ATTRIBUTES);
		options.add(3, p4);
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
		changedCols.add(clusterColName);
		return changedCols;
	}
	
	public List<Cluster> getClusters() {
		return this.clusters;
	}

	public int getNumClusters() {
		return (int) options.get(0).getSelected();
	}
}

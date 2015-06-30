package prerna.algorithm.learning.unsupervised.clustering;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.IAnalyticRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.util.Cluster;
import prerna.math.SimilarityWeighting;
import prerna.om.SEMOSSParam;

public abstract class AbstractClusteringRoutine implements IClustering, IAnalyticRoutine {

	protected List<SEMOSSParam> options;
	protected final String NUM_CLUSTERS_KEY = "numClusters";
	protected final String INSTANCE_INDEX_KEY = "instanceIdx";
	protected String clusterColumnID = "";
	
	// keeping track of cluster information
	protected List<Cluster> clusters;
	protected List<Integer> numInstancesInCluster;

	// values from data
	protected ITableDataFrame dataFrame;
	protected String[] attributeNames;
	protected boolean[] isNumeric;
	
	// defined in SEMOSS options
	protected int numClusters;
	protected int instanceIndex;

	protected Map<String, Double> numericalWeights;
	protected Map<String, Double> categoricalWeights;
	
	public AbstractClusteringRoutine() {
		this.options = new ArrayList<SEMOSSParam>();

		SEMOSSParam p1 = new SEMOSSParam();
		p1.setName(this.NUM_CLUSTERS_KEY);
		options.add(0, p1);

		SEMOSSParam p2 = new SEMOSSParam();
		p2.setName(this.INSTANCE_INDEX_KEY);
		options.add(1, p2);
	}
	
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
				numericalEntropy.add(dataFrame.getEntropyDensity(attribute));
			} else {
				categoricalNames.add(attribute);
				categoricalEntropy.add(dataFrame.getEntropyDensity(attribute));
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
		Iterator<Object[]> it = dataFrame.iterator();
		Object[] values = it.next();
		
		Cluster firstCluster = new Cluster(categoricalWeights, numericalWeights);
		firstCluster.addToCluster(values, attributeNames, isNumeric);
		clusters.add(firstCluster);
		// update cluster instance count
		numInstancesInCluster.add(1);
		
		// create a cluster to serve as a combination of all the starting seeds
		Cluster combinedInstances = new Cluster(categoricalWeights, numericalWeights);
		combinedInstances.addToCluster(values, attributeNames, isNumeric);
		
		
		for(int i = 1; i < numClusters; i++) {
			double simVal = 2;
			Object[] bestInstance = null;
			while(it.hasNext()) {
				Object[] instance = it.next();
				double val = combinedInstances.getSimilarityForInstance(instance, attributeNames, isNumeric, instanceIndex);
				if(val < simVal) {
					bestInstance = instance;
				}
				if(val == 0) {
					break;
				}
			}
			// update combined cluster
			combinedInstances.addToCluster(bestInstance, attributeNames, isNumeric);
			
			// create new cluster and add as a seed
			Cluster newCluster = new Cluster(categoricalWeights, numericalWeights);
			newCluster.addToCluster(bestInstance, attributeNames, isNumeric);
			clusters.add(newCluster);
			// update cluster instance count
			numInstancesInCluster.add(1);
			
			// generate new iterator
			it = dataFrame.iterator();
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

	@Override
	public Map<String, Object> getResultMetadata() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public String getResultDescription() {
		// TODO Auto-generated method stub
		return null;
	}
}

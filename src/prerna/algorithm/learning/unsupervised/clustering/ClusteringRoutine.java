package prerna.algorithm.learning.unsupervised.clustering;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.util.Cluster;
import prerna.om.SEMOSSParam;

public class ClusteringRoutine extends AbstractClusteringRoutine {

	public ClusteringRoutine() {
		super();
	}
	
	@Override
	public ITableDataFrame runAlgorithm(ITableDataFrame... data) {
		// values defined in options
		this.numClusters = (int) options.get(0).getSelected();
		this.instanceIndex = (int) options.get(1).getSelected();
		
		this.dataFrame = data[0];
		this.isNumeric = dataFrame.isNumeric();
		this.attributeNames = dataFrame.getColumnHeaders();
		
		int numRows = dataFrame.getNumRows();
		if(numClusters > numRows) {
			throw new IllegalArgumentException("Cannot have more clusters than instances");
		}
		
		calculateWeights();
		initializeClusters();
		int maxIt = 100_000;
		boolean go = true;
		int currIt = 0;
		while(go) {
			
			
			
			// break if taking too many iterations
			if(currIt > maxIt) {
				go = false;
			}
		}
		
		return null;
	}
	
	@Override
	public int findBestClusterForInstance(Object[] instance, List<Cluster> clusters) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean updateInstanceIndex(Object[] instance, Cluster clusterToAdd, int indexOfCluster) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getResultDescription() {
		// TODO Auto-generated method stub
		return null;
	}

}

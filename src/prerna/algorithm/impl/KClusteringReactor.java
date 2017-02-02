package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.ExpressionIterator;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;

public class KClusteringReactor extends MathReactor{

	private int numIterations;

	public KClusteringReactor() {
		setMathRoutine("KClustering");
	}

	@Override
	public Iterator process() {
		modExpression();
		Vector<String> clusteringAttributes = (Vector <String>) myStore.get(PKQLEnum.COL_DEF);
		//String filterColumn = null;
		ITableDataFrame dataFrame = ((ITableDataFrame)myStore.get("G"));
//		if(myStore.containsKey(PKQLEnum.COL_CSV)) {
//			filterColumn = ((Vector<String>)myStore.get(PKQLEnum.COL_CSV)).firstElement();
//			if(filterColumn.equals("Bounds")){
//				List<Object> filterValues = new ArrayList<>();
//				filterValues.add(1);
//				dataFrame.filter("Bounds", filterValues);		
//			}
//		}
		List<String> columnHeaders = Arrays.asList(dataFrame.getColumnHeaders());
		boolean regionColumnPresent = false;
		if(columnHeaders.contains("Region")) {
			regionColumnPresent = true;
			Object[] values = dataFrame.getColumn("Region");
			List<Object> filterValues = new ArrayList<>();
			for (Object value : values){
				if (value.toString().equals("Above Bounds") || value.toString().equals("Below Bounds"))
					continue;
				filterValues.add(value);
			}
			if(filterValues.size() > 0){
				dataFrame.filter("Region", filterValues);
			}
		}
		
		Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MAP_OBJ);
		int maxClusters = -1;
		if(options.containsKey("maxClusters".toUpperCase()))
			maxClusters = (int)options.get("maxClusters".toUpperCase());
		
		if (maxClusters == 0)
			maxClusters = (int)Math.ceil(Math.log(dataFrame.getNumRows())/Math.log(2));

		String[] columnsArray = columnHeaders.toArray(new String[columnHeaders.size()]);
		Vector<String> columnsVector = new Vector<String>(columnHeaders);
		List<Integer> clusteringAttributesIndexList = new ArrayList<Integer>(clusteringAttributes.size());
		for (String clusteringAttr : clusteringAttributes){
			clusteringAttributesIndexList.add(columnHeaders.indexOf(clusteringAttr));
		}
		
		this.numIterations = 1000;
		KMeansModel kMeans = null;
		int numClusters = 2, prevNumClusters = 2;
		double SSERateThreshold = 0.3;
		double prevSSE = 0.0, orgSSE = -1.0;
		int markStagnant = 0, stagnantDur = 0, stagnantThreshold = 2;
		HashMap<Integer,Double> clusterNumToSSE = new HashMap<>();
		while(true)
		{
			if(numClusters > maxClusters)
				break;
			Iterator itr = getTinkerData(columnsVector, dataFrame, false);	
			kMeans = new KMeansModel(itr,clusteringAttributesIndexList, this.numIterations, numClusters);
			int clusterNumIncrement = 1;
			double currSSE = kMeans.getSSE();
			if(orgSSE < 0){
				orgSSE = currSSE;
			}else{
				double SSE = currSSE;
				double normalizedDeltaSSE = (prevSSE - SSE)/orgSSE;
				double normalizedDeltaNumClusters = (numClusters - prevNumClusters) * 1.0/numClusters;
				double SSERate = normalizedDeltaSSE/normalizedDeltaNumClusters;
				System.out.println("KMeans result: " + numClusters + ", " + SSERate);
				if (SSERate < SSERateThreshold){
					if (stagnantDur == 0)
						markStagnant = numClusters;
					stagnantDur += 1;
					if (stagnantDur > stagnantThreshold)
						break;
				}else{
					markStagnant = 0;
					stagnantDur = 0;
					clusterNumIncrement = (int)(SSERate * normalizedDeltaNumClusters * numClusters/SSERateThreshold);//(int)(numClusters * numClusters/prevNumClusters) - numClusters;
					clusterNumIncrement = Math.min((int)Math.sqrt(maxClusters), clusterNumIncrement);
					clusterNumIncrement = Math.max(clusterNumIncrement, 1);
				}
			}
			clusterNumToSSE.put(numClusters, currSSE);
			prevNumClusters = numClusters;
			numClusters += clusterNumIncrement;
			prevSSE = currSSE;
		}
		
		if (stagnantDur == stagnantThreshold){
			Iterator itr = getTinkerData(columnsVector, dataFrame, false);	
			kMeans = new KMeansModel(itr,clusteringAttributesIndexList, this.numIterations, markStagnant);
		}else{
			int bestClusterNum = -1;
			double minClusterScore = Double.MAX_VALUE;
			for(Integer clusterNum : clusterNumToSSE.keySet()){
				double SSE = clusterNumToSSE.get(clusterNum);
				if (minClusterScore > SSE)
				{
					minClusterScore = SSE;
					bestClusterNum = clusterNum;
				}
			}
			Iterator itr = getTinkerData(columnsVector, dataFrame, false);	
			kMeans = new KMeansModel(itr,clusteringAttributesIndexList, this.numIterations, bestClusterNum);
		}
			

		Map<List<Object>,Integer> clusters = kMeans.clusterResult();

		String script = columnsArray[0];
		if(regionColumnPresent){
			dataFrame.unfilter("Region");
		}
		Iterator resultItr = getTinkerData(columnsVector, dataFrame, false);
		ClusterIterator expItr = new ClusterIterator(resultItr, columnsArray,script, clusters, regionColumnPresent);
		String nodeStr = myStore.get(whoAmI).toString();
		myStore.put(nodeStr, expItr);
		
		HashMap<String,Object> additionalInfo = new HashMap<>();
		additionalInfo.put("KClusters", kMeans.getMetaData());
		myStore.put("ADDITIONAL_INFO", additionalInfo);
		myStore.put("STATUS", STATUS.SUCCESS);

		return expItr;
	}
}

class ClusterIterator extends ExpressionIterator{

	protected Map<List<Object>,Integer> clusters;
	boolean regionColumnPresent = false;

	protected ClusterIterator() {

	}

	public ClusterIterator(Iterator results, String [] columnsUsed, String script, Map<List<Object>,Integer> clusters, boolean regionColumnPresent)
	{
		this.clusters = clusters;
		this.regionColumnPresent = regionColumnPresent;
		setData(results, columnsUsed, script);
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return (results != null && results.hasNext());
	}

	@Override
	public Object next() {
		Object retObject = "No Cluster";

		if(results != null && !errored)
		{
			setOtherBindings();
			List<Object> key = new ArrayList<>(columnsUsed.length);
			for(int i=0; i<columnsUsed.length; i++)
				key.add(otherBindings.get(columnsUsed[i]));
			if(clusters.containsKey(key))
				retObject = "AutoCalculated " + (clusters.get(key) + 1);
			else if (regionColumnPresent)
				retObject = otherBindings.get("Region");
		}
		return retObject;
	}
}
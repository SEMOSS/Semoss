package prerna.algorithm.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import javax.script.Bindings;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.ExpressionIterator;
import prerna.ds.H2.H2Frame;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.util.Constants;

public class KClusteringReactor extends MathReactor{
	
	private int numClusters;
	private int numIterations;
		
	@Override
	public Iterator process() {
		modExpression();
		Vector<String> columns = (Vector <String>) myStore.get(PKQLEnum.COL_DEF);
		String filterColumn = null;

		if(myStore.containsKey(PKQLEnum.COL_CSV)) {
			filterColumn = ((Vector<String>)myStore.get(PKQLEnum.COL_CSV)).firstElement();
			columns.add(filterColumn);
		}
		
		String[] columnsArray = convertVectorToArray(columns);
		Iterator itr = getTinkerData(columns, (ITableDataFrame)myStore.get("G"), true);
		int numRows = ((ITableDataFrame)myStore.get("G")).getNumRows();
		if(myStore.containsKey(PKQLEnum.MATH_PARAM)) {
			Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MATH_PARAM);
			
			if(options.containsKey("numClusters".toUpperCase())) {
				this.numClusters = Integer.parseInt(options.get("numClusters".toUpperCase()) + "");
			} else {
				// TODO: need to throw an error saying number of clusters is required
				this.numClusters = (int)Math.round(Math.pow(numRows, 0.33));
			}
			if(options.containsKey("numIterations".toUpperCase())) {
				this.numIterations = Integer.parseInt(options.get("numIterations".toUpperCase()) + "");
			} else {
				// TODO: need to throw an error saying number of clusters is required
				this.numIterations = 100;
			}
			
		} else {
			//TODO: need to throw an error saying parameters are required
			this.numClusters = (int)Math.round(Math.pow(numRows, 0.33));;
			this.numIterations = 100;
		}		
		
		boolean boundsPresent = filterColumn != null;
		KMeansModel kMeans = new KMeansModel(itr, this.numClusters, this.numIterations,boundsPresent);
		
		Map<Object,Integer> clusters =  kMeans.clusterResult();
		
		String script = columnsArray[0];
		
		H2Frame frame = (H2Frame)myStore.get("G");
		Iterator resultItr = getTinkerData(columns, frame, true);
		ClusterIterator expItr = new ClusterIterator(resultItr, columnsArray,script, clusters);
		String nodeStr = myStore.get(whoAmI).toString();
		myStore.put(nodeStr, expItr);
		Map<String,Object> additionalInfo = new HashMap<>();
		additionalInfo.put("Centres", kMeans.getClusterCentres());
		myStore.put("ADDITIONAL_INFO", additionalInfo);
		myStore.put("STATUS", STATUS.SUCCESS);
		
		return expItr;
	}
}

class ClusterIterator extends ExpressionIterator{
	
	protected Map<Object,Integer> clusters;
	
	protected ClusterIterator() {
		
	}
	
	public ClusterIterator(Iterator results, String [] columnsUsed, String script, Map<Object,Integer> clusters)
	{
		this.clusters = clusters;
		setData(results, columnsUsed, script);
	}
		
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return (results != null && results.hasNext());
	}
	
	@Override
	public Object next() {
		Object retObject = null;
		
		if(results != null && !errored)
		{
			setOtherBindings();
			retObject = clusters.get(otherBindings.get(columnsUsed[0]));
		}
		return retObject;
	}
}
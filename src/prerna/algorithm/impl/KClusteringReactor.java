package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.script.Bindings;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.ExpressionIterator;
import prerna.ds.h2.H2Frame;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.util.Constants;

public class KClusteringReactor extends MathReactor{
	
	private int numIterations;
		
	@Override
	public Iterator process() {
		modExpression();
		Vector<String> columns = (Vector <String>) myStore.get(PKQLEnum.COL_DEF);
		String filterColumn = null;
		ITableDataFrame dataFrame = ((ITableDataFrame)myStore.get("G"));
		if(myStore.containsKey(PKQLEnum.COL_CSV)) {
			filterColumn = ((Vector<String>)myStore.get(PKQLEnum.COL_CSV)).firstElement();
			if(filterColumn.equals("Bounds")){
				List<Object> filterValues = new ArrayList<>();
				filterValues.add(1);
				dataFrame.filter("Bounds", filterValues);		
			}
		}
		
		String[] columnsArray = convertVectorToArray(columns);
		Iterator itr = getTinkerData(columns, dataFrame, false);	
		this.numIterations = 10000;
		KMeansModel kMeans = new KMeansModel(itr, this.numIterations);
		
		Map<List<Object>,Integer> clusters = kMeans.clusterResult();
		
		String script = columnsArray[0];
		if(filterColumn != null && filterColumn.equals("Bounds"))
			dataFrame.unfilter("Bounds");
		Iterator resultItr = getTinkerData(columns, dataFrame, false);
		ClusterIterator expItr = new ClusterIterator(resultItr, columnsArray,script, clusters);
		String nodeStr = myStore.get(whoAmI).toString();
		myStore.put(nodeStr, expItr);
		/*Map<String,Object> additionalInfo = new HashMap<>();
		additionalInfo.put("Centres", kMeans.getClusterCentres());
		myStore.put("ADDITIONAL_INFO", additionalInfo);*/
		myStore.put("STATUS", STATUS.SUCCESS);
		
		return expItr;
	}
}

class ClusterIterator extends ExpressionIterator{
	
	protected Map<List<Object>,Integer> clusters;
	
	protected ClusterIterator() {
		
	}
	
	public ClusterIterator(Iterator results, String [] columnsUsed, String script, Map<List<Object>,Integer> clusters)
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
		Object retObject = new Integer(-1);
		
		if(results != null && !errored)
		{
			setOtherBindings();
			List<Object> key = new ArrayList<>(columnsUsed.length);
			for(int i=0; i<columnsUsed.length; i++)
				key.add(otherBindings.get(columnsUsed[i]));
			if(clusters.containsKey(key))
				retObject = clusters.get(key);
		}
		return retObject;
	}
}
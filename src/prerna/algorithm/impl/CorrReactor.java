package prerna.algorithm.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.ds.TinkerFrame;

public class CorrReactor extends BaseReducerReactor {

	@Override
	public Object reduce() {
		// current correlation algorithm requires the full amount of data
		// can later look at algorithms to stream and compute an approximate
		// correlation value
		List<double[]> values = new Vector<double[]>();
		
		while(this.inputIterator.hasNext() && !errored) {
			Object data = this.inputIterator.next();
			double[] retObject = new double[ids.length];
			if(data instanceof Map) {
				for(int colIndex = 0;colIndex < ids.length; colIndex++) {
					Map<String, Object> mapData = (Map<String, Object>)data; //cast to map
					retObject[colIndex] = (double) ((Vertex)mapData.get(ids[colIndex])).property(TinkerFrame.TINKER_NAME).value();
				}
			} else {
				for(int colIndex = 0; colIndex < ids.length; colIndex++) {
					retObject[colIndex] = ((Number) ((Object[]) data)[colIndex]).doubleValue();
				}
			}
			values.add(retObject);
		}
		
		double[][] A = (double[][]) values.toArray(new double[][]{});
		PearsonsCorrelation correlation = new PearsonsCorrelation(A);
		double[][] correlationArray = correlation.getCorrelationMatrix().getData();	
		
		return correlationArray;
	}

	@Override
	public HashMap<HashMap<Object, Object>, Object> reduceGroupBy(Vector<String> groupBys, Vector<String> processedColumns, String[] columnsArray,
			Iterator it) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public Map<String, Object> getColumnDataMap() {
		// this cannot be added into a frame
		// just return null
		return null;
	}
}

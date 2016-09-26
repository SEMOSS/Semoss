package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Vector;

import javax.script.Bindings;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptException;

import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.ExpressionIterator;
import prerna.ds.TinkerFrame;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.util.Constants;

public class QuantilesReactor extends MathReactor{
	@Override
	public Iterator process() {
		modExpression();
		Vector<String> columns = (Vector <String>) myStore.get(PKQLEnum.COL_DEF);
		String[] columnsArray = convertVectorToArray(columns);

		ITableDataFrame df = (ITableDataFrame)  myStore.get("G");
		Double[] valuesArray = df.getColumnAsNumeric(columnsArray[0]);
		ArrayList<Double> valuesList = new ArrayList<>();
		for(Double value : valuesArray)
			valuesList.add(value);
		int numRows = valuesList.size();
		
		String script = columnsArray[0];
		
		Collections.sort(valuesList);
		
		Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MATH_PARAM);
		int numTiles = Integer.parseInt(options.get("numTiles".toUpperCase()) + "");
		List<Double> quantileValues = new ArrayList<>(numTiles - 1);
		for(int i=1;i<numTiles;i++){
			int rank = (int)Math.round((i * numRows)/numTiles);
			double result = valuesList.get(rank);
			if(numRows % 2 == 0 && Math.ceil(result) == result)
				result = (result + valuesList.get(rank + 1))/2;
			quantileValues.add(result);
		}
		
		Map<Object,Integer> tiles = new HashMap<>();
		Iterator itr = getTinkerData(columns, df, false);
		while(itr.hasNext()){
			Object[] row = (Object[])itr.next();
			Double value = (Double) row[0];
			int quantile = -1;
			for(int i=0; i < quantileValues.size(); i++){
				if(value < quantileValues.get(i)){
					quantile = i + 1;
					break;
				}
			}
			quantile = (quantile == -1) ? numTiles : quantile;
			tiles.put(row[0], quantile);
		}
		Iterator resultItr = getTinkerData(columns, df, false);
		QuantilesIterator expItr = new QuantilesIterator(resultItr,columnsArray,script,tiles);
		String nodeStr = myStore.get(whoAmI).toString();
		HashMap<String,Object> additionalInfo = new HashMap<>();
		additionalInfo.put("Quantiles", quantileValues);
		myStore.put("ADDITIONAL_INFO", additionalInfo);
		myStore.put(nodeStr, expItr);
		myStore.put("STATUS", STATUS.SUCCESS);
		
		return expItr;
	}
}

class QuantilesIterator extends ExpressionIterator{
	
	protected Map<Object,Integer> tiles;
	
	protected QuantilesIterator() {
		
	}
	
	public QuantilesIterator(Iterator results, String [] columnsUsed, String script, Map<Object,Integer> tiles)
	{
		this.tiles = tiles;
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
			retObject = tiles.get(otherBindings.get(columnsUsed[0]));
		}
		return retObject;
	}
}
package prerna.algorithm.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.ExpressionIterator;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;

public class BoundsFilterReactor extends MathReactor{
	
	private double tolerance;
	private double lRegSlope;
	private double lRegIntercept;
		
	@Override
	public Iterator process() {
		modExpression();
		Vector<String> columns = (Vector <String>) myStore.get(PKQLEnum.COL_DEF);
		String[] columnsArray = convertVectorToArray(columns);
		Iterator itr = getTinkerData(columns, (ITableDataFrame)myStore.get("G"), true);
		int numRows = ((ITableDataFrame)myStore.get("G")).getNumRows();
		
		if(myStore.containsKey(PKQLEnum.MATH_PARAM)) {
			Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MATH_PARAM);
			
			if(options.containsKey("tolerance".toUpperCase())) {
				this.tolerance = Integer.parseInt(options.get("tolerance".toUpperCase()) + "");
			} else {
				// TODO: need to throw an error saying number of clusters is required
				System.err.println("Provide tolerance");
				this.tolerance = 50;
			}			
		} else {
			System.err.println("Provide tolerance");
		}
		double sigmaXY = 0, sigmaX = 0, sigmaY = 0, sigmaX2 = 0;
		while(itr.hasNext()){
			Object[] data = (Object[])itr.next();
			double x = (double)data[0];
			double y = (double)data[1];
			sigmaX += x;
			sigmaY += y;
			sigmaX2 += x * x;
			sigmaXY += x * y;
		}
		this.lRegSlope = (sigmaXY - (sigmaX * sigmaY/numRows))/(sigmaX2 - (sigmaX * sigmaX/numRows));
		this.lRegIntercept = (sigmaY - (this.lRegSlope * sigmaX))/numRows;
		
		double normalMaxRange = 0;
		Iterator itr2 = getTinkerData(columns, (ITableDataFrame)myStore.get("G"), true);
		while(itr2.hasNext()){
			Object[] data = (Object[])itr2.next();
			double x = (double)data[0];
			double y = (double)data[1];
			double dist = Math.abs((lRegSlope * x) + lRegIntercept - y);
			double sinTheta = 1/(Math.sqrt(Math.pow(this.lRegSlope, 2) + 1));
			normalMaxRange = Math.max(dist * sinTheta, normalMaxRange);
		}
		normalMaxRange *= tolerance/100;
		
		Iterator resultItr = getTinkerData(columns, (ITableDataFrame)myStore.get("G"), true);
		
		BoundsIterator expItr = new BoundsIterator(resultItr, columnsArray, tolerance, lRegSlope, lRegIntercept, normalMaxRange);
		String nodeStr = myStore.get(whoAmI).toString();
		myStore.put(nodeStr, expItr);
		HashMap<String,Object> additionalInfo = new HashMap<>();
		additionalInfo.put("BoundsTolerance", tolerance);
		myStore.put("ADDITIONAL_INFO", additionalInfo);
		myStore.put("STATUS", STATUS.SUCCESS);
		
		return expItr;
	}
}

class BoundsIterator extends ExpressionIterator{
	
	double tolerance;
	double lRegSlope;
	double lRegIntercept;
	double normalMaxRange;
	double sinTheta;
	
	protected BoundsIterator() {
		
	}
	
	public BoundsIterator(Iterator results, String [] columnsUsed, double tolerance, double lRegSlope, double lRegIntercept,double normalMaxRange)
	{
		this.tolerance = tolerance;
		this.lRegSlope = lRegSlope;
		this.lRegIntercept = lRegIntercept;
		this.normalMaxRange = normalMaxRange;
		sinTheta = 1/(Math.sqrt(Math.pow(this.lRegSlope, 2) + 1));
		setData(results, columnsUsed, columnsUsed[0]);
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
			double x = (double) otherBindings.get(columnsUsed[0]);
			double y = (double) otherBindings.get(columnsUsed[1]);
			double dist = y - (lRegSlope * x) - lRegIntercept;

			retObject = 0;
			if(Math.abs(dist*sinTheta) <= normalMaxRange)
				retObject = 1;
			else if (dist > 0)
				retObject = 0;
			else
				retObject = 2;
		}
		return retObject;
	}
}
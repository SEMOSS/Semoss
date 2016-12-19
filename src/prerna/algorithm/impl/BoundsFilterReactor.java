package prerna.algorithm.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.ExpressionIterator;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.sablecc.meta.MathPkqlMetadata;

public class BoundsFilterReactor extends MathReactor{
	
	private double tolerance;
	private double lRegSlope;
	private double lRegIntercept;
	
	public BoundsFilterReactor() {
		setMathRoutine("BoundsFilter");
	}
	
	@Override
	public Iterator process() {
		modExpression();
		Vector<String> columns = (Vector <String>) myStore.get(PKQLEnum.COL_DEF);
		String[] columnsArray = convertVectorToArray(columns);
		Iterator itr = getTinkerData(columns, (ITableDataFrame)myStore.get("G"), true);
		int numRows = ((ITableDataFrame)myStore.get("G")).getNumRows();
		
		if(myStore.containsKey(PKQLEnum.MAP_OBJ)) {
			Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MAP_OBJ);
			
			if(options.containsKey("tolerance".toUpperCase())) {
				this.tolerance = Double.parseDouble(options.get("tolerance".toUpperCase()) + "");
			} else {
				// TODO: need to throw an error saying number of clusters is required
				System.err.println("Provide tolerance");
				this.tolerance = 1;
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
		
		Iterator itr2 = getTinkerData(columns, (ITableDataFrame)myStore.get("G"), true);
		double[] distance = new double[numRows];
		double averageDist = 0;
		//double cosTheta = 1/(Math.sqrt(Math.pow(this.lRegSlope, 2) + 1));
		double maxDist = Double.MIN_VALUE;
		for(int i=0;i<numRows;i++){
			Object[] data = (Object[])itr2.next();
			double x = (double)data[0];
			double y = (double)data[1];
			distance[i] = y - (lRegSlope * x) - lRegIntercept;
			averageDist += distance[i]/numRows;
			maxDist = Math.max(maxDist, Math.abs(distance[i]));
			//normalMaxRange = Math.max(dist * sinTheta, normalMaxRange);
		}
		Arrays.sort(distance);
		double variance = 0;
		for(int i=0;i<numRows;i++){
			variance += Math.pow(distance[i]-averageDist, 2)/numRows;
		}		
		double maxTgtDist = averageDist + (Math.sqrt(variance) * tolerance);
		double maxSliderVal = maxDist/Math.sqrt(variance);
		
		Iterator itr3 = getTinkerData(columns, (ITableDataFrame)myStore.get("G"), true);
		HashMap<String,Integer> boundCounts = new HashMap();
		boundCounts.put("Within Bounds",0);
		boundCounts.put("Above Bounds",0);
		boundCounts.put("Below Bounds",0);
		for(int i=0;i<numRows;i++){
			Object[] data = (Object[])itr3.next();
			double x = (double)data[0];
			double y = (double)data[1];
			double dist = y - (lRegSlope * x) - lRegIntercept;

			if(Math.abs(dist) <= maxTgtDist)
				boundCounts.put("Within Bounds", boundCounts.get("Within Bounds") + 1);
			else if (dist > 0)
				boundCounts.put("Above Bounds", boundCounts.get("Above Bounds") + 1);
			else
				boundCounts.put("Below Bounds", boundCounts.get("Below Bounds") + 1);
		}
		
		Iterator resultItr = getTinkerData(columns, (ITableDataFrame)myStore.get("G"), true);
		
		BoundsIterator expItr = new BoundsIterator(resultItr, columnsArray, tolerance, lRegSlope, lRegIntercept, maxTgtDist);
		String nodeStr = myStore.get(whoAmI).toString();
		myStore.put(nodeStr, expItr);
		HashMap<String,Object> bounds = new HashMap<>();
		bounds.put("Tolerance", tolerance);
		//bounds.put("AverageDistance", averageDist);
		bounds.put("StandardDeviation", Math.sqrt(variance));
		bounds.put("BoundCounts", boundCounts);
		
		HashMap<String,Object> additionalInfo = new HashMap<>();
		additionalInfo.put("Bounds", bounds);
		myStore.put("ADDITIONAL_INFO", additionalInfo);
		myStore.put("STATUS", STATUS.SUCCESS);
		
		return expItr;
	}
	
	public IPkqlMetadata getPkqlMetadata() {
		MathPkqlMetadata metadata = new MathPkqlMetadata();
		metadata.setPkqlStr((String) myStore.get(PKQLEnum.MATH_FUN));
		metadata.setColumnsOperatedOn((Vector<String>) myStore.get(PKQLEnum.COL_DEF));
		metadata.setProcedureName("boundary generation");
		metadata.setAdditionalInfo(myStore.get("ADDITIONAL_INFO"));
		return metadata;
	}
}

class BoundsIterator extends ExpressionIterator{
	
	double tolerance;
	double lRegSlope;
	double lRegIntercept;
	double normalMaxRange;
	//double cosTheta;
	
	protected BoundsIterator() {
		
	}
	
	public BoundsIterator(Iterator results, String [] columnsUsed, double tolerance, double lRegSlope, double lRegIntercept,double normalMaxRange)
	{
		this.tolerance = tolerance;
		this.lRegSlope = lRegSlope;
		this.lRegIntercept = lRegIntercept;
		this.normalMaxRange = normalMaxRange;
		//cosTheta = 1/(Math.sqrt(Math.pow(this.lRegSlope, 2) + 1));
		setData(results, columnsUsed, columnsUsed[0]);
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
			double x = (double) otherBindings.get(columnsUsed[0]);
			double y = (double) otherBindings.get(columnsUsed[1]);
			double dist = y - (lRegSlope * x) - lRegIntercept;
			
			if(Math.abs(dist) <= normalMaxRange)
				retObject = "Within Bounds";
			else if (dist > 0)
				retObject = "Above Bounds";
			else
				retObject = "Below Bounds";
		}
		return retObject;
	}
}
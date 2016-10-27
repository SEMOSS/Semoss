package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;
import org.apache.commons.math3.util.FastMath;
import org.apache.commons.math3.util.MathUtils;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics.*;

import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;

public class SkewReactor extends BaseReducerReactor   {
	
	@Override
	public Object reduce() {
		int count = 0;
		double[] sumAndSumOfSquares = new double[2];; // index 0 for sum, index 1 for sum squared
		double StdDev = 0.0;
		double Variance=0.0;
		double accum3 = 0.0;
		double mean=0.0;
		double d = 0.0;

		while(inputIterator.hasNext() && !errored) {
			ArrayList dec = (ArrayList)getNextValue();
			//System.out.println(dec.get(0));
			mean=((Number)dec.get(1)).doubleValue();
			d = ((Number)dec.get(0)).doubleValue()-mean;
			accum3 += d * d * d;
			//  System.out.println("accum value is ");
			//System.out.println(accum3);
			//System.out.println("d value is");
			//System.out.println(d);
			if(dec.get(0) instanceof Number) {
				sumAndSumOfSquares[0] += ((Number)dec.get(0)).doubleValue();
				sumAndSumOfSquares[1] += (Math.pow(((Number)dec.get(0)).doubleValue(), 2));
				count++;
			}
		}
		double sum = sumAndSumOfSquares[0];
		double sumOfSquares = sumAndSumOfSquares[1];
		double average = sum/count;
		StdDev = Math.sqrt(((count * average * average) - (2 * average * sum) + sumOfSquares)/count);
		//System.out.println("stdDev is");
		//System.out.println(StdDev);
		Variance=StdDev*StdDev;
		//System.out.println(Variance);



		while(inputIterator.hasNext() && !errored)
		{
			ArrayList dec = (ArrayList)getNextValue();


		}
		accum3 /= Variance * FastMath.sqrt(Variance);
		double n0 = count;

		// Calculate skewness
		double skew = (n0 / ((n0 - 1) * (n0 - 2))) * accum3;
		System.out.println("skew value is");
		System.out.println(skew);
		return skew;

	}

	@Override
	public HashMap<HashMap<Object, Object>, Object> reduceGroupBy(Vector<String> groupBys,
			Vector<String> processedColumns, String[] columnsArray, Iterator it) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public Map<String, Object> getColumnDataMap() {
		return getBaseColumnDataMap("Skew");
	}
}

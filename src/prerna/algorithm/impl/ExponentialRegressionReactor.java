package prerna.algorithm.impl;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.ExpressionIterator;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.sablecc.meta.MathPkqlMetadata;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;



public class ExponentialRegressionReactor extends MathReactor { // TODO create BaseMapperReactor once more mapping algorithms have been added

	public ExponentialRegressionReactor() {
		setMathRoutine("Exponential Regression");
	}

	@Override
	public Iterator process() {
		modExpression();
		Vector <String> columns = (Vector <String>)myStore.get(PKQLEnum.COL_DEF);
		String[] columnsArray = convertVectorToArray(columns);

		Iterator iterator = getTinkerData(columns, (ITableDataFrame)myStore.get("G"), false);
		ITableDataFrame df = (ITableDataFrame)myStore.get("G");
		int rowCount = df.getNumRows();

		double[] xData = new double[rowCount];
		double[] yData = new double[rowCount];
		//double[] result = new double[rowCount];
		boolean xPresent = false;
		if (columnsArray.length>1)
			xPresent =true;
		
		Iterator itr = getTinkerData(columns, df, false);
		int i =0;
		while(itr.hasNext()){
			Object[] row = (Object[])itr.next();
			if(xPresent){
				Double xValue = (Double) row[0];
				xData[i] = xValue;
			}
			Double yValue = (Double) row[1];
			yData[i] = yValue;
			i++;
		}
		
		//Decide the type of polynomial to fit
		Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MAP_OBJ);
		String regType = options.get("TYPE").toString().toUpperCase() + "";
		int degree =1;
		if(options.containsKey("DEGREE")){
			degree = Integer.parseInt( options.get("DEGREE").toString());
		}
		TrendLine t;
		switch(regType){
		case "LINEAR":
			t = new PolyTrendLine(1);
			break;
		case "QUADRATIC":
			t = new PolyTrendLine(2);
			break;
		case "CUBIC":
			t = new PolyTrendLine(3);
			break;
		case "BIQUADRATIC":
			t = new PolyTrendLine(4);
			break;
		case "QUINTIC":
			t = new PolyTrendLine(5);
			break;
		case "EXP":
			t = new ExpTrendLine();
			break;
		case "POWER":
			t = new PowerTrendLine();
			break; 
		case "LOG":
			t = new LogTrendLine();
			break;
		case "POLYNOMIAL":
			t = new PolyTrendLine(degree);
			break;
		default:
			t = new LogTrendLine();
		}
		t.setValues(yData, xData);
		//System.out.println( "Predicted Value is" +t.predict(23413));	

		String nodeStr = myStore.get(whoAmI).toString();
		String expression = myStore.get("MATH_FUN").toString();
		
		List<List<Object>> keys = new ArrayList<>(rowCount);
		String[] columnHeaders = df.getColumnHeaders();
		Vector<String> allCols = new Vector<>(Arrays.asList(columnHeaders));
		HashMap<List<Object>,Double> result  =  new HashMap<>();
		Iterator dataItr = getTinkerData(allCols, df, false);
		
		String script = columnsArray[0];
		List<String> colArray =(List) Arrays.asList( columnHeaders);
		int position = colArray.indexOf(script);
		
		while(dataItr.hasNext()){
			Object[] row = (Object[]) dataItr.next();
			List<Object> rowList = Arrays.asList(row);
			keys.add(rowList);
		}
		
		for(List<Object> key: keys){
			//System.out.println( "Predicted Value is" +key.get(i));
			double predictedVal = t.predict((Double)(key.get(position)));
			result.put(key, predictedVal);
		}
		
		Iterator resultItr = getTinkerData(allCols, df, false);
		
		//Code for prediction 

		if(options.containsKey("PREDICT")){
			Double xForPrediction = Double.parseDouble (options.get("PREDICT").toString());
			if(xForPrediction != null){
			HashMap<String,Object> returnData = new HashMap<>();
			Object predictedValue = t.predict(xForPrediction);
			returnData.put("predictedValue", predictedValue);
			myStore.put("ADDITIONAL_INFO", returnData);
			}
		}
		
		String[] allColsArray = convertVectorToArray(allCols);
		TrendIterator expIt = new TrendIterator(resultItr, allColsArray,script,result );
		myStore.put(nodeStr, expIt);
		myStore.put("STATUS",STATUS.SUCCESS);

		return expIt;
	}
	public IPkqlMetadata getPkqlMetadata() {
		MathPkqlMetadata metadata = new MathPkqlMetadata();
		metadata.setPkqlStr((String) myStore.get(PKQLEnum.MATH_FUN));
		metadata.setColumnsOperatedOn((Vector<String>) myStore.get(PKQLEnum.COL_DEF));
		metadata.setProcedureName("Exponential Regression");
		metadata.setAdditionalInfo(myStore.get("ADDITIONAL_INFO"));
		return metadata;
	}

}

class TrendIterator extends ExpressionIterator{
	
	protected Map<List<Object>,Double> result;
	
	protected TrendIterator() {
		
	}
	
	public TrendIterator(Iterator results, String [] columnsUsed, String script, Map<List<Object>,Double> result)
	{
		this.result = result;
		setData(results, columnsUsed, script);
	}
		
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return (results != null && results.hasNext());
	}
	
	@Override
	public Object next() {
		Object retObject = new Integer(0);
		
		if(results != null && !errored)
		{
			setOtherBindings();
			List<Object> keyList = new ArrayList<>(columnsUsed.length);
			for(int i=0;i<columnsUsed.length;i++)
				keyList.add(otherBindings.get(columnsUsed[i]));
			retObject = result.get(keyList);
			//retObject = result.get(otherBindings.get(columnsUsed[0]));
		}
		return retObject;
	}
}

//Class for Exponential TrendLine
class ExpTrendLine extends OLSTrendLine {
    @Override
    protected double[] xVector(double x) {
        return new double[]{1,x};
    }

    @Override
    protected boolean logY() {return true;}
}

//Adding additional class for Power TrendLine
class PowerTrendLine extends OLSTrendLine {
    @Override
    protected double[] xVector(double x) {
        return new double[]{1,Math.log(x)};
    }

    @Override
    protected boolean logY() {return true;}

}

//Adding additional class for Log TrendLine
class LogTrendLine extends OLSTrendLine {
    @Override
    protected double[] xVector(double x) {
        return new double[]{1,Math.log(x)};
    }

    @Override
    protected boolean logY() {return false;}
}


//Adding additional class for Polynomial TrendLine
//For linear models, just set the degree to 1 when calling the constructor
// I.e. TrendLine t = new PolyTrendLine(2);
class PolyTrendLine extends OLSTrendLine {
    final int degree;
    public PolyTrendLine(int degree) {
        if (degree < 0) throw new IllegalArgumentException("The degree of the polynomial must not be negative");
        this.degree = degree;
    }
    protected double[] xVector(double x) { // {1, x, x*x, x*x*x, ...}
        double[] poly = new double[degree+1];
        double xi=1;
        for(int i=0; i<=degree; i++) {
            poly[i]=xi;
            xi*=x;
        }
        return poly;
    }
    @Override
    protected boolean logY() {return false;}
}
package prerna.algorithm.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;

public class BenfordLawReactor extends MathReactor{
	
	public BenfordLawReactor() {
		setMathRoutine("BenfordLaw");
	}
	
	@Override
	public Iterator process() {
		modExpression();
		Vector<String> columns = (Vector <String>) myStore.get(PKQLEnum.COL_DEF);
		String[] columnsArray = convertVectorToArray(columns);
		
		String nodeStr = myStore.get(whoAmI).toString();

		ITableDataFrame df = (ITableDataFrame)  myStore.get("G");
		Double[] valuesArray = df.getColumnAsNumeric(columnsArray[0]);
		HashMap<Integer,Double> sdMap = new HashMap<>();
		double percentFactor = 100.0/df.getNumRows();
		for(Double value : valuesArray){
			int sd = Integer.valueOf(String.valueOf(value).substring(0,1));
			if(!sdMap.containsKey(sd))
				sdMap.put(sd, percentFactor);
			else
				sdMap.put(sd,sdMap.get(sd) + percentFactor);
		}
		HashMap<Integer,Double> bdMap = new HashMap<>();
		for(int i=1; i <= 9; i++){
			Double value = Math.log(1 + (1.0/i))/0.023;
			bdMap.put(i, value);
		}
		HashMap<HashMap<String,Object>,Object> result = new HashMap<HashMap<String,Object>,Object>();
		for(int i : sdMap.keySet())
		{
			HashMap<String,Object> key = new HashMap<>();
			key.put("actualDistribution", i);
			result.put(key, sdMap.get(i));
		}
		for(int i : bdMap.keySet())
		{
			HashMap<String,Object> key = new HashMap<>();
			key.put("idealDistribution", i);
			result.put(key, bdMap.get(i));
		}
		HashMap<String,Object> additionalInfo = new HashMap<>();
		additionalInfo.put("actualDistribution", sdMap);
		additionalInfo.put("idealDistribution", bdMap);
		
		myStore.put("ADDITIONAL_INFO", additionalInfo);
		myStore.put("STATUS", STATUS.SUCCESS);
		myStore.put(nodeStr, result);
		
		return null;
	}
}
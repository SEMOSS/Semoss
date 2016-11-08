package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;

import org.apache.spark.sql.DataFrame;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.ExpressionIterator;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;

public class MonetaryUnitSamplingReactor extends MathReactor{
	
	enum RiskClassificationType{
		SignificantHigherLower,
		SignificantNormal
	}
	
	enum SamplingPopulationType{
		posItems,
		negItems,
		allItems
	}
	
	@Override
	public Iterator process() {
		modExpression();
		Vector<String> columns = (Vector <String>) myStore.get(PKQLEnum.COL_DEF);
		
		String[] columnsArray = convertVectorToArray(columns);
		ITableDataFrame dataFrame = (ITableDataFrame)myStore.get("G");
		
		Iterator resultItr = getTinkerData(columns, dataFrame, false);
		String script = columnsArray[0];
		
		Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MATH_PARAM);
		
		RiskClassificationType riskType = RiskClassificationType.valueOf((String)options.get("riskType".toUpperCase()));
		String colName = columnsArray[1];
		double performanceMateriality = (double)options.get("performanceMateriality".toUpperCase());
		int riskLevel = (int)options.get("riskLevel".toUpperCase());;
		SamplingPopulationType samplingPopulationType = SamplingPopulationType.valueOf((String)options.get("samplingPopulationType".toUpperCase()));
		int sampleSize = (int)options.get("sampleSize".toUpperCase());
		long seed = new Random().nextLong();
		boolean selectAllHighValueItems = (int)options.get("selectAllHighValueItems".toUpperCase()) > 0;
		
		Map<String,Object> samplingDetails = new HashMap<>();
		Map<Object, Integer> sampleResults = extractSamples(columns, dataFrame, riskType, colName, performanceMateriality, riskLevel, samplingPopulationType, sampleSize, seed, selectAllHighValueItems, samplingDetails);
		
		MUSampleIterator expItr = new MUSampleIterator(resultItr, columnsArray,script, sampleResults);
		
		String nodeStr = myStore.get(whoAmI).toString();
		myStore.put(nodeStr, expItr);
		HashMap<String,Object> additionalInfo = new HashMap<>();
		additionalInfo.put("MonetaryUnitSampling", samplingDetails);
		myStore.put("ADDITIONAL_INFO", additionalInfo);
		myStore.put("STATUS", STATUS.SUCCESS);
		
		return expItr;
	}
	
	List<Integer[]> getSampleSizeTables(RiskClassificationType riskType){
		
		List<Integer[]> sampleSizeTable = new ArrayList<>();
		if(riskType == RiskClassificationType.SignificantNormal){
			sampleSizeTable.add(new Integer[]{1,1,1,2,3});
			sampleSizeTable.add(new Integer[]{2,1,2,3,6});
			sampleSizeTable.add(new Integer[]{3,1,3,5,9});
			sampleSizeTable.add(new Integer[]{4,1,3,6,12});
			sampleSizeTable.add(new Integer[]{5,1,4,8,15});
			sampleSizeTable.add(new Integer[]{6,2,5,9,18});
			sampleSizeTable.add(new Integer[]{7,2,5,11,21});
			sampleSizeTable.add(new Integer[]{8,2,6,12,24});
			sampleSizeTable.add(new Integer[]{9,2,7,14,27});
			sampleSizeTable.add(new Integer[]{10,2,7,15,30});
			sampleSizeTable.add(new Integer[]{15,3,11,23,45});
			sampleSizeTable.add(new Integer[]{20,4,14,30,60});
			sampleSizeTable.add(new Integer[]{25,5,18,38,75});
			sampleSizeTable.add(new Integer[]{30,6,21,45,75});
			sampleSizeTable.add(new Integer[]{40,8,28,60,75});
			sampleSizeTable.add(new Integer[]{50,10,35,75,75});
			sampleSizeTable.add(new Integer[]{100,20,70,75,75});
			sampleSizeTable.add(new Integer[]{200,40,75,75,75});
		}
		if(riskType == RiskClassificationType.SignificantHigherLower){
			sampleSizeTable.add(new Integer[]{1,1,2,1,1,4,2});
			sampleSizeTable.add(new Integer[]{2,2,3,1,1,6,2});
			sampleSizeTable.add(new Integer[]{3,2,5,1,2,10,4});
			sampleSizeTable.add(new Integer[]{4,3,6,1,2,12,4});
			sampleSizeTable.add(new Integer[]{5,3,8,1,3,16,6});
			sampleSizeTable.add(new Integer[]{6,4,9,2,3,18,6});
			sampleSizeTable.add(new Integer[]{7,5,11,2,4,22,8});
			sampleSizeTable.add(new Integer[]{8,5,12,2,4,24,8});
			sampleSizeTable.add(new Integer[]{9,6,14,2,5,28,10});
			sampleSizeTable.add(new Integer[]{10,6,15,2,5,30,10});
			sampleSizeTable.add(new Integer[]{15,9,23,3,8,46,16});
			sampleSizeTable.add(new Integer[]{20,12,30,4,10,60,20});
			sampleSizeTable.add(new Integer[]{25,15,38,5,13,76,26});
			sampleSizeTable.add(new Integer[]{30,18,45,6,15,90,30});
			sampleSizeTable.add(new Integer[]{40,24,60,8,20,120,40});
			sampleSizeTable.add(new Integer[]{50,30,75,10,25,150,50});
			sampleSizeTable.add(new Integer[]{100,60,150,20,50,300,100});
		}
		
		return sampleSizeTable;
	}
	
	Map<String,Double> getColStats(Iterator<Object> itr, int colIndex){
		double posItems = 0;
		double negItems = 0;
		double allItems = 0;
		double totItems = 0;
	    while(itr.hasNext()){
	    	Object[] row = (Object[]) itr.next();
	        double val = (double)(row[colIndex]);
	        posItems += (val > 0) ? val : 0;
	        negItems += (val < 0)? Math.abs(val) : 0;
	        allItems += Math.abs(val);
	        totItems += val;
	    }
	    Map<String,Double> result = new HashMap<>();
	    result.put("posItems", posItems);
	    result.put("negItems", negItems);
	    result.put("allItems", allItems);
	    result.put("totItems", totItems);
	    return result;
	}
	 
	int getMinSampleSize(RiskClassificationType riskType, double populationSize,double performanceMateriality, int riskLevel){
	    double populationMaterialityRatio = populationSize/performanceMateriality;
	    List<Integer[]> sampleSizeTable = getSampleSizeTables(riskType);
	    int sampleIndex = sampleSizeTable.size() - 1;
	    for (Integer[] row : sampleSizeTable){
	        if(populationMaterialityRatio <= row[0]){
	            sampleIndex = sampleSizeTable.indexOf(row) - 1;
	            break;
	        }
	    }
	    sampleIndex = (sampleIndex < 0) ? 0 : sampleIndex;
	    return sampleSizeTable.get(sampleIndex)[riskLevel];
	}
	
	Map<Object,Integer> extractSamples(Vector<String> columns, ITableDataFrame dataFrame, RiskClassificationType riskType,String colName,double performanceMateriality,int riskLevel,SamplingPopulationType samplingPopulationType,int sampleSize,long seed,boolean selectAllHighValueItems, Map<String, Object> samplingDetails){
		Iterator<Object> itr = (Iterator<Object>) getTinkerData(columns, dataFrame, false);
		int colIndex = columns.indexOf(colName);
		Map<String,Double> colStats = getColStats(itr, colIndex);
		
	    double sumHighValueItems = 0;
	    if(selectAllHighValueItems){
	    	Iterator<Object> itr2 = (Iterator<Object>) getTinkerData(columns, dataFrame, false);
	        while(itr2.hasNext()){
	        	Object[] row = (Object[]) itr2.next();
	            double rowVal = (double)row[colIndex];
	            if ((samplingPopulationType == SamplingPopulationType.posItems) && (rowVal < 0))
	                continue;
	            if ((samplingPopulationType == SamplingPopulationType.negItems) && (rowVal > 0))
	                continue;
	            if (Math.abs(rowVal) >= performanceMateriality)
	            	sumHighValueItems += Math.abs(rowVal);
	        }
	    }
	    
	    double populationSize = colStats.get(samplingPopulationType.name()) - sumHighValueItems;
	    /*int minSampleSize = getMinSampleSize(riskType,populationSize,performanceMateriality,riskLevel);
	    if(sampleSize < minSampleSize){
	        System.out.println("Number of samples is less than minimum value(" + minSampleSize + ") specified by AAM.");
	        //return null;
	    }*/
	    double sampleInterval = populationSize/sampleSize;
	    Random random = new Random(seed);
	    double sampleThreshold = random.nextDouble() * sampleInterval;
	    double rowTotal = 0;
	    
	    samplingDetails.put("Risk Type",riskType);
	    samplingDetails.put("Sampling Column",colName);
	    samplingDetails.put("Performance Materiality",performanceMateriality);
	    samplingDetails.put("Risk Level",riskLevel);
	    samplingDetails.put("Sampling Population Type",samplingPopulationType);
	    samplingDetails.put("Sample Size",sampleSize);
	    samplingDetails.put("Seed",seed);
	    samplingDetails.put("Starting dollar",sampleThreshold);
	    samplingDetails.put("Sampling Interval",sampleInterval);
	    
	    Map<Object,Integer> result = new HashMap<>();
	    Iterator<Object> itr3 = (Iterator<Object>) getTinkerData(columns, dataFrame, false);
	    while(itr3.hasNext()){
	    	Object[] row = (Object[]) itr3.next();
	        Object rowKey = row[0];
	        double colVal = (double)row[colIndex];
	        if ((samplingPopulationType == SamplingPopulationType.posItems) && (colVal < 0)){
	        	result.put(rowKey, 0);
	            continue;
	        }
	        if ((samplingPopulationType == SamplingPopulationType.negItems) && (colVal > 0)){
	        	result.put(rowKey, 0);
	        	continue;
	        }
	        if ((selectAllHighValueItems) && (Math.abs(colVal) >= performanceMateriality)){
	        	result.put(rowKey, 1);
	            continue;
	        }
	        rowTotal += Math.abs(colVal);
	        while (rowTotal > sampleThreshold){
	            if (result.containsKey(rowKey))
	                result.put(rowKey, result.get(rowKey)+ 1);
	            else
	            	result.put(rowKey, 1);
	            sampleThreshold += sampleInterval;
	        }
	        if(!result.containsKey(rowKey))
	        	result.put(rowKey, 0);
	    }
	    return result;
	}
}

class MUSampleIterator extends ExpressionIterator{
	
	protected Map<Object,Integer> resultMap;
	
	protected MUSampleIterator() {
		
	}
	
	public MUSampleIterator(Iterator results, String [] columnsUsed, String script, Map<Object,Integer> resultMap)
	{
		this.resultMap = resultMap;
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
			Object key = otherBindings.get(columnsUsed[0]);
			retObject = resultMap.get(key);
		}
		return retObject;
	}
}
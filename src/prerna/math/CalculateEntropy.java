package prerna.math;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import prerna.util.ArrayUtilityMethods;

public class CalculateEntropy {

	private Map<Object, Integer> countHash = new ConcurrentHashMap<Object, Integer>();
	private Object[] dataArr;
	
	private double entropy;
	private double entropyDensity;
	private int numUniqueValues;
	
	public CalculateEntropy() {
		
	}
	
	public void addDataToCountHash() {
		int i = 0;
		int size = dataArr.length;
		for(; i < size; i++) {
			Object val = dataArr[i];
			if(countHash.containsKey(val)) {
				int count = countHash.get(val) + 1;
				countHash.put(val, count);
			} else {
				countHash.put(val, 1);
				numUniqueValues++;
			}
		}
	}
	
	public double calculateEntropy() {
		Object[] arr = countHash.values().toArray();
		int[] countArr = ArrayUtilityMethods.convertObjArrToIntArr(arr);
		entropy = StatisticsUtilityMethods.calculateEntropy(countArr);
		
		return entropy;
	}
	
	public double calculateEntropyDensity() {
		Object[] arr = countHash.values().toArray();
		int[] countArr = ArrayUtilityMethods.convertObjArrToIntArr(arr);
		entropy = StatisticsUtilityMethods.calculateEntropy(countArr);
		
		entropyDensity = entropy / numUniqueValues *-1;
		return entropyDensity;
	}
	
	public double getEntropy() {
		return entropy;
	}
	
	public double getEntropyDensity() {
		return entropyDensity;
	}
	
	public int getNumUniqueValues() {
		return numUniqueValues;
	}
	
	public void setNumUniqueValues(int numUniqueValues) {
		this.numUniqueValues = numUniqueValues;
	}
	
	public void setDataArr(Object[] dataArr) {
		this.dataArr = dataArr;
	}
	
	public Object[] getDataArr() {
		return dataArr;
	}
	
	public void setCountHash(Map<Object, Integer> countHash) {
		this.countHash = countHash;
	}
	
	public Map<Object, Integer> getCountHash() {
		return countHash;
	}
}

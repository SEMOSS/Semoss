/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
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
		entropyDensity = StatisticsUtilityMethods.calculateEntropyDensity(countArr);
		
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

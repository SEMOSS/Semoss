/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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

package prerna.algorithm.learning.unsupervised.outliers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.IAnalyticTransformationRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.BTreeDataFrame;
import prerna.ds.ExactStringMatcher;
import prerna.math.BarChart;
import prerna.math.StatisticsUtilityMethods;
import prerna.om.SEMOSSParam;
import prerna.util.ArrayUtilityMethods;

public class EntropyDensityStatistic implements IAnalyticTransformationRoutine {

	private static final String INSTANCE_INDEX = "instanceIndex";
	private static final String SKIP_ATTRIBUTES = "skipAttributes";

	private static final String BIN_COL_NAME_ADDED = "_BINS";
	
	private List<SEMOSSParam> options;
	private List<String> skipAttributes;

	private int instanceIndex;
	private String[] attributeNames;
	private ITableDataFrame dataFrame;

	private List<String> addedCols = new ArrayList<String>(); 
	private String outlierVal;
	
	Map<Object, Double> retHash;
	
	public EntropyDensityStatistic() {
		options = new ArrayList<SEMOSSParam>();

		SEMOSSParam p1 = new SEMOSSParam();
		p1.setName(INSTANCE_INDEX);
		options.add(0, p1);

		SEMOSSParam p2 = new SEMOSSParam();
		p2.setName(SKIP_ATTRIBUTES);
		options.add(1, p1);
	}

	@Override
	public ITableDataFrame runAlgorithm(ITableDataFrame... data) {
		this.instanceIndex = 0; //(int) options.get(0).getSelected();
		this.skipAttributes = new ArrayList<String>(); //(List<String>) options.get(1).getSelected();
		this.dataFrame = data[0];
		this.attributeNames = dataFrame.getColumnHeaders();

		// double check that all info in dataTable is categorical
		boolean[] isNumeric = dataFrame.isNumeric();
		for(int i = 0; i < isNumeric.length; i++) {
			if(i == instanceIndex) {
				continue;
			}
			if (isNumeric[i]) {
				String[] names = new String[2];
				names[0] = attributeNames[instanceIndex];
				names[1] = attributeNames[i] + BIN_COL_NAME_ADDED;
				
				//search if binning has already occurred and in data frame
				if(ArrayUtilityMethods.arrayContainsValue(attributeNames, names[1])) {
					continue; // already created bin
				}
				
				int numInstances = dataFrame.getNumRows();
				Object[] instances = new Object[numInstances];
				Double[] values = new Double[numInstances];
				Iterator<Object[]> it = dataFrame.iterator();
				int counter = 0;
				while(it.hasNext()) {
					Object[] row = it.next();
					instances[counter] = row[instanceIndex];
					try {
						values[counter] = ((Number) row[i]).doubleValue();
					} catch (ClassCastException e) {
						values[counter] = null;
					}
					counter++;
				}
				
				BarChart chart = new BarChart(values);
				String[] binValues = chart.getAssignmentForEachObject();
				ITableDataFrame table = new BTreeDataFrame(names);
				Map<String, Object> addBinValues = new HashMap<String, Object>();
				for(int j = 0; j < values.length; j++) {
					addBinValues.put(names[0], instances[j]);
					addBinValues.put(names[1], binValues[j]);
					table.addRow(addBinValues);
				}
				addedCols.add(names[1]);
				
				this.dataFrame.join(table, attributeNames[instanceIndex], attributeNames[instanceIndex], 1.0, new ExactStringMatcher() );
				skipAttributes.add(attributeNames[i]);

				if(i < instanceIndex) {
					instanceIndex--;
				}
			}
		}
		dataFrame.setColumnsToSkip(skipAttributes);
		attributeNames = dataFrame.getColumnHeaders();

		Map<String, Map<String, Integer>> frequencyCounts = dataFrame.getUniqueColumnValuesAndCount();
		Map<Object, Double> results = score(frequencyCounts);

		String attributeName = attributeNames[instanceIndex];
		// to avoid adding columns with same name
		int counter = 0;
		this.outlierVal = attributeName + "_EntropyBasedOutlier_" + counter;
		while(ArrayUtilityMethods.arrayContainsValue(attributeNames, outlierVal)) {
			counter++;
			this.outlierVal = attributeName + "_EntropyBasedOutlier_" + counter;
		}
		ITableDataFrame returnTable = new BTreeDataFrame(new String[]{attributeName, outlierVal});
		for(Object instance : results.keySet()) {
			Map<String, Object> row = new HashMap<String, Object>();
			row.put(attributeName, instance);
			row.put(outlierVal, results.get(instance));
			returnTable.addRow(row);
		}
		
		return returnTable;
	}

	public Map<Object, Double> score(Map<String, Map<String, Integer>> frequencyCounts) {
		retHash = new HashMap<Object, Double>();
		double max = 0;
		double min = Double.POSITIVE_INFINITY;
		
		Iterator<List<Object[]>> it = dataFrame.uniqueIterator(attributeNames[instanceIndex]);
		int numInstances = dataFrame.getNumRows();
		while(it.hasNext()) {
			List<Object[]> instance = it.next();
			Object instanceName = instance.get(0)[instanceIndex];
			double entropyVal = 0;
			for(int i = 0; i < attributeNames.length; i++) {
				if(i == instanceIndex) {
					continue;
				}
				Map<String, Integer> attributeCounts = frequencyCounts.get(attributeNames[i]);
				int numUniqueVals = attributeCounts.values().size();
				double averageProb = 0;
				for(int j = 0; j < instance.size(); j++) {
					Object[] instanceRow = instance.get(j);
					int occurance = attributeCounts.get(instanceRow[i]);
					averageProb += (double) occurance / numInstances;
				}
				averageProb /= instance.size();
				double rowEntropy = -1.0 * StatisticsUtilityMethods.logBase2(averageProb) / StatisticsUtilityMethods.logBase2(numUniqueVals);
				entropyVal += rowEntropy;
			}
			
			if(entropyVal > max) {
				max = entropyVal;
			}
			if(entropyVal < min) {
				min = entropyVal;
			}
			
			retHash.put(instanceName, entropyVal);
		}
		
		double range = max-min;
		// normalize
		for(Object key : retHash.keySet()) {
			Double value = (retHash.get(key) - min) / range; 
			retHash.put(key, value);
		}
		
		return retHash;
	}

	@Override
	public String getName() {
		return "Entropy Density Outlier Statistic";
	}
	
	@Override
	public String getResultDescription() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setSelectedOptions(Map<String, Object> selected) {
		Set<String> keySet = selected.keySet();
		for(String key : keySet) {
			for(SEMOSSParam param : options) {
				if(param.getName().equals(key)){
					param.setSelected(selected.get(key));
					break;
				}
			}
		}
	}

	@Override
	public List<SEMOSSParam> getOptions() {
		return this.options;
	}

	@Override
	public String getDefaultViz() {
		return "prerna.ui.components.playsheets.LocalOutlierVizPlaySheet";
	}

	@Override
	public List<String> getChangedColumns() {
		addedCols.add(this.outlierVal);
		return addedCols;
	}

	@Override
	public Map<String, Object> getResultMetadata() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public Map<Object, Double> getResults() {
		return this.retHash;
	}
}

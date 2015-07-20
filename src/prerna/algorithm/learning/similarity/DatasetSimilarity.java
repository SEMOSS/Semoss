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
package prerna.algorithm.learning.similarity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.IAnalyticRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.util.Cluster;
import prerna.algorithm.learning.util.IClusterDistanceMode;
import prerna.algorithm.learning.util.IClusterDistanceMode.DistanceMeasure;
import prerna.ds.BTreeDataFrame;
import prerna.math.SimilarityWeighting;
import prerna.om.SEMOSSParam;

public class DatasetSimilarity implements IAnalyticRoutine {

	private static final String INSTANCE_INDEX = "instanceIndex";
	protected static final String DISTANCE_MEASURE = "distanceMeasure";
	protected static final String SKIP_ATTRIBUTES = "skipAttributes";

	private List<SEMOSSParam> options;
	
	private ITableDataFrame dataFrame;
	private String[] attributeNames;
	private boolean[] isNumeric;
	private Map<String, IClusterDistanceMode.DistanceMeasure> distanceMeasure;
	private List<String> skipAttributes;
	
	private int instanceIndex;
	private Cluster cluster;
	
	private String changedColumn;
	
	private Map<String, Double> results = new HashMap<String, Double>();

	public DatasetSimilarity() {
		options = new ArrayList<SEMOSSParam>();
		
		SEMOSSParam p1 = new SEMOSSParam();
		p1.setName(INSTANCE_INDEX);
		options.add(0, p1);
		
		SEMOSSParam p2 = new SEMOSSParam();
		p2.setName(DISTANCE_MEASURE);
		options.add(1, p2);
		
		SEMOSSParam p3 = new SEMOSSParam();
		p3.setName(SKIP_ATTRIBUTES);
		options.add(2, p3);
	}
	
	@Override
	public ITableDataFrame runAlgorithm(ITableDataFrame... data) {
		this.instanceIndex = (int) options.get(0).getSelected();
		this.distanceMeasure = (Map<String, DistanceMeasure>) options.get(1).getSelected();
		this.skipAttributes = (List<String>) options.get(2).getSelected();
		
		this.dataFrame = data[0];
		dataFrame.setColumnsToSkip(skipAttributes);
		this.attributeNames = dataFrame.getColumnHeaders();
		this.isNumeric = dataFrame.isNumeric();
		// set the type of distance measure to be used for each numerical property - default is using mean
		if(this.distanceMeasure == null) {
			distanceMeasure = new HashMap<String, IClusterDistanceMode.DistanceMeasure>();
			for(int i = 0; i < attributeNames.length; i++) {
				if(isNumeric[i]) {
					distanceMeasure.put(attributeNames[i], DistanceMeasure.MEAN);
				}
			}
		} else {
			for(int i = 0; i < attributeNames.length; i++) {
				if(!distanceMeasure.containsKey(attributeNames[i])) {
					distanceMeasure.put(attributeNames[i], DistanceMeasure.MEAN);
				}
			}
		}
		
		Map<String, Double> numericalWeights = new HashMap<String, Double>();
		Map<String, Double> categoricalWeights = new HashMap<String, Double>();
		// fills in numericalWeights and categoricalWeights maps
		SimilarityWeighting.calculateWeights(dataFrame, instanceIndex, attributeNames, isNumeric, numericalWeights, categoricalWeights);
		cluster = new Cluster(categoricalWeights, numericalWeights);
		cluster.setDistanceMode(distanceMeasure);
		
		generateClusterCenters();
		getSimilarityValuesForInstances();
		
		String attributeName = attributeNames[instanceIndex];
		this.changedColumn = attributeName + "_SIMILARITY";
		ITableDataFrame returnTable = new BTreeDataFrame(new String[]{attributeName, changedColumn});
		for(Object instance : results.keySet()) {
			Map<String, Object> row = new HashMap<String, Object>();
			row.put(attributeName, instance);
			row.put(changedColumn, results.get(instance));
			returnTable.addRow(row, row);
		}
		
		return returnTable;
	}
	
	private void generateClusterCenters() {
		Iterator<List<Object[]>> it = dataFrame.scaledUniqueIterator(attributeNames[instanceIndex], false);
		while(it.hasNext()) {
			cluster.addToCluster(it.next(), attributeNames, isNumeric);
		}
	}
	
	
	public void getSimilarityValuesForInstances() {
		Iterator<List<Object[]>> it = dataFrame.scaledUniqueIterator(attributeNames[instanceIndex], false);
		while(it.hasNext()) {
			List<Object[]> instance = it.next();
			String instanceName = (String) instance.get(0)[instanceIndex];
			double sim = cluster.getSimilarityForInstance(instance, attributeNames, isNumeric, instanceIndex);
			results.put(instanceName, sim);
		}
	}

	@Override
	public String getName() {
		return "Determine similarity of dataset";
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getChangedColumns() {
		List<String> changedCols = new ArrayList<String>();
		changedCols.add(changedColumn);
		return changedCols;
	}

	@Override
	public Map<String, Object> getResultMetadata() {
		// TODO Auto-generated method stub
		return null;
	}
	
}

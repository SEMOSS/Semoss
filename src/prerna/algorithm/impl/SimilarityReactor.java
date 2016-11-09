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
package prerna.algorithm.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.util.Cluster;
import prerna.algorithm.learning.util.IClusterDistanceMode;
import prerna.algorithm.learning.util.IClusterDistanceMode.DistanceMeasure;
import prerna.math.SimilarityWeighting;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.util.ArrayUtilityMethods;

public class SimilarityReactor extends MathReactor {

	public static final String INSTANCE_INDEX = "instanceIndex";
	public static final String DUPLICATION_RECONCILIATION	= "dupReconciliation";
	
	private List<String> attributeNamesList;
	private String[] attributeNames;
	private Map<String, IClusterDistanceMode.DistanceMeasure> distanceMeasure;
	
	private int instanceIndex;
	private Cluster cluster;
	
	private String similarityColName;
	
	private Map<String, Double> results = new HashMap<String, Double>();

	public Iterator process() {
		modExpression();
		
		///////////////// start of initializing some stuff... needs to be put away somewhere else
		if(myStore.containsKey(PKQLEnum.MATH_PARAM)) {
			Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MATH_PARAM);
			if(options.containsKey(INSTANCE_INDEX.toUpperCase())) {
				this.instanceIndex = Integer.parseInt(options.get(INSTANCE_INDEX.toUpperCase()) + "");
			} else {
				// i think its a fair assumption that the first column written can be assumed to be the instance
				this.instanceIndex = 0;
			}
			
			//TODO: need to add something for the similarity metric
			// if it is mean/max/min
			
		} else {
			//TODO: need to throw an error saying parameters are required
			return null;
		}
		
		ITableDataFrame dataFrame = (ITableDataFrame)myStore.get("G");

		this.attributeNamesList = (List<String>)myStore.get(PKQLEnum.COL_DEF);
		this.attributeNames = attributeNamesList.toArray(new String[]{});
		
		boolean[] isNumeric = new boolean[this.attributeNames.length];
		for(int i = 0; i < this.attributeNames.length; i++) {
			isNumeric[i] = dataFrame.isNumeric(this.attributeNames[i]);
		}
		
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
		
		generateClusterCenters(dataFrame, isNumeric);
		getSimilarityValuesForInstances(dataFrame, isNumeric);
		
		String[] allColNames = dataFrame.getColumnHeaders();
		String attributeName = attributeNames[instanceIndex];

		// to avoid adding columns with same name
		int counter = 0;
		this.similarityColName = attributeName + "_SIMILARITY";
		while(ArrayUtilityMethods.arrayContainsValue(allColNames, similarityColName)) {
			counter++;
			this.similarityColName = attributeName + "_SIMILARITY_" + counter;
		}

		Map<String, String> dataType = new HashMap<>();
		dataType.put(similarityColName, "double");
		dataFrame.connectTypes(attributeName, similarityColName, dataType);
		for(Object instance : results.keySet()) {
			Double val = results.get(instance);

			Map<String, Object> clean = new HashMap<String, Object>();
			clean.put(attributeName, instance);
			clean.put(similarityColName, val);

			dataFrame.addRelationship(clean);
		}
		
		myStore.put("STATUS",STATUS.SUCCESS);

		return null;
	}
	
	private void generateClusterCenters(ITableDataFrame dataFrame, boolean[] isNumeric) {
		Iterator<List<Object[]>> it = this.getUniqueScaledData(attributeNames[instanceIndex], attributeNamesList, dataFrame);
		while(it.hasNext()) {
			cluster.addToCluster(it.next(), attributeNames, isNumeric);
		}
	}
	
	public void getSimilarityValuesForInstances(ITableDataFrame dataFrame, boolean[] isNumeric) {
		Iterator<List<Object[]>> it = this.getUniqueScaledData(attributeNames[instanceIndex], attributeNamesList, dataFrame);
		while(it.hasNext()) {
			List<Object[]> instance = it.next();
			String instanceName = (String) instance.get(0)[instanceIndex];
			double sim = cluster.getSimilarityForInstance(instance, attributeNames, isNumeric, instanceIndex);
			results.put(instanceName, sim);
		}
	}
}

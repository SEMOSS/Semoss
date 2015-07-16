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
package prerna.ui.components.playsheets;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import prerna.algorithm.learning.similarity.DatasetSimilarity;
import prerna.math.BarChart;
import prerna.om.SEMOSSParam;
import prerna.util.ArrayUtilityMethods;

public class DatasetSimilairtyColumnChartPlaySheet extends ColumnChartPlaySheet {

	private int instanceIndex;
	private String changedCol;
	private List<String> skipAttributes;

	public DatasetSimilairtyColumnChartPlaySheet() {
		super();
	}
	
	@Override
	public void createData() {
		if(dataFrame == null || dataFrame.isEmpty())
			super.createData();
	}

	@Override
	public void runAnalytics() {
		DatasetSimilarity alg = new DatasetSimilarity();
		List<SEMOSSParam> options = alg.getOptions();
		Map<String, Object> selectedOptions = new HashMap<String, Object>();
		selectedOptions.put(options.get(0).getName(), instanceIndex); // default of 0 is acceptable
		alg.setSelectedOptions(selectedOptions);
		dataFrame.performAction(alg);
		
		this.changedCol = alg.getChangedColumns().get(0);
	}
	
	public void processQueryData() {
		Object[] simValues = dataFrame.getColumn(changedCol);
		Double[] dSimValues = ArrayUtilityMethods.convertObjArrToDoubleWrapperArr(simValues);
		BarChart chart = new BarChart(dSimValues, changedCol);
		Hashtable<String, Object>[] bins = null;
		if(chart.isUseCategoricalForNumericInput()) {
			chart.calculateCategoricalBins("?", true, true);
			chart.generateJSONHashtableCategorical();
			bins = chart.getRetHashForJSON();
		} else {
			chart.generateJSONHashtableNumerical();
			bins = chart.getRetHashForJSON();
		}
		
		Hashtable<String, Object> retHash = new Hashtable<String, Object>();
		Object[] binArr = new Object[]{bins};
		retHash.put("dataSeries", binArr);
		retHash.put("names", new String[]{changedCol.concat(" Similarity Distribution to Dataset Center")});
		
		this.dataHash = retHash;
	}
	
	public void setInstanceIndex(int instanceIndex) {
		this.instanceIndex = instanceIndex;
	}
	
	public void setSkipAttributes(List<String> skipAttributes) {
		this.skipAttributes = skipAttributes;
	}
}

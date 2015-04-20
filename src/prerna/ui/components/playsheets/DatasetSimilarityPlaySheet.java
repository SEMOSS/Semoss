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

import java.text.DecimalFormat;
import java.util.ArrayList;

import prerna.algorithm.learning.similarity.DatasetSimilarity;

public class DatasetSimilarityPlaySheet extends GridPlaySheet {
	
	double[] simValues;
	
	@Override
	public void runAnalytics() {
		DatasetSimilarity alg = new DatasetSimilarity(list, names);
		alg.generateClusterCenters();
		list = alg.getMasterTable();
		names = alg.getMasterNames();
		simValues = alg.getSimilarityValuesForInstances();
		
		int i = 0;
		int size = list.size();
		int props = list.get(0).length;
		ArrayList<Object[]> newList = new ArrayList<Object[]>();
		DecimalFormat df = new DecimalFormat("#%");
		for (; i < size; i++) {
			Object[] newRow = new Object[props + 1];
			Object[] oldRow = list.get(i);
			System.arraycopy(oldRow, 0, newRow, 0, props);
			newRow[props] = df.format(simValues[i]);
			newList.add(newRow);
		}
		
		i = 0;
		size = names.length;
		String[] newNames = new String[size + 1];
		System.arraycopy(names, 0, newNames, 0, size);
		newNames[size] = "Similaritity To Dataset";
		
		list = newList;
		names = newNames;
	}
	
	public double[] getSimValues() {
		return simValues;
	}
	
	public void setSimValues(double[] simValues) {
		this.simValues = simValues;
	}
}

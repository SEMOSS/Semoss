/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.ui.components.playsheets;

import java.util.ArrayList;
import java.util.Hashtable;

import prerna.algorithm.cluster.DatasetSimilarity;
import prerna.math.BarChart;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Utility;

public class DatasetSimilairtyColumnChartPlaySheet extends ColumnChartPlaySheet {

	double[] simValues;
	
	public DatasetSimilairtyColumnChartPlaySheet() {
		super();
	}
	
	@Override
	public void createData()
	{
		generateData();
		runAlgorithm();
		dataHash = processQueryData();
	}

	private void generateData() {
		list = new ArrayList<Object[]>();
		
		SesameJenaSelectWrapper sjsw = Utility.processQuery(engine, query);
		names = sjsw.getVariables();
		int length = names.length;
		while(sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
			Object[] row = new Object[length];
			int i = 0;
			for(; i < length; i++) {
				row[i] = sjss.getVar(names[i]);
			}
			list.add(row);
		}
	}
	
	private void runAlgorithm() {
		DatasetSimilarity alg = new DatasetSimilarity(list, names);
		alg.generateClusterCenters();
		simValues = alg.getSimilarityValuesForInstances();		
	}
	
	public Hashtable<String, Object> processQueryData() {
		BarChart chart = new BarChart(simValues, names[0]);
		Hashtable<String, Object>[] bins = chart.getRetHashForJSON();
		
		Hashtable<String, Object> retHash = new Hashtable<String, Object>();
		Object[] binArr = new Object[]{bins};
		retHash.put("dataSeries", binArr);
		retHash.put("names", new String[]{names[0].concat(" Similarity Distribution to Dataset Center")});
		
		return retHash;
	}
	
}

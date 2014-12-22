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

import prerna.algorithm.cluster.ClusteringClassification;

public class ClusteringClassificationPlaySheet extends GridPlaySheet{
	
	@Override
	public void createData() {
		super.createData();
		ClusteringClassification alg = new ClusteringClassification(list, names);
		alg.execute();

		double[] accuracy = alg.getAccuracy();
		double[] precision = alg.getPrecision();
		
		list = new ArrayList<Object[]>();
		int size = names.length;
		int i;
		for(i = 1; i < size; i++) {
			Object[] row = new Object[]{names[i], String.format("%.2f%%", accuracy[i-1]*100), String.format("%.2f", precision[i-1])};
			list.add(row);
		}
			
		names = new String[]{"Attribute","Accuracy","Precision"};
	}
	
}

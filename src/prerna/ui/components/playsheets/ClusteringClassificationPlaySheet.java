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

package prerna.ui.components.playsheets;

import java.text.DecimalFormat;
import java.util.ArrayList;

import prerna.algorithm.cluster.DatasetSimilarity;

public class DatasetSimilarityPlaySheet extends GridPlaySheet{
	
	@Override
	public void runAnalytics() {
		DatasetSimilarity alg = new DatasetSimilarity(list, names);
		alg.generateClusterCenters();
		double[] simValues = alg.getSimilarityValuesForInstances();
		
		int i = 0;
		int size = list.size();
		int props = list.get(0).length;
		ArrayList<Object[]> newList = new ArrayList<Object[]>();
		DecimalFormat df = new DecimalFormat("#%");
		for(; i < size; i++) {
			Object[] newRow = new Object[props+1];
			Object[] oldRow = list.get(i);
			System.arraycopy(oldRow, 0, newRow, 0, props);
			newRow[props] = df.format(simValues[i]);
			newList.add(newRow);
		}
		
		i = 0;
		size = names.length;
		String[] newNames = new String[size+1];
		System.arraycopy(names, 0, newNames, 0, size);
		newNames[size] = "Similaritity To Dataset";
		
		list = newList;
		names = newNames;
	}

}

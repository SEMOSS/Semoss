package prerna.ui.components.playsheets;

import java.util.ArrayList;

import prerna.algorithm.weka.impl.WekaClassification;

public class WekaClassificationPlaySheet extends GridPlaySheet{

	@Override
	public void createData() {
		super.createData();
		WekaClassification alg = new WekaClassification("Test", list, names);
		try {
			alg.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		String[] classNames = alg.getClassNames();
		double[] accuracy = alg.getAccuracy();
		double[] percision = alg.getPercision();
		
		int size = classNames.length;
		names = new String[size + 1];
		int i;
		for(i = 0; i < size; i++) {
			names[i+1] = classNames[i];
		}
		
		list = new ArrayList<Object[]>();
		Object[] row = new Object[size + 1];
		row[0] = "Accuracy (% correct)";
		for(i = 0; i < size; i++) {
			row[i+1] = String.format("%.2f%%", accuracy[i]);
		}
		list.add(row);
		
		row = new Object[size + 1];
		row[0] = "Percision (Kappa Value)";
		for(i = 0; i < size; i++) {
			row[i+1] = String.format("%.2f", percision[i]);
		}
		list.add(row);
	}
	
}

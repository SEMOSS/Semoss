package prerna.ui.components.playsheets;

import java.util.ArrayList;

import prerna.algorithm.weka.impl.WekaClassification;

public class WekaClassificationPlaySheet extends GridPlaySheet{
	
	private String modelName;
	
	@Override
	public void createData() {
		super.createData();
		WekaClassification alg = new WekaClassification("Test", list, names, modelName);
		try {
			alg.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		double[] accuracy = alg.getAccuracy();
		double[] precision = alg.getPrecision();
		list = new ArrayList<Object[]>();
		int size = accuracy.length;
		int i;
		for(i = 0; i < size; i++) {
			Object[] row = new Object[]{names[i+1], String.format("%.2f%%", accuracy[i]), String.format("%.2f", precision[i])};
			list.add(row);
		}
		
		names = new String[]{"Attribute","Accuracy","Precision"};
	}
	
	@Override
	public void setQuery(String query) {
		String[] querySplit = query.split("\\+\\+\\+");
		this.query = querySplit[0];
		this.modelName = querySplit[1];
	}
	
}

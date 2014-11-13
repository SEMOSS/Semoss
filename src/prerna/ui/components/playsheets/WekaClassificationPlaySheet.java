package prerna.ui.components.playsheets;

import prerna.algorithm.weka.impl.WekaClassification;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;

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
	}
	
	@Override
	public void setQuery(String query) {
		String[] querySplit = query.split("\\+\\+\\+");
		this.query = querySplit[0];
		this.modelName = querySplit[1].trim();
	}
	
	@Override
	public Object getVariable(String varName, SesameJenaSelectStatement sjss){
		return sjss.getVar(varName);
	}
	
}

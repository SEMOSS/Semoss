package prerna.ui.components.playsheets;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;

import prerna.algorithm.weka.impl.WekaClassification;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Utility;

public class WekaClassificationPlaySheet extends DendrogramPlaySheet{
	
	private String modelName;
	private WekaClassification alg = null;
	
	public WekaClassificationPlaySheet() {
		super();
	}
	
	@Override
	public void createData() {
		generateData();
		runAlgorithm();
		dataHash = processQueryData();
	}
	
	public void runAlgorithm() {
		alg = new WekaClassification(list, names, modelName);
		try {
			alg.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public Hashtable processQueryData() {
		Map<String, Map> rootMap = alg.getJ48Tree();
		
		System.out.println(rootMap);
		HashSet hashSet = new HashSet();
		
		processTree(rootMap, hashSet);

		String root = engine.getEngineName();
		
		Hashtable<String, Object> allHash = new Hashtable<String, Object>();
		allHash.put("name", root);
		allHash.put("children", hashSet);
		return allHash;
	}
	
	public void generateData() {
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
	
	@Override
	public void setQuery(String query) {
		String[] querySplit = query.split("\\+\\+\\+");
		this.query = querySplit[0];
		this.modelName = querySplit[1].trim();
	}
	
	
}

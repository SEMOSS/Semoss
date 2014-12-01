package prerna.ui.components.playsheets;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.weka.impl.WekaClassification;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Utility;

public class WekaClassificationPlaySheet extends DendrogramPlaySheet{

	private static final Logger LOGGER = LogManager.getLogger(WekaClassificationPlaySheet.class.getName());
	
	private String modelName;
	private WekaClassification alg = null;
	private int classColumn = -1;
	
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
		if(classColumn<0) {
			LOGGER.info("Creating classifier to predict column "+names[names.length-1]);
			alg = new WekaClassification(list, names, modelName, names.length - 1);
		} else {
			LOGGER.info("Creating classifier to predict column "+names[classColumn]);
			alg = new WekaClassification(list, names, modelName, classColumn);
		}
		try {
			alg.execute();
			alg.processTreeString();
			System.out.println(alg.getTreeAsString());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public Hashtable processQueryData() {
		Map<String, Map> rootMap = alg.getTreeMap();
		
		System.out.println(rootMap);
		HashSet hashSet = new HashSet();
		
		processTree(rootMap, hashSet);

		String root = engine.getEngineName();
		
		Hashtable<String, Object> allHash = new Hashtable<String, Object>();
		allHash.put("name", root);
		allHash.put("children", hashSet);
		
		DecimalFormat df = new DecimalFormat("#%");
		Hashtable<String, Object> statHash = new Hashtable<String, Object>();
		statHash.put("Accuracy", df.format(alg.getAccuracy()/100));
		statHash.put("Precision", df.format(alg.getPrecision()/100));
		allHash.put("stats", statHash);
		
		return allHash;
	}
	
	private void generateData() {
		if(query!=null) {
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
	}
	
	/**
	 * Method addPanel. Creates a panel and adds the table to the panel.
	 */
	@Override
	public void addPanel()
	{
		if(jTab==null) {
			super.addPanel();
		} else {
			String lastTabName = jTab.getTitleAt(jTab.getTabCount()-1);
			LOGGER.info("Parsing integer out of last tab name");
			int count = 1;
			if(jTab.getTabCount()>1)
				count = Integer.parseInt(lastTabName.substring(0,lastTabName.indexOf(".")))+1;
			addPanelAsTab(count+". Classification");
		}
	}
	
	@Override
	public void setQuery(String query) {
		String[] querySplit = query.split("\\+\\+\\+");
		this.query = querySplit[0];
		this.modelName = querySplit[1].trim();
	}
	
	public void setModelName(String modelName) {
		this.modelName = modelName;
	}
	
	public void setClassColumn(int classColumn){
		this.classColumn = classColumn;
	}

}

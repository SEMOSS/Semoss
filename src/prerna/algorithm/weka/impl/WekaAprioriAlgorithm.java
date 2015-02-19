package prerna.algorithm.weka.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import weka.associations.Apriori;
import weka.associations.AssociationRule;
import weka.associations.AssociationRules;
import weka.associations.Item;
import weka.core.Attribute;
import weka.core.Instances;

public class WekaAprioriAlgorithm {

	private static final Logger LOGGER = LogManager.getLogger(WekaAprioriAlgorithm.class.getName());

	private Instances data;
	private String[] names;
	private ArrayList<Object[]> list;
	
	private Map<Integer, Collection<Item>> premises;
	private Map<Integer, Collection<Item>> consequences;
	private Map<Integer, Integer> counts;
	
	private String[] retNames;
	private ArrayList<Object[]> retList;
	
	public WekaAprioriAlgorithm() {
		
	}
	
	public WekaAprioriAlgorithm(ArrayList<Object[]> list, String[] names) {
		this.list = list;
		this.names = names;

		LOGGER.info("Starting apriori algorithm using... ");
	}

	public void execute() throws Exception {
		premises = new HashMap<Integer, Collection<Item>>();
		consequences = new HashMap<Integer, Collection<Item>>();
		counts = new HashMap<Integer, Integer>();
		
		LOGGER.info("Generating Weka Instances object...");
		this.data = WekaUtilityMethods.createInstancesFromQueryUsingBinNumerical("Apriori dataset", list, names);

		Apriori apriori = new Apriori();
		LOGGER.info("Running Apriori Algorithm...");
		apriori.buildAssociations( data );
		LOGGER.info("Finished Running Algorithm...");
		
		System.out.println(apriori.toString());
		
		AssociationRules rules = apriori.getAssociationRules();
		List<AssociationRule> ruleList = rules.getRules();
		int numRule = 0;
		for(AssociationRule rule : ruleList) {
			premises.put(numRule, rule.getPremise());
			consequences.put(numRule, rule.getConsequence());
			counts.put(numRule, rule.getTotalSupport());
			numRule++;
		}
	}
	
	public void generateDecisionRuleTable() {
		LOGGER.info("Generating Decision Table...");
		int numCols = names.length;
		retNames = new String[numCols];
		int i = 1;
		for(; i < numCols; i++) {
			retNames[i-1] = names[i];
		}
		retNames[i-1] = "Count";
		
		retList = new ArrayList<Object[]>();

		// generate hashmap to prevent constantly looking up indicies
		Map<String, Integer> indexMap = new HashMap<String, Integer>();
		int index = 0;
		for(String varName : names) {
			indexMap.put(varName, index);
			index++;
		}
		
		for(Integer numRule : premises.keySet()) {
			Object[] tableRow = new Object[numCols];
			Collection<Item> premise = premises.get(numRule);
			Collection<Item> consequence = consequences.get(numRule);
			int count = counts.get(numRule);
			
			fillRow(tableRow, premise, indexMap);
			fillRow(tableRow, consequence, indexMap);
			tableRow[numCols-1] = count;
			
			retList.add(tableRow);
		}
	}
	
	private void fillRow(Object[] tableRow, Collection<Item> values, Map<String, Integer> indexMap) {
		for(Item item : values) {
			Attribute category = item.getAttribute();
			String name = category.name();
			String value = item.getItemValueAsString();
			
			tableRow[indexMap.get(name)] = value;
		}
	}
	
	public Instances getData() {
		return data;
	}

	public void setData(Instances data) {
		this.data = data;
	}

	public String[] getNames() {
		return names;
	}

	public void setNames(String[] names) {
		this.names = names;
	}

	public ArrayList<Object[]> getList() {
		return list;
	}

	public void setList(ArrayList<Object[]> list) {
		this.list = list;
	}

	public Map<Integer, Collection<Item>> getPremises() {
		return premises;
	}

	public void setPremises(Map<Integer, Collection<Item>> premises) {
		this.premises = premises;
	}

	public Map<Integer, Collection<Item>> getConsequences() {
		return consequences;
	}

	public void setConsequences(Map<Integer, Collection<Item>> consequences) {
		this.consequences = consequences;
	}

	public Map<Integer, Integer> getCounts() {
		return counts;
	}

	public void setCounts(Map<Integer, Integer> counts) {
		this.counts = counts;
	}

	public String[] getRetNames() {
		return retNames;
	}

	public void setRetNames(String[] retNames) {
		this.retNames = retNames;
	}

	public ArrayList<Object[]> getRetList() {
		return retList;
	}

	public void setRetList(ArrayList<Object[]> retList) {
		this.retList = retList;
	} 

	
}

package prerna.algorithm.learning.weka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
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

import com.ibm.icu.text.DecimalFormat;

public class WekaAprioriAlgorithm {

	private static final Logger LOGGER = LogManager.getLogger(WekaAprioriAlgorithm.class.getName());

	private Instances data;
	private String[] names;
	private ArrayList<Object[]> list;
	
	private Map<Integer, Collection<Item>> premises;
	private Map<Integer, Collection<Item>> consequences;
	private Map<Integer, Integer> counts;
	private Map<Integer, Double> confidenceIntervals;
	
	private String[] retNames;
	private ArrayList<Object[]> retList;
	
	private int numRules = 100; // number of rules to output
	private double confPer = 0.9; // min confidence lvl (percentage)
	private double minSupport = 0.1; // min number of rows required for rule (percentage of total rows of data)
	private double maxSupport = 1.0; // max number of rows required for rule (percentage of total rows of data)
	
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
		confidenceIntervals = new HashMap<Integer, Double>();
		
		LOGGER.info("Generating Weka Instances object...");
		this.data = WekaUtilityMethods.createInstancesFromQueryUsingBinNumerical("Apriori dataset", list, names);

		Apriori apriori = new Apriori();
		apriori.setNumRules(numRules);
		apriori.setMinMetric(confPer);
		apriori.setLowerBoundMinSupport(minSupport);
		apriori.setUpperBoundMinSupport(maxSupport);
		
		LOGGER.info("Running Apriori Algorithm...");
		apriori.buildAssociations( data );
		LOGGER.info("Finished Running Algorithm...");
		
//		System.out.println(apriori.toString());
		
		// get and store rules
		AssociationRules rules = apriori.getAssociationRules();
		List<AssociationRule> ruleList = rules.getRules();
		int numRule = 0;
		for(AssociationRule rule : ruleList) {
			premises.put(numRule, rule.getPremise());
			consequences.put(numRule, rule.getConsequence());
			counts.put(numRule, rule.getTotalSupport());
			confidenceIntervals.put(numRule, rule.getPrimaryMetricValue());
			numRule++;
		}
	}
	
	public List<Hashtable<String, Object>> generateDecisionRuleVizualization() {
		// return if no rules found
		if(premises.isEmpty() && consequences.isEmpty() && counts.isEmpty()) {
			return new ArrayList<Hashtable<String, Object>>();
		}
		
		LOGGER.info("Generating Decision Viz Data...");
		DecimalFormat format = new DecimalFormat("0.00");
		List<Hashtable<String, Object>> dataHashList = new ArrayList<Hashtable<String, Object>>();

		for(Integer numRule : premises.keySet()) {
			Collection<Item> premise = premises.get(numRule);
			Collection<Item> consequence = consequences.get(numRule);
			int count = counts.get(numRule);
			double confidence = confidenceIntervals.get(numRule);
			
			String rule = getConcatedItems(premise) + " => " + getConcatedItems(consequence);
			Hashtable<String, Object> dataHash = new Hashtable<String, Object>();
			dataHash.put("rule", rule);
			dataHash.put("count", count);
			dataHash.put("confidence", format.format(confidence));
			
			dataHashList.add(dataHash);
		}
		
		return dataHashList;
	}
	
	private String getConcatedItems(Collection<Item> values) {
		String retVal = "";
		for(Item item : values) {
			Attribute category = item.getAttribute();
			String name = category.name().trim();
			String value = item.getItemValueAsString().trim();
			if(retVal.equals("")) {
				retVal = name + "=" + value;
			} else {
				retVal += retVal + " & " + name + "=" + value;
			}
		}
		
		return retVal;
	}
	
	
	public void generateDecisionRuleTable() {
		// return if no rules found
		if(premises.isEmpty() && consequences.isEmpty() && counts.isEmpty()) {
			return;
		}
		
		LOGGER.info("Generating Decision Table...");
		int numCols = names.length;
		retNames = new String[numCols + 1];
		int i = 1;
		for(; i < numCols; i++) {
			retNames[i-1] = names[i];
		}
		retNames[numCols-1] = "Count";
		retNames[numCols] = "Confidence";
		
		retList = new ArrayList<Object[]>();

		// generate hashmap to prevent constantly looking up indicies
		Map<String, Integer> indexMap = new HashMap<String, Integer>();
		int index = 0;
		for(String varName : names) {
			indexMap.put(varName, index);
			index++;
		}
		
		DecimalFormat format = new DecimalFormat("0.00");
		
		for(Integer numRule : premises.keySet()) {
			Object[] tableRow = new Object[numCols+1];
			Collection<Item> premise = premises.get(numRule);
			Collection<Item> consequence = consequences.get(numRule);
			int count = counts.get(numRule);
			double confidence = confidenceIntervals.get(numRule);
			
			fillRow(tableRow, premise, indexMap);
			fillRowWithAsterisk(tableRow, consequence, indexMap);
			tableRow[numCols-1] = count;
			tableRow[numCols] = format.format(confidence);

			retList.add(tableRow);
		}
	}
	
	private void fillRow(Object[] tableRow, Collection<Item> values, Map<String, Integer> indexMap) {
		for(Item item : values) {
			Attribute category = item.getAttribute();
			String name = category.name();
			String value = item.getItemValueAsString();
			
			tableRow[indexMap.get(name)-1] = value;
		}
	}
	
	private void fillRowWithAsterisk(Object[] tableRow, Collection<Item> values, Map<String, Integer> indexMap) {
		for(Item item : values) {
			Attribute category = item.getAttribute();
			String name = category.name();
			String value = "*** " + item.getItemValueAsString() + " ***";
			
			tableRow[indexMap.get(name)-1] = value;
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

	public Map<Integer, Double> getConfidenceIntervals() {
		return confidenceIntervals;
	}

	public void setConfidenceIntervals(Map<Integer, Double> confidenceIntervals) {
		this.confidenceIntervals = confidenceIntervals;
	}
	
	public int getNumRules() {
		return numRules;
	}

	public void setNumRules(int numRules) {
		this.numRules = numRules;
	}

	public double getConfPer() {
		return confPer;
	}

	public void setConfPer(double confPer) {
		this.confPer = confPer;
	}

	public double getMinSupport() {
		return minSupport;
	}

	public void setMinSupport(double minSupport) {
		this.minSupport = minSupport;
	}
	
	public double getMaxSupport() {
		return maxSupport;
	}

	public void setMaxSupport(double maxSupport) {
		this.maxSupport = maxSupport;
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

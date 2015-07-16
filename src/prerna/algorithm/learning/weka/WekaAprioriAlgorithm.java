package prerna.algorithm.learning.weka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.IAnalyticRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.om.SEMOSSParam;
import weka.associations.Apriori;
import weka.associations.AssociationRule;
import weka.associations.AssociationRules;
import weka.associations.Item;
import weka.core.Attribute;
import weka.core.Instances;

import com.ibm.icu.text.DecimalFormat;

public class WekaAprioriAlgorithm implements IAnalyticRoutine {

	private static final Logger LOGGER = LogManager.getLogger(WekaAprioriAlgorithm.class.getName());

	private static final String NUM_RULES = "numRules";
	private static final String CONFIDENCE_LEVEL = "confPer";
	private static final String MIN_SUPPORT = "minSupport";
	private static final String MAX_SUPPORT = "maxSupport";
	private static final String SKIP_ATTRIBUTES	= "skipAttributes";

	private Instances instancesData;
	private String[] names;
	
	private List<SEMOSSParam> options;
	
	private Map<Integer, Collection<Item>> premises;
	private Map<Integer, Collection<Item>> consequences;
	private Map<Integer, Integer> counts;
	private Map<Integer, Double> confidenceIntervals;
	
	private String[] columnHeaders;
	private ArrayList<Object[]> tabularData;
	
	private int numRules; // number of rules to output
	private double confPer; // min confidence lvl (percentage)
	private double minSupport; // min number of rows required for rule (percentage of total rows of data)
	private double maxSupport; // max number of rows required for rule (percentage of total rows of data)
	private List<String> skipAttributes;
	
	public WekaAprioriAlgorithm() {
		this.options = new ArrayList<SEMOSSParam>();

		SEMOSSParam p1 = new SEMOSSParam();
		p1.setName(NUM_RULES);
		options.add(0, p1);

		SEMOSSParam p2 = new SEMOSSParam();
		p2.setName(CONFIDENCE_LEVEL);
		options.add(1, p2);
		
		SEMOSSParam p3 = new SEMOSSParam();
		p3.setName(MIN_SUPPORT);
		options.add(2, p3);
		
		SEMOSSParam p4 = new SEMOSSParam();
		p4.setName(MAX_SUPPORT);
		options.add(3, p4);
		
		SEMOSSParam p5 = new SEMOSSParam();
		p5.setName(SKIP_ATTRIBUTES);
		options.add(4, p5);
	}
	
	@Override
	public ITableDataFrame runAlgorithm(ITableDataFrame... data) {
		this.numRules = (int) options.get(0).getSelected();
		this.confPer = (double) options.get(1).getSelected();
		this.minSupport = (double) options.get(2).getSelected();
		this.maxSupport = (double) options.get(3).getSelected();
		this.skipAttributes = (List<String>) options.get(4).getSelected();

		ITableDataFrame dataFrame = data[0];
		LOGGER.info("Generating Weka Instances object...");
		this.names = dataFrame.getColumnHeaders();
		this.instancesData = WekaUtilityMethods.createInstancesFromQueryUsingBinNumerical("Apriori dataset", dataFrame.getData(), names);
		LOGGER.info("Starting apriori algorithm using... ");
		
		premises = new HashMap<Integer, Collection<Item>>();
		consequences = new HashMap<Integer, Collection<Item>>();
		counts = new HashMap<Integer, Integer>();
		confidenceIntervals = new HashMap<Integer, Double>();
		
		Apriori apriori = new Apriori();
		apriori.setNumRules(numRules);
		apriori.setMinMetric(confPer);
		apriori.setLowerBoundMinSupport(minSupport);
		apriori.setUpperBoundMinSupport(maxSupport);
		
		LOGGER.info("Running Apriori Algorithm...");
		try {
			apriori.buildAssociations(instancesData);
		} catch (Exception e) {
			e.printStackTrace();
		}
		LOGGER.info("Finished Running Algorithm...");
		
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
		
		return null;
	}

	public Hashtable<String, Object> generateDecisionRuleVizualization() {
		// return if no rules found
		if(premises.isEmpty() && consequences.isEmpty() && counts.isEmpty()) {
			return new Hashtable<String, Object>();
		}
		
		LOGGER.info("Generating Decision Viz Data...");
		DecimalFormat format = new DecimalFormat("0.00");

		List<List<Object>> retItemList = new ArrayList<List<Object>>();
		for(Integer numRule : premises.keySet()) {
			Collection<Item> premise = premises.get(numRule);
			Collection<Item> consequence = consequences.get(numRule);
			int count = counts.get(numRule);
			double confidence = confidenceIntervals.get(numRule);
			
			List<Object> item = new ArrayList<Object>();
			item.add(count);
			item.add(format.format(confidence));
			item.add(getConcatedItems(premise));
			item.add(getConcatedItems(consequence));
			
			retItemList.add(item);
		}

		String[] headers = new String[]{ "Count", "Confidence","Premises", "Consequence"};

		Hashtable<String, Object> retHash = new Hashtable<String, Object>();
		retHash.put("headers", headers);
		retHash.put("data", retItemList);
		
		return retHash;
	}
	
	private String getConcatedItems(Collection<Item> values) {
		String retVal = "";
		for(Item item : values) {
			Attribute category = item.getAttribute();
			String name = category.name().trim();
			String value = item.getItemValueAsString().trim();
			if(retVal.equals("")) {
				retVal = name + " = " + value;
			} else {
				retVal += retVal + " & " + name + " = " + value + " ";
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
		columnHeaders = new String[numCols + 1];
		int i = 1;
		for(; i < numCols; i++) {
			columnHeaders[i-1] = names[i];
		}
		columnHeaders[numCols-1] = "Count";
		columnHeaders[numCols] = "Confidence";
		
		tabularData = new ArrayList<Object[]>();

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

			tabularData.add(tableRow);
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
	
	@Override
	public String getName() {
		return "Aprirori Algorithm";
	}

	@Override
	public String getResultDescription() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setSelectedOptions(Map<String, Object> selected) {
		Set<String> keySet = selected.keySet();
		for(String key : keySet) {
			for(SEMOSSParam param : options) {
				if(param.getName().equals(key)){
					param.setSelected(selected.get(key));
					break;
				}
			}
		}
	}

	@Override
	public List<SEMOSSParam> getOptions() {
		return this.options;
	}

	@Override
	public String getDefaultViz() {
		return "prerna.ui.components.playsheets.WekaAprioriVizPlaySheet";
	}

	@Override
	public List<String> getChangedColumns() {
		return null;
	}

	@Override
	public Map<String, Object> getResultMetadata() {
		// TODO Auto-generated method stub
		return null;
	} 
	
	public List<Object[]> getTabularData() {
		return this.tabularData;
	}
	
	public String[] getColumnHeaders() {
		return this.columnHeaders;
	}

}

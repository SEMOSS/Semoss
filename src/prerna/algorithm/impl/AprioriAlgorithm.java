package prerna.algorithm.impl;


import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import prerna.algorithm.learning.weka.WekaAprioriAlgorithm;
import prerna.algorithm.learning.weka.WekaUtilityMethods;
import weka.core.Instances;

public class AprioriAlgorithm extends BaseReducer{

	private int numRules = 10; // number of rules to output
	private double confPer = 0.1; // min confidence lvl (percentage)
	private double minSupport = 0.1; // min number of rows required for rule (percentage of total rows of data)
	private double maxSupport = 1.0; // max number of rows required for rule (percentage of total rows of data)	
	
	@Override
	public void set(Iterator inputIterator, String[] ids, String script) {
		super.set(inputIterator, ids, script, null);
	}

	@Override
	public Object reduce() {
		Instances instances = WekaUtilityMethods.createInstancesWithNumericBinsFromIterator(inputIterator, ids);
		WekaAprioriAlgorithm alg = new WekaAprioriAlgorithm();
		Map<String, Object> selectedOptions = new Hashtable<String, Object>();
		//need to add additional checks for values positive, etc.
		if(minSupport > maxSupport) {
			throw new IllegalArgumentException("min support must be smaller than max support");
		}
		selectedOptions.put(WekaAprioriAlgorithm.NUM_RULES, numRules); 
		selectedOptions.put(WekaAprioriAlgorithm.CONFIDENCE_LEVEL, confPer); 
		selectedOptions.put(WekaAprioriAlgorithm.MIN_SUPPORT, minSupport); 
		selectedOptions.put(WekaAprioriAlgorithm.MAX_SUPPORT, maxSupport); 
		selectedOptions.put(WekaAprioriAlgorithm.SKIP_ATTRIBUTES, new ArrayList<String>()); 
		alg.setSelectedOptions(selectedOptions);
		try {
			alg.runAlgorithm(instances);
			alg.generateDecisionRuleTable();
			List<Object[]> ret = alg.getTabularData();
			ret.add(0, alg.getColumnHeaders());
			return ret;
		} catch(IllegalArgumentException e) {
			return new Object[]{};
		}
	}

	@Override
	public void setData(Iterator inputIterator, String[] ids, String script) {
		super.set(inputIterator, ids, script, null);
	}

	@Override
	public Object execute() {
		return reduce();
	}
	
	
	public void setNumRules(int numRules) {
		this.numRules = numRules;
	}
	
	public void setConfPer(double confPer) {
		this.confPer = confPer;
	}
	
	public void setMinSupport(double minSupport) {
		this.minSupport = minSupport;
	}
	
	public void setMaxSupport(double maxSupport) {
		this.maxSupport = maxSupport;
	}
}
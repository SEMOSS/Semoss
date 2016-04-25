package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import prerna.algorithm.learning.weka.WekaAprioriAlgorithm;
import prerna.algorithm.learning.weka.WekaClassification;
import prerna.algorithm.learning.weka.WekaUtilityMethods;
import weka.core.Instances;

public class J48Algorithm extends BaseReducer{

	private int instanceIndex;
	
	@Override
	public void set(Iterator inputIterator, String[] ids, String script) {
		super.set(inputIterator, ids, script, null);
	}
	
	public void setInstanceIndex(int instanceIndex) {
		this.instanceIndex = instanceIndex;
	}

	@Override
	public Object reduce() {
		Instances instances = WekaUtilityMethods.createInstancesFromIterator(inputIterator, ids, instanceIndex);
		WekaClassification alg = new WekaClassification();
		Map<String, Object> selectedOptions = new Hashtable<String, Object>();
		selectedOptions .put(WekaClassification.MODEL_NAME, "J48"); 
		selectedOptions.put(WekaClassification.CLASS_NAME, ids[instanceIndex]); // TODO: again, pass this by name, not by index
		selectedOptions.put(WekaAprioriAlgorithm.SKIP_ATTRIBUTES, new ArrayList<String>()); 
		alg.setSelectedOptions(selectedOptions);
		alg.runAlgorithm(instances);
		alg.processTreeString();
		//TODO: what do i return to the user?
		//currently giving back the tree as a string
		return alg.getTreeAsString();
	}

	@Override
	public void setData(Iterator inputIterator, String[] ids, String script) {
		super.set(inputIterator, ids, script, null);
	}

	@Override
	public Object execute() {
		return reduce();
	}
	
}

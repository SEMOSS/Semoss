package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.learning.weka.WekaAprioriAlgorithm;
import prerna.algorithm.learning.weka.WekaClassification;
import prerna.algorithm.learning.weka.WekaUtilityMethods;
import weka.core.Instances;

public class J48Reactor extends BaseReducerReactor{

	private int instanceIndex;
	
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
		//currently giving back the tree as a string
		return alg.getTreeAsString();
	}

	@Override
	public HashMap<HashMap<Object, Object>, Object> reduceGroupBy(Vector<String> groupBys, Vector<String> processedColumns, String[] columnsArray,
			Iterator it) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public Map<String, Object> getColumnDataMap() {
		// this cannot be added into a frame
		// just return null
		return null;
	}
	


}

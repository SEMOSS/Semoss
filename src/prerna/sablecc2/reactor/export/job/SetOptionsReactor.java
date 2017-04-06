package prerna.sablecc2.reactor.export.job;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.sablecc2.om.GenRowStruct;

public class SetOptionsReactor extends JobBuilderReactor{

	@Override
	protected void buildJob() {
		
		Map<String, List<Object>> newOptions = new HashMap<>();
		//Grab the keys in the nounstore
		Set<String> nounKeys = getNounStore().getNounKeys();
		for(String nounKey : nounKeys) {
			
			newOptions.putIfAbsent(nounKey, new ArrayList<>());			
			GenRowStruct genRow = getNounStore().getNoun(nounKey);
			for(int i = 0; i < genRow.size(); i++) {
				
				Object nextVal = genRow.get(i);
				newOptions.get(nounKey).add(nextVal);
			}
		
		}
		
		job.setOptions(newOptions);
	}
}

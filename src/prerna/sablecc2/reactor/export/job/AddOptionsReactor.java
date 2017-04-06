package prerna.sablecc2.reactor.export.job;

import java.util.Set;

import prerna.sablecc2.om.GenRowStruct;

public class AddOptionsReactor extends JobBuilderReactor {

	@Override
	protected void buildJob() {
		
		//Grab the keys in the nounstore
		Set<String> nounKeys = getNounStore().getNounKeys();
		for(String nounKey : nounKeys) {
			
			GenRowStruct genRow = getNounStore().getNoun(nounKey);
			for(int i = 0; i < genRow.size(); i++) {
				
				//for each key/val we want to add that option to the job
				Object nextVal = genRow.get(i);
				job.addOption(nounKey, nextVal);
			
			}
		
		}
	}

}

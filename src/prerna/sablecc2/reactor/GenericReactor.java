package prerna.sablecc2.reactor;

import java.util.List;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PkslDataTypes;

public class GenericReactor extends AbstractReactor {

	public GenericReactor() {
		setName("Generic");
	}
	
	@Override
	public NounMetadata execute() {
		// THIS IS A SPECIAL CASE
		// we want to merge up into the parent
		// but unlike the mergeup routine
		// we want to replace anything that is a variable
		// with the actual object
		String key = (String)getProp("KEY");

		GenRowStruct allNouns = store.getNoun(NounStore.all);
		GenRowStruct thisStruct;
		if(store.getNoun(key) == null) {
			thisStruct = store.makeNoun(key);
		} else {
			thisStruct = store.getNoun(key);
		}

		int numNouns = allNouns.size();
		for(int nounIdx = 0; nounIdx < numNouns; nounIdx++) {
			Object noun = allNouns.get(nounIdx);
			PkslDataTypes nounType = allNouns.getMeta(nounIdx);
//			if(noun instanceof String) {
			if(nounType == PkslDataTypes.COLUMN) {
				NounMetadata value = this.planner.getVariableValue((String)noun);
				if(value != null) {
					thisStruct.add(value);
				} else {
					thisStruct.add(noun, nounType);
				}
			} else {
				thisStruct.add(noun, nounType);
			}
		}

		// just add this to the parent
		parentReactor.getNounStore().addNoun(key, thisStruct);

		//push up the props
		for(String propKey : this.propStore.keySet()) {
			parentReactor.setProp(propKey, getProp(propKey));
		}
		return null;
	}

	@Override
	public void mergeUp() {
		String key = (String)getProp("KEY");

		GenRowStruct allNouns = store.getNoun(NounStore.all);
		GenRowStruct thisStruct;
		if(store.getNoun(key) == null) {
			thisStruct = store.makeNoun(key);
		} else {
			thisStruct = store.getNoun(key);
		}

		int numNouns = allNouns.size();
		for(int nounIdx = 0; nounIdx < numNouns; nounIdx++) {
			thisStruct.add(allNouns.getNoun(nounIdx));
		}

		// just add this to the parent
		parentReactor.getNounStore().addNoun(key, thisStruct);

		//push up the props
		for(String propKey : this.propStore.keySet()) {
			parentReactor.setProp(propKey, getProp(propKey));
		}
	}
	
	@Override
	public List<NounMetadata> getInputs() {
		// this is used primarily for the planner
		// we do not need to add these steps since 
		// the parent will automatically take these 
		// into consideration
		return null;
	}
	@Override
	public List<NounMetadata> getOutputs() {
		// this is used primarily for the planner
		// we do not need to add these steps since 
		// the parent will automatically take these 
		// into consideration
		return null;
	}
}

package prerna.sablecc2.reactor;


import java.util.Vector;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PkslDataTypes;

public class GenericReactor extends AbstractReactor {

	public GenericReactor() {
		setName("Generic");
	}
	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	public Object Out() {
		updatePlan();
		
		if(this.type != IReactor.TYPE.REDUCE && this.store.isSQL())
		{
			// 2 more scenarios here
			// if parent reactor is not null
			// merge
			// if not execute it
			// if the whole thing is done through SQL, then just add the expression
			if(this.parentReactor != null)
			{
				mergeUp();
				return parentReactor;
			}
			// else assimilated with the other execute
/*			else
			{
				// execute it
			}
*/		
		}
		// the case below should not actually happen.. it should be done through the script chain
		else if(parentReactor == null)
		{
			// execute it
			//return execute();
		}
		else if(parentReactor != null) return parentReactor;
		// else all the merging has already happened
		return null;
	}


	@Override
	protected void mergeUp() {

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
			if(noun instanceof String) {
				NounMetadata value = this.planner.getVariable((String)noun);
				if(value != null) {
					thisStruct.add(value, nounType);
				} else {
					thisStruct.add(noun, nounType);
				}
			} else {
				thisStruct.add(noun, nounType);
			}
		}
//		thisStruct.merge(allNouns);

		// just add this to the parent
		parentReactor.getNounStore().addNoun(key, thisStruct);
		
		//push up the props
		for(String propKey : this.propStore.keySet()) {
			parentReactor.setProp(propKey, getProp(propKey));
		}
	}

	@Override
	protected void updatePlan() {
		
	}
	@Override
	public Vector<NounMetadata> getInputs() {
		// TODO Auto-generated method stub
		return null;
	}
}

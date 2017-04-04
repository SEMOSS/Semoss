package prerna.sablecc2.reactor;

import java.util.List;
import java.util.Vector;

import prerna.sablecc2.om.Filter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class FilterReactor extends AbstractReactor{

	@Override
	public void In()
	{
        curNoun("all");
	}
	
	@Override
	public Object Out()
	{
		// just return the parent
		// in the deinit flow
		// execute will push the filter
		// to the parent
		return parentReactor;
	}
	
	public Object execute() {
		// the translation will set each component 
		// under a different noun
		// we have 3 nouns:
		// LCOL, RCOL, COMPARATOR
		// return a new NounMetadata with the filter
		
		GenRowStruct lcol = store.getNoun("LCOL");
		GenRowStruct comparator = store.getNoun("COMPARATOR");
		GenRowStruct rcol = store.getNoun("RCOL");
		
		Filter thisFilter = new Filter(lcol, comparator.get(0).toString(), rcol);
		NounMetadata filterNoun = new NounMetadata(thisFilter, PkslDataTypes.FILTER);
		return filterNoun;
	}
	
	@Override
	public void mergeUp() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(this.signature, PkslDataTypes.FILTER);
		outputs.add(output);
		return outputs;
	}

	@Override
	public List<NounMetadata> getInputs() {
		List<NounMetadata> inputs = new Vector<NounMetadata>();
		// store the lcol and the rcol
		inputs.add(store.getNoun("LCOL").getNoun(0));
		inputs.add(store.getNoun("RCOL").getNoun(0));
		inputs.add(store.getNoun("COMPARATOR").getNoun(0));
		return inputs;
	}
	
}

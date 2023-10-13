package prerna.reactor;

import java.util.List;

import prerna.sablecc2.om.Filter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class FilterReactor extends AbstractReactor {

	public NounMetadata execute() {
		// the translation will set each component 
		// under a different noun
		// we have 3 nouns:
		// LCOL, RCOL, COMPARATOR
		// return a new NounMetadata with the filter
		
		GenRowStruct lcol = store.getNoun("LCOL");
		GenRowStruct comparator = store.getNoun("COMPARATOR");
		GenRowStruct rcol = store.getNoun("RCOL");
		
		Filter thisFilter = new Filter(lcol, comparator.get(0).toString(), rcol);
		NounMetadata filterNoun = new NounMetadata(thisFilter, PixelDataType.FILTER);
		return filterNoun;
	}
	
	@Override
	public void mergeUp() {
		// we need to push the filter object to the parent
		// creation of the filter object is lazy
		// so it doesn't evaluate yet
		// so we will just call execute to get the object
		NounMetadata filterNoun = (NounMetadata) execute();
		// and we will just push this into the cur row of the parent
		this.parentReactor.getCurRow().add(filterNoun);
	}
	
	@Override
	public List<NounMetadata> getInputs() {
		// we do not want this to be added to the planner
		// as its own OP
		// since a filter is only useful when evaluated 
		// within another expression
		// we will have merge up handle this
		return null;
	}
	
//	@Override
//	public List<NounMetadata> getOutputs() {
//		// we do not want this to be added to the planner
//		// as its own OP
//		// since a filter is only useful when evaluated 
//		// within another expression
//		// we will have merge up handle this
//		return null;
//	}
}

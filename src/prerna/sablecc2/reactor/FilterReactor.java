package prerna.sablecc2.reactor;

import prerna.sablecc2.om.Filter;
import prerna.sablecc2.om.GenRowStruct;

public class FilterReactor extends AbstractReactor{

	// keeps the noun store
	// sets the value for comparator 
	// and for left col
	// accumulates the right column and then sets it on join
	
	@Override
	public void In()
	{
        curNoun("all");
	}
	
	@Override
	public Object Out()
	{
		// get the property called LCOL
		// get the property called comparator
		// get the nounstore to see the columns added
		// set it up in parent
		// I want to get all the columns from here
		// and then add it to the parent as a filter
		
		GenRowStruct lcol = store.getNoun("LCOL");
		GenRowStruct comparator = store.getNoun("COMPARATOR");
		GenRowStruct rcol = store.getNoun("RCOL");
		
		Filter thisFilter = new Filter(lcol, comparator.get(0).toString(), rcol);
		// just add this to the parent
		parentReactor.getCurRow().addFilter(thisFilter);
		return parentReactor;
	}
	
	@Override
	public void mergeUp() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updatePlan() {
		// TODO Auto-generated method stub
		
	}
	
}

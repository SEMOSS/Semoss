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
		
//		Filter thisFilter = new Filter(colName, comparator, allNouns);
		
		GenRowStruct thisStruct = store.makeNoun("f");
//		thisStruct.addFilter(thisFilter);

		// just add this to the parent
		parentReactor.getNounStore().addNoun("f", thisStruct);
		//mergeUp();
		return parentReactor;
	}
	
	@Override
	public Object execute()
	{
		GenRowStruct struct = store.getNoun("f");
		Filter filter = (Filter) struct.get(0);
		filter.getSelector();
		
		return true;
	}

	@Override
	protected void mergeUp() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void updatePlan() {
		// TODO Auto-generated method stub
		
	}
	
}

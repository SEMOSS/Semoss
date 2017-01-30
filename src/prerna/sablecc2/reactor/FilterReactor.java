package prerna.sablecc2.reactor;

import java.util.Vector;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.NounStore;

public class FilterReactor extends SampleReactor {

	// keeps the noun store
	// sets the value for comparator 
	// and for left col
	// accumulates the right column and then sets it on join
	
	
	public void In()
	{
        curNoun("all");
	}
	
	public Object Out()
	{
		// get the property called LCOL
		// get the property called comparator
		// get the nounstore to see the columns added
		// set it up in parent
		// I want to get all the columns from here
		// and then add it to the parent as a filter
		
		String colName = (String)getProp("LCOL");
		String comparator = (String)getProp("COMPARATOR");
		
		GenRowStruct allNouns = store.getNoun(NounStore.all);
		Join thisFilter = new Join(colName, comparator, allNouns);
		
		GenRowStruct thisStruct = store.makeNoun("f");
		thisStruct.addFilter(thisFilter);

		// just add this to the parent
		parentReactor.getNounStore().addNoun("f", thisStruct);
		//mergeUp();
		return parentReactor;
	}
	
}

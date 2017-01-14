package prerna.sablecc2.reactor;

import java.util.Vector;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.NounStore;

public interface IReactor {
	
	public enum STATUS {STARTED, INPROGRESS, COMPLETED, FAILED};
	
	// is this a map or a reduce
	public enum TYPE{MAP, FLATMAP, REDUCE};
	
	// in call
	public void In();
	
	// out call
	// for now I would say the return is the object
	public Object Out();
	
	// sets the name of the operation and the signature
	// full operation includes the nouns
	public void setPKSL(String operation, String fullOperation);
	
	public String[] getPKSL();
	
	// add previous reactor as its dependency -- is this the same as the parent reactor
	// assimilated into the composition parent reactor
	//public void setPrevReactor(IReactor prevReactor); 
	
	// set the parent reactor for a composition, we start here
	// for a pipeline this will become the child
	public void setParentReactor(IReactor parentReactor);
	
	// sets the child reactor
	public void setChildReactor(IReactor childReactor);
	
	// // gets the parent reactor
	public IReactor getParentReactor();
	
	// sets the current noun it is working through
	public void curNoun(String noun);
	
	// returns the current row
	public GenRowStruct getCurRow();
	
	// completes the noun
	public void closeNoun(String noun);

	// gets the nounstore
	public NounStore getNounStore();
	
	// gets all the inputs i.e. the noun names
	// the second string is the meaning
	// gives JSON with the following values
	// name of the noun
	// Meaning of the noun
	// Optional / Mandatory
	// Single Value or multiple values
	// are projections the output ?
	public Vector<NounMetadata> getInputs();
		
	// gets the errored PKQL
	// alternately can set the error message here
	public void getErrorMessage();

	// gets the status of what happened to this reactor
	public STATUS getStatus();
	
	public TYPE getType();

	// get name
	public String getName();
	
	// sets the name
	public void setName(String name);
	
	// sets the pksl planner
	public void setPKSLPlanner(PKSLPlanner planner);
	
	// sets the string for alias i.e. as
	public void setAs(String [] asName);
	
	// adds a property
	public void setProp(String key, Object value);
	
	// gets the property
	public Object getProp(String key);;
	
	/**
	 * elements <- Generic Row <- Generic Noun <- Reactor
	 * reactors work on the generic noun
	 * doing something to it
	 * There is a good possibility where the nouns are getting composed through the reactor itself
	 * 
	 * 
	 * sum(p=[sum(c*d), a, b])
	 * this clearly is a reduce within a reduce but can be performed by a SQL possibly
	 * 
	 * 
	 * The first sum comes through here I try to see if the prevReactor has been completed, if not try to see if there is 
	 * a way for me combine the operation of this with prev reactor
	 * there are 2 things that decide that
	 * a. Does the previous reactor have any dependency
	 * b. Is this reactor also having dependency
	 * If both of these are false I can combine it else, I cannot
	 * 
	 * If this is the first operation, creates a nounstore
	 * Goes into the second operation
	 * second operation also gets a nounstore created 
	 * When the second operation finishes i.e. in the out of the second operation
	 * It is evaluated to see if this is a map operation / can it be completely done in SQL if so it proceeds to be assimilated
	 * 
	 */
	
	
	/*
	 * a. What should it listen on - This might not even be required anymore I feel like. This is needed, as a way to describe to say I need a, b along with here the default values I would use if you do not provide them. Possibly return a method signature as a template which you can fill. May be this should even be using default widget so you can select as opposed to type would be a cool interface
b. The current query struct needs to be modified to accomodate the new way. We need a couple of things here. I need to be accomodate for action(a=[123], b=[abd], a=[234]) - see the repeat in a. This needs to be maintained twice, once with cardinality and once without it so you can get the total piece instead of getting it one by one and stitching it

c. I need to know when I can close out a function vs. jam it on an existing for querying or even when I cannot execute it anymore - My suggestion is do this only for the viz panel specific constructs and not to try to make it generic

d. needs access to the previous operation - does this mean the query struct, and every thing else associated with it ? Should I try to assimilate it as well ? Should we execute this immediately or should we wait until a proper stage is reached before executing it so I can say

select(s=[ab,cd]) | groupby(s=[xyz]) | filter | persist(s=[My new column]) | project(s=[abcd])<-- I kind of like this format much better than the object oriented away

e. Needs access to the next operation ?

f. Input dependency i.e. what are the things it depends on - needs to be separated out by variable vs. frame columns etc. 

g. What type of operation is it - Is it a map operation or is it a reduce operation. I almost feel anything that is being piped should be all part of one operation. Which means, I also need to understand if there is a next operation being executed

h. Success of the operation 

i. Where did fail, possibly the same string with where it errored like H2  (*) - GetErroredOutput

j. Sets the query struct so as to add it to the query struct.. until such point a reduce has been reached. < -- maybe we just spit out code instead of trying to execute compile and then execute

k. Gets the query struct ? or possibly try to get it from overall

// if I am able to go through the gen_row to see if it can be expressed as query - I should be able to go through this
// Obviously the only kink in the puzzle is if I have a piece depending on one of the pieces within this for additional aclculation then it is possible to do that before proceeding


// I need to know all the columns that are available in the current frame as well as other frames, just to make sure there is no namespace collision
// 

	 * 
	 * 
	 * 
	 * 
	 */
	
	
}

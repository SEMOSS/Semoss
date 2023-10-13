package prerna.reactor;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import prerna.engine.api.IHeadersDataRow;
import prerna.om.Insight;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public interface IReactor {
	
	enum STATUS {STARTED, INPROGRESS, COMPLETED, FAILED};
	
	// is this a map or a reduce
	enum TYPE{MAP, FLATMAP, REDUCE};
	
	
	String SIBLING = "SIBLING";
	String PARENT = "PARENT";
	String CHILD = "CHILD";
	String LAST_KEY = "LAST_KEY";

	String MERGE_INTO_QS_FORMAT = "qsMergeFormat";
	String MERGE_INTO_QS_FORMAT_SCALAR = "scalar";
	String MERGE_INTO_QS_DATATYPE = "qsMergeDataType";
	
	/**********************************
	 * To Be implemented by each reactor
	 * ******************************/
	
	// in call
	void In();
	
	// out call
	// for now I would say the return is the object
	Object Out();
	
	List<NounMetadata> getOutputs();
	
	void mergeUp();
	
	void updatePlan();
	
	Map<String, List<Map>> getStoreMap();
	
	// execute method - GREEDY translation
	NounMetadata execute();
	//
	/*******************************************/
	
	
	
	/*******************************************
	 * Implemented by the Absract
	 *******************************************/
	// sets the name of the operation and the signature
	// full operation includes the nouns
	void setPixel(String operation, String fullOperation);
	
	String[] getPixel();
	
	// add previous reactor as its dependency -- is this the same as the parent reactor
	// assimilated into the composition parent reactor
	//public void setPrevReactor(IReactor prevReactor); 
	
	// set the parent reactor for a composition, we start here
	// for a pipeline this will become the child
	void setParentReactor(IReactor parentReactor);

	// // gets the parent reactor
	IReactor getParentReactor();
	
	// sets the child reactor
	void setChildReactor(IReactor childReactor);
	
	List<IReactor> getChildReactors();
	
	// sets the current noun it is working through
	void curNoun(String noun);
	
	// returns the current row
	GenRowStruct getCurRow();
	
	// completes the noun
	void closeNoun(String noun);

	// gets the nounstore
	NounStore getNounStore();
	
	void setNounStore(NounStore store);

	// gets all the inputs i.e. the noun names
	// the second string is the meaning
	// gives JSON with the following values
	// name of the noun
	// Meaning of the noun
	// Optional / Mandatory
	// Single Value or multiple values
	// are projections the output ?
	List<NounMetadata> getInputs();
	
	// gets the status of what happened to this reactor
	STATUS getStatus();
	
	TYPE getType();

	// get name
	String getName();
	
	// sets the name
	void setName(String name);
	
	// sets the pksl planner
	void setPixelPlanner(PixelPlanner planner);
	
	PixelPlanner getPixelPlanner();
	
	// sets the string for alias i.e. as
	void setAs(String [] asName);
	
	// adds a property
	@Deprecated
	void setProp(String key, Object value);
	
	// gets the property
	@Deprecated
	Object getProp(String key);
	
	//returns whether reactor has Prop
	@Deprecated
	boolean hasProp(String key);
	
	// map call implement if your type is map
	IHeadersDataRow map(IHeadersDataRow row);
	
	// reduce call implement if the type is reduce
	Object reduce(Iterator iterator);
	
	// gets the signature
	String getSignature();

	String getOriginalSignature();

	void modifySignature(String stringToFind, String stringReplacement);

	void modifySignatureFromLambdas();

	void setInsight(Insight insight);
	
	/**
	 * 
	 * @param name
	 * @return
	 */
	Logger getLogger(String name);

	/**
	 * 
	 * @param name
	 * @param partial
	 * @return
	 */
	Logger getLogger(String name, boolean partial);

	
	String getHelp();
	
	String getReactorDescription();
	
	/**
	 * Determine if this reactor should be merged up to be put into a QS as is vs. executed directly
	 * @return
	 */
	boolean canMergeIntoQs();
	
	Map<String, Object> mergeIntoQsMetadata();
	
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
	
	/**
	 * THIS IS ONLY USED FOR TESTING PURPOSES!!!
	 */
	public static void printReactorStackTrace() {
//		System.err.println("PRINTING STACK TRACE!!!!");
//		System.err.println("PRINTING STACK TRACE!!!!");
//		System.err.println("PRINTING STACK TRACE!!!!");
//		System.err.println("PRINTING STACK TRACE!!!!");
//		StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
//		int index = 0;
//		for (StackTraceElement element : stackTrace) {
//			String className = element.getClassName();
//			try {
//				Class c = Class.forName(className);
//				if(c.newInstance() instanceof IReactor) {
//					System.err.println();
//					System.err.println("Index: " + index++ );
//					System.err.println("ClassName: " + element.getClassName());
//					System.err.println("MethodName: " + element.getMethodName());
//					System.err.println("FileName: " + element.getFileName());
//					System.err.println("LineNumber: " + element.getLineNumber());
//				}
//			} catch(Exception e) {
//
//			}
//		}
	}
	
}

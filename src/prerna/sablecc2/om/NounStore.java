package prerna.sablecc2.om;

import java.util.Hashtable;


public class NounStore {

	// each noun is typically a gen row struct
	// I need to keep track of a couple of things
	// a. Need to keep track of how many times a particular noun came through
	// b. Keep the general array for what was the array for a given time
	// c. Keep another general array which is a sum total of everything so far
	
	String operationName = null;
	
	public Hashtable <String, Integer> nounCount = new Hashtable<String, Integer>();
	// S_1 would be the first time we saw S, S_2 would be the second time, S_3 would be.. well you know it
	public Hashtable <String, GenRowStruct> nounByNumber = new Hashtable<String, GenRowStruct>();
	public Hashtable <String, GenRowStruct> nounRow = new Hashtable<String, GenRowStruct>();
	
	public final static String selector = "s";
	public final static String projector = "p";
	public final static String filter = "f";
	public final static String all = "all";
	
	
	public NounStore(String operationName)
	{
		this.operationName = operationName;
	}
	
	// adds the noun
	public void addNoun(String nounName, GenRowStruct struct)
	{
		GenRowStruct curStruct = struct;
		// see if the noun exists first
		if(nounRow.containsKey(nounName))
		{
			curStruct = nounRow.get(nounName);
			curStruct.merge(struct);
		}	
		nounRow.put(nounName, curStruct);
		
		// see if the count exists
		int count = 0;
		if(nounCount.containsKey(nounName))
			count = nounCount.get(nounName);
		count++;
		nounCount.put(nounName, count);
		
		// add this to the noun by number
		nounByNumber.put(nounName+"_"+count, struct);
	}
	
	// get the total number of nouns
	public int getCountForNoun(String nounName)
	{
		return nounCount.get(nounName);
	}
	
	// get the number of nouns
	public int getNounNum()
	{
		return nounRow.size();
	}
	
	// gets all the nouns for a particular noun name
	public GenRowStruct getNoun(String nounName)
	{
		return nounRow.get(nounName);
	}
	
	// gets the noun at a particular number
	public GenRowStruct getNoun(String nounName, int number)
	{
		return nounByNumber.get(nounName + "_" + number);
	}
	
/*	// make a child nounstore
	// pattern is the node that is coming in
	public NounStore makeChildStore(String operation)
	{
		NounStore retStore = this;
		if(!this.operationName.equals(operation))
		{
			retStore = new NounStore(operation);
			childStore.put(operation, retStore);
		}
		// else there is a very good possibility they are just doing this for beautification
		return retStore;
	}
*/	
	// find if this is possible through query in this frame
	public boolean isSQL()
	{
		// this should call each of the is SQL in the gen row struct
		// and give back the result
		return true;
	}
	
	public GenRowStruct makeNoun(String noun)
	{
		GenRowStruct newRow = new GenRowStruct();
		addNoun(noun, newRow);
		return newRow;
	}
}

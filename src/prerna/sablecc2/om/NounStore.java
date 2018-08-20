package prerna.sablecc2.om;

import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;


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
	public LinkedHashMap <String, GenRowStruct> nounRow = new LinkedHashMap<String, GenRowStruct>();
	
	public final static String selector = "s";
	public final static String projector = "p";
	public final static String filter = "f";
	public final static String all = "all";
	public final static String joins = "j";
	
	
	public NounStore(String operationName)
	{
		this.operationName = operationName;
	}
	
	// adds the noun
	public void addNoun(String nounName, GenRowStruct struct)
	{
		// initialize to the current struct
		GenRowStruct curStruct = struct;
		
		// see if the noun exists first
		// make the curstruct to be the actual struct
		if(nounRow.containsKey(nounName))
		{
			curStruct = nounRow.get(nounName);
			// I am creating a new one here
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
	
	public int size()
	{
		return nounRow.size();
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
	
	public Set<String> getNounKeys() {
		return nounRow.keySet();
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
		
		// for now.. I will not keep the caridnality
		if(nounRow.containsKey(noun))
			newRow = nounRow.get(noun);
		else
		{
			addNoun(noun, newRow);
		}
		return newRow;
	}
	
	public String getDataString() {
		String s = "";
		if(this.nounRow == null) return s;
		
		for(String key : this.nounRow.keySet()) {
			s += "KEY: "+key;
			s += "\n";
			s += "GENROWSTRUCT: "+nounRow.get(key);
			s += "\n";
		}
		
		return s;
	}
	
	// need someway to get the actual store / data
	public Hashtable<String, Object> getDataHash()
	{
		Hashtable<String, Object> retHash = new Hashtable<String, Object>();
		
		// see if there are keys
		// if there 
		Set<String> keys = nounRow.keySet();
		for(String thisKey : keys) {
			List <Object> values = nounRow.get(thisKey).getAllValues();
			
			Object finalValue = values;
			if(values.size() == 1)
				finalValue = values.get(0);

			retHash.put(thisKey, finalValue);
		}
		
		return retHash;
	}
}

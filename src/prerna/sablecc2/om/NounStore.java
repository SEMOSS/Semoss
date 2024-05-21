package prerna.sablecc2.om;

import java.io.IOException;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.gson.GsonUtility;

public class NounStore implements Serializable{

	private static final Logger classLogger = LogManager.getLogger(NounStore.class);

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

	public GenRowStruct removeNoun(String nounName) {
		return nounRow.remove(nounName);
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

	/**
	 * 
	 * @param inputMap
	 * @return
	 */
	public static NounStore generateNounFromMap(Map<String, List<Map<String, Object>>> inputMap) {
		NounStore store = new NounStore("all");
		for(String key : inputMap.keySet()) {
			GenRowStruct grs = store.makeNoun(key);

			List<Map<String, Object>> inputMapVals = inputMap.get(key);
			for(Map<String, Object> nounInput : inputMapVals) {
				NounMetadata noun = new NounMetadata(nounInput.get("value"), PixelDataType.valueOf(nounInput.get("type")+""));
				grs.add(noun);
			}
		}

		return store;
	}

	/**
	 * Flushes a Json object into basic Java inputs and assigns into the noun store
	 * @param object
	 * @return
	 * @throws IOException 
	 */
	public static NounStore flushJsonToNounStore(JsonObject object) {
		try {
			NounStore store = new NounStore("all");
			for(String key : object.keySet()) {
				// every key in the top level will be a gen row struct in the pixel expression
				GenRowStruct grs = new GenRowStruct();

				JsonElement value = object.get(key);
				if(value.isJsonArray()) {
					JsonArray array = value.getAsJsonArray();
					for(int i = 0; i < array.size(); i++) {
						NounMetadata noun = flushJsonToNounStore(array.get(i));
						grs.add(noun);
					}
				} else {
					NounMetadata noun = flushJsonToNounStore(value);
					grs.add(noun);
				}

				// store it in the noun store
				store.addNoun(key, grs);
			}
			return store; 
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred parsing the json input. Detailed message = " + e.getMessage());
		}
	}
	
	/**
	 * 
	 * @param element
	 * @param grs
	 * @return
	 * @throws IOException 
	 */
	private static NounMetadata flushJsonToNounStore(JsonElement element) throws IOException {
		if(element.isJsonNull()) {
			return new NounMetadata(null, PixelDataType.NULL_VALUE);
		} if(element.isJsonObject()) {
			Map<String, Object> map = GsonUtility.getDefaultGson().fromJson(element, Map.class);
			return new NounMetadata(map, PixelDataType.MAP);
		} else if(element.isJsonPrimitive()){
			JsonPrimitive primitive = element.getAsJsonPrimitive();
			if(primitive.isNumber()) {
				Number num = primitive.getAsNumber();
				if(num.intValue() == num.doubleValue()) {
					return new NounMetadata(num.intValue(), PixelDataType.CONST_INT);
				} else {
					return new NounMetadata(num.intValue(), PixelDataType.CONST_INT);
				}
			} else if(primitive.isBoolean()) {
				return new NounMetadata(primitive.getAsBoolean(), PixelDataType.BOOLEAN);
			} else {
				return new NounMetadata(primitive.getAsString(), PixelDataType.CONST_STRING);					
			}
		}
		
		throw new IllegalArgumentException("Unable to parse element = " + element.toString());
	}
}

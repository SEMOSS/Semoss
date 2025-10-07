package prerna.sablecc2.om;

import java.io.IOException;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.gson.GsonUtility;

/**
 * The NounStore class is responsible for storing and managing "nouns" (data
 * elements) within the system, typically in the context of a parsing or
 * expression evaluation engine. It keeps track of noun occurrences, their
 * values, and provides methods for adding, retrieving, and manipulating these
 * nouns.
 */
public class NounStore implements Serializable {

	private static final Logger classLogger = LogManager.getLogger(NounStore.class);

	// each noun is typically a gen row struct
	// I need to keep track of a couple of things
	// a. Need to keep track of how many times a particular noun came through
	// b. Keep the general array for what was the array for a given time
	// c. Keep another general array which is a sum total of everything so far

	/**
	 * The name of the operation associated with this NounStore.
	 */
	String operationName = null;

	/**
	 * A map to store the count of each noun that has been added. The key is the
	 * noun name (String), and the value is the count (Integer).
	 */
	public Map<String, Integer> nounCount = new ConcurrentHashMap<>();
	/**
	 * A map to store each instance of a noun, indexed by its name and occurrence
	 * number. For example, "S_1" for the first occurrence of noun "S", "S_2" for
	 * the second, and so on.
	 */
	public Map<String, GenRowStruct> nounByNumber = new ConcurrentHashMap<>();
	/**
	 * A map to store the most recent or merged GenRowStruct for each noun name. The
	 * key is the noun name (String), and the value is the GenRowStruct.
	 */
	public Map<String, GenRowStruct> nounRow = new LinkedHashMap<>();

	// Constants for common noun types or operations
	public final static String selector = "s";
	public final static String projector = "p";
	public final static String filter = "f";
	public final static String all = "all";
	public final static String joins = "j";

	/**
	 * Constructs a new NounStore with a specified operation name.
	 * 
	 * @param operationName The name of the operation associated with this
	 *                      NounStore.
	 */
	public NounStore(String operationName) {
		this.operationName = operationName;
	}

	/**
	 * Adds a noun to the store. If the noun already exists, its GenRowStruct is
	 * merged with the new one. The noun's count is incremented, and the new
	 * GenRowStruct is stored with an occurrence number.
	 * 
	 * @param nounName The name of the noun to add.
	 * @param struct   The GenRowStruct associated with the noun.
	 */
	public void addNoun(String nounName, GenRowStruct struct) {
		GenRowStruct curStruct = null;
		if (nounRow.containsKey(nounName)) {
			curStruct = nounRow.get(nounName);
			// I am creating a new one here
			curStruct.merge(struct);
		} else {
			curStruct = struct;
		}
		nounRow.put(nounName, curStruct);

		// see if the count exists
		int count = 0;
		if (nounCount.containsKey(nounName)) {
			count = nounCount.get(nounName);
		}
		count++;
		nounCount.put(nounName, count);
		nounByNumber.put(nounName + "_" + count, struct);
	}

	/**
	 * Returns the number of unique nouns stored in this NounStore.
	 * 
	 * @return The number of unique nouns.
	 */
	public int size() {
		return nounRow.size();
	}

	/**
	 * Returns the total number of times a specific noun has been added to the
	 * store.
	 * 
	 * @param nounName The name of the noun.
	 * @return The count of the specified noun.
	 */
	public int getCountForNoun(String nounName) {
		return nounCount.get(nounName);
	}

	/**
	 * Returns the number of unique nouns stored in this NounStore. This method is a
	 * duplicate of {@link #size()}.
	 * 
	 * @return The number of unique nouns.
	 */
	public int getNounNum() {
		return nounRow.size();
	}

	/**
	 * Returns a set of all unique noun names (keys) currently in the store.
	 * 
	 * @return A Set of String representing the noun names.
	 */
	public Set<String> getNounKeys() {
		return nounRow.keySet();
	}

	/**
	 * Retrieves the GenRowStruct for a given noun name. This typically returns the
	 * merged or most recent GenRowStruct associated with the noun.
	 * 
	 * @param nounName The name of the noun.
	 * @return The GenRowStruct associated with the noun name.
	 */
	public GenRowStruct getGenRowStruct(String nounName) {
		return nounRow.get(nounName);
	}

	/**
	 * Retrieves a specific occurrence of a noun's GenRowStruct based on its name
	 * and number. For example, to get the first occurrence of noun "S", use
	 * `getGenRowStruct("S", 1)`.
	 * 
	 * @param nounName The name of the noun.
	 * @param number   The occurrence number of the noun (e.g., 1 for the first, 2
	 *                 for the second).
	 * @return The GenRowStruct for the specified noun occurrence.
	 */
	public GenRowStruct getGenRowStruct(String nounName, int number) {
		return nounByNumber.get(nounName + "_" + number);
	}

	/**
	 * Removes a noun and its associated GenRowStruct from the store.
	 * 
	 * @param nounName The name of the noun to remove.
	 * @return The GenRowStruct that was removed, or null if the noun was not found.
	 */
	public GenRowStruct removeNoun(String nounName) {
		return nounRow.remove(nounName);
	}

	/*
	 * // make a child nounstore // pattern is the node that is coming in public
	 * NounStore makeChildStore(String operation) { NounStore retStore = this;
	 * if(!this.operationName.equals(operation)) { retStore = new
	 * NounStore(operation); childStore.put(operation, retStore); } // else there is
	 * a very good possibility they are just doing this for beautification return
	 * retStore; }
	 */
	/**
	 * Checks if the nouns within this store are suitable for SQL operations. This
	 * method currently always returns true, but is intended to call `isSQL()` on
	 * each contained GenRowStruct.
	 * 
	 * @return Always returns true.
	 */
	public boolean isSQL() {
		// this should call each of the is SQL in the gen row struct
		// and give back the result
		return true;
	}

	/**
	 * Creates or retrieves a GenRowStruct for a given noun. If the noun already
	 * exists, its existing GenRowStruct is returned. Otherwise, a new GenRowStruct
	 * is created, added to the store, and returned.
	 * 
	 * @param noun The name of the noun.
	 * @return The GenRowStruct for the specified noun.
	 */
	public GenRowStruct makeGenRowStruct(String noun) {
		GenRowStruct newRow = new GenRowStruct();

		// for now.. I will not keep the caridnality
		if (nounRow.containsKey(noun)) {
			newRow = nounRow.get(noun);
		} else {
			addNoun(noun, newRow);
		}
		return newRow;
	}

	/**
	 * Retrieves the data stored in the NounStore as a Hashtable. Each noun name
	 * maps to either a single value or a list of values if multiple values are
	 * associated with the noun.
	 * 
	 * @return A Hashtable where keys are noun names and values are their associated
	 *         data.
	 */
	public Hashtable<String, Object> getDataHash() {
		Hashtable<String, Object> retHash = new Hashtable<String, Object>();

		// see if there are keys
		// if there
		Set<String> keys = nounRow.keySet();
		for (String thisKey : keys) {
			List<Object> values = nounRow.get(thisKey).getAllValues();

			Object finalValue = values;
			if (values.size() == 1) {
				finalValue = values.get(0);
			}

			retHash.put(thisKey, finalValue);
		}

		return retHash;
	}

	/**
	 * Generates a NounStore from a map of input data. The input map's keys
	 * represent noun names, and their values are lists of maps, where each inner
	 * map contains "value" and "type" for creating NounMetadata.
	 * 
	 * @param inputMap A map where keys are noun names and values are lists of maps
	 *                 containing noun data (value and type).
	 * @return A new NounStore populated with the data from the input map.
	 */
	public static NounStore generateNounFromMap(Map<String, List<Map<String, Object>>> inputMap) {
		NounStore store = new NounStore("all");
		for (String key : inputMap.keySet()) {
			GenRowStruct grs = store.makeGenRowStruct(key);

			List<Map<String, Object>> inputMapVals = inputMap.get(key);
			for (Map<String, Object> nounInput : inputMapVals) {
				NounMetadata noun = new NounMetadata(nounInput.get("value"),
						PixelDataType.valueOf(nounInput.get("type") + ""));
				grs.add(noun);
			}
		}

		return store;
	}

	/**
	 * Flushes a JsonObject into basic Java inputs and assigns them into a new
	 * NounStore. Each top-level key in the JsonObject becomes a noun name, and its
	 * corresponding JsonElement is converted into NounMetadata and added to the
	 * noun's GenRowStruct.
	 * 
	 * @param object The JsonObject to flush into the NounStore.
	 * @return A new NounStore populated with data from the JsonObject.
	 * @throws IllegalArgumentException If an error occurs during JSON parsing.
	 */
	public static NounStore flushJsonToNounStore(JsonObject object) {
		try {
			NounStore store = new NounStore("all");
			for (String key : object.keySet()) {
				// every key in the top level will be a gen row struct in the pixel expression
				GenRowStruct grs = new GenRowStruct();

				JsonElement value = object.get(key);
				if (value.isJsonArray()) {
					JsonArray array = value.getAsJsonArray();
					for (int i = 0; i < array.size(); i++) {
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
			throw new IllegalArgumentException(
					"An error occurred parsing the json input. Detailed message = " + e.getMessage());
		}
	}

	/**
	 * Converts a JsonElement into a NounMetadata object. This method handles null,
	 * JSON objects (converting them to maps), and JSON primitives (numbers,
	 * booleans, and strings) by determining their appropriate PixelDataType.
	 * 
	 * @param element The JsonElement to convert.
	 * @return A NounMetadata object representing the JsonElement.
	 * @throws IOException              If an I/O error occurs during conversion
	 *                                  (though not directly used in the current
	 *                                  implementation, it's part of the original
	 *                                  signature).
	 * @throws IllegalArgumentException If the JsonElement type cannot be parsed.
	 */
	private static NounMetadata flushJsonToNounStore(JsonElement element) throws IOException {
		if (element.isJsonNull()) {
			return new NounMetadata(null, PixelDataType.NULL_VALUE);
		}
		if (element.isJsonObject()) {
			Map<String, Object> map = GsonUtility.getDefaultGson().fromJson(element, Map.class);
			return new NounMetadata(map, PixelDataType.MAP);
		} else if (element.isJsonPrimitive()) {
			JsonPrimitive primitive = element.getAsJsonPrimitive();
			if (primitive.isNumber()) {
				Number num = primitive.getAsNumber();
				if (num.intValue() == num.doubleValue()) {
					return new NounMetadata(num.intValue(), PixelDataType.CONST_INT);
				} else {
					return new NounMetadata(num.intValue(), PixelDataType.CONST_INT);
				}
			} else if (primitive.isBoolean()) {
				return new NounMetadata(primitive.getAsBoolean(), PixelDataType.BOOLEAN);
			} else {
				return new NounMetadata(primitive.getAsString(), PixelDataType.CONST_STRING);
			}
		}

		throw new IllegalArgumentException("Unable to parse element = " + element.toString());
	}
}

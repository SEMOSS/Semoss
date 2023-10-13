package prerna.reactor.export;

import java.util.Hashtable;
import java.util.Map;

import prerna.engine.api.IHeadersDataRow;

public class KeyValueFormatter extends AbstractFormatter {

	private Map<Object, Object> dataStore = new Hashtable<Object, Object>();
	
	@Override
	public void addData(IHeadersDataRow nextData) {
		// TODO: how do i figure out what the key is from the values???
		// right now, assuming first return is key
		// everything else is value
		Object[] values = nextData.getValues();
		String[] headers = nextData.getHeaders();
		int numVals = values.length;
		if(numVals > 1) {
			// we have more than 2 values
			// put everything else as an array for the value
			Object[] keyVals = new Object[numVals-1];
			for(int keyIndex = 0; keyIndex < numVals-1; keyIndex++) {
				keyVals[keyIndex] = values[keyIndex+1];
			}
			dataStore.put(headers[0], keyVals);
		} else {
			// if it is a one to one key-val
			// just put it as the object
			// instead of making an array of length 1
			// unsure if this is good idea or bad...
			// will see once we try to connect w/ FE
			dataStore.put(headers[0], values[0]);
		}
	}

	@Override
	public Object getFormattedData() {
		return dataStore;
	}

	@Override
	public void clear() {
		// since data store is passed by reference
		// we cannot simply clear it
		// but we need to generate a new object
		dataStore = new Hashtable<Object, Object>();
	}

	@Override
	public String getFormatType() {
		return "KEYVALUE";
	}
}

package prerna.sablecc;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc.PKQLEnum.PKQLReactor;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.util.Constants;

public class InputReactor extends AbstractReactor {
	
	// this is responsible for doing the replacing of variables in the script with their actual objects
	// the map of vars to objects will either sit on runner or in insight
	// for now we will put it on the insight since runner isn't singleton wrt insight

	// need to get the values to sync from the api
	Hashtable <String, String[]> values2SyncHash = new Hashtable <String, String[]>();

	public InputReactor() {
		String [] thisReacts = {PKQLEnum.VAR_TERM, PKQLEnum.EXPR_TERM, PKQLEnum.API, Constants.ENGINE};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLReactor.INPUT.toString();
		
		// when the data is coming from an API (i.e. an engine or a file)
		String [] dataFromApi = {PKQLEnum.COL_CSV, Constants.ENGINE};
		values2SyncHash.put(PKQLEnum.API, dataFromApi);
	}

	@Override
	public Iterator process() {

		// get the list of options
		// it is either an api (wrapper)
		// or csv col (list)
		// etc.
		// just iterate through and add to options
		// then set back into mystore
		List<Object> options = new Vector<Object>();
		if(myStore.get(PKQLEnum.API) != null)
		{			
			Iterator<IHeadersDataRow> it = (Iterator) myStore.get(PKQLEnum.API);
			while(it.hasNext()){
				IHeadersDataRow ss = it.next();
				options.add(ss.getValues()[0]);
			}
		}
		else if(myStore.get(PKQLEnum.ROW_CSV) != null)
		{			
			options = (List) myStore.get(PKQLEnum.ROW_CSV);
		}
		myStore.put("options", options);
	
		return null;
	}
	
	/**
	 * Gets the values to load into the reactor
	 * This is used to synchronize between the various reactors that can feed into this reactor
	 * @param input			The type of child reactor
	 */
	public String[] getValues2Sync(String input) {
		return values2SyncHash.get(input);
	}

	@Override
	public IPkqlMetadata getPkqlMetadata() {
		// TODO Auto-generated method stub
		return null;
	}
}

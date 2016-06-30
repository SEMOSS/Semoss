package prerna.engine.impl.rdf;

import java.util.Hashtable;
import java.util.Iterator;

import prerna.ds.QueryStruct;
import prerna.ds.util.FileIterator;
import prerna.engine.api.IApi;
import prerna.engine.api.IHeadersDataRow;

public class CSVApi implements IApi {

	// UPDATE - Drag and Drop CSV now creates a full database instead of undergoing the logic 
	// of this reactor which would take the data and add it to an in-memory frame
	
	// values map to store the parameters required to use the API builder
	Hashtable <String, Object> values = new Hashtable<String, Object>();
	// to use the CSV API you need the query struct and the file location
	// if the query struct is empty, then it works are a prim_key file upload
	String [] params = {"QUERY_STRUCT", "FILE"};

	@Override
	public Iterator<IHeadersDataRow> process() {
		// get the query struct
		QueryStruct qs = (QueryStruct) values.get(params[0]); 
		// get the file location
		String fileName = values.get(params[1]) + "";

		// OLD CODE - tried to have it such that the FE would never know the location of the file
		// by using a key in the FileStore.. however, the FileStore is emptied every time the server starts
		// so when the PKQL is saved with the unqiue id in the FileStore, it would no longer be valid
		// look to see if this file is actually a unique id for a file that is on the server
//		if(FileStore.getInstance().containsKey(fileName)) {
//			fileName = FileStore.getInstance().get(fileName);
//		}

		// pass in delimiter as a comma and return the FileIterator which uses the QS (if not empty) to 
		// to determine what selectors to send
		return new FileIterator(fileName, ',', qs, null); 
	}

	@Override
	public void set(String key, Object value) {
		values.put(key, value);
	}

	@Override
	public String[] getParams() {
		return params;
	}
}

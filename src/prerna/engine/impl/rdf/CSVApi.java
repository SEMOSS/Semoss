package prerna.engine.impl.rdf;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import prerna.ds.util.FileIterator;
import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner;

public class CSVApi extends AbstractApiReactor {

	// TODO: should modify this to be a bit more of a unique name so it does not
	// relate to a column name
	private final String FILE_KEY = "file";
	
	@Override
	public Iterator<IHeadersDataRow> process() {
		super.process();

		// the mapOptions stores all the information being passed from the user
		// this will contain the fileName and the data types from each column
		String fileName = "";
		Map<String, String> dataTypeMap = new Hashtable<String, String>();
		for(Map<String, String> keyVal : this.mapOptions) {
			// note, each keyVal map should only contain a single key-value pair
			if(keyVal.containsKey(FILE_KEY)) {
				fileName = keyVal.get(FILE_KEY);
			} else {
				dataTypeMap.putAll(keyVal);
			}
		}

		// pass in delimiter as a comma and return the FileIterator which uses the QS (if not empty) to 
		// to determine what selectors to send
		
		// the qs is passed from AbstractApiReactor
		this.put((String) getValue(PKQLEnum.API), new FileIterator(fileName, ',', this.qs, dataTypeMap));
		this.put("RESPONSE", "success");
		this.put("STATUS", PKQLRunner.STATUS.SUCCESS);
		
		return null;
	}
}

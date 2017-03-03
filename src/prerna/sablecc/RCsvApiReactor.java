package prerna.sablecc;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.r.RFileWrapper;
import prerna.engine.impl.rdf.AbstractApiReactor;
import prerna.sablecc.meta.FilePkqlMetadata;
import prerna.sablecc.meta.IPkqlMetadata;

public class RCsvApiReactor extends AbstractApiReactor {

	// TODO: should modify this to be a bit more of a unique name so it does not
	// relate to a column name
	public final static String FILE_KEY = "file";
	public final static String DATA_TYPE_MAP = "dataTypeMap";

	private String fileName;
	private Map<String, String> dataTypeMap;
	
	@Override
	public Iterator<IHeadersDataRow> process() {
		// this will create the QS
		super.process();
		
		// the mapOptions stores all the information being passed from the user
		// this will contain the fileName and the data types from each column
		dataTypeMap = new Hashtable<String, String>();
		List<String> headerNames = (List<String>)myStore.get(PKQLEnum.COL_CSV);
		
		this.mapOptions = (Map<Object, Object>) myStore.get(PKQLEnum.MAP_OBJ);
		for(Object key : this.mapOptions.keySet()) {
			if(key.equals(FILE_KEY)) {
				fileName = (String)this.mapOptions.get(FILE_KEY);
				this.put("FILE_LOCATION", fileName);
			} else {
				dataTypeMap.put(key.toString(), (String)this.mapOptions.get(key));
			}
		}
		
		// pass in delimiter as a comma and return the FileIterator which uses the QS (if not empty) to 
		// to determine what selectors to send
		
		RFileWrapper fileWrapper = new RFileWrapper(this.fileName);
		fileWrapper.composeRChangeHeaderNamesScript();
		fileWrapper.composeRScript(this.qs, this.dataTypeMap, headerNames);
		
		this.put((String) getValue(PKQLEnum.API), fileWrapper);
		this.put("RESPONSE", "success");
		this.put("STATUS", PKQLRunner.STATUS.SUCCESS);
		
		return null;
	}
	
	public IPkqlMetadata getPkqlMetadata() {
		FilePkqlMetadata fileData = new FilePkqlMetadata();
		fileData.setFileLoc(this.fileName);
		fileData.setDataMap(this.dataTypeMap);
		fileData.setSelectors((Vector<String>) getValue(PKQLEnum.COL_CSV));
		fileData.setTableJoin((List<Map<String, Object>>) getValue(PKQLEnum.TABLE_JOINS));
		fileData.setPkqlStr((String) getValue(PKQLEnum.API));
		
		return fileData;
	}
	
}

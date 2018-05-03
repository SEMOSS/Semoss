package prerna.sablecc;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.ds.util.CsvFileIterator;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.rdf.AbstractApiReactor;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.sablecc.meta.FilePkqlMetadata;
import prerna.sablecc.meta.IPkqlMetadata;

public class CsvApiReactor extends AbstractApiReactor {

	// TODO: should modify this to be a bit more of a unique name so it does not
	// relate to a column name
	public final static String FILE_KEY = "file";
	public final static String DATA_TYPE_MAP_KEY = "dataTypeMap";
	public final static String NEW_HEADERS_KEY = "newHeaders";
	
	private String fileName;
	private Map<String, String> dataTypeMap;
	private Map<String, String> newHeaders;

	@Override
	public Iterator<IHeadersDataRow> process() {
		super.process();

		// get the file location
		if(this.mapOptions.containsKey(FILE_KEY)) {
			this.fileName = (String)this.mapOptions.get(FILE_KEY);
		} else {
			throw new IllegalArgumentException("Must define the file in the map options to load the excel file");
		}
		
		// get the data types
		if(this.mapOptions.containsKey(DATA_TYPE_MAP_KEY)) {
			this.dataTypeMap = (Map<String, String>) this.mapOptions.get(DATA_TYPE_MAP_KEY);
		}
		
		// get any modified headers
		if(this.mapOptions.containsKey(NEW_HEADERS_KEY)) {
			this.newHeaders = (Map<String, String>) this.mapOptions.get(NEW_HEADERS_KEY);
		}
		
		this.qs.setQsType(SelectQueryStruct.QUERY_STRUCT_TYPE.CSV_FILE);
		CsvQueryStruct csvQs = new CsvQueryStruct();
		//copy qs values to csvQS
		csvQs.merge(this.qs);
		
		//set csvQS specific values
		csvQs.setCsvFilePath(fileName);
		csvQs.setDelimiter(',');
		csvQs.setColumnTypes(this.dataTypeMap);
		csvQs.setNewHeaderNames(this.newHeaders);
		this.qs = csvQs;
		
		// pass in delimiter as a comma and return the FileIterator which uses the QS (if not empty) to 
		// to determine what selectors to send
		// the qs is passed from AbstractApiReactor
		CsvFileIterator csvIt = new CsvFileIterator (csvQs);
		this.put((String) getValue(PKQLEnum.API), csvIt);
		
		this.put("RESPONSE", "success");
		this.put("STATUS", PKQLRunner.STATUS.SUCCESS);
		this.put(PKQLEnum.QUERY_STRUCT, csvQs);
		return null;
	}
	
	public IPkqlMetadata getPkqlMetadata() {
		FilePkqlMetadata fileData = new FilePkqlMetadata();
		fileData.setFileLoc(this.fileName);
		fileData.setDataMap(this.dataTypeMap);
		fileData.setNewHeaders(this.newHeaders);
		fileData.setSelectors((Vector<String>) getValue(PKQLEnum.COL_CSV));
		fileData.setTableJoin((List<Map<String, Object>>) getValue(PKQLEnum.TABLE_JOINS));
		fileData.setPkqlStr((String) getValue(PKQLEnum.API));
		fileData.setType(FilePkqlMetadata.FILE_TYPE.CSV);
		
		return fileData;
	}
}

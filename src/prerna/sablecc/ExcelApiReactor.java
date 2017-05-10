package prerna.sablecc;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.ds.util.ExcelFileIterator;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.rdf.AbstractApiReactor;
import prerna.sablecc.meta.FilePkqlMetadata;
import prerna.sablecc.meta.IPkqlMetadata;

public class ExcelApiReactor extends AbstractApiReactor {

	// TODO: should modify this to be a bit more of a unique name so it does not
	// relate to a column name
	public final static String FILE_KEY = "file";
	public final static String SHEET_KEY = "sheetName";
	public final static String DATA_TYPE_MAP = "dataTypeMap";

	private String fileName;
	private String sheetName;
	private Map<String, String> dataTypeMap;
	
	@Override
	public Iterator<IHeadersDataRow> process() {
		super.process();

		// the mapOptions stores all the information being passed from the user
		// this will contain the fileName and the data types from each column
		for(Object key : this.mapOptions.keySet()) {
			if(key.equals(FILE_KEY)) {
				fileName = (String)this.mapOptions.get(FILE_KEY);
			} else if(key.equals(SHEET_KEY)) {
				sheetName = (String)this.mapOptions.get(SHEET_KEY);
			} else if(key.equals(DATA_TYPE_MAP)) {
				this.dataTypeMap = (Map<String, String>) this.mapOptions.get(key);
			}
		}
		
		// pass in delimiter as a comma and return the FileIterator which uses the QS (if not empty) to 
		// to determine what selectors to send
		
		// the qs is passed from AbstractApiReactor
		this.put((String) getValue(PKQLEnum.API), new ExcelFileIterator(this.fileName, this.sheetName, this.qs, this.dataTypeMap));
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
		fileData.setType(FilePkqlMetadata.FILE_TYPE.EXCEL);
		fileData.setSheetName(this.sheetName);
		
		return fileData;
	}
}
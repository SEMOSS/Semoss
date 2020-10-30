//package prerna.sablecc;
//
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.Vector;
//
//import prerna.ds.util.flatfile.ExcelFileIterator;
//import prerna.engine.api.IHeadersDataRow;
//import prerna.engine.impl.rdf.AbstractApiReactor;
//import prerna.query.querystruct.ExcelQueryStruct;
//import prerna.sablecc.meta.FilePkqlMetadata;
//import prerna.sablecc.meta.IPkqlMetadata;
//
//public class ExcelApiReactor extends AbstractApiReactor {
//
//	// TODO: should modify this to be a bit more of a unique name so it does not
//	// relate to a column name
//	public final static String FILE_KEY = "file";
//	public final static String SHEET_KEY = "sheetName";
//	public final static String DATA_TYPE_MAP_KEY = "dataTypeMap";
//	public final static String NEW_HEADERS_KEY = "newHeaders";
//
//	private String fileName;
//	private String sheetName;
//	private Map<String, String> dataTypeMap;
//	private Map<String, String> newHeaders;
//	
//	@Override
//	public Iterator<IHeadersDataRow> process() {
//		super.process();
//
//		// the mapOptions stores all the information being passed from the user
//		
//		// get the file location
//		if(this.mapOptions.containsKey(FILE_KEY)) {
//			this.fileName = (String)this.mapOptions.get(FILE_KEY);
//		} else {
//			throw new IllegalArgumentException("Must define the file in the map options to load the excel file");
//		}
//		
//		// get the sheet name
//		if(this.mapOptions.containsKey(SHEET_KEY)) {
//			this.sheetName = (String)this.mapOptions.get(SHEET_KEY);
//		} else {
//			throw new IllegalArgumentException("Must define the sheet in the excel file to load");
//		}
//		
//		// get the data types
//		if(this.mapOptions.containsKey(DATA_TYPE_MAP_KEY)) {
//			this.dataTypeMap = (Map<String, String>) this.mapOptions.get(DATA_TYPE_MAP_KEY);
//		}
//		
//		// get any modified headers
//		if(this.mapOptions.containsKey(NEW_HEADERS_KEY)) {
//			this.newHeaders = (Map<String, String>) this.mapOptions.get(NEW_HEADERS_KEY);
//		}
//		ExcelQueryStruct xlQS = new ExcelQueryStruct();
//		xlQS.merge(this.qs);
//		
//		//set xlQS specific values 
//		xlQS.setFilePath(this.fileName);
//		xlQS.setColumnTypes(this.dataTypeMap);
//		xlQS.setNewHeaderNames(this.newHeaders);
//		xlQS.setSheetName(this.sheetName);
//		this.qs = xlQS;
//		
//		// the qs is passed from AbstractApiReactor
//		this.put((String) getValue(PKQLEnum.API), new ExcelFileIterator(xlQS));
//		this.put("RESPONSE", "success");
//		this.put("STATUS", PKQLRunner.STATUS.SUCCESS);
//		this.put(PKQLEnum.QUERY_STRUCT, xlQS);
//		
//		return null;
//	}
//	
//	public IPkqlMetadata getPkqlMetadata() {
//		FilePkqlMetadata fileData = new FilePkqlMetadata();
//		fileData.setFileLoc(this.fileName);
//		fileData.setDataMap(this.dataTypeMap);
//		fileData.setSheetName(this.sheetName);
//		fileData.setNewHeaders(this.newHeaders);
//		fileData.setSelectors((Vector<String>) getValue(PKQLEnum.COL_CSV));
//		fileData.setTableJoin((List<Map<String, Object>>) getValue(PKQLEnum.TABLE_JOINS));
//		fileData.setPkqlStr((String) getValue(PKQLEnum.API));
//		fileData.setType(FilePkqlMetadata.FILE_TYPE.EXCEL);
//
//		return fileData;
//	}
//}
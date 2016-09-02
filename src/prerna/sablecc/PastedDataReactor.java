package prerna.sablecc;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.QueryStruct;
import prerna.ds.TinkerMetaHelper;
import prerna.ds.util.FileIterator;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class PastedDataReactor extends AbstractReactor {

	public PastedDataReactor() {
		String [] thisReacts = {PKQLEnum.ROW_CSV, PKQLEnum.FILTER, PKQLEnum.JOINS, PKQLEnum.WORD_OR_NUM, PKQLEnum.EXPLAIN};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.PASTED_DATA;
	}

	@Override
	public Iterator process() {
		System.out.println("Processed.. " + myStore);
		
		// save the file with the date
		Date date = new Date();
		String modifiedDate = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss_SSSS").format(date);
		String fileInfo = myStore.get(PKQLEnum.PASTED_DATA).toString().replace("<startInput>", "").replace("<endInput>", "");
	
		String path = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "\\copyPastedData" + modifiedDate;
		File file = new File(path);
		FileWriter fw = null;
		try {
			fw = new FileWriter(file);
			fw.write(fileInfo);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(fw != null) {
				try {
					fw.flush();
					fw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		System.out.println( "Saved Filename: " + path);
		
		// create a helper to get the headers for qs and edge hash
		CSVFileHelper helper = new CSVFileHelper();
		String delimiter = ( (List<Object>) myStore.get(PKQLEnum.WORD_OR_NUM)).get(0).toString(); // why does this come back as an array
		helper.setDelimiter(delimiter.charAt(0));
		helper.parse(path);

		String[] headers = helper.getHeaders();
		ITableDataFrame frame = (ITableDataFrame) myStore.get("G");
		Map<String, Set<String>> edgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(headers);
		this.put("EDGE_HASH", edgeHash);
		
		QueryStruct qs = new QueryStruct();
		for(String header : headers) {
			qs.addSelector(header, null);
		}
		Iterator it = new FileIterator(path, delimiter.charAt(0), qs, null);
		
		String nodeStr = (String)myStore.get(whoAmI);
		myStore.put(nodeStr, it);
//		
//		
//		Vector <String> selectors = new Vector<String>();
//		Vector<Object> headers = values.get(0);
//		for(Object o : headers) {
//			selectors.add(o + "");
//		}
//		
//		Vector <Hashtable> filtersToBeElaborated = new Vector<Hashtable>();
////		Vector <String> selectors = new Vector<String>();
//		Vector <Hashtable> filters = new Vector<Hashtable>();
//		Vector <Hashtable> joins = new Vector<Hashtable>();
//
//		if(myStore.containsKey(PKQLEnum.COL_CSV) && ((Vector)myStore.get(PKQLEnum.COL_CSV)).size() > 0)
//			selectors = (Vector<String>) myStore.get(PKQLEnum.COL_CSV);
//		if(myStore.containsKey(PKQLEnum.FILTER) && ((Vector)myStore.get(PKQLEnum.FILTER)).size() > 0)
//			filters = (Vector<Hashtable>) myStore.get(PKQLEnum.FILTER);
//		if(myStore.containsKey(PKQLEnum.JOINS) && ((Vector)myStore.get(PKQLEnum.JOINS)).size() > 0)
//			joins = (Vector<Hashtable>) myStore.get(PKQLEnum.JOINS);
//
//		Map<String, Set<String>> edgeHash = null;
//		if(qs.relations != null && !qs.relations.isEmpty()) {
//			edgeHash = qs.getReturnConnectionsHash();
//		} else {
//			ITableDataFrame frame = (ITableDataFrame) myStore.get("G");
//			edgeHash = frame.createPrimKeyEdgeHash(selectors.toArray(new String[]{}));
//		}
//		this.put("EDGE_HASH", edgeHash);
//		this.put("QUERY_STRUCT", qs);
//
//		String nodeStr = (String)myStore.get(whoAmI);
//		myStore.put(nodeStr, new CsvTableWrapper(values));

		// eventually I need this iterator to set this back for this particular node
		
	
		return null;
	}

	@Override
	public String explain() {
		String msg = "";
		msg += "PastedDataReactor";
		return msg;
	}
}

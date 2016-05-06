package prerna.sablecc;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import cern.colt.Arrays;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.util.FileIterator;
import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectWrapper;
import prerna.util.DIHelper;

public class ImportDataReactor extends AbstractReactor {

	Hashtable <String, String[]> values2SyncHash = new Hashtable <String, String[]>();

	public ImportDataReactor()
	{
		String [] thisReacts = {PKQLEnum.API, PKQLEnum.JOINS, PKQLEnum.CSV_TABLE, PKQLEnum.PASTED_DATA};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.IMPORT_DATA;

		String [] dataFromApi = {PKQLEnum.COL_CSV, "ENGINE", "EDGE_HASH"};
		values2SyncHash.put(PKQLEnum.API, dataFromApi);

		String [] dataFromTable = {"EDGE_HASH"};
		values2SyncHash.put(PKQLEnum.CSV_TABLE, dataFromTable);
		
		String [] dataFromPastedData = {"EDGE_HASH"};
		values2SyncHash.put(PKQLEnum.PASTED_DATA, dataFromPastedData);
	}

	@Override
	public Iterator process() {
		modExpression();
		System.out.println("My Store on COL CSV " + myStore);
		
		ITableDataFrame frame = (ITableDataFrame) myStore.get("G");
		String[] startingHeaders = frame.getColumnHeaders();
		
		Vector<Map<String,String>> joinCols = new Vector<Map<String,String>>();
		Vector<Map<String, String>> joins = (Vector<Map<String, String>>) myStore.get(PKQLEnum.JOINS);
		String joinType = "";
		if(joins!=null){
			for(Map<String,String> join : joins){
				Map<String,String> joinMap = new HashMap<String,String>();
				joinMap.put(join.get(PKQLEnum.TO_COL), join.get(PKQLEnum.FROM_COL));
				joinCols.add(joinMap);
				joinType = join.get(PKQLEnum.REL_TYPE);
			}
		}

		Iterator it = null;
		IEngine engine = null;
		
		if(myStore.containsKey(PKQLEnum.API)) {			
			Map<String, Set<String>> edgeHash = (Map<String, Set<String>>) this.getValue(PKQLEnum.API + "_EDGE_HASH");
			engine = (IEngine) DIHelper.getInstance().getLocalProp((this.getValue(PKQLEnum.API + "_ENGINE")+"").trim());
			if(engine != null) {
				Map[] mergedMaps = frame.mergeQSEdgeHash(edgeHash, engine, joinCols);
				myStore.put("edgeHash", mergedMaps[0]);
				myStore.put("logicalToValue", mergedMaps[1]);
			} 
//			else {
//				it  = (Iterator) myStore.get(PKQLEnum.API);
//				Map<String, String> dataType = new HashMap<>();
//				String[] types = new String[]{};
//				String[] headers = new String[]{};
//				if(it instanceof FileIterator) {
//					types = ((FileIterator) it).getTypes();
//					headers = ((FileIterator) it).getHeaders();
//				} 
//				
//				for(int i = 0; i < headers.length;i++) {
//					dataType.put(headers[i], types[i]);
//				}
//				frame.mergeEdgeHash(edgeHash, dataType);
//				myStore.put("edgeHash", edgeHash);
//			}
			
			it  = (Iterator) myStore.get(PKQLEnum.API);
			
		} else if(myStore.containsKey(PKQLEnum.CSV_TABLE)) {
			Map<String, Set<String>> edgeHash = (Map<String, Set<String>>) this.getValue(PKQLEnum.CSV_TABLE + "_EDGE_HASH");
			it = (Iterator) myStore.get(PKQLEnum.CSV_TABLE);
			
//			Map<String, String> dataType = new HashMap<>();
//			String[] types = new String[]{};
//			String[] headers = new String[]{};
//			if(it instanceof CsvTableWrapper) {
//				types = ((CsvTableWrapper) it).getTypes();
//				headers = ((CsvTableWrapper) it).getHeaders();
//			}
//			
//			for(int i = 0; i < headers.length;i++) {
//				dataType.put(headers[i], types[i]);
//			}
//			frame.mergeEdgeHash(edgeHash, dataType);
//			myStore.put("edgeHash", edgeHash);

		} else if(myStore.containsKey(PKQLEnum.PASTED_DATA)) {
			Map<String, Set<String>> edgeHash = (Map<String, Set<String>>) this.getValue(PKQLEnum.PASTED_DATA + "_EDGE_HASH");
			it = (Iterator) myStore.get(PKQLEnum.PASTED_DATA);

//			Map<String, String> dataType = new HashMap<>();
//			String[] types = new String[]{};
//			String[] headers = new String[]{};
//			if(it instanceof FileIterator) {
//				types = ((FileIterator) it).getTypes();
//				headers = ((FileIterator) it).getHeaders();
//			} 
//			
//			for(int i = 0; i < headers.length;i++) {
//				dataType.put(headers[i], types[i]);
//			}
//			frame.mergeEdgeHash(edgeHash, dataType);
//			myStore.put("edgeHash", edgeHash);
		}
		
		
		//TODO: need to make all these wrappers that give a IHeaderDataRow be the same type to get this info
//		if(it instanceof FileIterator) {
//			String[] types = ((FileIterator) it).getTypes();
//			frame.addMetaDataTypes( ((FileIterator) it).getHeaders(), types);
//		} else if(it instanceof CsvTableWrapper) {
//			String[] types = ((CsvTableWrapper) it).getTypes();
//			frame.addMetaDataTypes( ((CsvTableWrapper) it).getHeaders(), types);
//		}
		
		// put in values for frame specific import data reactors to grab instead of having to do it based on api/csv table
		myStore.put("iterator", it);
		myStore.put("startingHeaders", startingHeaders);
		myStore.put(PKQLEnum.JOINS, joinCols);
		myStore.put(PKQLEnum.REL_TYPE, joinType);
		
		return null;
	}

	// gets all the values to synchronize for this 
	public String[] getValues2Sync(String input)
	{
		return values2SyncHash.get(input);
	}

	protected String createResponseString(ISelectWrapper it){
		Map<String, Object> map = it.getResponseMeta();
		String mssg = "";
		for(String key : map.keySet()){
			if(!mssg.isEmpty()){
				mssg = mssg + " \n";
			}
			mssg = mssg + key + ": " + map.get(key).toString();
		}
		String retStr = "Sucessfully added data using : \n" + mssg;
		return retStr;
	}
	
	protected String createResponseString(String[] headers){
		return "Successfully added data using:\n headers= " + Arrays.toString(headers);
	}
}

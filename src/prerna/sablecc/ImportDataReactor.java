package prerna.sablecc;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.util.FileIterator;
import prerna.engine.api.IEngine;
import prerna.engine.api.IScriptReactor;
import prerna.engine.api.ISelectWrapper;
import prerna.util.DIHelper;

public class ImportDataReactor extends AbstractReactor {

	Hashtable <String, String[]> values2SyncHash = new Hashtable <String, String[]>();

	public ImportDataReactor()
	{
		String [] thisReacts = {PKQLEnum.API, PKQLEnum.JOINS, PKQLEnum.CSV_TABLE};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.IMPORT_DATA;

		String [] dataFromApi = {PKQLEnum.COL_CSV, "ENGINE", "EDGE_HASH"};
		values2SyncHash.put(PKQLEnum.API, dataFromApi);

		String [] dataFromTable = {"EDGE_HASH"};
		values2SyncHash.put(PKQLEnum.CSV_TABLE, dataFromTable);
	}

	@Override
	public Iterator process() {
		modExpression();
		String nodeStr = (String)myStore.get(whoAmI);
		System.out.println("My Store on COL CSV " + myStore);
		
		ITableDataFrame frame = (ITableDataFrame) myStore.get("G");
		String[] startingHeaders = frame.getColumnHeaders();
		
		Vector<Map<String,String>> joinCols = new Vector<Map<String,String>>();
		Vector<Map<String, String>> joins = (Vector<Map<String, String>>) myStore.get(PKQLEnum.JOINS);
		if(joins!=null){
			for(Map<String,String> join : joins){
				Map<String,String> joinMap = new HashMap<String,String>();
				joinMap.put(join.get(PKQLEnum.TO_COL), join.get(PKQLEnum.FROM_COL));
				joinCols.add(joinMap);
			}
		}

		Iterator it = null;
		Map[] mergedMaps = new Map[2];
		IEngine engine = null;
		
		if(myStore.containsKey(PKQLEnum.API)) {			
			Map<String, Set<String>> edgeHash = (Map<String, Set<String>>) this.getValue(PKQLEnum.API + "_EDGE_HASH");
			engine = (IEngine) DIHelper.getInstance().getLocalProp((this.getValue(PKQLEnum.API + "_ENGINE")+"").trim());
			if(engine != null) {
				mergedMaps = frame.mergeQSEdgeHash(edgeHash, engine, joinCols);
			} else {
				frame.mergeEdgeHash(edgeHash);
			}
			it  = (Iterator) myStore.get(PKQLEnum.API);
			
		} else if(myStore.containsKey(PKQLEnum.CSV_TABLE)) {
			Map<String, Set<String>> edgeHash = (Map<String, Set<String>>) this.getValue(PKQLEnum.CSV_TABLE + "_EDGE_HASH");
			frame.mergeEdgeHash(edgeHash);

			it = (Iterator) myStore.get(PKQLEnum.CSV_TABLE);
		}
		
		
		// need to make all these wrappers that give a IHeaderDataRow be the same type to get this info
		if(it instanceof FileIterator) {
			String[] types = ((FileIterator) it).getTypes();
			frame.addMetaDataTypes( ((FileIterator) it).getHeaders(), types);
		} else if(it instanceof CsvTableWrapper) {
			String[] types = ((CsvTableWrapper) it).getTypes();
			frame.addMetaDataTypes( ((CsvTableWrapper) it).getHeaders(), types);
		}
		
		IScriptReactor reactor = frame.getImportDataReactor();
		reactor.put("iterator", it);
		if(mergedMaps[0] != null) {
			reactor.put("edgeHash", mergedMaps[0]);
		}
		if(mergedMaps[1] != null) {
			reactor.put("logicalToValue", mergedMaps[1]);
		}
		reactor.put("startingHeaders", startingHeaders);
		// now run it
		reactor.process();
		
		List<Object[]> data = frame.getData();
		System.out.println("view data now");
		for(int i = 0; i < data.size() && i < 15; i++) {
			System.out.println(Arrays.toString(data.get(i)));
		}
		
		System.out.println(frame.getTableHeaderObjects());
		
		
		// get rid of this bifurcation
		// push this into the iterators
		if(it instanceof ISelectWrapper) {
			myStore.put(nodeStr, createResponseString((ISelectWrapper)it));
		} else {
//			myStore.put(nodeStr, "Successfully added data using:\n headers= " + headers);
		}
		myStore.put("STATUS", "SUCCESS");

		return null;
	}

	// gets all the values to synchronize for this 
	public String[] getValues2Sync(String input)
	{
		return values2SyncHash.get(input);
	}

	private String createResponseString(ISelectWrapper it){

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
}

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
import prerna.ds.H2.TinkerH2Frame;
import prerna.ds.util.FileIterator;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.ISelectWrapper;
import prerna.util.ArrayUtilityMethods;
import prerna.util.DIHelper;
import prerna.util.Utility;

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
		// put the joins in a list to feed into merge edge hash
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
		Map<String, Set<String>> edgeHash = null;
		Map[] mergedMaps = new Map[2];
		IEngine engine = null;

		if(myStore.containsKey(PKQLEnum.API)) {			
			Object value = myStore.get(PKQLEnum.API);
			edgeHash = (Map<String, Set<String>>) this.getValue(PKQLEnum.API + "_EDGE_HASH");
			engine = (IEngine) DIHelper.getInstance().getLocalProp((this.getValue(PKQLEnum.API + "_ENGINE")+"").trim());
			mergedMaps = frame.mergeQSEdgeHash(edgeHash, engine, joinCols);

			it  = (Iterator) myStore.get(PKQLEnum.API);
			
		} else if(myStore.containsKey(PKQLEnum.CSV_TABLE)) {
			edgeHash = (Map<String, Set<String>>) this.getValue(PKQLEnum.CSV_TABLE + "_EDGE_HASH");
			frame.mergeEdgeHash(edgeHash);

			it = (Iterator) myStore.get(PKQLEnum.CSV_TABLE);
		}

		Map<Integer, Set<Integer>> cardinality = null;
		String[] headers = null;
		boolean addRow = false;
		boolean isPrimKey = false;
		while(it.hasNext()){
			IHeadersDataRow ss = (IHeadersDataRow) it.next();
			if(cardinality == null) { // during first loop
				cardinality = Utility.getCardinalityOfValues(ss.getHeaders(), mergedMaps[0]);
				headers = ss.getHeaders();
//				System.out.println("Got headers: " + Arrays.toString(headers));

				// TODO: annoying, need to determine if i need to create a prim key edge hash
				if(mergedMaps[0] == null && cardinality.isEmpty()) {
					Map<String, Set<String>> primKeyEdgeHash = frame.createPrimKeyEdgeHash(headers);
					frame.mergeEdgeHash(primKeyEdgeHash);
					isPrimKey = true;
				}

				// TODO: need to have a smart way of determining when it is an "addRow" vs. "addRelationship"
				// TODO: h2Builder addRelationship only does update query which does nothing if frame is empty
				boolean empty = frame.isEmpty();
				if((engine == null && empty) || (frame instanceof TinkerH2Frame && ( allHeadersAccounted(startingHeaders, headers) || empty ) )) {
					addRow = true;
				}
			}

			// TODO: need to have a smart way of determining when it is an "addRow" vs. "addRelationship"
			// TODO: h2Builder addRelationship only does update query which does nothing if frame is empty
			if(addRow || isPrimKey) {
				frame.addRow(ss.getValues(), ss.getRawValues(), ss.getHeaders());
			} else {
				frame.addRelationship(ss.getHeaders(), ss.getValues(), ss.getRawValues(), cardinality, mergedMaps[1]);
			}
		}
		
		if(engine == null) {
			if(it instanceof FileIterator) {
				String[] types = ((FileIterator) it).getTypes();
				frame.addMetaDataTypes(headers, types);
			}
		}

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
			myStore.put(nodeStr, "Successfully added data using:\n headers= " + headers);
		}
		myStore.put("STATUS", "SUCCESS");

		return null;
	}

	public boolean allHeadersAccounted(String[] headers1, String[] headers2) {
		if(headers1.length != headers2.length) {
			return false;
		}
		
		for(String header1 : headers1) {
			if(!ArrayUtilityMethods.arrayContainsValue(headers2, header1)) {
				return false;
			}
		}
		
		return true;
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

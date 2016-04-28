package prerna.sablecc;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
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

		Map<String, Set<String>> edgeHash = null;

		if(myStore.containsKey(PKQLEnum.API)) {
			edgeHash = (Map<String, Set<String>>) this.getValue(PKQLEnum.API + "_EDGE_HASH");
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp((this.getValue(PKQLEnum.API + "_ENGINE")+"").trim());
			Map[] mergedMaps = frame.mergeQSEdgeHash(edgeHash, engine, joinCols);

			Object value = myStore.get(PKQLEnum.API);
			ISelectWrapper it = (ISelectWrapper)value;

			if(it.hasNext()) {
				System.out.println("We have something to add");
			}

			while(it.hasNext()){
				ISelectStatement ss = (ISelectStatement) it.next();
				//			System.out.println(((ISelectStatement)ss).getPropHash());
				frame.addRelationship(ss.getPropHash(), ss.getRPropHash(), mergedMaps[0], mergedMaps[1]);
			}
			myStore.put(nodeStr, createResponseString(it));

		} else if(myStore.containsKey(PKQLEnum.CSV_TABLE)) {
			edgeHash = (Map<String, Set<String>>) this.getValue(PKQLEnum.CSV_TABLE + "_EDGE_HASH");
			frame.mergeEdgeHash(edgeHash);

			Object value = myStore.get(PKQLEnum.CSV_TABLE);
			Iterator<Vector<Object>> it = (Iterator<Vector<Object>>) value;
			Vector<Object> headers = it.next();
			System.out.println("Headers for csv are: " + headers);

			while(it.hasNext()) {
				Object[] row = it.next().toArray();
				frame.addRow(row, row);
			}

			myStore.put(nodeStr, "Successfully added CSV data using:\n headers= " + headers);
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

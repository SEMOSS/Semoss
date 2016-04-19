package prerna.sablecc;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.QueryStruct;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngineWrapper;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.util.DIHelper;

public class ImportDataReactor extends AbstractReactor {
	
	Hashtable <String, String[]> values2SyncHash = new Hashtable <String, String[]>();
	
	public ImportDataReactor()
	{
		String [] thisReacts = {PKQLEnum.API, PKQLEnum.JOINS};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.IMPORT_DATA;

		String [] dataFromApi = {PKQLEnum.COL_CSV, "ENGINE", "QUERY_STRUCT"};
		values2SyncHash.put(PKQLEnum.API, dataFromApi);
	}

	@Override
	public Iterator process() {
		modExpression();
		String nodeStr = (String)myStore.get(whoAmI);
		System.out.println("My Store on COL CSV " + myStore);
		
		ITableDataFrame frame = (ITableDataFrame) myStore.get("G");
		
		ISelectWrapper it = null;
		Object value = myStore.get(PKQLEnum.API);
		
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
		it = (ISelectWrapper)value;

		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp((this.getValue(PKQLEnum.API + "_ENGINE")+"").trim());
		QueryStruct qs = (QueryStruct) this.getValue(PKQLEnum.API + "_QUERY_STRUCT");
		
		Map<String, Set<String>> edgeHash = qs.getReturnConnectionsHash();
		
		Map[] mergedMaps = frame.mergeQSEdgeHash(edgeHash, engine, joinCols);
		while(it.hasNext()){
			ISelectStatement ss = (ISelectStatement) it.next();
			System.out.println(((ISelectStatement)ss).getPropHash());
			frame.addRelationship(ss.getPropHash(), ss.getRPropHash(), mergedMaps[0], mergedMaps[1]);
		}
		
		myStore.put("STATUS", "SUCCESS");
		
		myStore.put(nodeStr, createResponseString(it));
		
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

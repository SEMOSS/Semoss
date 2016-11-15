package prerna.sablecc;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.DataFrameHelper;
import prerna.engine.api.ISelectWrapper;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.sablecc.meta.IPkqlMetadata;

public class RemoveDataReactor extends AbstractReactor {

	Hashtable<String, String[]> values2SyncHash = new Hashtable<String, String[]>();

	public RemoveDataReactor() {
		String[] thisReacts = { PKQLEnum.API, PKQLEnum.JOINS};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.REMOVE_DATA;

		String[] dataFromApi = { PKQLEnum.COL_CSV };
		values2SyncHash.put(PKQLEnum.API, dataFromApi);
	}

	@Override
	public Iterator process() {
		modExpression();
		String nodeStr = (String) myStore.get(whoAmI);
		System.out.println("My Store on COL CSV " + myStore);

		ITableDataFrame frame = (ITableDataFrame) myStore.get("G");

		ISelectWrapper it = null;
		Object value = myStore.get(PKQLEnum.API);

		// put the joins in a list to feed into merge edge hash
		Vector<Map<String, String>> joinCols = new Vector<Map<String, String>>();
		Vector<Map<String, String>> joins = (Vector<Map<String, String>>) myStore.get(PKQLEnum.JOINS);
		if (joins != null) {
			for (Map<String, String> join : joins) {
				Map<String, String> joinMap = new HashMap<String, String>();
				joinMap.put(join.get(PKQLEnum.TO_COL), join.get(PKQLEnum.FROM_COL));
				joinCols.add(joinMap);
			}
		}
		it = (ISelectWrapper) value;

//		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp((this.getValue(PKQLEnum.API + "_ENGINE")+"").trim());
//		QueryStruct qs = (QueryStruct) this.getValue(PKQLEnum.API + "_QUERY_STRUCT");
		//
		// Map<String, Set<String>> edgeHash = qs.getReturnConnectionsHash();
		//
		// Map[] mergedMaps = frame.mergeQSEdgeHash(edgeHash, engine, joinCols);

		// if(frame instanceof H2Frame) {
		// while(it.hasNext()){
		// ISelectStatement ss = (ISelectStatement) it.next();
		// System.out.println(((ISelectStatement)ss).getPropHash());
		// frame.removeRelationship(ss.getPropHash(), ss.getRPropHash());
		// }
		// } else if(frame instanceof TinkerFrame) {
		// IMetaData metaData = ((TinkerFrame)frame)
		// H2Frame tempFrame = TableDataFrameFactory.convertToH2Frame(frame);
		// while(it.hasNext()){
		// ISelectStatement ss = (ISelectStatement) it.next();
		// System.out.println(((ISelectStatement)ss).getPropHash());
		// tempFrame.removeRelationship(ss.getPropHash(), ss.getRPropHash());
		// }
		//
		// }

		DataFrameHelper.removeData(frame, it);
		myStore.put("STATUS", STATUS.SUCCESS);

		myStore.put(nodeStr, createResponseString(it));

		return null;
	}

	// gets all the values to synchronize for this
	public String[] getValues2Sync(String input) {
		return values2SyncHash.get(input);
	}

	private String createResponseString(ISelectWrapper it) {
		Map<String, Object> map = it.getResponseMeta();
		String mssg = "";
		for (String key : map.keySet()) {
			if (!mssg.isEmpty()) {
				mssg = mssg + " \n";
			}
			mssg = mssg + key + ": " + map.get(key).toString();
		}
		String retStr = "Sucessfully deleted data using : \n" + mssg;
		return retStr;
	}

	@Override
	public IPkqlMetadata getPkqlMetadata() {
		// TODO Auto-generated method stub
		return null;
	}
}

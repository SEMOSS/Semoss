package prerna.sablecc;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerMetaHelper;

public class CsvTableReactor extends AbstractReactor {

	public CsvTableReactor() {
		String[] thisReacts = { PKQLEnum.ROW_CSV, PKQLEnum.FILTER, PKQLEnum.JOINS, PKQLEnum.EXPLAIN };
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.CSV_TABLE;
	}

	@Override
	public Iterator process() {

		System.out.println("Processed.. " + myStore);

		List<Vector<Object>> values = (List<Vector<Object>>) myStore.get(PKQLEnum.ROW_CSV);
		if (values.size() < 2) {
			System.out.println("error, not enough data... how do i send this up to return to FE?");
		}

		Vector<String> selectors = new Vector<String>();
		Vector<Object> headers = values.get(0);
		for (Object o : headers) {
			selectors.add(o + "");
		}

		// Vector <Hashtable> filtersToBeElaborated = new Vector<Hashtable>();
		//// Vector <String> selectors = new Vector<String>();
		// Vector <Hashtable> filters = new Vector<Hashtable>();
		// Vector <Hashtable> joins = new Vector<Hashtable>();
		//
		// if(myStore.containsKey(PKQLEnum.COL_CSV) &&
		// ((Vector)myStore.get(PKQLEnum.COL_CSV)).size() > 0)
		// selectors = (Vector<String>) myStore.get(PKQLEnum.COL_CSV);
		// if(myStore.containsKey(PKQLEnum.FILTER) &&
		// ((Vector)myStore.get(PKQLEnum.FILTER)).size() > 0)
		// filters = (Vector<Hashtable>) myStore.get(PKQLEnum.FILTER);
		// if(myStore.containsKey(PKQLEnum.JOINS) &&
		// ((Vector)myStore.get(PKQLEnum.JOINS)).size() > 0)
		// joins = (Vector<Hashtable>) myStore.get(PKQLEnum.JOINS);
		//
		// QueryStruct qs = new QueryStruct();
		// processQueryStruct(qs, selectors, filters, joins);
		//
		Map<String, Set<String>> edgeHash = null;
		// if(qs.relations != null && !qs.relations.isEmpty()) {
		// edgeHash = qs.getReturnConnectionsHash();
		// } else {
		ITableDataFrame frame = (ITableDataFrame) myStore.get("G");
		edgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(selectors.toArray(new String[] {}));
		// }
		this.put("EDGE_HASH", edgeHash);
		// this.put("QUERY_STRUCT", qs);

		String nodeStr = (String) myStore.get(whoAmI);
		myStore.put(nodeStr, new CsvTableWrapper(values));

		// eventually I need this iterator to set this back for this particular node
		return null;
	}

	@Override
	public String explain() {
		String msg = "";
		msg += "CsvTableReactor";
		return msg;
	}

	// public void processQueryStruct(QueryStruct qs, Vector <String> selectors,
	// Vector <Hashtable> filters, Vector <Hashtable> joins)
	// {
	//
	// for(int selectIndex = 0;selectIndex < selectors.size();selectIndex++)
	// {
	// String thisSelector = selectors.get(selectIndex);
	// if(thisSelector.contains("__")){
	// String concept = thisSelector.substring(0, thisSelector.indexOf("__"));
	// String property = thisSelector.substring(thisSelector.indexOf("__")+2);
	// qs.addSelector(concept, property);
	// }
	// else
	// {
	// qs.addSelector(thisSelector, null);
	// }
	// }
	// for(int filterIndex = 0;filterIndex < filters.size();filterIndex++)
	// {
	// Object thisObject = filters.get(filterIndex);
	// System.out.println(thisObject.getClass());
	// if(thisObject instanceof Hashtable)
	// System.out.println("I just dont what is wrong.. ");
	//
	// Hashtable thisFilter = (Hashtable)filters.get(filterIndex);
	// String fromCol = (String)thisFilter.get("FROM_COL");
	// String toCol = null;
	// Vector filterData = new Vector();
	// if(thisFilter.containsKey("TO_COL"))
	// {
	// toCol = (String)thisFilter.get("TO_COL");
	// //filtersToBeElaborated.add(thisFilter);
	// //tinkerSelectors.add(toCol);
	// // need to pull this from tinker frame and do the due
	// // interestingly this could be join
	// }
	// else
	// {
	// // this is a vector do some processing here
	// filterData = (Vector)thisFilter.get("TO_DATA");
	// String comparator = (String)thisFilter.get("COMPARATOR");
	// // String concept = fromCol.substring(0, fromCol.indexOf("__"));
	// // String property = fromCol.substring(fromCol.indexOf("__")+2);
	// qs.addFilter(fromCol, comparator, filterData);
	// }
	// }
	// for(int joinIndex = 0;joinIndex < joins.size();joinIndex++)
	// {
	// Hashtable thisJoin = (Hashtable)joins.get(joinIndex);
	//
	// String fromCol = (String)thisJoin.get("FROM_COL");
	// String toCol = (String)thisJoin.get("TO_COL");
	//
	// String relation = (String)thisJoin.get("REL_TYPE");
	// qs.addRelation(fromCol, toCol, relation);
	// }
	// }
}

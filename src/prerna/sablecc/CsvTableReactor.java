package prerna.sablecc;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.ds.TinkerMetaHelper;

public class CsvTableReactor extends AbstractReactor {

	public CsvTableReactor() {
		String [] thisReacts = {PKQLEnum.ROW_CSV, PKQLEnum.FILTER, PKQLEnum.JOINS};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.CSV_TABLE;
	}

	@Override
	public Iterator process() {

		System.out.println("Processed.. " + myStore);
		
		List<Vector<Object>> values = (List<Vector<Object>>) myStore.get(PKQLEnum.ROW_CSV);
		if(values.size() < 2) {
			System.out.println("error, not enough data... how do i send this up to return to FE?");
		}
		
		Vector <String> selectors = new Vector<String>();
		Vector<Object> headers = values.get(0);
		for(Object o : headers) {
			selectors.add(o + "");
		}
		
		Map<String, Set<String>> edgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(selectors.toArray(new String[]{}));
		this.put("EDGE_HASH", edgeHash);

		String nodeStr = (String)myStore.get(whoAmI);
		myStore.put(nodeStr, new CsvTableWrapper(values));

		// eventually I need this iterator to set this back for this particular node
		return null;
	}
}

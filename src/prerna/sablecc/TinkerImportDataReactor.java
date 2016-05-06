package prerna.sablecc;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import prerna.ds.TinkerFrame;
import prerna.ds.util.FileIterator;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.ISelectWrapper;
import prerna.util.Utility;

public class TinkerImportDataReactor extends ImportDataReactor{

	@Override
	public Iterator process() {
		super.process();
		String nodeStr = (String)myStore.get(whoAmI);
		
		TinkerFrame frame = (TinkerFrame) myStore.get("G");
		Iterator<IHeadersDataRow> it = (Iterator<IHeadersDataRow>) myStore.get("iterator");
		Map<String, Set<String>> edgeHash = (Map<String, Set<String>>) myStore.get("edgeHash");
		Map<String, String> logicalToValue = (Map<String, String>) myStore.get("logicalToValue");
		
		Map<Integer, Set<Integer>> cardinality = null;
		String[] headers = null;
		
		boolean isPrimKey = false;
		while(it.hasNext()){
			IHeadersDataRow ss = it.next();
			
			if(cardinality == null) { // happens only during first loop
				cardinality = Utility.getCardinalityOfValues(ss.getHeaders(), edgeHash);
				headers = ss.getHeaders();

				// TODO: annoying, need to determine if i need to create a prim key edge hash
				if( !(it instanceof ISelectWrapper) ) {
					Map<String, Set<String>> primKeyEdgeHash = frame.createPrimKeyEdgeHash(headers);
					//TODO: need to make all these wrappers that give a IHeaderDataRow be the same type to get this info
					String[] types = null;
					if(it instanceof FileIterator) {
						types = ((FileIterator) it).getTypes();
					} else if(it instanceof CsvTableWrapper) {
						types = ((CsvTableWrapper) it).getTypes();
					}
					
					Map<String, String> dataType = new HashMap<>();
					for(int i = 0; i < types.length; i++) {
						dataType.put(headers[i], types[i]);
					}
					frame.mergeEdgeHash(primKeyEdgeHash, dataType);
					
					isPrimKey = true;
				}
			}

			// TODO: need to have a smart way of determining when it is an "addRow" vs. "addRelationship"
			if(isPrimKey) {
				frame.addRow(ss.getValues(), ss.getRawValues(), ss.getHeaders());
			} else {
				frame.addRelationship(ss.getHeaders(), ss.getValues(), ss.getRawValues(), cardinality, logicalToValue);
			}
		}
		
		// get rid of this bifurcation
		// push this into the iterators
		if(it instanceof ISelectWrapper) {
			myStore.put(nodeStr, createResponseString((ISelectWrapper)it));
		} else {
			myStore.put(nodeStr, createResponseString(headers));
		}
		myStore.put("STATUS", "SUCCESS");
		
		return null;
	}

}

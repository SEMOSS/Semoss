package prerna.sablecc;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.ds.H2.TinkerH2Frame;
import prerna.ds.util.FileIterator;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.ISelectWrapper;
import prerna.util.ArrayUtilityMethods;

public class H2ImportDataReactor extends ImportDataReactor {
	
	@Override
	public Iterator process() {
		super.process();
		String nodeStr = (String)myStore.get(whoAmI);
		
		TinkerH2Frame frame = (TinkerH2Frame) myStore.get("G");
		Iterator<IHeadersDataRow> it = (Iterator<IHeadersDataRow>) myStore.get("iterator");
		Map<String, Set<String>> edgeHash = (Map<String, Set<String>>) myStore.get("edgeHash");
		Map<String, String> logicalToValue = (Map<String, String>) myStore.get("logicalToValue");
		String[] startingHeaders = (String[]) myStore.get("startingHeaders");
		
		Vector<Map<String, String>> joins = (Vector<Map<String, String>>) myStore.get(PKQLEnum.JOINS);
		String joinType = (String)myStore.get(PKQLEnum.REL_TYPE);
		
		String[] headers = null;
		boolean addRow = false;
		boolean isPrimKey = false;
		
		while(it.hasNext()){
			IHeadersDataRow ss = (IHeadersDataRow) it.next();
			if(headers == null) { // during first loop
				headers = ss.getHeaders();

				// TODO: annoying, need to determine if i need to create a prim key edge hash
				if(edgeHash == null) {
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

				// TODO: need to have a smart way of determining when it is an "addRow" vs. "addRelationship"
				// TODO: h2Builder addRelationship only does update query which does nothing if frame is empty
				if(allHeadersAccounted(startingHeaders, headers) || frame.isEmpty() ) {
					addRow = true;
				}
			}

			// TODO: need to have a smart way of determining when it is an "addRow" vs. "addRelationship"
			// TODO: h2Builder addRelationship only does update query which does nothing if frame is empty
			if(addRow && isPrimKey) {
				frame.addRow(ss.getValues(), ss.getRawValues(), ss.getHeaders());
			} else {
				frame.processIterator(it, ss.getHeaders(), logicalToValue, joins, joinType);
				break;
			}
		}
		
		inputResponseString(it, headers);
		myStore.put("STATUS", "SUCCESS");
		
		return null;
	}
	
	private boolean allHeadersAccounted(String[] headers1, String[] headers2) {
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
}

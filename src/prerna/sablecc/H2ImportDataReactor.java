package prerna.sablecc;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.ds.H2.H2Builder;
import prerna.ds.H2.TinkerH2Frame;
import prerna.engine.api.IHeadersDataRow;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

public class H2ImportDataReactor extends AbstractReactor{

	private TinkerH2Frame frame = null;
	
	public H2ImportDataReactor(TinkerH2Frame frame) {
		this.frame = frame;
	}
	
	@Override
	public Iterator process() {
		
		Iterator<IHeadersDataRow> it = (Iterator<IHeadersDataRow>) myStore.get("iterator");
		Map<String, Set<String>> edgeHash = (Map<String, Set<String>>) myStore.get("edgeHash");
		Map<String, String> logicalToValue = (Map<String, String>) myStore.get("logicalToValue");
		String[] startingHeaders = (String[]) myStore.get("startingHeaders");
		
		Vector<Map<String, String>> joins = (Vector<Map<String, String>>) myStore.get(PKQLEnum.JOINS);
		String joinType = (String)myStore.get(PKQLEnum.REL_TYPE);
		
		Map<Integer, Set<Integer>> cardinality = null;
		String[] headers = null;
		boolean addRow = false;
		boolean isPrimKey = false;
		
		while(it.hasNext()){
			IHeadersDataRow ss = (IHeadersDataRow) it.next();
			if(cardinality == null) { // during first loop
				cardinality = Utility.getCardinalityOfValues(ss.getHeaders(), edgeHash);
				headers = ss.getHeaders();

				// TODO: annoying, need to determine if i need to create a prim key edge hash
				if(edgeHash == null || cardinality.isEmpty()) {
					Map<String, Set<String>> primKeyEdgeHash = frame.createPrimKeyEdgeHash(headers);
					
					Object[] values = ss.getValues();
					String[] types = new String[values.length];
					Map<String, String> dataType = new HashMap<>();
					for(int i = 0; i < values.length; i++) {
						Object[] type = Utility.findTypes(values[i].toString());
						dataType.put(headers[i], type[0].toString());
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
//				frame.addRelationship(ss.getHeaders(), ss.getValues(), ss.getRawValues(), cardinality, logicalToValue);
			}
		}
		
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
	
	//is headers2 a subset of headers1
	private boolean isSubset(String[] headers1, String[] headers2) {
		for(String header2 : headers2) {
			if(!ArrayUtilityMethods.arrayContainsValue(headers1, header2)) {
				return false;
			}
		}
		
		return true;
	}

}

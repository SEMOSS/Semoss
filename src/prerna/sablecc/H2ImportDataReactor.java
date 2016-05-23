package prerna.sablecc;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.ds.H2.H2Frame;
import prerna.ds.util.FileIterator;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.ISelectWrapper;
import prerna.sablecc.PKQLRunner.STATUS;

public class H2ImportDataReactor extends ImportDataReactor {
	
	@Override
	public Iterator process() {
		super.process();
		String nodeStr = (String)myStore.get(whoAmI);
		
		H2Frame frame = (H2Frame) myStore.get("G");
		Iterator<IHeadersDataRow> it = (Iterator<IHeadersDataRow>) myStore.get("iterator");
		Map<String, Set<String>> edgeHash = (Map<String, Set<String>>) myStore.get("edgeHash");
		Map<String, String> logicalToValue = (Map<String, String>) myStore.get("logicalToValue");
		String[] startingHeaders = (String[]) myStore.get("startingHeaders");
		Vector<Map<String, String>> joins = (Vector<Map<String, String>>) myStore.get(PKQLEnum.JOINS);
		String joinType = (String)myStore.get(PKQLEnum.REL_TYPE);
		
		boolean addRow = false;
		
		//TODO: need to make all these wrappers that give a IHeaderDataRow be the same type to get this info
		String[] types = null;
		String[] headers = null;
		if(it instanceof FileIterator) {
			types = ((FileIterator) it).getTypes();
			headers = ((FileIterator) it).getHeaders();
		} else if(it instanceof CsvTableWrapper) {
			types = ((CsvTableWrapper) it).getTypes();
			headers = ((CsvTableWrapper) it).getHeaders();
		} else if(it instanceof ISelectWrapper) {
			headers = ((ISelectWrapper)it).getDisplayVariables();
		}
		
		String[] headersCopy = new String[headers.length];
		// TODO: annoying, need to determine if i need to create a prim key edge hash
		if(edgeHash == null) {
			Map<String, Set<String>> primKeyEdgeHash = frame.createPrimKeyEdgeHash(headers);
			Map<String, String> dataType = new HashMap<>();
			for(int i = 0; i < types.length; i++) {
				dataType.put(headers[i], types[i]);
				headersCopy[i] = headers[i];
			}
			
			// TODO: does this need to also occur when there is an edge hash?
			updateDataForJoins(primKeyEdgeHash, dataType, headersCopy, joins);
			frame.mergeEdgeHash(primKeyEdgeHash, dataType);
			headers = headersCopy; 
		} else {
			for(int i = 0; i < headers.length; i++) {
				headersCopy[i] = headers[i];
			}
			// the only thing that needs to be updated is the headers
			// this is to account for when we have an existing frame
			// while the behavior should be to "join" in the case when all headers are accounted
			// we actually only need to perform inserts, instead of updates
			// note that the edgeHash has already been merged during MergeQSEdgeHash
			updateDataForJoins(edgeHash, new HashMap<String, String>(), headersCopy, joins);
			headers = headersCopy; 
		}
		
		//am i simply adding more data to the same columns or am i adding new columns as well?
		//if frame is empty then just add row
		if(allHeadersAccounted(startingHeaders, headers, joins) || frame.isEmpty() ) {
			addRow = true;
		}

		// TODO: need to have a smart way of determining when it is an "addRow" vs. "addRelationship"
		// TODO: h2Builder addRelationship only does update query which does nothing if frame is empty
		if(addRow) {
			while(it.hasNext()) {
				IHeadersDataRow ss = (IHeadersDataRow) it.next();
				frame.addRow(ss.getValues(), ss.getRawValues(), headers);
			}
		} else {
			frame.processIterator(it, headers, logicalToValue, joins, joinType);
		}
		
//		List<Object[]> frameData = frame.getData();
//		for(Object[] nextRow : frameData) {
//			System.out.println(Arrays.toString(nextRow));
//		}
		
		inputResponseString(it, headers);
		myStore.put("STATUS", STATUS.SUCCESS);
		
		return null;
	}
	
	private boolean allHeadersAccounted(String[] headers1, String[] headers2, Vector<Map<String, String>> joins) {
		if(headers1.length != headers2.length) {
			return false;
		}
		
		//add values to a set and compare
		Set<String> header1Set = new HashSet<>();
		Set<String> header2Set = new HashSet<>();

		//make a set with headers1
		for(String header : headers1) {
			header1Set.add(header);
		}
		
		//make a set with headers2
		for(String header : headers2) {
			header2Set.add(header);
		}
		
		//add headers1 headers to headers2set if there is a matching join and remove the other header
		for(Map<String, String> join : joins) {
			for(String key : join.keySet()) {
				header2Set.add(key);
				header2Set.remove(join.get(key));
			}
		}
		
		//take the difference
		header2Set.removeAll(header1Set);
		
		//return true if header sets matched, false otherwise
		return header2Set.size() == 0;
//		for(String header1 : headers1) {
//			if(!ArrayUtilityMethods.arrayContainsValue(adjustedHeaders2, header1)) {
//				//check here if 
//				return false;
//			}
//		}
//		
//		return true;
	}
}
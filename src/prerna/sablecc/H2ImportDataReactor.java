package prerna.sablecc;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.ds.TinkerMetaHelper;
import prerna.ds.H2.H2Frame;
import prerna.ds.util.FileIterator;
import prerna.ds.util.WebApiIterator;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.ISelectWrapper;
import prerna.sablecc.PKQLRunner.STATUS;

public class H2ImportDataReactor extends ImportDataReactor {
	
	@Override
	public Iterator process() {
		// use the import data reactor to go thorugh teh logic to get the necessary data 
		super.process();
		
		// get all the appropriate values
		
		// get the frame
		H2Frame frame = (H2Frame) myStore.get("G");
		// get the iterator containing the information to add
		Iterator<IHeadersDataRow> it = (Iterator<IHeadersDataRow>) myStore.get("iterator");

		// TODO: terrible use of the edgeHash... the values of the edgeHash are not actually used
		// its purpose is a null check to determine if we need to call mergeEdgeHash
		// stupid since if it wasn't null (i.e. API Reactor called with an engine), the super.process() will
		// do the QSMergeEdgeHash but the super will never handle any case where a mergeEdgeHash needs to occur
		Map<String, Set<String>> edgeHash = (Map<String, Set<String>>) myStore.get("edgeHash");
		Map<String, String> logicalToValue = (Map<String, String>) myStore.get("logicalToValue");
		
		// this is used to help with the determination if the frame should be doing an update vs. an insert
		// with respect to the new values being added
		String[] startingHeaders = (String[]) myStore.get("startingHeaders");
		
		// get the join data 
		Vector<Map<String, String>> joins = (Vector<Map<String, String>>) myStore.get(PKQLEnum.JOINS);
		String joinType = (String)myStore.get(PKQLEnum.REL_TYPE);
		
		//TODO: need to make all these wrappers that give a IHeaderDataRow be the same type to get this info
		String[] types = null;
		String[] headers = null;
		//add provision for ImportApiIterator
		if(it instanceof WebApiIterator) {
			types = ((WebApiIterator) it).getTypes();
			headers = ((WebApiIterator) it).getHeaders();
		}else if(it instanceof FileIterator) {
			types = ((FileIterator) it).getTypes();
			headers = ((FileIterator) it).getHeaders();
		} else if(it instanceof CsvTableWrapper) {
			types = ((CsvTableWrapper) it).getTypes();
			headers = ((CsvTableWrapper) it).getHeaders();
		} else if(it instanceof ISelectWrapper) {
			headers = ((ISelectWrapper)it).getDisplayVariables();
		}
		
		String[] headersCopy = new String[headers.length];

		// if the edge hash is not defined from when the super.process()
		// we need to perform the mergeEdgeHash logic here
		
		if(edgeHash == null) {
			Map<String, Set<String>> primKeyEdgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(headers);
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
		
		
		// TODO: is this the best way to determine when it is an "addRow" vs. "addRelationship"
		// TODO: h2Builder addRelationship only executes update queries which does nothing if frame is empty
		
		// set the addRow logic to false
		boolean addRow = false;
		// if all the headers are accounted or the frame is empty, then the logic should only be inserting
		// the values from the iterator into the frame
		if(allHeadersAccounted(startingHeaders, headers, joins) || frame.isEmpty() ) {
			addRow = true;
		}
		if(addRow) {
			frame.addRowsViaIterator(it);
		} else {
			// if logicalToValue is null, it will create it within the processIterator method
			frame.processIterator(it, headers, logicalToValue, joins, joinType);
		}
		
		// store the response string
		inputResponseString(it, headers);
		// set status to success
		myStore.put("STATUS", STATUS.SUCCESS);
		
		return null;
	}
	
	
	/**
	 * Determine if all the headers are taken into consideration within the iterator
	 * This helps to determine if we need to perform an insert vs. an update query to fill the frame
	 * @param headers1				The original set of headers in the frame
	 * @param headers2				The new set of headers from the iterator
	 * @param joins					Needs to take into consideration the joins since we can join on 
	 * 								columns that do not have the same names
	 * @return
	 */
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
	}
}
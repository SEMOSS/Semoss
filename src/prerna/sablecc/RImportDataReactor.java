package prerna.sablecc;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.ds.TinkerMetaHelper;
import prerna.ds.R.RDataTable;
import prerna.ds.util.FileIterator;
import prerna.ds.util.WebApiIterator;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.r.RFileWrapper;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.util.Utility;

public class RImportDataReactor extends ImportDataReactor {

	/*
	 * This class makes the underlying assumption that we are only filling in a 
	 * R data table.  We will never be performing logic to 
	 * @see prerna.sablecc.ImportDataReactor#process()
	 */

	@Override
	public Iterator process() {
		modExpression();
		System.out.println("My Store on IMPORT DATA REACTOR: " + myStore);
		
		Vector<Map<String, String>> joins = (Vector<Map<String, String>>) myStore.get(PKQLEnum.JOINS);
		// currently not handling join logic
		if(joins != null && !joins.isEmpty()) {
			throw new IllegalArgumentException("R Frame cannot handle table join logic.");
		}
		
		RDataTable frame = (RDataTable) myStore.get("G");
		
		boolean performMerge = true;
		
		Object it = null;
		if(myStore.containsKey(PKQLEnum.API)) {			
			Map<String, Set<String>> edgeHash = (Map<String, Set<String>>) this.getValue(PKQLEnum.API + "_EDGE_HASH");
			IEngine engine = Utility.getEngine((this.getValue(PKQLEnum.API + "_ENGINE")+"").trim());
			it  = myStore.get(PKQLEnum.API);

			if(engine != null) {
				// put the edge hash and the logicalToValue maps within the myStore
				// will be used when the data is actually imported
				Map[] mergedMaps = frame.mergeQSEdgeHash(edgeHash, engine, new Vector<Map<String, String>>());
				performMerge = false;
			} 
		} else if(myStore.containsKey(PKQLEnum.PASTED_DATA)) {
			it = myStore.get(PKQLEnum.PASTED_DATA);
			
		} else if(myStore.containsKey(PKQLEnum.CSV_TABLE)) {
			it = myStore.get(PKQLEnum.CSV_TABLE);
			
		}
		
		// update the data id on the frame
		frame.updateDataId();
		

		//TODO: need to make all these wrappers that give a IHeaderDataRow be the same type to get this info
		// in a generic fashion instead of all this stupid casting
		String[] types = null;
		String[] headers = null;
		//add provision for ImportApiIterator
		if(it instanceof WebApiIterator) {
			types = ((WebApiIterator) it).getTypes();
			headers = ((WebApiIterator) it).getHeaders();

		} else if(it instanceof FileIterator) {
			types = ((FileIterator) it).getTypes();
			headers = ((FileIterator) it).getHeaders();

		} else if(it instanceof CsvTableWrapper) {
			types = ((CsvTableWrapper) it).getTypes();
			headers = ((CsvTableWrapper) it).getHeaders();

		} else if(it instanceof IRawSelectWrapper) {
			headers = ((IRawSelectWrapper)it).getDisplayVariables();

		} else if(it instanceof RFileWrapper) {			
			types = ((RFileWrapper) it).getTypes();
			headers = ((RFileWrapper) it).getHeaders();
			
		}
		
		// if the edge hash is not defined
		// we need to perform the mergeEdgeHash logic here
		if(performMerge) {
			Map<String, Set<String>> primKeyEdgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(headers);
			Map<String, String> dataType = new HashMap<>();
			for(int i = 0; i < types.length; i++) {
				dataType.put(headers[i], types[i]);
			}
			
			frame.mergeEdgeHash(primKeyEdgeHash, dataType);
		} 
		
		if(it instanceof Iterator) {
			frame.createTableViaIterator((Iterator<IHeadersDataRow>) it);
			inputResponseString((Iterator) it, headers);
		} else if(it instanceof RFileWrapper) {
			// this only happens when we have a csv file
			frame.createTableViaCsvFile( (RFileWrapper) it);
		}
		
		// metadata is stored in the frame to know the type of each column about to be added
		// just pass the iterator and let the frame do its thing
		
		// store the response string
		// set status to success
		myStore.put("STATUS", STATUS.SUCCESS);
		
		return null;
	}
	
}

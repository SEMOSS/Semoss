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
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.sablecc.PKQLRunner.STATUS;

public class RImportDataReactor extends ImportDataReactor {

	/*
	 * This class makes the underlying assumption that we are only filling in a 
	 * R data table.  We will never be performing logic to 
	 * @see prerna.sablecc.ImportDataReactor#process()
	 */
	
	@Override
	public Iterator process() {
		// use the import data reactor to go thorugh teh logic to get the necessary data 
		super.process();
		
		// get the frame
		RDataTable frame = (RDataTable) myStore.get("G");
		// get the iterator containing the information to add
		Iterator<IHeadersDataRow> it = (Iterator<IHeadersDataRow>) myStore.get("iterator");

		// TODO: terrible use of the edgeHash... the values of the edgeHash are not actually used
		// its purpose is a null check to determine if we need to call mergeEdgeHash
		// stupid since if it wasn't null (i.e. API Reactor called with an engine), the super.process() will
		// do the QSMergeEdgeHash but the super will never handle any case where a mergeEdgeHash needs to occur
		Map<String, Set<String>> edgeHash = (Map<String, Set<String>>) myStore.get("edgeHash");
		Map<String, String> logicalToValue = (Map<String, String>) myStore.get("logicalToValue");
		
		// get the join data 
		Vector<Map<String, String>> joins = (Vector<Map<String, String>>) myStore.get(PKQLEnum.JOINS);
		
		if(!joins.isEmpty()) {
			throw new IllegalArgumentException("Cannot extend an existing RDataTable. Must convert to a frame that can be extended.");
		}
		
		//TODO: need to make all these wrappers that give a IHeaderDataRow be the same type to get this info
		// in a generic fashion instead of all this stupid casting
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
			
		} else if(it instanceof IRawSelectWrapper) {
			headers = ((IRawSelectWrapper)it).getDisplayVariables();
			
		}
		
		// if the edge hash is not defined from when the super.process()
		// we need to perform the mergeEdgeHash logic here
		if(edgeHash == null) {
			Map<String, Set<String>> primKeyEdgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(headers);
			Map<String, String> dataType = new HashMap<>();
			for(int i = 0; i < types.length; i++) {
				dataType.put(headers[i], types[i]);
			}
			
			frame.mergeEdgeHash(primKeyEdgeHash, dataType);
		} 
		
		// metadata is stored in the frame to know the type of each column about to be added
		// just pass the iterator and let the frame do its thing
		frame.createTableViaIterator(it);
		
		// store the response string
		inputResponseString(it, headers);
		// set status to success
		myStore.put("STATUS", STATUS.SUCCESS);
		
		return null;
	}
	
}

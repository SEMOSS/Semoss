package prerna.sablecc;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.ds.TinkerFrame;
import prerna.ds.TinkerMetaHelper;
import prerna.ds.util.FileIterator;
import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.util.Utility;

public class TinkerImportDataReactor extends ImportDataReactor{

	@Override
	public Iterator process() {
		// use the import data reactor to go through the logic to get the necessary data 
		super.process();
		
		// get all the appropriate values
		
		// get the frame
		TinkerFrame frame = (TinkerFrame) myStore.get("G");
		// get the iterator containing the information to add
		Iterator<IHeadersDataRow> it = (Iterator<IHeadersDataRow>) myStore.get("iterator");
		
		// get the edge hash and the logicalToVAlue hash
		// these values are a pair (are either both null or both not null)
		// they are not null when the child reactor is an APIReactor and contains an engine
		// in this case, the metadata has already been merged with the existing data
		Map<String, Set<String>> edgeHash = (Map<String, Set<String>>) myStore.get("edgeHash");
		Map<String, String> logicalToValue = (Map<String, String>) myStore.get("logicalToValue");

		// get the join data 
		Vector<Map<String,String>> joinCols = (Vector<Map<String, String>>) myStore.get(PKQLEnum.JOINS);
		
		// cardinality helps determine the relationship between the instance values in a column
		// this is helpful in optimizing the extraction of the upstream node and downstream node when creating
		// these vertices in the TinkerFrame
		Map<Integer, Set<Integer>> cardinality = null;
		
		// get the headers as this shouldn't change with each iteration
		String[] headers = null;
		// default assumption is that the metamodel has been defined and the primKey is false
		boolean isPrimKey = false;
		while(it.hasNext()){
			IHeadersDataRow ss = it.next();
			
			if(headers == null) { // happens only during first loop
				headers = ss.getHeaders();

				// TODO this merge edge hash should go inside the super.process()!!!!
				
				// if the edge hash is not defined from when the super.process()
				// we need to perform the mergeEdgeHash logic here

				if(edgeHash == null) {
					Map<String, Set<String>> primKeyEdgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(headers);
					//TODO: need to make all these wrappers that give a IHeaderDataRow be the same type to get this info
					String[] types = null;
					if(it instanceof FileIterator) {
						types = ((FileIterator) it).getTypes();
					} else if(it instanceof CsvTableWrapper) {
						types = ((CsvTableWrapper) it).getTypes();
					}
					
					String[] headersCopy = new String[types.length]; // need to create a copy to not override settings in iterator
					Map<String, String> dataType = new HashMap<>();
					for(int i = 0; i < types.length; i++) {
						dataType.put(headers[i], types[i]);
						headersCopy[i] = headers[i];
					}
					
					updateDataForJoins(primKeyEdgeHash, dataType, headersCopy, joinCols);
					frame.mergeEdgeHash(primKeyEdgeHash, dataType);
					headers = headersCopy; // change variable pointer instead of modifying iterator header pointer
					isPrimKey = true;
				} else {
					// get the cardinality mapping to be used
					cardinality = Utility.getCardinalityOfValues(ss.getHeaders(), edgeHash);
				}
			}

			// TODO: need to have a smart way of determining when it is an "addRow" vs. "addRelationship"
			if(isPrimKey) {
				frame.addRow(ss.getValues(), headers);
			} else {
				frame.addRelationship(ss.getHeaders(), ss.getValues(), cardinality, logicalToValue);
			}
		}
		
		// store the response string
		inputResponseString(it, headers);
		// set status to success
		myStore.put("STATUS", STATUS.SUCCESS);
		
		return null;
	}
}

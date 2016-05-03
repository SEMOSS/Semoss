package prerna.sablecc;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import prerna.ds.TinkerFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.util.Utility;

public class TinkerImportDataReactor extends AbstractReactor{

	private TinkerFrame frame = null;
	
	public TinkerImportDataReactor(TinkerFrame frame) {
		this.frame = frame;
	}
	
	@Override
	public Iterator process() {
		
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
				if(edgeHash == null || cardinality.isEmpty()) {
					Map<String, Set<String>> primKeyEdgeHash = frame.createPrimKeyEdgeHash(headers);
					frame.mergeEdgeHash(primKeyEdgeHash);
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
		
		return null;
	}

}

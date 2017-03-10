package prerna.sablecc;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import prerna.ds.TinkerMetaHelper;
import prerna.ds.r.RDataTable;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.r.RFileWrapper;
import prerna.sablecc.PKQLRunner.STATUS;

public class RImportDataReactor extends ImportDataReactor {

	/*
	 * This class makes the underlying assumption that we are only filling in a 
	 * R data table.  We will never be performing logic to 
	 * @see prerna.sablecc.ImportDataReactor#process()
	 */

	@Override
	public Iterator process() {
		if(myStore.get(PKQLEnum.CHILD_ERROR) != null && (boolean) myStore.get(PKQLEnum.CHILD_ERROR)) {
			myStore.put("STATUS", STATUS.ERROR);
			String nodeStr = (String)myStore.get(PKQLEnum.EXPR_TERM);
			if(myStore.get(PKQLEnum.CHILD_ERROR_MESSAGE) != null) {
				myStore.put(nodeStr, myStore.get(PKQLEnum.CHILD_ERROR_MESSAGE));
			}
			return null;
		}
		
		super.process();
		
		RDataTable frame = (RDataTable) myStore.get("G");
		if(dataIterator == null) {
			if(myStore.get(PKQLEnum.API) != null) {
				RFileWrapper rFileWrap = (RFileWrapper) myStore.get(PKQLEnum.API);
				Map<String, String> dataTypes = new HashMap<>();
	
				this.newHeaders = rFileWrap.getHeaders();
				String[] types = rFileWrap.getTypes();
				for(int i = 0; i < types.length; i++) {
					dataTypes.put(newHeaders[i], types[i]);
				}
				
				this.edgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(this.newHeaders);
				frame.mergeEdgeHash(edgeHash, dataTypes);
	
				// this only happens when we have a csv file
				frame.createTableViaCsvFile(rFileWrap);
			}
		} else if(dataIterator instanceof Iterator) {
			frame.createTableViaIterator((Iterator<IHeadersDataRow>) dataIterator);
			inputResponseString((Iterator) dataIterator, newHeaders);
		}
		
		// store the response string
		inputResponseString(dataIterator, newHeaders);
		// set status to success
		myStore.put("STATUS", STATUS.SUCCESS);
		
		return null;
	}
	
}

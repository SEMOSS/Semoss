package prerna.sablecc;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import prerna.ds.TinkerMetaHelper;
import prerna.ds.r.RDataTable;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.r.RCsvFileWrapper;
import prerna.engine.impl.r.RExcelFileWrapper;
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
			// TODO: these need to have a common interface
			// instead of having the frame determine what syntax to use
			// the file iterator itself should just produce the syntax
			// and give it to the frame to run
			// will help to dedup code
			if(myStore.get(PKQLEnum.API) != null && myStore.get(PKQLEnum.API) instanceof RCsvFileWrapper) {
				RCsvFileWrapper rFileWrap = (RCsvFileWrapper) myStore.get(PKQLEnum.API);
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
			} else if(myStore.get(PKQLEnum.API) != null && myStore.get(PKQLEnum.API) instanceof RExcelFileWrapper) {
				RExcelFileWrapper rFileWrap = (RExcelFileWrapper) myStore.get(PKQLEnum.API);
				Map<String, String> dataTypes = new HashMap<>();
	
				this.newHeaders = rFileWrap.getHeaders();
				String[] types = rFileWrap.getTypes();
				for(int i = 0; i < types.length; i++) {
					dataTypes.put(newHeaders[i], types[i]);
				}
				
				this.edgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(this.newHeaders);
				frame.mergeEdgeHash(edgeHash, dataTypes);
	
				// this only happens when we have a csv file
				frame.createTableViaExcelFile(rFileWrap);
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

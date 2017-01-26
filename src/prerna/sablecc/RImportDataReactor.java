package prerna.sablecc;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import prerna.ds.TinkerMetaHelper;
import prerna.ds.r.RDataTable;
import prerna.ds.util.FileIterator;
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
		super.process();
		
		RDataTable frame = (RDataTable) myStore.get("G");
		
		if(dataIterator == null) {
			Map<String, String> dataTypes = new HashMap<>();

			this.newHeaders = ((RFileWrapper) myStore.get(PKQLEnum.API)).getHeaders();
			String[] types = ((RFileWrapper) myStore.get(PKQLEnum.API)).getTypes();
			for(int i = 0; i < types.length; i++) {
				dataTypes.put(newHeaders[i], types[i]);
			}
			
			this.edgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(this.newHeaders);
			frame.mergeEdgeHash(edgeHash, dataTypes);

			// this only happens when we have a csv file
			frame.createTableViaCsvFile( (RFileWrapper) myStore.get(PKQLEnum.API));
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

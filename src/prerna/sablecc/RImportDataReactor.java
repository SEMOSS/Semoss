package prerna.sablecc;

import java.util.Iterator;

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
		super.process();
		
		RDataTable frame = (RDataTable) myStore.get("G");
		
		if(dataIterator instanceof RFileWrapper) {
			// this only happens when we have a csv file
			frame.createTableViaCsvFile( (RFileWrapper) dataIterator);
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

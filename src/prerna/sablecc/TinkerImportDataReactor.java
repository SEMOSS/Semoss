package prerna.sablecc;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import prerna.ds.TinkerFrame;
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
		
		// cardinality helps determine the relationship between the instance values in a column
		// this is helpful in optimizing the extraction of the upstream node and downstream node when creating
		// these vertices in the TinkerFrame
		Map<Integer, Set<Integer>> cardinality = Utility.getCardinalityOfValues(this.newHeaders, this.edgeHash);;
		
		while(this.dataIterator.hasNext()){
			IHeadersDataRow ss = this.dataIterator.next();
			
			if(isPrimKey) {
				frame.addRow(ss.getValues(), this.newHeaders);
			} else {
				frame.addRelationship(this.newHeaders, ss.getValues(), cardinality, this.modifyNamesMap);
			}
		}
		
		// store the response string
		inputResponseString(this.dataIterator, this.newHeaders);
		// set status to success
		myStore.put("STATUS", STATUS.SUCCESS);
		
		return null;
	}
}

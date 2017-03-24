package prerna.sablecc2.reactor.imports;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import prerna.ds.h2.H2Frame;
import prerna.ds.util.FileIterator;
import prerna.ds.util.RdbmsFrameUtility;
import prerna.engine.api.IRawSelectWrapper;
import prerna.sablecc.PKQLEnum;
import prerna.util.ArrayUtilityMethods;

public class MetaH2Importer extends Importer{

	protected H2Frame frame;
	
	@Override
	public Iterator process() {
		// get the base information
		super.process();
		
		// get the frame
		frame = (H2Frame) myStore.get("G");
				
		// perform additional logic regarding if the frame should be ported on-disk
		int LIMIT_SIZE = RdbmsFrameUtility.getLimitSize();
		boolean overLimit = false;
		int numNewRecords = -1;

		if(frame.isInMem()) {
			if(this.dataIterator instanceof FileIterator) {
				// get if the frame is over the limit of acceptable values
				overLimit = ((FileIterator) this.dataIterator).numberRowsOverLimit(LIMIT_SIZE);
				// this value will be -1 if the overLimit is true
				// if we are already over limit, we do not need to continue
				// iterating through the file to get the number of records
				// we know we are going to switch to in-memory
				numNewRecords = ((FileIterator) this.dataIterator).getNumRecords();
			} else if(this.dataIterator instanceof IRawSelectWrapper && myStore.containsKey(PKQLEnum.API)) {
				// get if the frame is over the limit of acceptable values
				numNewRecords = ((Double) this.getValue(PKQLEnum.API + "_QUERY_NUM_CELLS")).intValue();
				overLimit = numNewRecords > LIMIT_SIZE ;
			}
			
			if(overLimit) {
				// let the method determine where the new schema will be
				frame.convertToOnDiskFrame(null);
			} else if(!this.isFrameEmpty) {
				// this is a very very rough approximation
				int newRecordRows = numNewRecords / newHeaders.length;
				int currNumRows = frame.getNumRows();
				
				// we grab whichever has a larger number of rows
				// and multiple by the total number of headers expected after we perform the join
				
				// 1) get the number of rows
				int approxNumRows = Math.max(newRecordRows, currNumRows);
				// 2) get the unique number of headers
				Set<String> uniqueHeaders = new HashSet<String>();
				for(String header : startingHeaders) {
					uniqueHeaders.add(header);
				}
				// note: we do this after we update the headers to account for the join columns
				// having different names to get the correct number of headers
				for(String header : newHeaders) {
					uniqueHeaders.add(header);
				}
				
				int approxNumCells = approxNumRows * uniqueHeaders.size();
				if(approxNumCells > LIMIT_SIZE) {
					// let the method determine where the new schema will be
					frame.convertToOnDiskFrame(null);
				}
			}
		}
		
		return null;
	}
	
	protected boolean allHeadersAccounted(String[] startHeaders, String[] newHeaders) {
		int newHeadersSize = newHeaders.length;
		for(int i = 0; i <  newHeadersSize; i++) {
			// need each of the new headers to be included in the start headers
			if(!ArrayUtilityMethods.arrayContainsValue(startHeaders, newHeaders[i])) {
				return false;
			}
		}
		// we were able to iterate through all the new headers
		// and each one exists in the starting headers
		// so all of them are taking into consideration
		return true;
	}	
}

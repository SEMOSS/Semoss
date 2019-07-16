package prerna.sablecc;

import java.util.Iterator;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.rdbms.h2.H2Frame;
import prerna.ds.util.RdbmsFrameUtility;
import prerna.util.ArrayUtilityMethods;

public class MetaH2ImportDataReactor extends ImportDataReactor{

	protected H2Frame frame;
	
	@Override
	public Iterator process() {
		// get the base information
		super.process();
		
		// get the frame
		frame = (H2Frame) myStore.get("G");
		OwlTemporalEngineMeta meta = frame.getMetaData();
		// perform additional logic regarding if the frame should be ported on-disk
		int LIMIT_SIZE = RdbmsFrameUtility.getLimitSize();
		boolean overLimit = false;
		int numNewRecords = -1;

//		if(frame.isInMem()) {
//			if(this.dataIterator instanceof IFileIterator) {
//				// get if the frame is over the limit of acceptable values
//				overLimit = ((IFileIterator) this.dataIterator).numberRowsOverLimit(LIMIT_SIZE);
//				// this value will be -1 if the overLimit is true
//				// if we are already over limit, we do not need to continue
//				// iterating through the file to get the number of records
//				// we know we are going to switch to in-memory
//				numNewRecords = ((IFileIterator) this.dataIterator).getNumRecords();
//			} else if(this.dataIterator instanceof IRawSelectWrapper && myStore.containsKey(PKQLEnum.API)) {
//				// get if the frame is over the limit of acceptable values
//				numNewRecords = ((Number) this.getValue(PKQLEnum.API + "_QUERY_NUM_CELLS")).intValue();
//				overLimit = numNewRecords > LIMIT_SIZE ;
//			}
//			
//			if(overLimit) {
//				// let the method determine where the new schema will be
//				frame.convertToOnDiskFrame(null);
//			} else if(!this.isFrameEmpty) {
//				// this is a very very rough approximation
//				int newRecordRows = numNewRecords / newHeaders.length;
//				int currNumRows = OwlTemporalEngineMetaHelper.getNumRows(frame);
//				
//				// we grab whichever has a larger number of rows
//				// and multiple by the total number of headers expected after we perform the join
//				
//				// 1) get the number of rows
//				int approxNumRows = Math.max(newRecordRows, currNumRows);
//				// 2) get the unique number of headers
//				Set<String> uniqueHeaders = new HashSet<String>();
//				for(String header : startingHeaders) {
//					uniqueHeaders.add(header);
//				}
//				// note: we do this after we update the headers to account for the join columns
//				// having different names to get the correct number of headers
//				for(String header : newHeaders) {
//					uniqueHeaders.add(header);
//				}
//				
//				int approxNumCells = approxNumRows * uniqueHeaders.size();
//				if(approxNumCells > LIMIT_SIZE) {
//					// let the method determine where the new schema will be
//					frame.convertToOnDiskFrame(null);
//				}
//			}
//		}
		
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
	
	
//	/**
//	 * Determine if all the headers are taken into consideration within the iterator
//	 * This helps to determine if we need to perform an insert vs. an update query to fill the frame
//	 * @param headers1				The original set of headers in the frame
//	 * @param headers2				The new set of headers from the iterator
//	 * @param joins					Needs to take into consideration the joins since we can join on 
//	 * 								columns that do not have the same names
//	 * @return
//	 */
//	protected boolean allHeadersAccounted(String[] headers1, String[] headers2, Vector<Map<String, String>> joins) {
//		if(headers1.length != headers2.length) {
//			return false;
//		}
//		
//		//add values to a set and compare
//		Set<String> header1Set = new HashSet<>();
//		Set<String> header2Set = new HashSet<>();
//
//		//make a set with headers1
//		for(String header : headers1) {
//			header1Set.add(header);
//		}
//		
//		//make a set with headers2
//		for(String header : headers2) {
//			header2Set.add(header);
//		}
//		
//		//add headers1 headers to headers2set if there is a matching join and remove the other header
//		for(Map<String, String> join : joins) {
//			for(String key : join.keySet()) {
//				header2Set.add(key);
//				header2Set.remove(join.get(key));
//			}
//		}
//		
//		//take the difference
//		header2Set.removeAll(header1Set);
//		
//		//return true if header sets matched, false otherwise
//		return header2Set.size() == 0;
//	}
	
}

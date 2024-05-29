package prerna.reactor.imports;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.util.IFileIterator;
import prerna.engine.api.IRawSelectWrapper;
import prerna.util.Constants;
import prerna.util.Utility;

public class FrameSizeRetrictions {

	private FrameSizeRetrictions() {
		
	}
	
	/**
	 * Restrictions set on the size of the frame
	 */
	private static int LIMIT_SIZE;
	private static boolean SIZE_RESTRICTED = false;;
	private static boolean RESTRICT_NATIVE = false;;

	static {
		// null check is only for testing
		// in case DIHelper isn't loaded
		String limitSize = (String) Utility.getDIHelperProperty(Constants.FRAME_SIZE_LIMIT);
		if (limitSize == null) {
			LIMIT_SIZE = Integer.MAX_VALUE;
		} else {
			try {
				int val = Integer.parseInt(limitSize.trim());
				if(val < 0) {
					LIMIT_SIZE = Integer.MAX_VALUE;
				} else {
					SIZE_RESTRICTED = true;
					LIMIT_SIZE = Integer.parseInt(limitSize.trim());
				}
			} catch(Exception e) {
				LIMIT_SIZE = 10_000;
			}
		}
		RESTRICT_NATIVE = Boolean.parseBoolean( Utility.getDIHelperProperty(Constants.FRAME_SIZE_LIMIT_NATIVE) + "");
	}
	
	/**
	 * Is frame within size limits
	 * @param frame
	 * @param it
	 * @return
	 */
	public static boolean frameWithinLimits(ITableDataFrame frame) {
		if(!SIZE_RESTRICTED || (!RESTRICT_NATIVE && frame instanceof NativeFrame) ) {
			// no restriction
			// or
			// depending on settings 
			// ignore for native frame - since nothing is stored in mem
			return true;
		}
		
		long frameSize = frame.size(frame.getName());
		if(frameSize > LIMIT_SIZE) {
			return false;
		}
		return true;
	}

	/**
	 * Is frame with imports within size limits
	 * @param frame
	 * @param it
	 * @return
	 * @throws Exception 
	 */
	public static boolean importWithinLimit(ITableDataFrame frame, IRawSelectWrapper it) throws Exception {
		if(!SIZE_RESTRICTED || (!RESTRICT_NATIVE && frame instanceof NativeFrame) ) {
			// no restriction
			// or
			// depending on settings 
			// ignore for native frame - since nothing is stored in mem
			return true;
		}

		long curFrameSize = frame.size(frame.getName());

		// we can increase performance since
		// a file requires us to loop through
		// we will just subtract the curFrame size
		if(it instanceof IFileIterator) {
			if(	((IFileIterator) it).getNumRecordsOverSize(LIMIT_SIZE - curFrameSize) ) {
				return false;
			}
		} else {
			long additionalRecords = it.getNumRecords();
			if(additionalRecords + curFrameSize > LIMIT_SIZE) {
				return false;
			}
		}
		
		// we passed!
		return true;
	}
	
	/**
	 * Is frame with imports within size limits
	 * @param frame
	 * @param it
	 * @return
	 * @throws Exception 
	 */
	public static boolean mergeWithinLimit(ITableDataFrame frame, IRawSelectWrapper it) throws Exception {
		if(!SIZE_RESTRICTED || (!RESTRICT_NATIVE && frame instanceof NativeFrame) ) {
			// no restriction
			// or
			// depending on settings 
			// ignore for native frame - since nothing is stored in mem
			return true;
		}
		
		// we need to account for the join column not adding 
		// additional records
		int numColumns = it.getHeaders().length;
		long additionalRecords = (long) ( ((double) (numColumns-1) / numColumns) * it.getNumRecords());

		long curFrameSize = frame.size(frame.getName());
		if(additionalRecords + curFrameSize > LIMIT_SIZE) {
			return false;
		}
		
		// we passed!
		return true;
	}
	
	/**
	 * Is the value larger than expected
	 * @param frame
	 * @param it
	 * @return
	 */
	public static boolean sizeWithinLimit(long size) {
		if(!SIZE_RESTRICTED) {
			return true;
		}
		
		if(size > LIMIT_SIZE) {
			return false;
		}
		
		// we passed!
		return true;
	}

}

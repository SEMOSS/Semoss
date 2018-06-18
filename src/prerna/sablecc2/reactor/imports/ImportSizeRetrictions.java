package prerna.sablecc2.reactor.imports;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.util.IFileIterator;
import prerna.engine.api.IDatasourceIterator;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class ImportSizeRetrictions {

	private ImportSizeRetrictions() {
		
	}
	
	/**
	 * Restrictions set on the size of the frame
	 */
	private static int LIMIT_SIZE;
	private static boolean SIZE_RESTRICTED = false;;
	
	static {
		// null check is only for testing
		// in case DIHelper isn't loaded
		if(DIHelper.getInstance() != null) {
			String limitSize = (String) DIHelper.getInstance().getProperty(Constants.FRAME_SIZE_LIMIT);
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
		}
	}
	
	/**
	 * Is frame within size limits
	 * @param frame
	 * @param it
	 * @return
	 */
	public static boolean frameWithinLimits(ITableDataFrame frame) {
		long frameSize = frame.size(frame.getTableName());
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
	 */
	public static boolean importWithinLimit(ITableDataFrame frame, IDatasourceIterator it) {
		if(!SIZE_RESTRICTED || frame instanceof NativeFrame) {
			// no restriction
			// or
			// native frame - since nothing is stored in mem
			return true;
		}

		long curFrameSize = frame.size(frame.getTableName());

		// we can increase performance since
		// a file requires us to loop through
		// we will just subtract the curFrame size
		if(it instanceof IFileIterator) {
			if(	((IFileIterator) it).numberRowsOverLimit(LIMIT_SIZE - curFrameSize) ) {
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

}

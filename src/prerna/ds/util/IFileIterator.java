package prerna.ds.util;

import prerna.engine.api.IRawSelectWrapper;

public interface IFileIterator extends IRawSelectWrapper {

	boolean getNumRecordsOverSize(long limitSize);

}
